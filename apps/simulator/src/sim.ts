import type { SimMessage } from '@loom/core';
import {
  assignTrip,
  createAgents,
  noisyPing,
  tickAgent,
  type DriverAgent,
} from './agents.js';
import { createHotspots, generateRequests, type HotspotOptions } from './generator.js';
import { createRng, type Rng } from './rng.js';
import { DEFAULT_CITY, fromLatLng, toLatLng, type City, type Point } from './world.js';

export interface SimulationOptions {
  drivers: number;
  ratePerSec: number;
  hotspots: number;
  seed: number;
  city?: City;
  gpsNoiseStdDevM?: number;
  hotspotWeight?: number;
  hotspotSigmaM?: number;
  /** Probability a roaming driver accepts an offer (default 0.8). */
  acceptProb?: number;
  /** Offer response delay range, simulated ms (default 500–3000). */
  responseDelayMs?: { min: number; max: number };
}

export interface OfferDecision {
  accept: boolean;
  /** Simulated ms of driver think time before the reply goes out. */
  delayMs: number;
}

/**
 * Pure simulation core: no I/O, no wall clock. One tick = one simulated second
 * (agents move, every driver pings — now with a status field — requests arrive
 * with destinations, and trip milestones fire as agents reach them). Fully
 * deterministic for a given options object and event sequence: movement and
 * generation draw from the main seeded rng; offer decisions draw from a
 * per-driver rng derived from the seed, so a driver's nth decision is the
 * same regardless of network arrival order.
 */
export class Simulation {
  readonly city: City;
  readonly agents: DriverAgent[];
  readonly hotspotCenters: Point[];
  tickCount = 0;

  private readonly rng: Rng;
  private readonly hotspotOpts: HotspotOptions;
  private readonly ratePerSec: number;
  private readonly gpsNoiseStdDevM: number;
  private readonly acceptProb: number;
  private readonly responseDelayMs: { min: number; max: number };
  private readonly byId = new Map<string, DriverAgent>();
  private readonly offerRngs = new Map<string, Rng>();

  constructor(opts: SimulationOptions) {
    this.city = opts.city ?? DEFAULT_CITY;
    this.rng = createRng(opts.seed);
    this.ratePerSec = opts.ratePerSec;
    this.gpsNoiseStdDevM = opts.gpsNoiseStdDevM ?? 10;
    this.acceptProb = opts.acceptProb ?? 0.8;
    this.responseDelayMs = opts.responseDelayMs ?? { min: 500, max: 3000 };
    this.agents = createAgents(this.city, opts.drivers, this.rng);
    this.agents.forEach((agent, i) => {
      this.byId.set(agent.id, agent);
      // Independent per-driver decision streams, derived from the seed.
      this.offerRngs.set(agent.id, createRng((opts.seed + 0x9e3779b9 * (i + 1)) >>> 0));
    });
    this.hotspotCenters = createHotspots(this.city, opts.hotspots, this.rng);
    this.hotspotOpts = {
      centers: this.hotspotCenters,
      weight: opts.hotspotWeight ?? 0.7,
      sigmaM: opts.hotspotSigmaM ?? 400,
    };
  }

  tick(dtSec = 1): SimMessage[] {
    const ts = this.tickCount * 1000;
    const out: SimMessage[] = [];
    for (const agent of this.agents) {
      const arrival = tickAgent(this.city, agent, this.rng, dtSec);
      const ping = noisyPing(this.city, agent, this.rng, this.gpsNoiseStdDevM);
      const { lat, lng } = toLatLng(this.city, ping);
      const status =
        agent.mode === 'to_pickup' ? 'en_route' : agent.mode === 'to_dest' ? 'in_trip' : 'available';
      out.push({ type: 'driver_ping', driverId: agent.id, lat, lng, ts, status });
      if (arrival !== null) {
        out.push({ type: 'trip_progress', tripId: arrival.tripId, driverId: agent.id, event: arrival.event, ts });
      }
    }
    const requests = generateRequests(this.city, this.rng, this.ratePerSec, dtSec, this.hotspotOpts);
    requests.forEach((pt, i) => {
      const { lat, lng } = toLatLng(this.city, pt);
      // Destination: uniform over the city, drawn from the main stream.
      const dest = toLatLng(this.city, {
        x: this.rng.next() * this.city.widthM,
        y: this.rng.next() * this.city.heightM,
      });
      out.push({
        type: 'ride_request',
        requestId: `r${this.tickCount}-${i}`,
        lat,
        lng,
        destLat: dest.lat,
        destLng: dest.lng,
        ts,
      });
    });
    this.tickCount++;
    return out;
  }

  /**
   * The driver's answer to an offer: accept with acceptProb if roaming (a
   * driver already on a trip always declines), after a think-time delay.
   */
  decideOffer(driverId: string): OfferDecision {
    const rng = this.offerRngs.get(driverId);
    const agent = this.byId.get(driverId);
    if (rng === undefined || agent === undefined) return { accept: false, delayMs: 0 };
    const { min, max } = this.responseDelayMs;
    const delayMs = rng.range(min, max);
    const accept = agent.mode === 'roaming' && rng.next() < this.acceptProb;
    return { accept, delayMs };
  }

  /** trip_assigned arrived: drive to pickup, then dest. False if the driver is unknown. */
  assignTrip(
    driverId: string,
    tripId: string,
    pickup: { lat: number; lng: number },
    dest: { lat: number; lng: number },
  ): boolean {
    const agent = this.byId.get(driverId);
    if (agent === undefined) return false;
    assignTrip(agent, {
      tripId,
      pickup: fromLatLng(this.city, pickup.lat, pickup.lng),
      dest: fromLatLng(this.city, dest.lat, dest.lng),
    });
    return true;
  }
}
