import type { SimMessage } from '@fleetline/core';
import { createAgents, noisyPing, tickAgent, type DriverAgent } from './agents.js';
import { createHotspots, generateRequests, type HotspotOptions } from './generator.js';
import { createRng, type Rng } from './rng.js';
import { DEFAULT_CITY, toLatLng, type City, type Point } from './world.js';

export interface SimulationOptions {
  drivers: number;
  ratePerSec: number;
  hotspots: number;
  seed: number;
  city?: City;
  gpsNoiseStdDevM?: number;
  hotspotWeight?: number;
  hotspotSigmaM?: number;
}

/**
 * Pure simulation core: no I/O, no wall clock. One tick = one simulated second
 * (agents move, every driver pings, requests arrive). Fully deterministic for a
 * given options object — benchmarks and tests rely on that.
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

  constructor(opts: SimulationOptions) {
    this.city = opts.city ?? DEFAULT_CITY;
    this.rng = createRng(opts.seed);
    this.ratePerSec = opts.ratePerSec;
    this.gpsNoiseStdDevM = opts.gpsNoiseStdDevM ?? 10;
    this.agents = createAgents(this.city, opts.drivers, this.rng);
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
      tickAgent(this.city, agent, this.rng, dtSec);
      const ping = noisyPing(this.city, agent, this.rng, this.gpsNoiseStdDevM);
      const { lat, lng } = toLatLng(this.city, ping);
      out.push({ type: 'driver_ping', driverId: agent.id, lat, lng, ts });
    }
    const requests = generateRequests(this.city, this.rng, this.ratePerSec, dtSec, this.hotspotOpts);
    requests.forEach((pt, i) => {
      const { lat, lng } = toLatLng(this.city, pt);
      out.push({ type: 'ride_request', requestId: `r${this.tickCount}-${i}`, lat, lng, ts });
    });
    this.tickCount++;
    return out;
  }
}
