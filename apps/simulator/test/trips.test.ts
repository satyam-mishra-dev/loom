import { describe, expect, it } from 'vitest';
import type { DriverPing, TripProgress } from '@loom/core';
import { Simulation } from '../src/sim.js';
import { toLatLng } from '../src/world.js';

// Driver agents on trips: offers decided by seeded per-driver rng, movement
// to pickup then dest, milestones emitted from movement — all deterministic.

function newSim(drivers = 1): Simulation {
  return new Simulation({ drivers, ratePerSec: 0, hotspots: 0, seed: 7 });
}

describe('offer decisions', () => {
  it('accept rate over many drivers approximates acceptProb, and is reproducible', () => {
    const draw = (): number => {
      const sim = new Simulation({ drivers: 1000, ratePerSec: 0, hotspots: 0, seed: 11, acceptProb: 0.8 });
      let accepts = 0;
      for (const agent of sim.agents) {
        if (sim.decideOffer(agent.id).accept) accepts++;
      }
      return accepts;
    };
    const accepts = draw();
    expect(accepts / 1000).toBeGreaterThan(0.75);
    expect(accepts / 1000).toBeLessThan(0.85);
    expect(draw()).toBe(accepts); // same seed, same decisions
  });

  it('response delay falls in the configured range', () => {
    const sim = newSim(100);
    for (const agent of sim.agents) {
      const { delayMs } = sim.decideOffer(agent.id);
      expect(delayMs).toBeGreaterThanOrEqual(500);
      expect(delayMs).toBeLessThan(3000);
    }
  });

  it('a driver already on a trip declines, and unknown drivers decline', () => {
    const sim = newSim();
    const agent = sim.agents[0]!;
    sim.assignTrip(agent.id, 't1', toLatLng(sim.city, agent.pos), toLatLng(sim.city, { x: 0, y: 0 }));
    expect(sim.decideOffer(agent.id).accept).toBe(false);
    expect(sim.decideOffer('ghost').accept).toBe(false);
  });
});

describe('trip driving', () => {
  it('drives to pickup (en_route), then dest (in_trip), emitting milestones, then roams', () => {
    const sim = newSim();
    const agent = sim.agents[0]!;
    // Pickup ~600m away, dest ~600m beyond that: reachable in bounded ticks.
    const pickup = { x: Math.min(agent.pos.x + 600, sim.city.widthM), y: agent.pos.y };
    const dest = { x: pickup.x, y: Math.min(agent.pos.y + 600, sim.city.heightM) };
    expect(sim.assignTrip(agent.id, 't1', toLatLng(sim.city, pickup), toLatLng(sim.city, dest))).toBe(true);
    expect(agent.mode).toBe('to_pickup');

    const progress: TripProgress[] = [];
    const statuses: string[] = [];
    for (let t = 0; t < 500 && progress.length < 2; t++) {
      for (const msg of sim.tick()) {
        if (msg.type === 'trip_progress') progress.push(msg);
        if (msg.type === 'driver_ping') statuses.push((msg as DriverPing).status ?? '');
      }
    }
    expect(progress.map((p) => p.event)).toEqual(['arrived_pickup', 'trip_done']);
    expect(progress.every((p) => p.tripId === 't1' && p.driverId === agent.id)).toBe(true);
    // Ping status tracked the trip phases in order.
    const compact = statuses.filter((s, i) => statuses[i - 1] !== s);
    expect(compact).toEqual(['en_route', 'in_trip', 'available']);
    expect(agent.mode).toBe('roaming');
    expect(agent.trip).toBeNull();
  });

  it('ride requests carry destinations', () => {
    const sim = new Simulation({ drivers: 1, ratePerSec: 5, hotspots: 0, seed: 3 });
    const requests = sim.tick().filter((m) => m.type === 'ride_request');
    expect(requests.length).toBeGreaterThan(0);
    for (const r of requests) {
      expect(Number.isFinite(r.destLat)).toBe(true);
      expect(Number.isFinite(r.destLng)).toBe(true);
    }
  });
});
