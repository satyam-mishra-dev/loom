import { describe, expect, it } from 'vitest';
import {
  URBAN_SPEED_MPS,
  estimateEtaSeconds,
  haversineMeters,
  rankCandidates,
  type DriverRecord,
} from '../src/index.js';

function driver(id: string, lat: number, lng: number, heartbeatMs: number): DriverRecord {
  return { driverId: id, cell: 'c', status: 'available', lat, lng, heartbeatMs };
}

const RIDER = { lat: 37.7749, lng: -122.4194 };

describe('haversineMeters', () => {
  it('is zero for identical points and symmetric', () => {
    expect(haversineMeters(RIDER.lat, RIDER.lng, RIDER.lat, RIDER.lng)).toBe(0);
    const there = haversineMeters(37.77, -122.42, 37.78, -122.4);
    const back = haversineMeters(37.78, -122.4, 37.77, -122.42);
    expect(there).toBeCloseTo(back, 9);
  });

  it('one degree of latitude is ~111.2 km', () => {
    const d = haversineMeters(37, -122, 38, -122);
    expect(d).toBeGreaterThan(110_000);
    expect(d).toBeLessThan(112_500);
  });
});

describe('rankCandidates', () => {
  it('orders by distance ascending', () => {
    const ranked = rankCandidates(RIDER.lat, RIDER.lng, [
      driver('far', RIDER.lat + 0.01, RIDER.lng, 1000),
      driver('near', RIDER.lat + 0.001, RIDER.lng, 1000),
      driver('mid', RIDER.lat + 0.005, RIDER.lng, 1000),
    ]);
    expect(ranked.map((c) => c.driverId)).toEqual(['near', 'mid', 'far']);
    expect(ranked[0]!.distanceM).toBeLessThan(ranked[1]!.distanceM);
  });

  it('breaks distance ties by freshest heartbeat', () => {
    const ranked = rankCandidates(RIDER.lat, RIDER.lng, [
      driver('older', RIDER.lat + 0.001, RIDER.lng, 1000),
      driver('fresher', RIDER.lat + 0.001, RIDER.lng, 5000),
    ]);
    expect(ranked.map((c) => c.driverId)).toEqual(['fresher', 'older']);
  });

  it('distance dominates freshness; full ties fall back to driverId', () => {
    const ranked = rankCandidates(RIDER.lat, RIDER.lng, [
      driver('near-stale', RIDER.lat + 0.001, RIDER.lng, 1000),
      driver('far-fresh', RIDER.lat + 0.005, RIDER.lng, 9000),
      driver('b', RIDER.lat, RIDER.lng, 2000),
      driver('a', RIDER.lat, RIDER.lng, 2000),
    ]);
    expect(ranked.map((c) => c.driverId)).toEqual(['a', 'b', 'near-stale', 'far-fresh']);
  });

  it('does not mutate its input', () => {
    const input = [
      driver('x', RIDER.lat + 0.01, RIDER.lng, 1),
      driver('y', RIDER.lat, RIDER.lng, 1),
    ];
    rankCandidates(RIDER.lat, RIDER.lng, input);
    expect(input.map((c) => c.driverId)).toEqual(['x', 'y']);
  });

  it('the shorter-ETA driver outranks the farther one, and ETA = distance ÷ urban speed', () => {
    const ranked = rankCandidates(RIDER.lat, RIDER.lng, [
      driver('far', RIDER.lat + 0.01, RIDER.lng, 1000),
      driver('near', RIDER.lat + 0.001, RIDER.lng, 1000),
    ]);
    expect(ranked.map((c) => c.driverId)).toEqual(['near', 'far']);
    expect(ranked[0]!.etaSeconds).toBeLessThan(ranked[1]!.etaSeconds);
    // ETA is exactly the naive model — distance over the assumed speed.
    expect(ranked[0]!.etaSeconds).toBeCloseTo(ranked[0]!.distanceM / URBAN_SPEED_MPS, 9);
    expect(estimateEtaSeconds(0)).toBe(0);
  });

  it('equal ETA ties break deterministically (freshest heartbeat, then driverId)', () => {
    const ranked = rankCandidates(RIDER.lat, RIDER.lng, [
      driver('b', RIDER.lat + 0.002, RIDER.lng, 2000),
      driver('a', RIDER.lat + 0.002, RIDER.lng, 2000),
      driver('fresh', RIDER.lat + 0.002, RIDER.lng, 9000),
    ]);
    expect(ranked.map((c) => c.driverId)).toEqual(['fresh', 'a', 'b']);
    expect(ranked[0]!.etaSeconds).toBeCloseTo(ranked[1]!.etaSeconds, 9);
  });
});
