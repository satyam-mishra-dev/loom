import { describe, expect, it } from 'vitest';
import { createHotspots, generateRequests, poissonCount } from '../src/generator.js';
import { createRng } from '../src/rng.js';
import { DEFAULT_CITY, distance, type Point } from '../src/world.js';

describe('poisson request generator', () => {
  it('produces approximately rate x duration events over a long window', () => {
    const rng = createRng(3);
    const rate = 5;
    const seconds = 2000;
    let total = 0;
    for (let t = 0; t < seconds; t++) total += poissonCount(rng, rate);
    const expected = rate * seconds; // 10_000; sd ~= 100, 5% tolerance is ~5 sd
    expect(total).toBeGreaterThan(expected * 0.95);
    expect(total).toBeLessThan(expected * 1.05);
  });

  it('handles large lambda without underflow', () => {
    const rng = createRng(5);
    const n = poissonCount(rng, 1000);
    expect(n).toBeGreaterThan(800);
    expect(n).toBeLessThan(1200);
  });

  it('hotspot clustering concentrates requests near hotspot centers', () => {
    const rng = createRng(9);
    const centers = createHotspots(DEFAULT_CITY, 3, rng);
    const clustered: Point[] = [];
    const uniform: Point[] = [];
    for (let t = 0; t < 200; t++) {
      clustered.push(
        ...generateRequests(DEFAULT_CITY, rng, 25, 1, { centers, weight: 0.7, sigmaM: 400 }),
      );
      uniform.push(
        ...generateRequests(DEFAULT_CITY, rng, 25, 1, { centers: [], weight: 0, sigmaM: 400 }),
      );
    }
    expect(clustered.length).toBeGreaterThan(4000);
    expect(uniform.length).toBeGreaterThan(4000);

    const meanNearest = (points: Point[]): number =>
      points.reduce((sum, p) => sum + Math.min(...centers.map((c) => distance(p, c))), 0) /
      points.length;

    // Clustered mass sits ~sigma from a center; uniform sits ~kilometers away.
    expect(meanNearest(clustered)).toBeLessThan(0.6 * meanNearest(uniform));
  });
});
