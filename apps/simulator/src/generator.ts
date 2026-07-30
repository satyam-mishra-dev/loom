import type { Rng } from './rng.js';
import { clampToCity, type City, type Point } from './world.js';

export interface HotspotOptions {
  centers: Point[];
  /** Probability a request originates from a hotspot rather than uniformly. */
  weight: number;
  /** Gaussian spread of requests around a hotspot center, meters. */
  sigmaM: number;
}

/** Number of Poisson(lambda) events. Chunked so exp(-lambda) never underflows. */
export function poissonCount(rng: Rng, lambda: number): number {
  let total = 0;
  let remaining = lambda;
  while (remaining > 0) {
    const chunk = Math.min(remaining, 30);
    total += poissonKnuth(rng, chunk);
    remaining -= chunk;
  }
  return total;
}

function poissonKnuth(rng: Rng, lambda: number): number {
  const limit = Math.exp(-lambda);
  let k = 0;
  let p = 1;
  do {
    k++;
    p *= rng.next();
  } while (p > limit);
  return k - 1;
}

/** Hotspot centers placed in the inner 80% of the city so their mass stays inside. */
export function createHotspots(city: City, count: number, rng: Rng): Point[] {
  return Array.from({ length: count }, () => ({
    x: rng.range(0.1, 0.9) * city.widthM,
    y: rng.range(0.1, 0.9) * city.heightM,
  }));
}

/** Ride-request origins for one tick: Poisson count, hotspot-clustered placement. */
export function generateRequests(
  city: City,
  rng: Rng,
  ratePerSec: number,
  dtSec: number,
  hotspots: HotspotOptions,
): Point[] {
  const count = poissonCount(rng, ratePerSec * dtSec);
  return Array.from({ length: count }, () => requestPoint(city, rng, hotspots));
}

function requestPoint(city: City, rng: Rng, { centers, weight, sigmaM }: HotspotOptions): Point {
  if (centers.length > 0 && rng.next() < weight) {
    const c = centers[rng.int(centers.length)]!;
    return clampToCity(city, {
      x: c.x + rng.gaussian(0, sigmaM),
      y: c.y + rng.gaussian(0, sigmaM),
    });
  }
  return { x: rng.next() * city.widthM, y: rng.next() * city.heightM };
}
