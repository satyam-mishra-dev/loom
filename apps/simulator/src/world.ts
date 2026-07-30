import type { Rng } from './rng.js';

/** Synthetic grid city: a widthM x heightM box with streets every blockM meters. */
export interface City {
  widthM: number;
  heightM: number;
  blockM: number;
  originLat: number;
  originLng: number;
}

/** Position in meters from the city's south-west corner. */
export interface Point {
  x: number;
  y: number;
}

export const DEFAULT_CITY: City = {
  widthM: 10_000,
  heightM: 10_000,
  blockM: 250,
  originLat: 37.7749,
  originLng: -122.4194,
};

const M_PER_DEG_LAT = 111_320;

/** Equirectangular projection — fine at city scale (<0.1% error over 10 km). */
export function toLatLng(city: City, p: Point): { lat: number; lng: number } {
  const mPerDegLng = M_PER_DEG_LAT * Math.cos((city.originLat * Math.PI) / 180);
  return { lat: city.originLat + p.y / M_PER_DEG_LAT, lng: city.originLng + p.x / mPerDegLng };
}

/** A random street intersection on the city grid. */
export function randomIntersection(city: City, rng: Rng): Point {
  const cols = Math.floor(city.widthM / city.blockM);
  const rows = Math.floor(city.heightM / city.blockM);
  return { x: rng.int(cols + 1) * city.blockM, y: rng.int(rows + 1) * city.blockM };
}

export function inBounds(city: City, p: Point): boolean {
  return p.x >= 0 && p.x <= city.widthM && p.y >= 0 && p.y <= city.heightM;
}

export function clampToCity(city: City, p: Point): Point {
  return {
    x: Math.min(city.widthM, Math.max(0, p.x)),
    y: Math.min(city.heightM, Math.max(0, p.y)),
  };
}

export function distance(a: Point, b: Point): number {
  return Math.hypot(a.x - b.x, a.y - b.y);
}
