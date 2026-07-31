import { gridDisk, latLngToCell } from 'h3-js';

/** Resolution 8: ~0.74 km^2 hexagons — city-block-scale dispatch cells. */
export const H3_RESOLUTION = 8;

/** H3 cell index for a coordinate at the given resolution (default 8). */
export function cellFor(lat: number, lng: number, resolution: number = H3_RESOLUTION): string {
  return latLngToCell(lat, lng, resolution);
}

/**
 * All cells within k grid steps of the origin cell (k-ring), origin included.
 * Sizes: k=0 → 1, k=1 → 7, k=2 → 19.
 */
export function kRing(cell: string, k: number): string[] {
  return gridDisk(cell, k);
}

const EARTH_RADIUS_M = 6_371_000;

/** Great-circle distance in meters. Exact enough at dispatch scale; used for scoring, not billing. */
export function haversineMeters(lat1: number, lng1: number, lat2: number, lng2: number): number {
  const rad = Math.PI / 180;
  const dLat = (lat2 - lat1) * rad;
  const dLng = (lng2 - lng1) * rad;
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(lat1 * rad) * Math.cos(lat2 * rad) * Math.sin(dLng / 2) ** 2;
  return 2 * EARTH_RADIUS_M * Math.asin(Math.sqrt(a));
}
