import { haversineMeters } from './geo.js';
import type { DriverRecord } from './geo-index.js';

export interface RankedCandidate extends DriverRecord {
  distanceM: number;
}

/**
 * Matcher scoring: closest driver first (haversine to the rider), freshest
 * heartbeat breaking ties, driverId as the final tiebreak so the order is
 * fully deterministic (gridDisk guarantees no ordering — someone must).
 */
export function rankCandidates(
  riderLat: number,
  riderLng: number,
  candidates: readonly DriverRecord[],
): RankedCandidate[] {
  return candidates
    .map((c) => ({ ...c, distanceM: haversineMeters(riderLat, riderLng, c.lat, c.lng) }))
    .sort(
      (a, b) =>
        a.distanceM - b.distanceM ||
        b.heartbeatMs - a.heartbeatMs ||
        (a.driverId < b.driverId ? -1 : 1),
    );
}
