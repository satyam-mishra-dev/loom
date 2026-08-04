import { haversineMeters } from './geo.js';
import type { DriverRecord } from './geo-index.js';

export interface RankedCandidate extends DriverRecord {
  distanceM: number;
  /** Estimated seconds for the driver to reach the rider (see estimateEtaSeconds). */
  etaSeconds: number;
}

/**
 * Assumed average urban driving speed, ~30 km/h. NOTE: naive constant-speed
 * ETA — real ETA needs routing (turns, one-ways, live traffic). It is a strict
 * function of straight-line distance, so folding it into the score cannot flip
 * a nearer driver behind a farther one; it makes "sooner arrival ranks higher"
 * explicit and gives the offer an ETA to surface.
 */
export const URBAN_SPEED_MPS = 8.33;

/** Naive ETA: great-circle distance to the rider ÷ assumed urban speed. */
export function estimateEtaSeconds(distanceM: number): number {
  return distanceM / URBAN_SPEED_MPS;
}

/**
 * Matcher scoring: soonest arrival first (naive ETA from haversine to the
 * rider), freshest heartbeat breaking ties, driverId as the final tiebreak so
 * the order is fully deterministic (gridDisk guarantees no ordering — someone
 * must). ETA is monotonic in distance, so this is still closest-first, now
 * expressed as the quantity the offer actually cares about.
 */
export function rankCandidates(
  riderLat: number,
  riderLng: number,
  candidates: readonly DriverRecord[],
): RankedCandidate[] {
  return candidates
    .map((c) => {
      const distanceM = haversineMeters(riderLat, riderLng, c.lat, c.lng);
      return { ...c, distanceM, etaSeconds: estimateEtaSeconds(distanceM) };
    })
    .sort(
      (a, b) =>
        a.etaSeconds - b.etaSeconds ||
        b.heartbeatMs - a.heartbeatMs ||
        (a.driverId < b.driverId ? -1 : 1),
    );
}
