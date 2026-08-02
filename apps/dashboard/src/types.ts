// The SSE contract, mirrored from apps/read-model/src/server.ts. Kept as a
// hand-written interface (not imported) because the dashboard is a separate
// browser build with its own toolchain — the shapes are small and stable.

export interface DriverDot {
  id: string;
  lat: number;
  lng: number;
  s: 'available' | 'claimed' | 'on_trip';
}

export interface TripArc {
  id: string;
  plat: number;
  plng: number;
  dlat: number;
  dlng: number;
  s: string;
}

export interface SurgeCell {
  cell: string;
  m: number;
  demand: number;
  supply: number;
}

export interface Counters {
  matchesPerSec: number;
  p50Ms: number;
  p99Ms: number;
  activeTrips: number;
  unmatchedRate: number;
  matchesTotal: number;
  unmatchedTotal: number;
  surgeMax: number;
  driversTotal: number;
}

export interface Snapshot {
  t: number;
  drivers: DriverDot[];
  driversTotal: number;
  driversShown: number;
  trips: TripArc[];
  surge: SurgeCell[];
  counters: Counters;
}
