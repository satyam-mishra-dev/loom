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

// ---- demo endpoints (mirrored from apps/read-model/src/demo.ts) ----

export interface ProofResult {
  drivers: number;
  requests: number;
  matched: number;
  unmatched: number;
  skipped: number;
  doubleAssignments: number;
  pgUniqueViolations: number;
  claimConflicts: number;
  cell: string;
  elapsedMs: number;
}

export interface CrashSnapshot {
  claimPresent: boolean;
  driverStatus: string | null;
  tripStatus: string | null;
  requestStatus: string | null;
  doubleAssignments: number;
}

export interface CrashResult {
  driverId: string;
  requestId: string;
  tripId: string;
  cell: string;
  faultAt: number;
  before: CrashSnapshot;
  after: CrashSnapshot;
  recovered: boolean;
  recoveryMs: number | null;
  invariantBreaches: number;
  sweptClaim: boolean;
  driverFreed: boolean;
  orphanCleared: boolean;
}

export interface OfferAttempt {
  driverId: string;
  offerId: string;
  result: 'accepted' | 'declined' | 'timed_out' | 'pending';
}

export interface TripEventView {
  type: string;
  event: string | null;
  at: string;
}

export interface TripHistory {
  tripId: string;
  status: string;
  driverId: string;
  requestId: string;
  rider: { lat: number; lng: number };
  dest: { lat: number; lng: number } | null;
  surgeMultiplier: number;
  createdAt: string;
  events: TripEventView[];
  offers: OfferAttempt[];
}
