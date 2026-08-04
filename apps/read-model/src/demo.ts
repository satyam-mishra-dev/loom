import { randomUUID } from 'node:crypto';
import { cellToLatLng } from 'h3-js';
import type { Redis } from 'ioredis';
import type pg from 'pg';
import {
  CLAIMS_BY_EXPIRY,
  CLAIM_PX_GRACE_MS,
  ClaimStore,
  DEFAULT_CLAIM_TTL_MS,
  DEFAULT_STALE_MS,
  GeoIndex,
  type ClaimRecord,
  HEARTBEAT_ZSET,
  REQUESTS_PROCESSING,
  REQUESTS_QUEUE,
  SURGE_HASH,
  cellFor,
  cellKey,
  claimKey,
  driverChannel,
  driverKey,
  offerReplyKey,
  type GeoPing,
} from '@loom/core';
import { MatcherCore, TripStore } from '@loom/matcher';

/**
 * The demo mechanisms behind the dashboard's signature controls — Proof Mode
 * and Crash the matcher — plus the trip inspector's history read. Every one of
 * these drives the REAL engine (real Lua claim, real Postgres, real partial
 * unique index, real janitor recovery); nothing here animates a canned result.
 *
 * All three run against an ISOLATED H3 cell far from the live simulator fleet,
 * so candidate search sees only the drivers each demo seeds — the outcome is
 * then deterministic and can't collide with the live traffic on the same
 * stores. Each run self-cleans (Postgres rows + Redis keys) so it is idempotent
 * and repeatable.
 */

export interface Deps {
  redis: Redis;
  pool: pg.Pool;
  center: { lat: number; lng: number };
}

const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

/** A cell far enough from the live fleet that its candidate search is ours alone. */
function isolatedCell(
  center: { lat: number; lng: number },
  dLat: number,
  dLng: number,
): {
  cell: string;
  lat: number;
  lng: number;
} {
  const cell = cellFor(center.lat + dLat, center.lng + dLng);
  const [lat, lng] = cellToLatLng(cell);
  return { cell, lat, lng };
}

// ---------------------------------------------------------------------------
// Proof Mode — the real 200-concurrent signature scenario, on demand.
// ---------------------------------------------------------------------------

export interface ProofResult {
  /** Drivers seeded into the one cell. */
  drivers: number;
  /** Concurrent ride requests fired at that cell. */
  requests: number;
  /** Requests that ended in a committed trip (must equal `drivers`). */
  matched: number;
  /** Requests that honestly found no driver (must equal requests − drivers). */
  unmatched: number;
  skipped: number;
  /** THE invariant, read straight from Postgres: drivers holding >1 active trip. */
  doubleAssignments: number;
  /** The never-happens backstop: partial-unique-index rejections during the run. */
  pgUniqueViolations: number;
  /** Losing claim attempts that actually collided — proof the race was real. */
  claimConflicts: number;
  /** The single H3 res-8 cell all drivers shared. */
  cell: string;
  elapsedMs: number;
}

const PROOF_DRIVERS = 20;
const PROOF_REQUESTS = 200;

/**
 * Seed `PROOF_DRIVERS` drivers into one cell, fire `PROOF_REQUESTS` concurrent
 * ride requests through the real `MatcherCore.matchRequest` path (real Lua
 * claim, real offer round-trip over pub/sub + reply lists, real Postgres, real
 * partial unique index — drivers auto-accept), and report the true outcome.
 *
 * This is the same scenario as `test/no-double-assignment.test.ts`, run live.
 */
export async function runProof(deps: Deps): Promise<ProofResult> {
  const { redis, pool } = deps;
  const origin = isolatedCell(deps.center, 0.6, 0.6);
  const runId = `proof-${Date.now()}-${Math.floor(Math.random() * 1e6)}`;
  const driverIds = Array.from({ length: PROOF_DRIVERS }, (_, i) => `${runId}-d${i}`);
  const requestIds = Array.from({ length: PROOF_REQUESTS }, (_, i) => `${runId}-r${i}`);

  // A dedicated connection so the claim Lua and the heavy concurrent load land
  // off the shared SSE-tick client.
  const work = redis.duplicate();
  // The fake fleet: subscribed to exactly this run's driver channels (never a
  // live driver's), it accepts every offer instantly over the real transport.
  const fleet = redis.duplicate();

  try {
    await fleet.subscribe(...driverIds.map(driverChannel));
    fleet.on('message', (_channel, message) => {
      try {
        const msg = JSON.parse(message) as { type?: string; offerId?: string };
        if (msg.type === 'offer' && typeof msg.offerId === 'string') {
          void work.lpush(offerReplyKey(msg.offerId), JSON.stringify({ accept: true }));
        }
      } catch {
        /* not our frame */
      }
    });

    const now = Date.now();
    const geo = new GeoIndex(work);
    const pings: GeoPing[] = driverIds.map((id, i) => ({
      driverId: id,
      lat: origin.lat + (i % 5) * 1e-4,
      lng: origin.lng + Math.floor(i / 5) * 1e-4,
    }));
    await geo.applyPings(pings, now);

    // Riders scattered a few hundred meters around the cell so scoring has real
    // distances to rank.
    const lats = requestIds.map((_, i) => origin.lat + ((i % 20) - 10) * 2e-4);
    const lngs = requestIds.map((_, i) => origin.lng + ((Math.floor(i / 20) % 20) - 5) * 2e-4);
    await pool.query(
      `INSERT INTO ride_requests (id, lat, lng, status)
       SELECT unnest($1::text[]), unnest($2::float8[]), unnest($3::float8[]), 'pending'`,
      [requestIds, lats, lngs],
    );

    const started = Date.now();
    const matcher = new MatcherCore({ redis: work, pool });
    const outcomes = await Promise.all(requestIds.map((id) => matcher.matchRequest(id)));
    const elapsedMs = Date.now() - started;

    const dup = await pool.query(
      `SELECT driver_id FROM trips WHERE request_id = ANY($1)
       GROUP BY driver_id HAVING count(*) > 1`,
      [requestIds],
    );

    return {
      drivers: PROOF_DRIVERS,
      requests: PROOF_REQUESTS,
      matched: outcomes.filter((o) => o === 'matched').length,
      unmatched: outcomes.filter((o) => o === 'unmatched').length,
      skipped: outcomes.filter((o) => o === 'skipped').length,
      doubleAssignments: dup.rowCount ?? 0,
      pgUniqueViolations: matcher.metrics.pgUniqueViolationsTotal,
      claimConflicts: matcher.metrics.claimConflictsTotal,
      cell: origin.cell,
      elapsedMs,
    };
  } finally {
    await cleanupByIds(deps, requestIds, driverIds, origin.cell).catch(() => undefined);
    fleet.disconnect();
    work.disconnect();
  }
}

// ---------------------------------------------------------------------------
// Crash the matcher — a real abandoned-claim fault the live janitor recovers.
// ---------------------------------------------------------------------------

export interface CrashHandle {
  driverId: string;
  requestId: string;
  tripId: string;
  cell: string;
  /** Epoch ms the wreckage was created. */
  faultAt: number;
}

export interface CrashSnapshot {
  claimPresent: boolean;
  driverStatus: string | null;
  tripStatus: string | null;
  requestStatus: string | null;
  /** THE invariant for this driver: >1 active trip. Must always be 0. */
  doubleAssignments: number;
  /** Durable proof the janitor reverted the orphan: an outbox event it authored. */
  orphanReverted: boolean;
}

export interface CrashResult extends CrashHandle {
  before: CrashSnapshot;
  after: CrashSnapshot;
  recovered: boolean;
  /** ms from fault to janitor recovery, or null if it never recovered in the window. */
  recoveryMs: number | null;
  /** Max double-assignments observed across the whole run (must be 0). */
  invariantBreaches: number;
  /** The janitor deleted the abandoned claim. */
  sweptClaim: boolean;
  /** The claimed driver returned to available. */
  driverFreed: boolean;
  /** The orphaned 'offered' trip was reverted (request handed back). */
  orphanCleared: boolean;
}

/**
 * Create the exact wreckage a SIGKILLed matcher leaves once its claim's lease
 * lapses: a driver `claimed` behind an already-expired claim (real Lua claim,
 * expiry in the past), an orphaned `offered` trip, and a request stuck
 * `matching`. The real janitor — running inside every live matcher — then
 * sweeps it with no matcher restart needed. This is the crash-variant signature
 * test (`test/no-double-assignment-crash.test.ts`) as a live control.
 */
export async function injectAbandonedClaim(deps: Deps): Promise<CrashHandle> {
  const { redis, pool } = deps;
  const origin = isolatedCell(deps.center, 0.7, -0.7);
  const runId = `crash-${Date.now()}-${Math.floor(Math.random() * 1e6)}`;
  const driverId = `${runId}-victim`;
  const requestId = `${runId}-req`;
  const tripId = randomUUID();

  const now = Date.now();
  await new GeoIndex(redis).applyPings([{ driverId, lat: origin.lat, lng: origin.lng }], now);

  // Claim through the REAL Lua with a normal (future) lease first, so the
  // janitor can't sweep it before the orphaned trip row exists.
  const claims = new ClaimStore(redis);
  const token = await claims.claimDriver(
    driverId,
    tripId,
    now,
    DEFAULT_STALE_MS,
    DEFAULT_CLAIM_TTL_MS,
  );
  if (token === null) throw new Error('could not seed the abandoned claim (driver not claimable)');

  await pool.query(
    `INSERT INTO ride_requests (id, lat, lng, dest_lat, dest_lng, status)
     VALUES ($1, $2, $3, $2, $3, 'matching')`,
    [requestId, origin.lat, origin.lng],
  );
  const offered = await new TripStore(pool).offerTrip({
    tripId,
    requestId,
    driverId,
    offerId: randomUUID(),
    claimToken: token,
    rider: { lat: origin.lat, lng: origin.lng },
  });
  if (!offered) throw new Error('could not seed the orphaned offered trip');

  // Now LAPSE the lease — the state a crashed matcher's claim reaches the moment
  // its deadline passes: expiresAt in the past, re-indexed in claims:by-expiry at
  // a past score. The value (with tripId + token) is what the janitor re-reads.
  const past = now - 1_000;
  const value: ClaimRecord = { tripId, token, expiresAt: past };
  await redis.set(claimKey(driverId), JSON.stringify(value), 'PX', CLAIM_PX_GRACE_MS);
  await redis.zadd(CLAIMS_BY_EXPIRY, past, driverId);

  return { driverId, requestId, tripId, cell: origin.cell, faultAt: now };
}

/** Read the live state of a crash fault (what the invariant chip is watching). */
export async function crashSnapshot(deps: Deps, h: CrashHandle): Promise<CrashSnapshot> {
  const { redis, pool } = deps;
  const [claimPresent, driverStatus, trip, req, dup, reverted] = await Promise.all([
    redis.exists(claimKey(h.driverId)),
    redis.hget(driverKey(h.driverId), 'status'),
    pool.query<{ status: string }>('SELECT status FROM trips WHERE id = $1', [h.tripId]),
    pool.query<{ status: string }>('SELECT status FROM ride_requests WHERE id = $1', [h.requestId]),
    pool.query(
      `SELECT driver_id FROM trips
       WHERE driver_id = $1 AND status IN ('matched', 'en_route', 'in_trip')
       GROUP BY driver_id HAVING count(*) > 1`,
      [h.driverId],
    ),
    pool.query(
      `SELECT 1 FROM trip_events WHERE trip_id = $1 AND payload->>'by' = 'janitor' LIMIT 1`,
      [h.tripId],
    ),
  ]);
  return {
    claimPresent: claimPresent === 1,
    driverStatus,
    tripStatus: trip.rows[0]?.status ?? null,
    requestStatus: req.rows[0]?.status ?? null,
    doubleAssignments: dup.rowCount ?? 0,
    orphanReverted: (reverted.rowCount ?? 0) > 0,
  };
}

/**
 * Inject the fault, watch the REAL janitor recover it (polling the stores),
 * assert the invariant never flips, then clean up. The janitor's guarantees are
 * tracked as latched observations — the claim is swept, the driver is freed, the
 * orphaned 'offered' trip is reverted — because in a live stack a matcher is
 * actively consuming the re-enqueued request, so those states are transient. If
 * no janitor is alive the window times out and it honestly reports recovered:false.
 */
export async function runCrashDemo(deps: Deps, timeoutMs = 18_000): Promise<CrashResult> {
  const handle = await injectAbandonedClaim(deps);
  const before = await crashSnapshot(deps, handle);
  let invariantBreaches = before.doubleAssignments;
  let after = before;
  let sweptClaim = false;
  let driverFreed = false;
  let orphanCleared = false;
  let recovered = false;
  let recoveryMs: number | null = null;

  const deadline = Date.now() + timeoutMs;
  try {
    while (Date.now() < deadline) {
      await sleep(100);
      const s = await crashSnapshot(deps, handle);
      after = s;
      invariantBreaches = Math.max(invariantBreaches, s.doubleAssignments);
      // Latch each janitor guarantee the first time it's observed.
      if (!s.claimPresent) sweptClaim = true;
      if (s.driverStatus === 'available') driverFreed = true;
      // The janitor reverted the orphan — durable (its outbox event) so we don't
      // depend on catching the transient 'matching' before a live matcher re-offers.
      if (s.orphanReverted) orphanCleared = true;
      if (sweptClaim && driverFreed && orphanCleared) {
        recovered = true;
        recoveryMs = Date.now() - handle.faultAt;
        break;
      }
    }
  } finally {
    await cleanupCrash(deps, handle).catch(() => undefined);
  }
  return {
    ...handle,
    before,
    after,
    recovered,
    recoveryMs,
    invariantBreaches,
    sweptClaim,
    driverFreed,
    orphanCleared,
  };
}

/** Remove all traces of a crash fault (queue entries + PG rows + Redis keys). */
export async function cleanupCrash(deps: Deps, h: CrashHandle): Promise<void> {
  const { redis, pool } = deps;
  // The janitor re-enqueues the request on recovery; pull it back off so the
  // live matcher doesn't pick up a request we're about to delete.
  await redis.lrem(REQUESTS_QUEUE, 0, h.requestId);
  await redis.lrem(REQUESTS_PROCESSING, 0, h.requestId);
  await pool.query(
    `DELETE FROM trip_events WHERE trip_id IN (SELECT id FROM trips WHERE request_id = $1 OR driver_id = $2)`,
    [h.requestId, h.driverId],
  );
  await pool.query('UPDATE ride_requests SET matched_trip_id = NULL WHERE id = $1', [h.requestId]);
  await pool.query('DELETE FROM trips WHERE request_id = $1 OR driver_id = $2', [
    h.requestId,
    h.driverId,
  ]);
  await pool.query('DELETE FROM ride_requests WHERE id = $1', [h.requestId]);
  const pipe = redis.pipeline();
  pipe.del(driverKey(h.driverId));
  pipe.del(claimKey(h.driverId));
  pipe.srem(cellKey(h.cell), h.driverId);
  pipe.zrem(HEARTBEAT_ZSET, h.driverId);
  pipe.zrem(CLAIMS_BY_EXPIRY, h.driverId);
  await pipe.exec();
}

// ---------------------------------------------------------------------------
// Trip inspector — the real event history behind one trip.
// ---------------------------------------------------------------------------

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
  /** Live surge multiplier for the pickup cell (1 = no surge). */
  surgeMultiplier: number;
  createdAt: string;
  events: TripEventView[];
  offers: OfferAttempt[];
}

interface EventRow {
  type: string;
  payload: {
    event?: string;
    driverId?: string;
    offerId?: string;
    by?: string;
  };
  created_at: Date;
}

/** Full real history for one trip, or null if it doesn't exist. */
export async function tripHistory(deps: Deps, tripId: string): Promise<TripHistory | null> {
  const { redis, pool } = deps;
  const trip = await pool.query<{
    id: string;
    status: string;
    driver_id: string;
    request_id: string;
    rider_lat: number;
    rider_lng: number;
    dest_lat: number | null;
    dest_lng: number | null;
    created_at: Date;
  }>(
    `SELECT t.id, t.status, t.driver_id, t.request_id, t.rider_lat, t.rider_lng,
            r.dest_lat, r.dest_lng, t.created_at
     FROM trips t JOIN ride_requests r ON r.id = t.request_id
     WHERE t.id = $1`,
    [tripId],
  );
  const row = trip.rows[0];
  if (row === undefined) return null;

  const ev = await pool.query<EventRow>(
    'SELECT type, payload, created_at FROM trip_events WHERE trip_id = $1 ORDER BY id',
    [tripId],
  );

  // Derive the offer cascade from the outbox: an 'offered' event opens an
  // attempt (OFFER_SENT); a later OFFER_DECLINED/OFFER_TIMED_OUT/OFFER_ACCEPTED
  // for that offerId resolves it.
  const offers: OfferAttempt[] = [];
  const byOfferId = new Map<string, OfferAttempt>();
  for (const e of ev.rows) {
    const p = e.payload;
    if (
      p.event === 'OFFER_SENT' &&
      typeof p.offerId === 'string' &&
      typeof p.driverId === 'string'
    ) {
      const attempt: OfferAttempt = { driverId: p.driverId, offerId: p.offerId, result: 'pending' };
      offers.push(attempt);
      byOfferId.set(p.offerId, attempt);
    } else if (typeof p.offerId === 'string') {
      const attempt = byOfferId.get(p.offerId);
      if (attempt !== undefined) {
        if (p.event === 'OFFER_ACCEPTED') attempt.result = 'accepted';
        else if (p.event === 'OFFER_DECLINED') attempt.result = 'declined';
        else if (p.event === 'OFFER_TIMED_OUT') attempt.result = 'timed_out';
      }
    }
  }

  const surgeRaw = await redis.hget(SURGE_HASH, cellFor(row.rider_lat, row.rider_lng));
  const surge = surgeRaw === null ? 1 : Number(surgeRaw);

  return {
    tripId: row.id,
    status: row.status,
    driverId: row.driver_id,
    requestId: row.request_id,
    rider: { lat: row.rider_lat, lng: row.rider_lng },
    dest:
      row.dest_lat === null || row.dest_lng === null
        ? null
        : { lat: row.dest_lat, lng: row.dest_lng },
    surgeMultiplier: Number.isFinite(surge) ? surge : 1,
    createdAt: row.created_at.toISOString(),
    events: ev.rows.map((e) => ({
      type: e.type,
      event: typeof e.payload.event === 'string' ? e.payload.event : null,
      at: e.created_at.toISOString(),
    })),
    offers,
  };
}

// ---------------------------------------------------------------------------

async function cleanupByIds(
  deps: Deps,
  requestIds: string[],
  driverIds: string[],
  cell: string,
): Promise<void> {
  const { redis, pool } = deps;
  await pool.query(
    'DELETE FROM trip_events WHERE trip_id IN (SELECT id FROM trips WHERE request_id = ANY($1))',
    [requestIds],
  );
  await pool.query('UPDATE ride_requests SET matched_trip_id = NULL WHERE id = ANY($1)', [
    requestIds,
  ]);
  await pool.query('DELETE FROM trips WHERE request_id = ANY($1)', [requestIds]);
  await pool.query('DELETE FROM ride_requests WHERE id = ANY($1)', [requestIds]);
  const pipe = redis.pipeline();
  for (const id of driverIds) {
    pipe.del(driverKey(id));
    pipe.del(claimKey(id));
    pipe.srem(cellKey(cell), id);
    pipe.zrem(HEARTBEAT_ZSET, id);
    pipe.zrem(CLAIMS_BY_EXPIRY, id);
  }
  await pipe.exec();
}
