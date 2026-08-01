import { randomUUID } from 'node:crypto';
import type { Redis } from 'ioredis';
import pg from 'pg';
import { pino, type Logger } from 'pino';
import {
  ClaimStore,
  DEFAULT_CLAIM_TTL_MS,
  DEFAULT_STALE_MS,
  GeoIndex,
  REQUESTS_PROCESSING,
  REQUESTS_QUEUE,
  rankCandidates,
} from '@fleetline/core';
import { createMetrics, type MatcherMetrics } from './metrics.js';

export interface MatcherOptions {
  redis: Redis;
  pool: pg.Pool;
  log?: Logger;
  /** Candidate search: stop expanding once this many fresh drivers found (default 8). */
  need?: number;
  /** gridDisk expansion cap (default 3). */
  maxK?: number;
  /** Heartbeat freshness required to claim (default 10s, same as the sweep). */
  freshMs?: number;
  /** Claim lease duration (default 8s — phase D's offer TTL). */
  claimTtlMs?: number;
  /** Injectable clock (tests). */
  now?: () => number;
}

export type MatchOutcome = 'matched' | 'unmatched' | 'skipped';

/**
 * The matching pipeline: request → candidates → score → ATOMIC CLAIM →
 * trip row → confirm. One instance = one consumer group; the signature test
 * runs two instances against the same stores to prove cross-instance safety.
 *
 * Write order per candidate is claim → Postgres trip INSERT → confirm. The
 * confirm comes after the commit so that EVERY Postgres failure — including
 * the never-happens partial-unique-index rejection — unwinds through the same
 * atomic releaseClaim while the claim still exists. (Confirming first would
 * delete the claim and leave the violation path nothing to release with.)
 * Phase D slots the offer/accept between the claim and this same
 * commit+confirm tail: the claim already carries {tripId, token, expiresAt}.
 */
export class MatcherCore {
  readonly metrics: MatcherMetrics = createMetrics();
  readonly geoIndex: GeoIndex;
  readonly claims: ClaimStore;

  private readonly redis: Redis;
  private readonly pool: pg.Pool;
  private readonly log: Logger;
  private readonly need: number;
  private readonly maxK: number;
  private readonly freshMs: number;
  private readonly claimTtlMs: number;
  private readonly now: () => number;

  private stopped = false;
  private loops: Promise<void>[] = [];
  private conns: Redis[] = [];

  constructor(opts: MatcherOptions) {
    this.redis = opts.redis;
    this.pool = opts.pool;
    this.log = opts.log ?? pino({ level: 'silent' });
    this.need = opts.need ?? 8;
    this.maxK = opts.maxK ?? 3;
    this.freshMs = opts.freshMs ?? DEFAULT_STALE_MS;
    this.claimTtlMs = opts.claimTtlMs ?? DEFAULT_CLAIM_TTL_MS;
    this.now = opts.now ?? Date.now;
    this.geoIndex = new GeoIndex(this.redis);
    this.claims = new ClaimStore(this.redis);
  }

  /**
   * Match one request end to end. Safe under at-least-once delivery: the
   * pending→matching UPDATE is the idempotency guard — whoever wins that row
   * owns the request, every other delivery is a counted no-op.
   */
  async matchRequest(requestId: string): Promise<MatchOutcome> {
    const started = performance.now();
    const claimed = await this.pool.query<{ lat: number; lng: number }>(
      `UPDATE ride_requests SET status = 'matching' WHERE id = $1 AND status = 'pending'
       RETURNING lat, lng`,
      [requestId],
    );
    const row = claimed.rows[0];
    if (row === undefined) {
      this.metrics.requestsSkippedTotal++;
      this.log.warn({ requestId }, 'request not pending — duplicate delivery or lost row, skipping');
      return 'skipped';
    }

    const { candidates } = await this.geoIndex.findCandidates(row.lat, row.lng, {
      need: this.need,
      maxK: this.maxK,
      nowMs: this.now(),
      staleMs: this.freshMs,
    });

    for (const candidate of rankCandidates(row.lat, row.lng, candidates)) {
      const tripId = randomUUID();
      const token = await this.claims.claimDriver(
        candidate.driverId,
        tripId,
        this.now(),
        this.freshMs,
        this.claimTtlMs,
      );
      if (token === null) {
        this.metrics.claimConflictsTotal++;
        continue;
      }

      let committed: boolean;
      try {
        committed = await this.insertTrip(tripId, requestId, candidate.driverId, row, token);
      } catch (err) {
        await this.claims.releaseClaim(candidate.driverId, token);
        throw err;
      }
      if (!committed) {
        await this.claims.releaseClaim(candidate.driverId, token);
        continue;
      }

      if (!(await this.claims.confirmClaim(candidate.driverId, token, this.now()))) {
        // Claim expired between claim and commit (PG write took > ttl). The
        // trip stands; the driver's stale claim is phase D janitor territory.
        this.metrics.confirmFailuresTotal++;
        this.log.error({ requestId, tripId, driverId: candidate.driverId }, 'confirm failed after commit');
      }
      this.metrics.matchesTotal++;
      this.metrics.matchLatencyMs.observe(performance.now() - started);
      this.log.info({ requestId, tripId, driverId: candidate.driverId }, 'matched');
      return 'matched';
    }

    // Honest failure: no claimable candidate after the full walk.
    await this.pool.query(`UPDATE ride_requests SET status = 'unmatched' WHERE id = $1`, [requestId]);
    this.metrics.unmatchedTotal++;
    this.metrics.matchLatencyMs.observe(performance.now() - started);
    this.log.info({ requestId, candidates: candidates.length }, 'unmatched');
    return 'unmatched';
  }

  /**
   * Trip row + request update in ONE transaction. Returns false (rolled back,
   * counted) on a unique violation; throws on anything else.
   */
  private async insertTrip(
    tripId: string,
    requestId: string,
    driverId: string,
    rider: { lat: number; lng: number },
    claimToken: string,
  ): Promise<boolean> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      await client.query(
        `INSERT INTO trips (id, request_id, driver_id, rider_lat, rider_lng, status, claim_token)
         VALUES ($1, $2, $3, $4, $5, 'matched', $6)`,
        [tripId, requestId, driverId, rider.lat, rider.lng, claimToken],
      );
      await client.query(
        `UPDATE ride_requests SET status = 'matched', matched_trip_id = $2 WHERE id = $1`,
        [requestId, tripId],
      );
      await client.query('COMMIT');
      return true;
    } catch (err) {
      await client.query('ROLLBACK');
      if (err instanceof pg.DatabaseError && err.code === '23505') {
        if (err.constraint === 'trips_one_active_per_driver') {
          // Redis let a double-claim through — the index just saved the
          // invariant. Must never happen; if it does, it is counted, loud,
          // and the walk continues with the next candidate.
          this.metrics.pgUniqueViolationsTotal++;
          this.log.error({ tripId, requestId, driverId }, 'PARTIAL UNIQUE INDEX VIOLATION');
        } else {
          this.log.error({ err, tripId, requestId, driverId }, 'unexpected unique violation');
        }
        return false;
      }
      throw err;
    } finally {
      client.release();
    }
  }

  /**
   * Start the intake consumers: N loops, each BLMOVE-ing ids from
   * requests:queue into requests:processing on its own connection (blocking
   * commands monopolize a socket).
   */
  async start(consumers: number): Promise<void> {
    this.stopped = false;
    await this.recoverProcessing();
    for (let i = 0; i < consumers; i++) {
      const conn = this.redis.duplicate();
      this.conns.push(conn);
      this.loops.push(this.consumeLoop(conn, i));
    }
  }

  /** Stops within ~1s (the BLMOVE timeout). In-flight matches finish first. */
  async stop(): Promise<void> {
    this.stopped = true;
    await Promise.all(this.loops);
    for (const conn of this.conns) conn.disconnect();
    this.loops = [];
    this.conns = [];
  }

  /**
   * Startup reaper for stuck processing entries: drain requests:processing
   * back onto the queue. ponytail: one shared processing list drained on ANY
   * instance start — a starting instance re-queues a peer's in-flight ids,
   * which the row guard then turns into counted skips (at-least-once, safe,
   * occasionally wasteful). The upgrade path is asynq's lease pattern:
   * per-consumer processing lists with heartbeats swept by the phase-D
   * janitor.
   */
  async recoverProcessing(): Promise<number> {
    let recovered = 0;
    while ((await this.redis.lmove(REQUESTS_PROCESSING, REQUESTS_QUEUE, 'LEFT', 'RIGHT')) !== null) {
      recovered++;
    }
    if (recovered > 0) this.log.warn({ recovered }, 'requeued stuck processing entries');
    return recovered;
  }

  private async consumeLoop(conn: Redis, consumer: number): Promise<void> {
    while (!this.stopped) {
      let requestId: string | null;
      try {
        requestId = await conn.blmove(REQUESTS_QUEUE, REQUESTS_PROCESSING, 'RIGHT', 'LEFT', 1);
      } catch (err) {
        if (this.stopped) return;
        this.log.error({ err, consumer }, 'queue pop failed');
        await new Promise((resolve) => setTimeout(resolve, 500));
        continue;
      }
      if (requestId === null) continue; // timeout tick — re-check stopped

      try {
        await this.matchRequest(requestId);
        await this.redis.lrem(REQUESTS_PROCESSING, 1, requestId);
      } catch (err) {
        // Ack withheld: the id stays in processing for the startup reaper.
        this.metrics.matchErrorsTotal++;
        this.log.error({ err, requestId, consumer }, 'match failed; left in processing');
      }
    }
  }
}
