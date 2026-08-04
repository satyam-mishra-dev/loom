import { randomUUID } from 'node:crypto';
import type { Redis } from 'ioredis';
import type pg from 'pg';
import { pino, type Logger } from 'pino';
import {
  ClaimStore,
  DEFAULT_CLAIM_TTL_MS,
  DEFAULT_OFFER_TTL_MS,
  DEFAULT_STALE_MS,
  GeoIndex,
  REQUESTS_PROCESSING,
  REQUESTS_QUEUE,
  TRIP_EVENTS_PROCESSING,
  TRIP_EVENTS_QUEUE,
  driverChannel,
  offerReplyKey,
  rankCandidates,
  type OfferMessage,
  type TripAssigned,
} from '@loom/core';
import { createMetrics, type MatcherMetrics } from './metrics.js';
import { TripStore } from './trip-store.js';

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
  /** Claim lease duration (default 12s — MUST exceed offerTtlMs, see claim.ts). */
  claimTtlMs?: number;
  /** Offer TTL: how long a driver gets to answer (default 8s). */
  offerTtlMs?: number;
  /** Cascade cap: offers per request before an honest unmatched (default 5). */
  maxOffers?: number;
  /** Injectable clock (tests). */
  now?: () => number;
}

export type MatchOutcome = 'matched' | 'unmatched' | 'skipped';

/** Requests stuck 'pending'/'matching' longer than this are re-enqueued at startup. */
const REQUEUE_PENDING_MS = 30_000;

/** Backoff before re-queuing a premature trip-progress event, so its predecessor can land first. */
const PREMATURE_RETRY_DELAY_MS = 50;

/**
 * The matching pipeline: request → candidates → score → ATOMIC CLAIM →
 * OFFER (pub/sub out, reply list back) → accept: trip matched → en_route;
 * decline/timeout: release, next candidate — max 5 offers, then an honest
 * unmatched. One instance = one consumer group; the signature test runs two
 * instances against the same stores to prove cross-instance safety.
 *
 * Write order per accepted offer is trip commit → confirmClaim, so that
 * EVERY Postgres failure — including the never-happens partial-unique-index
 * rejection — unwinds through the same atomic releaseClaim while the claim
 * still exists. On decline/timeout the order is trip revert (PG) →
 * releaseClaim (Redis): a crash between the two leaves a live claim that the
 * janitor can still see, release, and re-enqueue from — the other order
 * would leave an orphaned OFFERED row no sweep could find.
 */
export class MatcherCore {
  readonly metrics: MatcherMetrics = createMetrics();
  readonly geoIndex: GeoIndex;
  readonly claims: ClaimStore;
  readonly trips: TripStore;

  private readonly redis: Redis;
  private readonly pool: pg.Pool;
  private readonly log: Logger;
  private readonly need: number;
  private readonly maxK: number;
  private readonly freshMs: number;
  private readonly claimTtlMs: number;
  private readonly offerTtlMs: number;
  private readonly maxOffers: number;
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
    this.offerTtlMs = opts.offerTtlMs ?? DEFAULT_OFFER_TTL_MS;
    this.maxOffers = opts.maxOffers ?? 5;
    this.now = opts.now ?? Date.now;
    if (this.claimTtlMs <= this.offerTtlMs) {
      throw new Error(
        `claimTtlMs (${this.claimTtlMs}) must exceed offerTtlMs (${this.offerTtlMs}) — see claim.ts`,
      );
    }
    this.geoIndex = new GeoIndex(this.redis);
    this.claims = new ClaimStore(this.redis);
    this.trips = new TripStore(this.pool);
  }

  /**
   * Match one request end to end (the full cascade). Safe under
   * at-least-once delivery: the pending→matching UPDATE is the idempotency
   * guard — whoever wins that row owns the request, every other delivery is
   * a counted no-op. A janitor-re-enqueued request re-enters here with its
   * trip row parked at 'matching' and resumes cascading on the same trip.
   */
  async matchRequest(requestId: string): Promise<MatchOutcome> {
    const started = performance.now();
    // Claim the request (pending→matching guard) AND resolve any pre-crash
    // trip row for resume in ONE round trip: the correlated subquery reads the
    // (unmodified) trips table in the same statement, saving a separate SELECT
    // on every request. NULL existing_trip_id ⇒ fresh cascade.
    const claimed = await this.pool.query<{
      lat: number;
      lng: number;
      dest_lat: number | null;
      dest_lng: number | null;
      existing_trip_id: string | null;
    }>(
      `UPDATE ride_requests SET status = 'matching' WHERE id = $1 AND status = 'pending'
       RETURNING lat, lng, dest_lat, dest_lng,
         (SELECT id FROM trips WHERE request_id = $1) AS existing_trip_id`,
      [requestId],
    );
    const row = claimed.rows[0];
    if (row === undefined) {
      this.metrics.requestsSkippedTotal++;
      this.log.warn({ requestId }, 'request not pending — duplicate delivery or lost row, skipping');
      return 'skipped';
    }
    const rider = { lat: row.lat, lng: row.lng };
    const dest = { lat: row.dest_lat ?? row.lat, lng: row.dest_lng ?? row.lng };
    const tripId = row.existing_trip_id ?? randomUUID();

    const { candidates } = await this.geoIndex.findCandidates(rider.lat, rider.lng, {
      need: this.need,
      maxK: this.maxK,
      nowMs: this.now(),
      staleMs: this.freshMs,
    });

    // One connection for this call's offer replies: BLPOP blocks its socket,
    // so the shared client can never be the one waiting.
    let replyConn: Redis | null = null;
    let offers = 0;
    try {
      for (const candidate of rankCandidates(rider.lat, rider.lng, candidates)) {
        if (offers >= this.maxOffers) break;
        const driverId = candidate.driverId;
        const token = await this.claims.claimDriver(driverId, tripId, this.now(), this.freshMs, this.claimTtlMs);
        if (token === null) {
          this.metrics.claimConflictsTotal++;
          continue;
        }

        const offerId = randomUUID();
        let offered: boolean;
        try {
          offered = await this.trips.offerTrip({ tripId, requestId, driverId, offerId, claimToken: token, rider });
        } catch (err) {
          await this.claims.releaseClaim(driverId, token);
          throw err;
        }
        if (!offered) {
          // The trip row is not ours to offer anymore — a janitor or peer
          // took the request over. Back away entirely.
          await this.claims.releaseClaim(driverId, token);
          this.metrics.cascadeLostTotal++;
          this.log.warn({ requestId, tripId, driverId }, 'lost trip ownership mid-cascade');
          return 'skipped';
        }

        offers++;
        this.metrics.offersSentTotal++;
        const offer: OfferMessage = {
          type: 'offer',
          offerId,
          tripId,
          driverId,
          pickup: rider,
          expiresAt: this.now() + this.offerTtlMs,
        };
        await this.redis.publish(driverChannel(driverId), JSON.stringify(offer));

        replyConn ??= this.redis.duplicate();
        const verdict = await this.awaitReply(replyConn, offerId);

        if (verdict === true) {
          const accepted = await this.trips.acceptOffer({ tripId, offerId, requestId });
          if (accepted === 'conflict') {
            // Redis let a double-claim through — the partial unique index just
            // saved the invariant. The acceptOffer TX rolled back, so the trip
            // is still 'offered' and ours: revert it (offered → matching,
            // mirroring the decline path) BEFORE releasing the claim so the
            // cascade can try the next candidate. Without this the trip stays
            // 'offered' forever — the next candidate's offerTrip is an illegal
            // offered→OFFER_SENT and the request strands. If the revert lost the
            // row (a janitor/peer took it), fall through to finishUnmatched.
            this.metrics.pgUniqueViolationsTotal++;
            this.log.error({ tripId, requestId, driverId }, 'PARTIAL UNIQUE INDEX VIOLATION');
            const reverted = await this.trips.revertOffer(tripId, offerId, 'OFFER_DECLINED');
            await this.claims.releaseClaim(driverId, token);
            if (reverted) continue;
            break;
          }
          if (accepted === 'lost') {
            await this.claims.releaseClaim(driverId, token);
            this.metrics.cascadeLostTotal++;
            return 'skipped';
          }
          this.metrics.offerAcceptsTotal++;
          if (!(await this.claims.confirmClaim(driverId, token, this.now()))) {
            // Claim expired between offer and accept (should be unreachable:
            // claim TTL > offer TTL + margin). The trip stands; PG's index
            // keeps the driver single-booked even if the janitor frees it.
            this.metrics.confirmFailuresTotal++;
            this.log.error({ requestId, tripId, driverId }, 'confirm failed after accept');
          }
          // en_route commits BEFORE trip_assigned goes out: a fast driver's
          // arrived_pickup must never race the matched→en_route write.
          if (!(await this.trips.startEnRoute(tripId))) {
            this.log.error({ tripId }, 'matched trip refused en_route transition');
          }
          const assigned: TripAssigned = { type: 'trip_assigned', tripId, driverId, pickup: rider, dest };
          await this.redis.publish(driverChannel(driverId), JSON.stringify(assigned));
          this.metrics.matchesTotal++;
          this.metrics.matchLatencyMs.observe(performance.now() - started);
          this.log.info({ requestId, tripId, driverId, offers }, 'matched');
          return 'matched';
        }

        // Decline or timeout. PG revert FIRST, then the claim release (see
        // class header for the crash-ordering argument).
        if (verdict === null) this.metrics.offerTimeoutsTotal++;
        else this.metrics.offerDeclinesTotal++;
        const reverted = await this.trips.revertOffer(
          tripId,
          offerId,
          verdict === null ? 'OFFER_TIMED_OUT' : 'OFFER_DECLINED',
        );
        await this.claims.releaseClaim(driverId, token);
        if (!reverted) {
          this.metrics.cascadeLostTotal++;
          return 'skipped';
        }
      }
    } finally {
      replyConn?.disconnect();
    }

    // Honest failure: no claimable candidate, or the cascade cap. The trip
    // row (if any offers happened) parks at cancelled in the same TX.
    if (!(await this.trips.finishUnmatched(tripId, requestId))) {
      this.metrics.cascadeLostTotal++;
      return 'skipped';
    }
    this.metrics.unmatchedTotal++;
    // matchLatencyMs is deliberately observed on the MATCHED path only: an
    // unmatched request carries the full cascade timeout (up to ~40s), which
    // would smear the dashboard's live match p50/p99. The bench computes its
    // own matched-only DB percentiles and is unaffected either way.
    this.log.info({ requestId, offers, candidates: candidates.length }, 'unmatched');
    return 'unmatched';
  }

  /** BLPOP the offer's reply list: true = accept, false = decline, null = timeout. */
  private async awaitReply(conn: Redis, offerId: string): Promise<boolean | null> {
    const res = await conn.blpop(offerReplyKey(offerId), this.offerTtlMs / 1000);
    if (res === null) return null;
    try {
      const parsed = JSON.parse(res[1]) as { accept?: unknown };
      return parsed.accept === true;
    } catch {
      return false;
    }
  }

  /**
   * Start the consumers: N request loops + 1 trip-progress loop, each
   * BLMOVE-ing on its own connection (blocking commands monopolize a socket).
   */
  async start(consumers: number): Promise<void> {
    this.stopped = false;
    await this.recoverProcessing();
    for (let i = 0; i < consumers; i++) {
      const conn = this.redis.duplicate();
      this.conns.push(conn);
      this.loops.push(this.consumeLoop(conn, i));
    }
    const tripConn = this.redis.duplicate();
    this.conns.push(tripConn);
    this.loops.push(this.tripEventLoop(tripConn));
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
   * Startup reaper: drain both processing lists back onto their queues, and
   * re-enqueue requests stuck 'pending' past a grace (covers a crash between
   * a row INSERT/janitor revert and its LPUSH — a duplicate LPUSH is safe,
   * the row guard collapses it). ponytail: shared processing lists drained on
   * ANY instance start — a starting instance re-queues a peer's in-flight
   * ids, which the guards turn into counted skips (at-least-once, safe,
   * occasionally wasteful). The upgrade path is asynq's lease pattern:
   * per-consumer processing lists with heartbeats swept by the janitor.
   */
  async recoverProcessing(): Promise<number> {
    let recovered = 0;
    while ((await this.redis.lmove(REQUESTS_PROCESSING, REQUESTS_QUEUE, 'LEFT', 'RIGHT')) !== null) {
      recovered++;
    }
    while ((await this.redis.lmove(TRIP_EVENTS_PROCESSING, TRIP_EVENTS_QUEUE, 'LEFT', 'RIGHT')) !== null) {
      recovered++;
    }
    // Requests stranded past the grace window. 'pending' rows lost their LPUSH;
    // 'matching' rows were abandoned mid-cascade — a dead matcher, or a janitor
    // 'gone' repair that freed the driver but left the trip 'offered'. This one
    // broadening backstops BOTH the C1 conflict residual and the O2 past-grace
    // stranding: for a 'matching' row with a trip, janitorRevert the trip
    // (offered/matching → matching, request → pending) first, then re-enqueue.
    const stale = await this.pool.query<{ id: string; status: string }>(
      `SELECT id, status FROM ride_requests
       WHERE status IN ('pending', 'matching')
         AND created_at < now() - ($1::int * interval '1 millisecond')`,
      [REQUEUE_PENDING_MS],
    );
    for (const { id, status } of stale.rows) {
      if (status === 'matching') {
        const trip = await this.pool.query<{ id: string }>('SELECT id FROM trips WHERE request_id = $1', [id]);
        const tripId = trip.rows[0]?.id;
        if (tripId !== undefined) {
          // Revert the trip and hand the request back to pending. Null means the
          // trip went live (matched/en_route/…) and belongs to a live cascade —
          // leave it entirely.
          if ((await this.trips.janitorRevert(tripId)) === null) continue;
        } else {
          // 'matching' with no trip row: a matcher died right after the
          // pending→matching UPDATE. Nobody owns it — hand it back.
          await this.pool.query(
            `UPDATE ride_requests SET status = 'pending' WHERE id = $1 AND status = 'matching'`,
            [id],
          );
        }
      }
      await this.redis.lpush(REQUESTS_QUEUE, id);
      recovered++;
    }
    if (recovered > 0) this.log.warn({ recovered }, 'requeued stuck entries');
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

  /** Consume driver trip_progress: en_route → in_trip → completed, then free the driver. */
  private async tripEventLoop(conn: Redis): Promise<void> {
    while (!this.stopped) {
      let raw: string | null;
      try {
        raw = await conn.blmove(TRIP_EVENTS_QUEUE, TRIP_EVENTS_PROCESSING, 'RIGHT', 'LEFT', 1);
      } catch (err) {
        if (this.stopped) return;
        this.log.error({ err }, 'trip event pop failed');
        await new Promise((resolve) => setTimeout(resolve, 500));
        continue;
      }
      if (raw === null) continue;

      try {
        await this.handleTripProgress(raw);
        await this.redis.lrem(TRIP_EVENTS_PROCESSING, 1, raw);
      } catch (err) {
        this.metrics.matchErrorsTotal++;
        this.log.error({ err, raw }, 'trip event failed; left in processing');
      }
    }
  }

  private async handleTripProgress(raw: string): Promise<void> {
    let msg: { tripId?: unknown; driverId?: unknown; event?: unknown };
    try {
      msg = JSON.parse(raw) as typeof msg;
    } catch {
      this.metrics.tripEventGuardFailuresTotal++;
      return;
    }
    const { tripId, driverId, event } = msg;
    if (
      typeof tripId !== 'string' ||
      typeof driverId !== 'string' ||
      (event !== 'arrived_pickup' && event !== 'trip_done')
    ) {
      this.metrics.tripEventGuardFailuresTotal++;
      return;
    }
    const outcome = await this.trips.progress(tripId, driverId, event);
    if (outcome === 'premature') {
      // A predecessor event has not landed yet — happens when two matcher
      // instances split the trip-progress consumer and consume trip_done before
      // arrived_pickup. DO NOT drop it (that would strand the trip 'in_trip' and
      // the driver 'on_trip' forever): re-queue after a short backoff so the
      // predecessor commits first, then retry. This delivery is then acked by
      // the loop, leaving exactly the one re-queued copy.
      this.metrics.tripEventPrematureTotal++;
      await new Promise((resolve) => setTimeout(resolve, PREMATURE_RETRY_DELAY_MS));
      await this.redis.lpush(TRIP_EVENTS_QUEUE, raw);
      return;
    }
    if (outcome === 'terminal') {
      this.metrics.tripEventGuardFailuresTotal++;
      this.log.warn({ tripId, driverId, event }, 'trip progress dropped (duplicate/late/spoof/terminal)');
      return;
    }
    if (event === 'trip_done') {
      // Trip row is final; hand the driver back to the index. A crash between
      // the commit above and this call leaves the driver 'on_trip' with no
      // active trip — the janitor's reconcileStuckOnTrip backstop frees it.
      await this.claims.freeDriver(driverId);
      this.metrics.tripsCompletedTotal++;
      this.log.info({ tripId, driverId }, 'trip completed, driver freed');
    }
  }
}
