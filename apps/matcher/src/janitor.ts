import type { Redis } from 'ioredis';
import type pg from 'pg';
import { pino, type Logger } from 'pino';
import { ClaimStore, REQUESTS_QUEUE, driverKey } from '@loom/core';
import { createJanitorMetrics, type JanitorMetrics } from './metrics.js';
import { TripStore } from './trip-store.js';

export interface JanitorOptions {
  redis: Redis;
  pool: pg.Pool;
  log?: Logger;
  /** How often to sweep expired claims (default 1s). */
  sweepIntervalMs?: number;
  /** How often to reconcile drivers stuck 'on_trip' with no active trip (default 15s). */
  reconcileIntervalMs?: number;
  /** Max terminal-trip drivers checked per reconcile pass (default 1000). */
  reconcileLimit?: number;
  /** A trip with no progress for this long is abandoned and its driver retired to offline (default 30m). */
  tripMaxAgeMs?: number;
  /** Injectable clock (tests). */
  now?: () => number;
}

/**
 * THE JANITOR — the visibility timeout made real (asynq's
 * recoverer). A matcher that dies between claim and offer strands nobody,
 * because the claim's expiry lives in DATA: the value's expiresAt, indexed in
 * the claims:by-expiry ZSET written by the same Lua that created the claim.
 * Any surviving process — this loop inside every matcher, or a standalone
 * `npm run janitor --workspace=@loom/matcher` — can sweep it:
 *
 *   ZRANGEBYSCORE the due claims → for each, ONE Lua script re-reads the
 *   claim (it may have been confirmed/released since the scan) and, only if
 *   it is genuinely past expiresAt, deletes it and puts the driver back in
 *   its cell's available set → then the trip row is reverted
 *   OFFERED→MATCHING (guard + outbox in one TX) and the request re-enqueued
 *   onto requests:queue so the cascade runs again.
 *
 * The sweep is idempotent and multi-janitor-safe: the Lua re-check makes
 * concurrent sweepers race harmlessly (one releases, the rest see 'live' or
 * a missing key), and the PG revert is guarded by the trip machine. Clock
 * skew between claim writer and sweeper eats into the claim-vs-offer margin
 * (4s by default) — single-host deployments don't care; a fleet would add
 * asynq-style slack to the cutoff.
 */
export class Janitor {
  readonly metrics: JanitorMetrics = createJanitorMetrics();

  private readonly redis: Redis;
  private readonly claims: ClaimStore;
  private readonly trips: TripStore;
  private readonly log: Logger;
  private readonly sweepIntervalMs: number;
  private readonly reconcileIntervalMs: number;
  private readonly reconcileLimit: number;
  private readonly tripMaxAgeMs: number;
  private readonly now: () => number;

  private timer: NodeJS.Timeout | null = null;
  private reconcileTimer: NodeJS.Timeout | null = null;
  // Re-entry guards: a sweep/reconcile slower than its interval must not stack.
  private sweepRunning = false;
  private reconcileRunning = false;
  // The REAL in-flight promise (not just the latest scheduled one) so stop()
  // can await whatever is actually mid-flight before the pool/redis close.
  private sweepInFlight: Promise<void> = Promise.resolve();
  private reconcileInFlight: Promise<void> = Promise.resolve();

  constructor(opts: JanitorOptions) {
    this.redis = opts.redis;
    this.claims = new ClaimStore(opts.redis);
    this.trips = new TripStore(opts.pool);
    this.log = opts.log ?? pino({ level: 'silent' });
    this.sweepIntervalMs = opts.sweepIntervalMs ?? 1_000;
    this.reconcileIntervalMs = opts.reconcileIntervalMs ?? 15_000;
    this.reconcileLimit = opts.reconcileLimit ?? 1_000;
    this.tripMaxAgeMs = opts.tripMaxAgeMs ?? 1_800_000;
    this.now = opts.now ?? Date.now;
  }

  /** One pass over the due claims. Returns how many were released. */
  async sweepOnce(): Promise<number> {
    this.metrics.sweepsTotal++;
    const due = await this.claims.expiredClaims(this.now());
    let released = 0;
    for (const driverId of due) {
      try {
        const result = await this.claims.janitorRelease(driverId, this.now());
        if (result.kind === 'live') continue; // renewed view: not actually expired
        if (result.kind === 'gone') {
          // The PX net erased the claim key (every sweeper stayed dead past the
          // grace window). The Lua repaired the driver, but the erased value
          // took the tripId with it, so the orphaned trip row is still
          // 'offered' and its request stuck 'matching'. Revert the trip by
          // driver and re-enqueue the request (the O2 fix — this branch used to
          // leave both stranded forever).
          this.metrics.janitorGoneTotal++;
          const requestId = await this.trips.janitorRevertByDriver(driverId);
          if (requestId !== null) {
            await this.redis.lpush(REQUESTS_QUEUE, requestId);
            this.metrics.janitorRequeuedTotal++;
            this.log.warn(
              { driverId, requestId },
              'claim key gone; reverted orphaned offered trip, re-enqueued',
            );
          } else {
            this.log.warn({ driverId, repaired: result.repaired }, 'claim key gone before sweep');
          }
          continue;
        }
        this.metrics.janitorReleasedTotal++;
        released++;
        const requestId = await this.trips.janitorRevert(result.tripId);
        if (requestId !== null) {
          await this.redis.lpush(REQUESTS_QUEUE, requestId);
          this.metrics.janitorRequeuedTotal++;
          this.log.warn(
            { driverId, tripId: result.tripId, requestId },
            'released expired claim, re-enqueued',
          );
        } else {
          this.log.warn(
            { driverId, tripId: result.tripId },
            'released expired claim (trip already live/terminal)',
          );
        }
      } catch (err) {
        this.metrics.sweepErrorsTotal++;
        this.log.error({ err, driverId }, 'janitor release failed');
      }
    }
    return released;
  }

  /**
   * Backstop reconciler for the crash sliver where trip_done committed but the
   * matcher died before freeDriver ran — the driver is left 'on_trip' in Redis
   * with no active trip. Frees any such driver (freeDriver is a no-op unless it
   * really is stuck). Returns how many were freed.
   */
  async reconcileStuckOnTrip(): Promise<number> {
    this.metrics.reconcileSweepsTotal++;
    const candidates = await this.trips.driversWithTerminalTripOnly(this.reconcileLimit);
    let freed = 0;
    for (const driverId of candidates) {
      try {
        if ((await this.redis.hget(driverKey(driverId), 'status')) !== 'on_trip') continue;
        if (await this.claims.freeDriver(driverId)) {
          freed++;
          this.metrics.stuckDriversFreedTotal++;
          this.log.warn({ driverId }, 'freed driver stuck on_trip with no active trip');
        }
      } catch (err) {
        this.metrics.sweepErrorsTotal++;
        this.log.error({ err, driverId }, 'reconcile free failed');
      }
    }
    return freed;
  }

  /**
   * Abandonment reconciler — complements reconcileStuckOnTrip (which frees the
   * completed-but-not-freed sliver). A driver stuck 'on_trip' on a trip that
   * stopped progressing past tripMaxAgeMs is retired: driver on_trip → offline
   * (NEVER back to available, so no double-assignment) and the trip →
   * cancelled(abandoned). The claim silence-sweep deliberately skips on_trip
   * drivers, so without this an abandoned driver would hold their slot forever.
   * Redis is written FIRST so the driver is unclaimable before the trip is
   * touched; a crash between the two leaves the driver offline and the trip
   * still live, which the next pass re-selects and finishes (abandonDriver is
   * an idempotent no-op by then). Returns how many drivers were retired.
   */
  async abandonStale(): Promise<number> {
    const cutoff = new Date(this.now() - this.tripMaxAgeMs);
    const stale = await this.trips.staleActiveTrips(cutoff, this.reconcileLimit);
    let retired = 0;
    for (const { tripId, driverId } of stale) {
      try {
        await this.claims.abandonDriver(driverId);
        if (await this.trips.abandonTrip(tripId)) {
          retired++;
          this.metrics.tripsAbandonedTotal++;
          this.log.warn({ driverId, tripId }, 'abandoned stalled trip; retired driver to offline');
        }
      } catch (err) {
        this.metrics.sweepErrorsTotal++;
        this.log.error({ err, driverId, tripId }, 'abandon failed');
      }
    }
    return retired;
  }

  /** One reconcile tick: free completed-but-stuck drivers, then abandon stalled trips. */
  private async reconcilePass(): Promise<void> {
    await this.reconcileStuckOnTrip();
    await this.abandonStale();
  }

  start(): void {
    if (this.timer !== null) return;
    this.timer = setInterval(() => {
      if (this.sweepRunning) return; // previous sweep still running — skip this tick
      this.sweepRunning = true;
      this.sweepInFlight = this.sweepOnce()
        .then(
          () => undefined,
          (err: unknown) => {
            this.metrics.sweepErrorsTotal++;
            this.log.error({ err }, 'janitor sweep failed');
          },
        )
        .finally(() => {
          this.sweepRunning = false;
        });
    }, this.sweepIntervalMs);
    this.reconcileTimer = setInterval(() => {
      if (this.reconcileRunning) return;
      this.reconcileRunning = true;
      this.reconcileInFlight = this.reconcilePass()
        .then(
          () => undefined,
          (err: unknown) => {
            this.metrics.sweepErrorsTotal++;
            this.log.error({ err }, 'janitor reconcile failed');
          },
        )
        .finally(() => {
          this.reconcileRunning = false;
        });
    }, this.reconcileIntervalMs);
  }

  async stop(): Promise<void> {
    if (this.timer !== null) clearInterval(this.timer);
    if (this.reconcileTimer !== null) clearInterval(this.reconcileTimer);
    this.timer = null;
    this.reconcileTimer = null;
    await this.sweepInFlight;
    await this.reconcileInFlight;
  }
}
