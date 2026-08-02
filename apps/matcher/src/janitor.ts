import type { Redis } from 'ioredis';
import type pg from 'pg';
import { pino, type Logger } from 'pino';
import { ClaimStore, REQUESTS_QUEUE } from '@fleetline/core';
import { createJanitorMetrics, type JanitorMetrics } from './metrics.js';
import { TripStore } from './trip-store.js';

export interface JanitorOptions {
  redis: Redis;
  pool: pg.Pool;
  log?: Logger;
  /** How often to sweep (default 1s). */
  sweepIntervalMs?: number;
  /** Injectable clock (tests). */
  now?: () => number;
}

/**
 * THE JANITOR — the visibility timeout made real (notes §3 / asynq's
 * recoverer). A matcher that dies between claim and offer strands nobody,
 * because the claim's expiry lives in DATA: the value's expiresAt, indexed in
 * the claims:by-expiry ZSET written by the same Lua that created the claim.
 * Any surviving process — this loop inside every matcher, or a standalone
 * `npm run janitor --workspace=@fleetline/matcher` — can sweep it:
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
  private readonly now: () => number;

  private timer: NodeJS.Timeout | null = null;
  private sweeping: Promise<void> = Promise.resolve();

  constructor(opts: JanitorOptions) {
    this.redis = opts.redis;
    this.claims = new ClaimStore(opts.redis);
    this.trips = new TripStore(opts.pool);
    this.log = opts.log ?? pino({ level: 'silent' });
    this.sweepIntervalMs = opts.sweepIntervalMs ?? 1_000;
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
          // PX net erased the key (janitor was down past the grace window).
          // The driver is repaired; the trip row's id went with the value, so
          // its request revives via the matcher's stale-pending reaper.
          this.metrics.janitorGoneTotal++;
          this.log.warn({ driverId, repaired: result.repaired }, 'claim key gone before sweep');
          continue;
        }
        this.metrics.janitorReleasedTotal++;
        released++;
        const requestId = await this.trips.janitorRevert(result.tripId);
        if (requestId !== null) {
          await this.redis.lpush(REQUESTS_QUEUE, requestId);
          this.metrics.janitorRequeuedTotal++;
          this.log.warn({ driverId, tripId: result.tripId, requestId }, 'released expired claim, re-enqueued');
        } else {
          this.log.warn({ driverId, tripId: result.tripId }, 'released expired claim (trip already live/terminal)');
        }
      } catch (err) {
        this.metrics.sweepErrorsTotal++;
        this.log.error({ err, driverId }, 'janitor release failed');
      }
    }
    return released;
  }

  start(): void {
    if (this.timer !== null) return;
    this.timer = setInterval(() => {
      this.sweeping = this.sweepOnce().then(
        () => undefined,
        (err: unknown) => {
          this.metrics.sweepErrorsTotal++;
          this.log.error({ err }, 'janitor sweep failed');
        },
      );
    }, this.sweepIntervalMs);
  }

  async stop(): Promise<void> {
    if (this.timer !== null) clearInterval(this.timer);
    this.timer = null;
    await this.sweeping;
  }
}
