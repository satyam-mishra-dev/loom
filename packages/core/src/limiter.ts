import type { Redis } from 'ioredis';
import { GCRA_EPOCH_SEC, GcraLimiter, gcraDecide, type GcraConfig, type RateResult } from './gcra.js';

/**
 * Degradation strategy for the rate limiter — the limiter's own store is a
 * dependency that fails, and this class decides what happens when it does
 * (pattern notes §7, rate-limiter-flexible's "insurance" concept).
 *
 * DEFAULT: fail-OPEN with a local approximation. A public limiter that guards
 * capacity exists to protect the service; rejecting 100% of traffic because
 * the LIMITER's Redis died inverts the priority — the service was fine. So on
 * a Redis error OR a slow Redis (a short timeout routes "slow" to the fallback
 * too, or p99 inherits Redis's sickness), we fall back to an in-process GCRA
 * over a bounded Map. It limits approximately and per-instance: N instances
 * each admit up to the configured limit, so a fleet should set the fallback
 * limit to roughly global_limit / instance_count. On recovery, the local
 * counts are simply discarded — a brief over-admission window is accepted by
 * design; syncing them back is complexity with no payoff.
 *
 * FAIL-CLOSED (opt in via `failClosed`) is right only when the limit IS the
 * product — billing quotas, hard security throttles — where admitting
 * un-metered traffic is worse than rejecting it. Fleetline's request intake is
 * capacity protection, so it ships fail-open.
 */

export interface DegradingLimiterOptions {
  /** The limit applied on BOTH the Redis path and the local fallback. */
  config: GcraConfig;
  /** Reject instead of falling back when Redis is unavailable (default false = fail-open). */
  failClosed?: boolean;
  /** Redis call is abandoned to the fallback after this long (default 50ms). "Slow" == "down". */
  redisTimeoutMs?: number;
  /**
   * Max distinct keys the in-process fallback tracks. Bounds memory during an
   * outage; GCRA keys self-expire, but a cap guarantees the Map can't grow
   * without bound under a key-space attack (default 100_000).
   */
  fallbackMaxKeys?: number;
  /** Injectable clock for the fallback (tests). Unix ms. */
  now?: () => number;
}

export interface LimiterMetrics {
  /** Decisions served by the Redis path. */
  primaryTotal: number;
  /** Decisions served by the in-process fallback (Redis errored or was slow). */
  fallbackTotal: number;
  /** Requests rejected because the limit was hit (either path). */
  deniedTotal: number;
  /** Requests rejected because Redis was down AND failClosed is set. */
  failClosedRejectsTotal: number;
  /** Redis calls that exceeded redisTimeoutMs. */
  redisTimeoutsTotal: number;
  /** Redis calls that threw. */
  redisErrorsTotal: number;
}

export function createLimiterMetrics(): LimiterMetrics {
  return {
    primaryTotal: 0,
    fallbackTotal: 0,
    deniedTotal: 0,
    failClosedRejectsTotal: 0,
    redisTimeoutsTotal: 0,
    redisErrorsTotal: 0,
  };
}

const TIMEOUT = Symbol('gcra-redis-timeout');

export class DegradingLimiter {
  readonly metrics: LimiterMetrics = createLimiterMetrics();

  private readonly primary: GcraLimiter;
  private readonly config: GcraConfig;
  private readonly failClosed: boolean;
  private readonly redisTimeoutMs: number;
  private readonly fallbackMaxKeys: number;
  private readonly now: () => number;
  /** Local fallback state: key → stored TAT (epoch-offset seconds). Insertion-ordered for eviction. */
  private readonly local = new Map<string, number>();

  constructor(redis: Redis, opts: DegradingLimiterOptions) {
    this.primary = new GcraLimiter(redis);
    this.config = opts.config;
    this.failClosed = opts.failClosed ?? false;
    this.redisTimeoutMs = opts.redisTimeoutMs ?? 50;
    this.fallbackMaxKeys = opts.fallbackMaxKeys ?? 100_000;
    this.now = opts.now ?? Date.now;
  }

  /** Returns the decision. Never throws — a dead Redis becomes a fallback (or a fail-closed deny). */
  async limit(key: string, cost = 1): Promise<RateResult> {
    let result: RateResult;
    try {
      result = await this.withTimeout(this.primary.limit(key, this.config, cost));
      this.metrics.primaryTotal++;
    } catch (err) {
      if (err === TIMEOUT) this.metrics.redisTimeoutsTotal++;
      else this.metrics.redisErrorsTotal++;
      if (this.failClosed) {
        this.metrics.failClosedRejectsTotal++;
        this.metrics.deniedTotal++;
        return { allowed: false, remaining: 0, retryAfterMs: this.config.windowMs, resetAfterMs: this.config.windowMs };
      }
      result = this.localLimit(key, cost);
      this.metrics.fallbackTotal++;
    }
    if (!result.allowed) this.metrics.deniedTotal++;
    return result;
  }

  private async withTimeout(p: Promise<RateResult>): Promise<RateResult> {
    let timer: NodeJS.Timeout;
    const timeout = new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(TIMEOUT), this.redisTimeoutMs);
    });
    // A late-resolving primary must not surface as an unhandled rejection.
    p.catch(() => undefined);
    try {
      return await Promise.race([p, timeout]);
    } finally {
      clearTimeout(timer!);
    }
  }

  /** In-process GCRA over the bounded Map — the same `gcraDecide` the Lua mirrors. */
  private localLimit(key: string, cost: number): RateResult {
    const nowSec = this.now() / 1000 - GCRA_EPOCH_SEC;
    const decision = gcraDecide(this.local.get(key) ?? null, nowSec, this.config, cost);
    if (decision.allowed) {
      if (!this.local.has(key) && this.local.size >= this.fallbackMaxKeys) {
        // ponytail: FIFO eviction (oldest inserted), not true LRU — bounds
        // memory during an outage; upgrade to LRU if hot keys get evicted.
        const oldest = this.local.keys().next().value;
        if (oldest !== undefined) this.local.delete(oldest);
      }
      this.local.set(key, decision.newTatSec);
    }
    return {
      allowed: decision.allowed,
      remaining: decision.remaining,
      retryAfterMs: decision.retryAfterSec < 0 ? 0 : decision.retryAfterSec * 1000,
      resetAfterMs: decision.resetAfterSec * 1000,
    };
  }
}
