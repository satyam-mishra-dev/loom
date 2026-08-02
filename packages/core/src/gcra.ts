import type { Redis } from 'ioredis';

/**
 * GCRA (Generic Cell Rate Algorithm) rate limiter over Redis + Lua.
 *
 * Ported from the arithmetic in go-redis/redis_rate (pattern notes §4), not
 * copied. GCRA stores ONE value per key — the TAT (Theoretical Arrival Time),
 * the wall time at which the NEXT conforming request is scheduled. Everything
 * else (remaining burst, retry-after, reset-after) is derived from TAT and the
 * clock, so the limiter carries no window state and self-garbage-collects: an
 * idle key's TTL equals its drain time, so it expires exactly when it would
 * have refilled.
 *
 * Two layers here:
 *   - `gcraDecide` — the PURE arithmetic (no I/O, no clock). It is the spec,
 *     hand-unit-tested against computed expectations. The in-process fallback
 *     limiter (limiter.ts) runs this same function against a local Map.
 *   - `GcraLimiter` — the atomic Redis path. The Lua mirrors `gcraDecide` line
 *     for line but does the GET/SET around it, because the read-compute-write
 *     MUST be one uninterruptible step: N callers race, Redis runs their
 *     scripts one at a time, and the schedule can never be double-spent. The
 *     integration test proves the two implementations agree and that the Lua
 *     is atomic under concurrency.
 *
 * Precision: all time is in SECONDS offset from a fixed epoch (2017-01-01).
 * Raw unix-second floats lose precision on sub-millisecond emission intervals;
 * the offset keeps the mantissa where it counts. Redis server TIME is the
 * clock inside the script — never the caller's — so N app instances with
 * skewed clocks cannot corrupt a shared TAT.
 */

/** 2017-01-01T00:00:00Z. Subtracted from unix seconds so sub-ms intervals keep float precision. */
export const GCRA_EPOCH_SEC = 1_483_228_800;

export interface GcraConfig {
  /** Sustained units permitted per `windowMs` (the long-run rate). */
  limit: number;
  /** Window length in ms — `limit` units are permitted per this window. */
  windowMs: number;
  /**
   * Burst capacity in units: how many may arrive instantly before pacing kicks
   * in. Default = `limit` (the PerSecond(n) convention: n instantly, then
   * paced). `burst = 1` means one request, then strictly paced. Burst is
   * CAPACITY, not extra rate.
   */
  burst?: number;
}

export interface RateResult {
  allowed: boolean;
  /** Burst units still available (0 when denied). */
  remaining: number;
  /** ms to wait before a retry conforms; 0 when allowed (not throttled). */
  retryAfterMs: number;
  /** ms until the bucket fully drains — also the key's TTL. */
  resetAfterMs: number;
}

export interface GcraDecision {
  allowed: boolean;
  /** The TAT to persist — ONLY written when `allowed` (writing on deny starves hammering clients). */
  newTatSec: number;
  remaining: number;
  /** Seconds until a retry conforms; -1 when allowed (redis_rate's "not throttled" sentinel). */
  retryAfterSec: number;
  /** Seconds until the bucket fully drains. */
  resetAfterSec: number;
}

interface GcraParams {
  emissionIntervalSec: number;
  burstOffsetSec: number;
}

function paramsOf(cfg: GcraConfig): GcraParams {
  const periodSec = cfg.windowMs / 1000;
  const emissionIntervalSec = periodSec / cfg.limit; // time-cost of one unit
  const burst = cfg.burst ?? cfg.limit;
  return { emissionIntervalSec, burstOffsetSec: emissionIntervalSec * burst };
}

/**
 * The pure GCRA step. `prevTatSec` is the stored TAT (null if the key is
 * absent); `nowSec` and the result are epoch-offset seconds. The Lua below is
 * a faithful transcription — keep them in lockstep (the integration test
 * enforces it).
 */
export function gcraDecide(
  prevTatSec: number | null,
  nowSec: number,
  cfg: GcraConfig,
  cost = 1,
): GcraDecision {
  const { emissionIntervalSec, burstOffsetSec } = paramsOf(cfg);
  const tat = Math.max(prevTatSec ?? nowSec, nowSec); // never schedule in the past
  const increment = emissionIntervalSec * cost;
  const newTat = tat + increment;
  const allowAt = newTat - burstOffsetSec; // earliest wall time this request conforms
  const diff = nowSec - allowAt;
  if (diff < 0) {
    // DENY — change nothing. retry_after is how long until conformance; reset
    // is how long until the bucket is empty again.
    return {
      allowed: false,
      newTatSec: tat,
      remaining: 0,
      retryAfterSec: -diff,
      resetAfterSec: tat - nowSec,
    };
  }
  return {
    allowed: true,
    newTatSec: newTat,
    remaining: Math.floor(diff / emissionIntervalSec),
    retryAfterSec: -1,
    resetAfterSec: newTat - nowSec,
  };
}

// KEYS[1] the rate-limit key
// ARGV[1] burst  ARGV[2] rate(limit)  ARGV[3] periodSec  ARGV[4] cost
// Returns {allowed(1|0), remaining, retryAfterSec(string), resetAfterSec(string)}.
// Durations are stringified so their float precision survives RESP integer
// coercion. TAT is written ONLY on allow, with EX = ceil(resetAfter).
const GCRA_LUA = `
local key = KEYS[1]
local burst = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local period = tonumber(ARGV[3])
local cost = tonumber(ARGV[4])

local emission_interval = period / rate
local increment = emission_interval * cost
local burst_offset = emission_interval * burst

local t = redis.call('TIME')
local now = (tonumber(t[1]) - 1483228800) + (tonumber(t[2]) / 1000000)

local tat = redis.call('GET', key)
if not tat then tat = now else tat = tonumber(tat) end
if tat < now then tat = now end

local new_tat = tat + increment
local allow_at = new_tat - burst_offset
local diff = now - allow_at

if diff < 0 then
  return {0, 0, tostring(-diff), tostring(tat - now)}
end

local remaining = math.floor(diff / emission_interval)
local reset_after = new_tat - now
if reset_after > 0 then
  redis.call('SET', key, new_tat, 'EX', math.ceil(reset_after))
end
return {1, remaining, '-1', tostring(reset_after)}
`;

interface GcraCommands {
  flGcra(
    key: string,
    burst: string,
    rate: string,
    periodSec: string,
    cost: string,
  ): Promise<[number, number, string, string]>;
}

/**
 * Atomic distributed GCRA over Redis. One `limit()` = one Lua round trip =
 * one atomic read-compute-write. Fails loudly (rejects the promise) if Redis
 * is unreachable — the degradation strategy (fail-open with a local fallback,
 * or fail-closed) lives one layer up in DegradingLimiter, so this class stays
 * a single-responsibility primitive.
 */
export class GcraLimiter {
  private readonly redis: Redis & GcraCommands;

  constructor(redis: Redis) {
    redis.defineCommand('flGcra', { numberOfKeys: 1, lua: GCRA_LUA });
    this.redis = redis as Redis & GcraCommands;
  }

  async limit(key: string, cfg: GcraConfig, cost = 1): Promise<RateResult> {
    const periodSec = cfg.windowMs / 1000;
    const burst = cfg.burst ?? cfg.limit;
    const [allowed, remaining, retryAfterSec, resetAfterSec] = await this.redis.flGcra(
      key,
      String(burst),
      String(cfg.limit),
      String(periodSec),
      String(cost),
    );
    return {
      allowed: allowed === 1,
      remaining,
      retryAfterMs: retryAfterSec === '-1' ? 0 : Number(retryAfterSec) * 1000,
      resetAfterMs: Number(resetAfterSec) * 1000,
    };
  }
}
