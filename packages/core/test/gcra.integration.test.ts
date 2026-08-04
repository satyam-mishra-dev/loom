import { RedisContainer, type StartedRedisContainer } from '@testcontainers/redis';
import { Redis } from 'ioredis';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { GcraLimiter } from '../src/index.js';

/**
 * The GCRA Lua path against real Redis. The headline is the ATOMICITY PROOF:
 * N concurrent callers, a tight limit, exactly the allowed count pass — the
 * same single-Lua-script serialization that makes the driver claim safe, here
 * making the rate limiter safe. Deliberately shaped like the signature test:
 * 20 of 200.
 */
describe('GcraLimiter (testcontainers redis)', () => {
  let container: StartedRedisContainer;
  let redis: Redis;
  let limiter: GcraLimiter;

  beforeAll(async () => {
    container = await new RedisContainer('redis:7-alpine').start();
    redis = new Redis(container.getConnectionUrl());
    limiter = new GcraLimiter(redis);
  }, 120_000);

  afterAll(async () => {
    redis.disconnect();
    await container.stop();
  });

  beforeEach(async () => {
    await redis.flushall();
  });

  it('ATOMICITY: 200 concurrent calls, burst 20 → exactly 20 pass, 180 denied', async () => {
    // emission = 60s/20 = 3s per unit, so redis TIME advancing a few ms across
    // the 200 calls admits zero extras — the count is the burst, exactly.
    const cfg = { limit: 20, windowMs: 60_000, burst: 20 };
    const results = await Promise.all(
      Array.from({ length: 200 }, () => limiter.limit('rider-hot', cfg)),
    );
    const allowed = results.filter((r) => r.allowed);
    expect(allowed).toHaveLength(20);
    expect(results.filter((r) => !r.allowed)).toHaveLength(180);
    // Every denied caller is told a real, positive wait.
    for (const r of results) {
      if (!r.allowed) expect(r.retryAfterMs).toBeGreaterThan(0);
    }
  });

  it('sequential: burst drains, remaining counts down, then denies', async () => {
    const cfg = { limit: 5, windowMs: 1000, burst: 5 };
    const first = await limiter.limit('rider-1', cfg);
    expect(first.allowed).toBe(true);
    expect(first.remaining).toBe(4);

    let admitted = 1;
    for (let i = 0; i < 10; i++) {
      if ((await limiter.limit('rider-1', cfg)).allowed) admitted++;
    }
    // 5-unit burst, then paced at 5/sec — a handful of ms admits ~the burst.
    expect(admitted).toBeGreaterThanOrEqual(5);
    expect(admitted).toBeLessThanOrEqual(6);
  });

  it('never writes TAT on deny: a hammering client cannot push its own schedule back', async () => {
    const cfg = { limit: 1, windowMs: 10_000, burst: 1 }; // one every 10s
    expect((await limiter.limit('rider-1', cfg)).allowed).toBe(true);
    const denied1 = await limiter.limit('rider-1', cfg);
    const denied2 = await limiter.limit('rider-1', cfg);
    expect(denied1.allowed).toBe(false);
    expect(denied2.allowed).toBe(false);
    // If deny wrote TAT, retry_after would keep GROWING; it must not.
    expect(denied2.retryAfterMs).toBeLessThanOrEqual(denied1.retryAfterMs + 50);
  });

  it('self-GC: an allowed key gets a TTL equal to its drain time', async () => {
    const cfg = { limit: 10, windowMs: 1000, burst: 10 };
    await limiter.limit('rider-ttl', cfg);
    const pttl = await redis.pttl('rider-ttl');
    // reset_after for one unit of a 10/sec bucket ≈ 0.1s → EX ceil = 1s.
    expect(pttl).toBeGreaterThan(0);
    expect(pttl).toBeLessThanOrEqual(1000);
  });

  it('isolates keys: one rider hitting the limit does not throttle another', async () => {
    const cfg = { limit: 1, windowMs: 10_000, burst: 1 };
    expect((await limiter.limit('rider-a', cfg)).allowed).toBe(true);
    expect((await limiter.limit('rider-a', cfg)).allowed).toBe(false);
    expect((await limiter.limit('rider-b', cfg)).allowed).toBe(true); // untouched
  });
});
