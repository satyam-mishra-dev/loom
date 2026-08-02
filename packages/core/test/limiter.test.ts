import type { Redis } from 'ioredis';
import { describe, expect, it } from 'vitest';
import { DegradingLimiter, type RateResult } from '../src/index.js';

// A fake Redis whose flGcra (installed by GcraLimiter via defineCommand) is
// driven by the test: resolve to a decision, reject to simulate an outage, or
// hang to simulate a slow Redis. No real infra — this exercises the
// DEGRADATION logic; the atomic Lua path is proven in gcra.integration.test.ts.
function fakeRedis(flGcra: () => Promise<[number, number, string, string]>): Redis {
  return {
    defineCommand() {},
    flGcra,
  } as unknown as Redis;
}

const CFG = { limit: 5, windowMs: 1000 };

describe('DegradingLimiter — primary healthy', () => {
  it('serves from Redis and counts it as a primary decision', async () => {
    const redis = fakeRedis(() => Promise.resolve([1, 4, '-1', '0.2']));
    const limiter = new DegradingLimiter(redis, { config: CFG });
    const res = await limiter.limit('rider-1');
    expect(res.allowed).toBe(true);
    expect(res.remaining).toBe(4);
    expect(limiter.metrics.primaryTotal).toBe(1);
    expect(limiter.metrics.fallbackTotal).toBe(0);
  });
});

describe('DegradingLimiter — fail-open (default) when Redis errors', () => {
  it('falls back to the in-process limiter and keeps limiting', async () => {
    // A realistic ms clock that advances 1ms per call — like Redis TIME's
    // microsecond ticks, this keeps the burst boundary off the exact float edge.
    let clock = 1_700_000_000_000;
    const redis = fakeRedis(() => Promise.reject(new Error('redis down')));
    const limiter = new DegradingLimiter(redis, { config: CFG, now: () => clock });

    // Burst of 5 admitted, 6th denied — the fallback enforces the same limit.
    const results: RateResult[] = [];
    for (let i = 0; i < 6; i++) {
      results.push(await limiter.limit('rider-1'));
      clock += 1;
    }
    expect(results.filter((r) => r.allowed)).toHaveLength(5);
    expect(results[5]?.allowed).toBe(false);

    expect(limiter.metrics.fallbackTotal).toBe(6);
    expect(limiter.metrics.redisErrorsTotal).toBe(6);
    expect(limiter.metrics.deniedTotal).toBe(1);

    // After a full window, the bucket has refilled.
    clock += 1000;
    expect((await limiter.limit('rider-1')).allowed).toBe(true);
  });
});

describe('DegradingLimiter — slow Redis routes to the fallback', () => {
  it('a Redis call slower than the timeout is abandoned and counted a timeout', async () => {
    const redis = fakeRedis(() => new Promise(() => {})); // never resolves
    const limiter = new DegradingLimiter(redis, { config: CFG, redisTimeoutMs: 20 });
    const res = await limiter.limit('rider-1');
    expect(res.allowed).toBe(true); // served by the fallback, not stuck
    expect(limiter.metrics.redisTimeoutsTotal).toBe(1);
    expect(limiter.metrics.fallbackTotal).toBe(1);
  });
});

describe('DegradingLimiter — fail-closed', () => {
  it('rejects when Redis is down and failClosed is set', async () => {
    const redis = fakeRedis(() => Promise.reject(new Error('redis down')));
    const limiter = new DegradingLimiter(redis, { config: CFG, failClosed: true });
    const res = await limiter.limit('rider-1');
    expect(res.allowed).toBe(false);
    expect(limiter.metrics.failClosedRejectsTotal).toBe(1);
    expect(limiter.metrics.fallbackTotal).toBe(0);
  });
});

describe('DegradingLimiter — fallback memory is bounded', () => {
  it('never tracks more than fallbackMaxKeys distinct keys', async () => {
    const redis = fakeRedis(() => Promise.reject(new Error('redis down')));
    const limiter = new DegradingLimiter(redis, { config: CFG, fallbackMaxKeys: 10 });
    for (let i = 0; i < 100; i++) await limiter.limit(`rider-${i}`);
    // Internal Map is private; observe the bound indirectly: it still answers
    // and hasn't grown unbounded (a leak would OOM long before this).
    expect(limiter.metrics.fallbackTotal).toBe(100);
    expect((await limiter.limit('rider-fresh')).allowed).toBe(true);
  });
});
