import { RedisContainer, type StartedRedisContainer } from '@testcontainers/redis';
import { cellToLatLng } from 'h3-js';
import { Redis } from 'ioredis';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import {
  ClaimStore,
  GeoIndex,
  cellFor,
  cellKey,
  claimKey,
  driverKey,
  kRing,
  parseClaimValue,
} from '../src/index.js';

// The atomic claim against real Redis. All time is a fake clock (nowMs
// injected everywhere; the PX on the claim key is only a GC net and no test
// relies on it firing).

const T0 = 1_000_000;
const FRESH_MS = 10_000;
const TTL_MS = 8_000;
const CENTER = { lat: 37.7749, lng: -122.4194 };
const C0 = cellFor(CENTER.lat, CENTER.lng);

function centerOf(cell: string): { lat: number; lng: number } {
  const [lat, lng] = cellToLatLng(cell);
  return { lat, lng };
}

describe('atomic claim (testcontainers redis)', () => {
  let container: StartedRedisContainer;
  let redis: Redis;
  let index: GeoIndex;
  let claims: ClaimStore;

  beforeAll(async () => {
    container = await new RedisContainer('redis:7-alpine').start();
    redis = new Redis(container.getConnectionUrl());
    index = new GeoIndex(redis);
    claims = new ClaimStore(redis);
  }, 120_000);

  afterAll(async () => {
    redis.disconnect();
    await container.stop();
  });

  beforeEach(async () => {
    await redis.flushall();
  });

  async function seed(driverId: string, at = CENTER, nowMs = T0): Promise<void> {
    await index.applyPings([{ driverId, ...at }], nowMs);
  }

  it('claims an available fresh driver atomically: token, hash, set, claim key', async () => {
    await seed('d1');
    const token = await claims.claimDriver('d1', 'trip-1', T0, FRESH_MS, TTL_MS);

    expect(token).toMatch(/^[0-9a-f]{32}$/);
    expect(await redis.hget(driverKey('d1'), 'status')).toBe('claimed');
    expect(await redis.smembers(cellKey(C0))).toEqual([]);
    expect(parseClaimValue(await redis.get(claimKey('d1')))).toEqual({
      tripId: 'trip-1',
      token,
      expiresAt: T0 + TTL_MS,
    });
    // PX safety net armed, but never longer than the authoritative expiry.
    const pttl = await redis.pttl(claimKey('d1'));
    expect(pttl).toBeGreaterThan(0);
    expect(pttl).toBeLessThanOrEqual(TTL_MS);
  });

  it('fail-and-change-nothing: unknown, stale, already-claimed, out-of-set drivers', async () => {
    // Unknown driver: no hash at all.
    expect(await claims.claimDriver('ghost', 't', T0, FRESH_MS, TTL_MS)).toBeNull();

    // Stale heartbeat (exactly freshMs old is already too old).
    await seed('stale', CENTER, T0 - FRESH_MS);
    expect(await claims.claimDriver('stale', 't', T0, FRESH_MS, TTL_MS)).toBeNull();
    expect(await redis.hget(driverKey('stale'), 'status')).toBe('available');
    expect(await redis.smembers(cellKey(C0))).toContain('stale');

    // Already claimed.
    await seed('busy');
    expect(await claims.claimDriver('busy', 't1', T0, FRESH_MS, TTL_MS)).not.toBeNull();
    expect(await claims.claimDriver('busy', 't2', T0, FRESH_MS, TTL_MS)).toBeNull();

    // Hash says available but the set disagrees — refused, nothing written.
    await seed('drifted');
    await redis.srem(cellKey(C0), 'drifted');
    expect(await claims.claimDriver('drifted', 't', T0, FRESH_MS, TTL_MS)).toBeNull();
    expect(await redis.hget(driverKey('drifted'), 'status')).toBe('available');
    expect(await redis.exists(claimKey('drifted'))).toBe(0);
  });

  it('50 concurrent claims on one driver yield exactly one token', async () => {
    await seed('contested');
    const results = await Promise.all(
      Array.from({ length: 50 }, (_, i) => claims.claimDriver('contested', `trip-${i}`, T0, FRESH_MS, TTL_MS)),
    );
    const tokens = results.filter((t): t is string => t !== null);
    expect(tokens).toHaveLength(1);
    expect(parseClaimValue(await redis.get(claimKey('contested')))?.token).toBe(tokens[0]);
  });

  it('confirm: wrong token rejected untouched; right token flips to on_trip and deletes the claim', async () => {
    await seed('d1');
    const token = (await claims.claimDriver('d1', 'trip-1', T0, FRESH_MS, TTL_MS))!;

    expect(await claims.confirmClaim('d1', 'not-the-token', T0 + 1)).toBe(false);
    expect(await redis.hget(driverKey('d1'), 'status')).toBe('claimed');
    expect(await redis.exists(claimKey('d1'))).toBe(1);

    expect(await claims.confirmClaim('d1', token, T0 + 1)).toBe(true);
    expect(await redis.hget(driverKey('d1'), 'status')).toBe('on_trip');
    expect(await redis.exists(claimKey('d1'))).toBe(0);

    // A confirmed claim cannot be confirmed (or released) again.
    expect(await claims.confirmClaim('d1', token, T0 + 2)).toBe(false);
    expect(await claims.releaseClaim('d1', token)).toBe(false);
  });

  it('expired claim cannot confirm (fake clock) — expiresAt in the VALUE is authoritative', async () => {
    await seed('d1');
    const token = (await claims.claimDriver('d1', 'trip-1', T0, FRESH_MS, TTL_MS))!;

    // The PX has not fired (real time barely advanced) but the fake clock is
    // at the deadline: the in-value expiry decides, not the key's TTL.
    expect(await claims.confirmClaim('d1', token, T0 + TTL_MS)).toBe(false);
    expect(await redis.hget(driverKey('d1'), 'status')).toBe('claimed');
    expect(await redis.exists(claimKey('d1'))).toBe(1);

    // One tick before the deadline still confirms.
    await redis.del(claimKey('d1'));
    await redis.hset(driverKey('d1'), { status: 'available' });
    await redis.sadd(cellKey(C0), 'd1');
    const token2 = (await claims.claimDriver('d1', 'trip-2', T0, FRESH_MS, TTL_MS))!;
    expect(await claims.confirmClaim('d1', token2, T0 + TTL_MS - 1)).toBe(true);
  });

  it('release returns the driver to its CURRENT cell after movement while claimed', async () => {
    const neighbor = kRing(C0, 1).find((c) => c !== C0)!;
    await seed('mover', centerOf(C0));
    const token = (await claims.claimDriver('mover', 'trip-1', T0, FRESH_MS, TTL_MS))!;

    // Driver keeps pinging while claimed: hash cell moves, sets untouched.
    await index.applyPings([{ driverId: 'mover', ...centerOf(neighbor) }], T0 + 1000);
    expect(await redis.smembers(cellKey(neighbor))).toEqual([]);

    expect(await claims.releaseClaim('mover', token)).toBe(true);
    expect(await redis.hget(driverKey('mover'), 'status')).toBe('available');
    expect(await redis.smembers(cellKey(neighbor))).toEqual(['mover']);
    expect(await redis.smembers(cellKey(C0))).toEqual([]);
    expect(await redis.exists(claimKey('mover'))).toBe(0);

    // Released driver is claimable again.
    expect(await claims.claimDriver('mover', 'trip-2', T0 + 1000, FRESH_MS, TTL_MS)).not.toBeNull();
  });

  it('release with a wrong or stale token is a no-op', async () => {
    await seed('d1');
    const token = (await claims.claimDriver('d1', 'trip-1', T0, FRESH_MS, TTL_MS))!;
    expect(await claims.releaseClaim('d1', 'wrong')).toBe(false);
    expect(await redis.hget(driverKey('d1'), 'status')).toBe('claimed');
    expect(await claims.releaseClaim('d1', token)).toBe(true);
    // Second release: claim key already gone.
    expect(await claims.releaseClaim('d1', token)).toBe(false);
  });
});
