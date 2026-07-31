import { RedisContainer, type StartedRedisContainer } from '@testcontainers/redis';
import { cellToLatLng } from 'h3-js';
import { Redis } from 'ioredis';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import {
  GeoIndex,
  HEARTBEAT_ZSET,
  cellFor,
  cellKey,
  driverKey,
  kRing,
  type GeoPing,
} from '../src/index.js';

// Real Redis via Testcontainers — no mocked infra. All time is a fake clock
// (nowMs is injected everywhere), so staleness needs no sleeps.

const T0 = 1_000_000;
const CENTER = { lat: 37.7749, lng: -122.4194 };
const C0 = cellFor(CENTER.lat, CENTER.lng);

/** lat/lng of an H3 cell's center. */
function centerOf(cell: string): { lat: number; lng: number } {
  const [lat, lng] = cellToLatLng(cell);
  return { lat, lng };
}

/** Cells at exactly grid distance k from C0. */
function ringCells(k: number): string[] {
  const inner = new Set(kRing(C0, k - 1));
  return kRing(C0, k).filter((c) => !inner.has(c));
}

/** Deterministic wandering coordinate for driver i at round r (spans many cells). */
function walk(i: number, r: number): { lat: number; lng: number } {
  return {
    lat: 37.7 + ((i * 13 + r * 7) % 100) / 1000,
    lng: -122.45 + ((i * 29 + r * 11) % 100) / 1000,
  };
}

describe('geo index (testcontainers redis)', () => {
  let container: StartedRedisContainer;
  let redis: Redis;
  let index: GeoIndex;

  beforeAll(async () => {
    container = await new RedisContainer('redis:7-alpine').start();
    redis = new Redis(container.getConnectionUrl());
    index = new GeoIndex(redis);
  }, 120_000);

  afterAll(async () => {
    redis.disconnect();
    await container.stop();
  });

  beforeEach(async () => {
    await redis.flushall();
  });

  it('a ping writes the hash, the available set, and the heartbeat zset', async () => {
    await index.applyPings([{ driverId: 'd1', ...CENTER }], T0);

    expect(await redis.hgetall(driverKey('d1'))).toEqual({
      cell: C0,
      status: 'available',
      lat: String(CENTER.lat),
      lng: String(CENTER.lng),
      heartbeatMs: String(T0),
    });
    expect(await redis.smembers(cellKey(C0))).toEqual(['d1']);
    expect(await redis.zscore(HEARTBEAT_ZSET, 'd1')).toBe(String(T0));
  });

  it('crossing a cell boundary lands the driver in exactly one (new) set', async () => {
    const neighbor = ringCells(1)[0]!;
    await index.applyPings([{ driverId: 'd1', ...centerOf(C0) }], T0);
    await index.applyPings([{ driverId: 'd1', ...centerOf(neighbor) }], T0 + 1000);

    expect(await redis.smembers(cellKey(C0))).toEqual([]);
    expect(await redis.smembers(cellKey(neighbor))).toEqual(['d1']);
    expect(await redis.hget(driverKey('d1'), 'cell')).toBe(neighbor);
  });

  it('the newest ping per driver wins within one batch', async () => {
    const neighbor = ringCells(1)[0]!;
    await index.applyPings(
      [
        { driverId: 'd1', ...centerOf(C0) },
        { driverId: 'd1', ...centerOf(neighbor) },
      ],
      T0,
    );
    expect(await redis.smembers(cellKey(C0))).toEqual([]);
    expect(await redis.smembers(cellKey(neighbor))).toEqual(['d1']);
  });

  it('churn: 200 moving drivers stay consistent (one cell each, counts match)', async () => {
    const drivers = 200;
    for (let r = 0; r < 10; r++) {
      const batch: GeoPing[] = [];
      for (let i = 0; i < drivers; i++) batch.push({ driverId: `d${i}`, ...walk(i, r) });
      await index.applyPings(batch, T0 + r * 1000);
    }

    // Audit: every driver appears in exactly one available set, and it is the
    // set of the cell its hash claims.
    const memberOf = new Map<string, string>();
    for (const key of await redis.keys('cell:*:available')) {
      for (const id of await redis.smembers(key)) {
        expect(memberOf.has(id), `${id} present in two cell sets`).toBe(false);
        memberOf.set(id, key);
      }
    }
    expect(memberOf.size).toBe(drivers);
    for (let i = 0; i < drivers; i++) {
      const cell = await redis.hget(driverKey(`d${i}`), 'cell');
      expect(memberOf.get(`d${i}`)).toBe(cellKey(cell!));
    }
    expect(await redis.zcard(HEARTBEAT_ZSET)).toBe(drivers);
  }, 30_000);

  it('a claimed driver ping updates position/heartbeat but never rejoins available', async () => {
    const neighbor = ringCells(1)[0]!;
    await index.applyPings([{ driverId: 'd1', ...centerOf(C0) }], T0);
    // Simulate the matcher's claim: out of the set, status claimed.
    await redis.srem(cellKey(C0), 'd1');
    await redis.hset(driverKey('d1'), { status: 'claimed' });

    const to = centerOf(neighbor);
    await index.applyPings([{ driverId: 'd1', ...to }], T0 + 1000);

    expect(await redis.hgetall(driverKey('d1'))).toEqual({
      cell: neighbor,
      status: 'claimed',
      lat: String(to.lat),
      lng: String(to.lng),
      heartbeatMs: String(T0 + 1000),
    });
    expect(await redis.smembers(cellKey(C0))).toEqual([]);
    expect(await redis.smembers(cellKey(neighbor))).toEqual([]);
    expect(await redis.zscore(HEARTBEAT_ZSET, 'd1')).toBe(String(T0 + 1000));
  });

  it('sweepStale offlines silent drivers and prunes set + zset (fake clock)', async () => {
    await index.applyPings([{ driverId: 'stale', ...CENTER }], T0);
    await index.applyPings([{ driverId: 'fresh', ...CENTER }], T0 + 5000);

    const swept = await index.sweepStale(T0 + 10_001, 10_000);
    expect(swept).toEqual(['stale']);

    expect(await redis.hget(driverKey('stale'), 'status')).toBe('offline');
    expect(await redis.smembers(cellKey(C0))).toEqual(['fresh']);
    expect(await redis.zscore(HEARTBEAT_ZSET, 'stale')).toBeNull();
    expect(await redis.hget(driverKey('fresh'), 'status')).toBe('available');

    // Idempotent: nothing left to sweep.
    expect(await index.sweepStale(T0 + 10_001, 10_000)).toEqual([]);

    // A returning ping revives the offline driver.
    await index.applyPings([{ driverId: 'stale', ...CENTER }], T0 + 12_000);
    expect(await redis.hget(driverKey('stale'), 'status')).toBe('available');
    expect((await redis.smembers(cellKey(C0))).sort()).toEqual(['fresh', 'stale']);
  });

  it('findCandidates expands k only as needed, honors need/maxK, skips stale and claimed', async () => {
    const ring1 = ringCells(1).slice(0, 3);
    const ring2 = ringCells(2).slice(0, 5);
    const batch: GeoPing[] = [
      { driverId: 'a0', ...centerOf(C0) },
      { driverId: 'a1', ...centerOf(C0) },
      ...ring1.map((c, i) => ({ driverId: `b${i}`, ...centerOf(c) })),
      ...ring2.map((c, i) => ({ driverId: `c${i}`, ...centerOf(c) })),
    ];
    await index.applyPings(batch, T0);
    // One stale driver in the origin cell (heartbeat 20s old at query time)…
    await index.applyPings([{ driverId: 'zombie', ...centerOf(C0) }], T0 - 20_000);
    // …and one claimed driver in the origin cell.
    await index.applyPings([{ driverId: 'busy', ...centerOf(C0) }], T0);
    await redis.srem(cellKey(C0), 'busy');
    await redis.hset(driverKey('busy'), { status: 'claimed' });

    const at = { need: 0, maxK: 2, nowMs: T0, staleMs: 10_000 };

    const k0 = await index.findCandidates(CENTER.lat, CENTER.lng, { ...at, need: 1 });
    expect(k0.k).toBe(0); // satisfied without expanding
    expect(k0.candidates.map((c) => c.driverId).sort()).toEqual(['a0', 'a1']);

    const k1 = await index.findCandidates(CENTER.lat, CENTER.lng, { ...at, need: 3 });
    expect(k1.k).toBe(1);
    expect(k1.candidates).toHaveLength(5);

    const k2 = await index.findCandidates(CENTER.lat, CENTER.lng, { ...at, need: 10 });
    expect(k2.k).toBe(2);
    expect(k2.candidates).toHaveLength(10);

    // Cap respected: need can't be met, expansion stops at maxK.
    const capped = await index.findCandidates(CENTER.lat, CENTER.lng, { ...at, need: 50 });
    expect(capped.k).toBe(2);
    expect(capped.candidates).toHaveLength(10);

    // Neither the stale nor the claimed driver ever surfaces.
    for (const res of [k0, k1, k2, capped]) {
      const ids = res.candidates.map((c) => c.driverId);
      expect(ids).not.toContain('zombie');
      expect(ids).not.toContain('busy');
      for (const c of res.candidates) expect(c.status).toBe('available');
    }
  });

  it('self-heal: FLUSHALL mid-stream rebuilds the full index from live pings', async () => {
    const drivers = 200;
    const chunk = 20; // pings trickle in per-driver, not as one convenient full-fleet batch
    const fleet = (r: number): GeoPing[] =>
      Array.from({ length: drivers }, (_, i) => ({ driverId: `d${i}`, ...walk(i, r) }));

    const stream = async (r: number): Promise<void> => {
      const batch = fleet(r);
      for (let off = 0; off < drivers; off += chunk) {
        await index.applyPings(batch.slice(off, off + chunk), T0 + r * 1000 + off);
        await new Promise((resolve) => setTimeout(resolve, 5));
      }
    };

    await stream(0);
    expect(await redis.zcard(HEARTBEAT_ZSET)).toBe(drivers);

    await redis.flushall();
    const flushedAt = performance.now();
    let rebuildMs = -1;
    for (let r = 1; r < 20 && rebuildMs < 0; r++) {
      await stream(r);
      if ((await redis.zcard(HEARTBEAT_ZSET)) === drivers) {
        rebuildMs = performance.now() - flushedAt;
      }
    }
    expect(rebuildMs).toBeGreaterThan(0);

    // Rebuilt index passes the same consistency audit.
    const memberOf = new Map<string, string>();
    for (const key of await redis.keys('cell:*:available')) {
      for (const id of await redis.smembers(key)) {
        expect(memberOf.has(id)).toBe(false);
        memberOf.set(id, key);
      }
    }
    expect(memberOf.size).toBe(drivers);

    // Number destined for FAILURE-MODES.md: bounded by one full ping cycle.
    process.stdout.write(`self-heal rebuild after FLUSHALL: ${rebuildMs.toFixed(0)}ms\n`);
  }, 30_000);
});
