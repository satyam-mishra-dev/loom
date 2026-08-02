import { RedisContainer, type StartedRedisContainer } from '@testcontainers/redis';
import { cellToLatLng } from 'h3-js';
import { Redis } from 'ioredis';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { GeoIndex, SURGE_HASH, SurgeStore, cellFor, type GeoPing } from '../src/index.js';

/**
 * SurgeStore against real Redis: demand recorded (as the gateway would) vs
 * live supply (drivers in the available set, seeded through the real GeoIndex
 * path), recomputed and published to `cell:surge`. Proves the whole surge
 * pipeline end to end, including the 60s window pruning.
 */
describe('SurgeStore (testcontainers redis)', () => {
  let container: StartedRedisContainer;
  let redis: Redis;
  let surge: SurgeStore;
  let geo: GeoIndex;

  const CELL = cellFor(37.7749, -122.4194);
  const [LAT, LNG] = cellToLatLng(CELL);

  beforeAll(async () => {
    container = await new RedisContainer('redis:7-alpine').start();
    redis = new Redis(container.getConnectionUrl());
    surge = new SurgeStore(redis);
    geo = new GeoIndex(redis);
  }, 120_000);

  afterAll(async () => {
    redis.disconnect();
    await container.stop();
  });

  beforeEach(async () => {
    await redis.flushall();
  });

  async function seedDrivers(n: number, nowMs: number): Promise<void> {
    const pings: GeoPing[] = Array.from({ length: n }, (_, i) => ({
      driverId: `d${i}`,
      lat: LAT + (i % 4) * 1e-4,
      lng: LNG + Math.floor(i / 4) * 1e-4,
    }));
    // Keep the small jitter inside the one cell.
    for (const p of pings) expect(cellFor(p.lat, p.lng)).toBe(CELL);
    await geo.applyPings(pings, nowMs);
  }

  it('demand over supply surges the cell; the multiplier is the ratio, capped at 3', async () => {
    const now = Date.now();
    await seedDrivers(5, now);
    for (let i = 0; i < 10; i++) await surge.recordDemand(CELL, `r${i}`, now);

    const surges = await surge.recompute(now);
    expect(surges).toHaveLength(1);
    expect(surges[0]).toMatchObject({ cell: CELL, demand: 10, supply: 5, multiplier: 2 });

    await surge.publish(surges);
    expect(await redis.hget(SURGE_HASH, CELL)).toBe('2.000');
    expect(await surge.published()).toEqual({ [CELL]: 2 });
  });

  it('plentiful supply → no surge, nothing published', async () => {
    const now = Date.now();
    await seedDrivers(20, now);
    for (let i = 0; i < 3; i++) await surge.recordDemand(CELL, `r${i}`, now);

    const surges = await surge.recompute(now);
    expect(surges[0]).toMatchObject({ demand: 3, supply: 20, multiplier: 1 });
    await surge.publish(surges);
    // multiplier 1 is not "surging" — the hash stays empty.
    expect(await redis.exists(SURGE_HASH)).toBe(0);
  });

  it('demand with zero supply → capped at 3', async () => {
    const now = Date.now();
    for (let i = 0; i < 4; i++) await surge.recordDemand(CELL, `r${i}`, now);
    const surges = await surge.recompute(now);
    expect(surges[0]).toMatchObject({ demand: 4, supply: 0, multiplier: 3 });
  });

  it('demand older than the 60s window is pruned and stops surging', async () => {
    const now = Date.now();
    await seedDrivers(2, now);
    for (let i = 0; i < 8; i++) await surge.recordDemand(CELL, `old${i}`, now - 70_000);
    for (let i = 0; i < 2; i++) await surge.recordDemand(CELL, `new${i}`, now - 1_000);

    const surges = await surge.recompute(now);
    // Only the 2 in-window requests count: 2 demand / 2 supply = 1.0, no surge.
    expect(surges[0]).toMatchObject({ demand: 2, supply: 2, multiplier: 1 });

    // Once every request has aged out, the cell drops off the worklist.
    const later = await surge.recompute(now + 80_000);
    expect(later).toEqual([]);
    expect(await redis.scard('surge:cells')).toBe(0);
  });

  it('publish replaces the snapshot: a cell that stops surging disappears', async () => {
    const now = Date.now();
    await seedDrivers(2, now);
    for (let i = 0; i < 8; i++) await surge.recordDemand(CELL, `r${i}`, now);
    await surge.publish(await surge.recompute(now));
    expect(await redis.hget(SURGE_HASH, CELL)).toBe('3.000');

    // Supply arrives; the same cell is no longer surging.
    await seedDrivers(20, now + 1000);
    await surge.publish(await surge.recompute(now + 1000));
    expect(await redis.exists(SURGE_HASH)).toBe(0);
  });
});
