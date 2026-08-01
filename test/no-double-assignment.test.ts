import { PostgreSqlContainer, type StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import { RedisContainer, type StartedRedisContainer } from '@testcontainers/redis';
import { cellToLatLng } from 'h3-js';
import { Redis } from 'ioredis';
import type pg from 'pg';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { GeoIndex, cellFor, cellKey, driverKey, type GeoPing } from '@fleetline/core';
import { createPool, runMigrations } from '@fleetline/db';
import { MatcherCore } from '@fleetline/matcher';

/**
 * THE SIGNATURE TEST — no driver is ever double-assigned.
 *
 * 20 available drivers in ONE res-8 cell, 200 ride requests fired through the
 * real matching path concurrently (real Redis, real Postgres, real Lua claim,
 * real partial unique index — nothing mocked). Exactly 20 trips, exactly 180
 * honest unmatched, every assigned driver unique in Postgres AND consistent
 * in Redis, and the defense-in-depth index never fired.
 */

const DRIVERS = 20;
const REQUESTS = 200;

// Drivers seeded around an H3 cell's own center so tiny offsets can't cross
// a cell edge: all 20 provably share one res-8 cell.
const ORIGIN = cellFor(37.7749, -122.4194);
const [CELL_LAT, CELL_LNG] = cellToLatLng(ORIGIN);

describe('SIGNATURE: no double assignment', () => {
  let redisContainer: StartedRedisContainer;
  let pgContainer: StartedPostgreSqlContainer;
  let redis: Redis;
  let pool: pg.Pool;

  beforeAll(async () => {
    [redisContainer, pgContainer] = await Promise.all([
      new RedisContainer('redis:7-alpine').start(),
      new PostgreSqlContainer('postgres:16-alpine').start(),
    ]);
    redis = new Redis(redisContainer.getConnectionUrl());
    await runMigrations(pgContainer.getConnectionUri());
    pool = createPool(pgContainer.getConnectionUri());
  }, 240_000);

  afterAll(async () => {
    await pool.end();
    redis.disconnect();
    await Promise.all([redisContainer.stop(), pgContainer.stop()]);
  });

  beforeEach(async () => {
    await redis.flushall();
    await pool.query('TRUNCATE ride_requests, trips CASCADE');

    // Seed the fleet: 20 drivers, all available, all pinged into ONE cell
    // through the real GeoIndex ingestion path.
    const pings: GeoPing[] = Array.from({ length: DRIVERS }, (_, i) => ({
      driverId: `d${i}`,
      lat: CELL_LAT + (i % 5) * 1e-4,
      lng: CELL_LNG + Math.floor(i / 5) * 1e-4,
    }));
    await new GeoIndex(redis).applyPings(pings, Date.now());
    expect(await redis.scard(cellKey(ORIGIN))).toBe(DRIVERS);
    for (const p of pings) expect(cellFor(p.lat, p.lng)).toBe(ORIGIN);
  });

  async function insertRequests(ids: string[]): Promise<void> {
    // Riders scattered a few hundred meters around the cell so scoring has
    // real distances to rank.
    const lats = ids.map((_, i) => CELL_LAT + ((i % 20) - 10) * 2e-4);
    const lngs = ids.map((_, i) => CELL_LNG + ((Math.floor(i / 20) % 20) - 5) * 2e-4);
    await pool.query(
      `INSERT INTO ride_requests (id, lat, lng, status)
       SELECT unnest($1::text[]), unnest($2::float8[]), unnest($3::float8[]), 'pending'`,
      [ids, lats, lngs],
    );
  }

  async function assertInvariants(outcomes: string[], totalRequests: number): Promise<void> {
    // Outcomes as reported by the matcher(s) themselves.
    expect(outcomes.filter((o) => o === 'matched')).toHaveLength(DRIVERS);
    expect(outcomes.filter((o) => o === 'unmatched')).toHaveLength(totalRequests - DRIVERS);

    // Postgres: exactly 20 trips, exactly 180 unmatched, statuses add up.
    const counts = await pool.query<{ status: string; n: number }>(
      'SELECT status, count(*)::int AS n FROM ride_requests GROUP BY status ORDER BY status',
    );
    expect(counts.rows).toEqual([
      { status: 'matched', n: DRIVERS },
      { status: 'unmatched', n: totalRequests - DRIVERS },
    ]);
    const trips = await pool.query<{ driver_id: string; request_id: string; id: string }>(
      'SELECT id, driver_id, request_id FROM trips',
    );
    expect(trips.rows).toHaveLength(DRIVERS);

    // THE invariant, asserted in Postgres: no driver on two trips.
    const dupDrivers = await pool.query(
      'SELECT driver_id FROM trips GROUP BY driver_id HAVING count(*) > 1',
    );
    expect(dupDrivers.rows).toEqual([]);
    expect(new Set(trips.rows.map((t) => t.driver_id)).size).toBe(DRIVERS);

    // Every matched request has exactly one trip, and it points back at it.
    const dupRequests = await pool.query(
      'SELECT request_id FROM trips GROUP BY request_id HAVING count(*) > 1',
    );
    expect(dupRequests.rows).toEqual([]);
    const linkage = await pool.query<{ n: number }>(
      `SELECT count(*)::int AS n
       FROM ride_requests r JOIN trips t ON t.request_id = r.id
       WHERE r.status = 'matched' AND r.matched_trip_id = t.id`,
    );
    expect(linkage.rows[0]?.n).toBe(DRIVERS);

    // Redis agrees with Postgres: no tripped driver lingers in ANY available
    // set, every tripped driver is on_trip, no claim keys survive.
    const tripped = new Set(trips.rows.map((t) => t.driver_id));
    for (const key of await redis.keys('cell:*:available')) {
      for (const member of await redis.smembers(key)) {
        expect(tripped.has(member), `${member} has a trip but is still available in ${key}`).toBe(false);
      }
    }
    for (const driverId of tripped) {
      expect(await redis.hget(driverKey(driverId), 'status')).toBe('on_trip');
    }
    expect(await redis.keys('claim:*')).toEqual([]);
  }

  it(`${REQUESTS} concurrent requests, ${DRIVERS} drivers: exactly ${DRIVERS} trips, zero double assignment`, async () => {
    const ids = Array.from({ length: REQUESTS }, (_, i) => `req-${i}`);
    await insertRequests(ids);

    const matcher = new MatcherCore({ redis, pool });
    const outcomes = await Promise.all(ids.map((id) => matcher.matchRequest(id)));

    await assertInvariants(outcomes, REQUESTS);
    expect(matcher.metrics.pgUniqueViolationsTotal).toBe(0);
    expect(matcher.metrics.matchesTotal).toBe(DRIVERS);
    expect(matcher.metrics.unmatchedTotal).toBe(REQUESTS - DRIVERS);
    // The race was real: losing claim attempts actually collided.
    expect(matcher.metrics.claimConflictsTotal).toBeGreaterThan(0);
  }, 120_000);

  it('two matcher instances (cross-instance, not just cross-promise): invariants hold', async () => {
    const ids = Array.from({ length: REQUESTS }, (_, i) => `req-${i}`);
    await insertRequests(ids);

    // Separate connections and pools — two consumer groups sharing nothing
    // but the stores, like two matcher processes.
    const redisB = new Redis(redisContainer.getConnectionUrl());
    const poolB = createPool(pgContainer.getConnectionUri());
    try {
      const a = new MatcherCore({ redis, pool });
      const b = new MatcherCore({ redis: redisB, pool: poolB });

      // Interleaved 100/100 so neither instance gets a systematic head start.
      const outcomes = await Promise.all(
        ids.map((id, i) => (i % 2 === 0 ? a : b).matchRequest(id)),
      );

      await assertInvariants(outcomes, REQUESTS);
      expect(a.metrics.pgUniqueViolationsTotal + b.metrics.pgUniqueViolationsTotal).toBe(0);
      expect(a.metrics.matchesTotal + b.metrics.matchesTotal).toBe(DRIVERS);
      expect(a.metrics.unmatchedTotal + b.metrics.unmatchedTotal).toBe(REQUESTS - DRIVERS);
      // The contention was real across the pair of instances.
      expect(a.metrics.claimConflictsTotal + b.metrics.claimConflictsTotal).toBeGreaterThan(0);
    } finally {
      await poolB.end();
      redisB.disconnect();
    }
  }, 120_000);
});
