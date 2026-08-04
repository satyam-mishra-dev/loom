import { PostgreSqlContainer, type StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import { RedisContainer, type StartedRedisContainer } from '@testcontainers/redis';
import { Redis } from 'ioredis';
import type pg from 'pg';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import {
  GeoIndex,
  REQUESTS_PROCESSING,
  REQUESTS_QUEUE,
  cellKey,
  cellFor,
  driverKey,
  offerReplyKey,
} from '@loom/core';
import { createPool, runMigrations } from '@loom/db';
import { MatcherCore } from '../src/matcher.js';

// The matcher as a service: real consumer loops over real Redis lists, real
// trips in real Postgres. The 200-request concurrency proof lives in the
// repo-root signature test; this suite covers the service mechanics. Drivers
// auto-accept every offer here — cascade behaviors (decline, timeout,
// janitor) live in cascade.integration.test.ts.

const CENTER = { lat: 37.7749, lng: -122.4194 };
const C0 = cellFor(CENTER.lat, CENTER.lng);

async function waitFor(cond: () => boolean | Promise<boolean>, timeoutMs = 10_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!(await cond())) {
    if (Date.now() > deadline) throw new Error('waitFor timed out');
    await new Promise((r) => setTimeout(r, 25));
  }
}

describe('matcher service (testcontainers redis + postgres)', () => {
  let redisContainer: StartedRedisContainer;
  let pgContainer: StartedPostgreSqlContainer;
  let redis: Redis;
  let autoAccept: Redis;
  let pool: pg.Pool;
  let geo: GeoIndex;
  const cores: MatcherCore[] = [];

  beforeAll(async () => {
    [redisContainer, pgContainer] = await Promise.all([
      new RedisContainer('redis:7-alpine').start(),
      new PostgreSqlContainer('postgres:16-alpine').start(),
    ]);
    redis = new Redis(redisContainer.getConnectionUrl());
    await runMigrations(pgContainer.getConnectionUri());
    pool = createPool(pgContainer.getConnectionUri());
    geo = new GeoIndex(redis);

    // Every driver accepts every offer, instantly.
    autoAccept = redis.duplicate();
    autoAccept.on('pmessage', (_p: string, _c: string, message: string) => {
      const msg = JSON.parse(message) as { type: string; offerId?: string };
      if (msg.type === 'offer' && msg.offerId !== undefined) {
        void redis.lpush(offerReplyKey(msg.offerId), JSON.stringify({ accept: true }));
      }
    });
    await autoAccept.psubscribe('driver:*:msg');
  }, 240_000);

  afterAll(async () => {
    await pool.end();
    autoAccept.disconnect();
    redis.disconnect();
    await Promise.all([redisContainer.stop(), pgContainer.stop()]);
  });

  beforeEach(async () => {
    await redis.flushall();
    await pool.query('TRUNCATE trip_events, ride_requests, trips CASCADE');
  });

  afterEach(async () => {
    for (const core of cores.splice(0)) await core.stop();
  });

  function newCore(): MatcherCore {
    const core = new MatcherCore({ redis, pool });
    cores.push(core);
    return core;
  }

  async function seedDrivers(n: number): Promise<void> {
    await geo.applyPings(
      Array.from({ length: n }, (_, i) => ({ driverId: `d${i}`, ...CENTER })),
      Date.now(),
    );
  }

  async function insertRequests(ids: string[]): Promise<void> {
    await pool.query(
      `INSERT INTO ride_requests (id, lat, lng, status)
       SELECT unnest($1::text[]), $2, $3, 'pending'`,
      [ids, CENTER.lat, CENTER.lng],
    );
  }

  it('consumer loop: queued ids become matched trips and both lists drain', async () => {
    await seedDrivers(3);
    await insertRequests(['r1', 'r2']);
    await redis.lpush(REQUESTS_QUEUE, 'r1', 'r2');

    const core = newCore();
    await core.start(2);
    await waitFor(async () => {
      const res = await pool.query<{ n: number }>(
        `SELECT count(*)::int AS n FROM ride_requests WHERE status = 'matched'`,
      );
      return res.rows[0]?.n === 2;
    });

    const trips = await pool.query<{ request_id: string; driver_id: string }>(
      'SELECT request_id, driver_id FROM trips ORDER BY request_id',
    );
    expect(trips.rows.map((t) => t.request_id)).toEqual(['r1', 'r2']);
    expect(new Set(trips.rows.map((t) => t.driver_id)).size).toBe(2);
    expect(core.metrics.matchesTotal).toBe(2);
    expect(core.metrics.pgUniqueViolationsTotal).toBe(0);

    // Acked: nothing left queued or in flight.
    expect(await redis.llen(REQUESTS_QUEUE)).toBe(0);
    expect(await redis.llen(REQUESTS_PROCESSING)).toBe(0);

    // Matched drivers are on_trip and out of the available set.
    for (const { driver_id } of trips.rows) {
      expect(await redis.hget(driverKey(driver_id), 'status')).toBe('on_trip');
      expect(await redis.sismember(cellKey(C0), driver_id)).toBe(0);
    }
  });

  it('honest unmatched: zero candidates → status unmatched, counted and terminal', async () => {
    await insertRequests(['lonely']);
    expect(await newCore().matchRequest('lonely')).toBe('unmatched');
    const res = await pool.query(`SELECT status, matched_trip_id FROM ride_requests WHERE id = 'lonely'`);
    expect(res.rows[0]).toEqual({ status: 'unmatched', matched_trip_id: null });
    expect(cores[0]!.metrics.unmatchedTotal).toBe(1);
  });

  it('drivers claimed by someone else never surface; the remaining one matches', async () => {
    await seedDrivers(3);
    const core = newCore();
    // Two of three drivers already claimed by "someone else" — they left the
    // available set, so the candidate search never even offers them.
    expect(await core.claims.claimDriver('d0', 'other-trip', Date.now(), 10_000)).not.toBeNull();
    expect(await core.claims.claimDriver('d1', 'other-trip', Date.now(), 10_000)).not.toBeNull();

    await insertRequests(['r1']);
    expect(await core.matchRequest('r1')).toBe('matched');
    const trip = await pool.query('SELECT driver_id FROM trips');
    expect(trip.rows).toEqual([{ driver_id: 'd2' }]);
    expect(core.metrics.claimConflictsTotal).toBe(0);
  });

  // (The snapshot→claim race that increments claimConflictsTotal needs real
  // concurrency to stage honestly — it is asserted in the signature test.)

  it('startup reaper: stuck processing entries are re-queued and then processed', async () => {
    await seedDrivers(1);
    await insertRequests(['stuck']);
    // A dead matcher popped the id and died before acking.
    await redis.lpush(REQUESTS_PROCESSING, 'stuck');

    const core = newCore();
    await core.start(1); // recoverProcessing runs first
    await waitFor(async () => {
      const res = await pool.query<{ status: string }>(`SELECT status FROM ride_requests WHERE id = 'stuck'`);
      return res.rows[0]?.status === 'matched';
    });
    expect(await redis.llen(REQUESTS_PROCESSING)).toBe(0);
  });

  it('at-least-once redelivery is a counted skip, never a second trip', async () => {
    await seedDrivers(2);
    await insertRequests(['dup']);
    const core = newCore();
    expect(await core.matchRequest('dup')).toBe('matched');
    expect(await core.matchRequest('dup')).toBe('skipped');
    expect(core.metrics.requestsSkippedTotal).toBe(1);
    const trips = await pool.query('SELECT count(*)::int AS n FROM trips');
    expect(trips.rows[0]).toEqual({ n: 1 });
  });
});
