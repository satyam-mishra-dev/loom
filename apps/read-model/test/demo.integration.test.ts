import { PostgreSqlContainer, type StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import { RedisContainer, type StartedRedisContainer } from '@testcontainers/redis';
import { Redis } from 'ioredis';
import type pg from 'pg';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { createPool, runMigrations } from '@fleetline/db';
import { Janitor } from '@fleetline/matcher';
import {
  cleanupCrash,
  crashSnapshot,
  injectAbandonedClaim,
  runCrashDemo,
  runProof,
  tripHistory,
  type Deps,
} from '../src/demo.js';

// The dashboard's signature controls, exercised against real testcontainers
// Redis + Postgres — the same engine the live stack runs, no mocks.

const CENTER = { lat: 37.7749, lng: -122.4194 };

describe('demo mechanisms (testcontainers redis + postgres)', () => {
  let redisContainer: StartedRedisContainer;
  let pgContainer: StartedPostgreSqlContainer;
  let redis: Redis;
  let pool: pg.Pool;
  let deps: Deps;

  beforeAll(async () => {
    [redisContainer, pgContainer] = await Promise.all([
      new RedisContainer('redis:7-alpine').start(),
      new PostgreSqlContainer('postgres:16-alpine').start(),
    ]);
    redis = new Redis(redisContainer.getConnectionUrl());
    await runMigrations(pgContainer.getConnectionUri());
    pool = createPool(pgContainer.getConnectionUri());
    deps = { redis, pool, center: CENTER };
  }, 240_000);

  afterAll(async () => {
    await pool.end();
    redis.disconnect();
    await Promise.all([redisContainer.stop(), pgContainer.stop()]);
  });

  beforeEach(async () => {
    await redis.flushall();
    await pool.query('TRUNCATE trip_events, ride_requests, trips CASCADE');
  });

  it('Proof Mode: 20 drivers, 200 concurrent requests → exactly 20 matched, 0 double-assignments', async () => {
    const proof = await runProof(deps);

    expect(proof.drivers).toBe(20);
    expect(proof.requests).toBe(200);
    expect(proof.matched).toBe(20);
    expect(proof.unmatched).toBe(180);
    expect(proof.doubleAssignments).toBe(0);
    expect(proof.pgUniqueViolations).toBe(0);
    // The race was real: losing claim attempts actually collided.
    expect(proof.claimConflicts).toBeGreaterThan(0);

    // It cleaned up after itself — no proof rows or drivers linger.
    const rows = await pool.query<{ n: number }>('SELECT count(*)::int AS n FROM ride_requests');
    expect(rows.rows[0]?.n).toBe(0);
    expect(await pool.query<{ n: number }>('SELECT count(*)::int AS n FROM trips').then((r) => r.rows[0]?.n)).toBe(0);
    expect(await redis.zcard('drivers:by-heartbeat')).toBe(0);
  }, 120_000);

  it('Crash the matcher: abandoned claim is real wreckage the janitor recovers, invariant never flips', async () => {
    const handle = await injectAbandonedClaim(deps);

    // The wreckage a SIGKILLed matcher leaves: claim held, driver claimed,
    // trip orphaned 'offered', request stuck 'matching' — invariant intact.
    const before = await crashSnapshot(deps, handle);
    expect(before).toMatchObject({
      claimPresent: true,
      driverStatus: 'claimed',
      tripStatus: 'offered',
      requestStatus: 'matching',
      doubleAssignments: 0,
    });

    // A real janitor (no matcher alive) sweeps the expired claim.
    const janitor = new Janitor({ redis, pool });
    let after = before;
    for (let i = 0; i < 10; i++) {
      await janitor.sweepOnce();
      after = await crashSnapshot(deps, handle);
      expect(after.doubleAssignments).toBe(0); // never flips, mid-recovery
      if (!after.claimPresent && after.driverStatus === 'available') break;
    }

    // Recovered: claim released, driver back available, trip reverted to
    // matching, request re-enqueued pending.
    expect(after).toMatchObject({
      claimPresent: false,
      driverStatus: 'available',
      tripStatus: 'matching',
      requestStatus: 'pending',
    });
    expect(await redis.lrange('requests:queue', 0, -1)).toContain(handle.requestId);

    await cleanupCrash(deps, handle);
    const gone = await crashSnapshot(deps, handle);
    expect(gone).toMatchObject({ claimPresent: false, tripStatus: null, requestStatus: null, driverStatus: null });
    expect(await redis.lrange('requests:queue', 0, -1)).not.toContain(handle.requestId);
  }, 60_000);

  it('Crash demo (route path): a running janitor recovers the fault, recovered=true, invariant held', async () => {
    const janitor = new Janitor({ redis, pool, sweepIntervalMs: 300 });
    janitor.start();
    try {
      const result = await runCrashDemo(deps, 12_000);
      expect(result.before.claimPresent).toBe(true);
      expect(result.sweptClaim).toBe(true);
      expect(result.driverFreed).toBe(true);
      expect(result.orphanCleared).toBe(true);
      expect(result.recovered).toBe(true);
      expect(result.recoveryMs).not.toBeNull();
      expect(result.invariantBreaches).toBe(0);
    } finally {
      await janitor.stop();
    }
  }, 30_000);

  it('Trip inspector: returns the real event history, offer cascade and surge', async () => {
    // Seed a trip that declined one offer then accepted the next (request row
    // first, then the trip, then link matched_trip_id — the FK order).
    await pool.query(
      `INSERT INTO ride_requests (id, lat, lng, dest_lat, dest_lng, status)
       VALUES ('rq1', $1, $2, $3, $4, 'matched')`,
      [CENTER.lat, CENTER.lng, CENTER.lat + 0.01, CENTER.lng + 0.01],
    );
    await pool.query(
      `INSERT INTO trips (id, request_id, driver_id, rider_lat, rider_lng, status, claim_token, offer_id)
       VALUES ('tr1', 'rq1', 'dB', $1, $2, 'en_route', 'tok', 'off2')`,
      [CENTER.lat, CENTER.lng],
    );
    await pool.query(`UPDATE ride_requests SET matched_trip_id = 'tr1' WHERE id = 'rq1'`);
    await pool.query(
      `INSERT INTO trip_events (trip_id, type, payload) VALUES
        ('tr1','requested','{"requestId":"rq1"}'),
        ('tr1','matching','{"event":"MATCHING_STARTED"}'),
        ('tr1','offered','{"event":"OFFER_SENT","driverId":"dA","offerId":"off1"}'),
        ('tr1','matching','{"event":"OFFER_DECLINED","driverId":"dA","offerId":"off1"}'),
        ('tr1','offered','{"event":"OFFER_SENT","driverId":"dB","offerId":"off2"}'),
        ('tr1','matched','{"event":"OFFER_ACCEPTED","driverId":"dB","offerId":"off2"}'),
        ('tr1','en_route','{"event":"DRIVER_EN_ROUTE","driverId":"dB"}')`,
    );

    const h = await tripHistory(deps, 'tr1');
    expect(h).not.toBeNull();
    expect(h?.status).toBe('en_route');
    expect(h?.driverId).toBe('dB');
    expect(h?.dest).toMatchObject({ lat: CENTER.lat + 0.01, lng: CENTER.lng + 0.01 });
    // Ordered event chain.
    expect(h?.events.map((e) => e.type)).toEqual([
      'requested',
      'matching',
      'offered',
      'matching',
      'offered',
      'matched',
      'en_route',
    ]);
    // The cascade: first offer declined, second accepted.
    expect(h?.offers).toEqual([
      { driverId: 'dA', offerId: 'off1', result: 'declined' },
      { driverId: 'dB', offerId: 'off2', result: 'accepted' },
    ]);
    // No live surge published for this cell → multiplier 1.
    expect(h?.surgeMultiplier).toBe(1);

    expect(await tripHistory(deps, 'nope')).toBeNull();
  });
});
