import { spawn, type ChildProcess } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { PostgreSqlContainer, type StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import { RedisContainer, type StartedRedisContainer } from '@testcontainers/redis';
import { Redis } from 'ioredis';
import type pg from 'pg';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import {
  CLAIMS_BY_EXPIRY,
  GeoIndex,
  REQUESTS_QUEUE,
  cellFor,
  cellKey,
  driverKey,
  offerReplyKey,
  type GeoPing,
} from '@fleetline/core';
import { createPool, runMigrations } from '@fleetline/db';

/**
 * THE SIGNATURE TEST, CRASH VARIANT — a matcher SIGKILLed mid-cascade
 * strands nobody, because the claim TTL lives in the DATA, not the process.
 *
 * A REAL matcher process (tsx apps/matcher/src/main.ts) runs against real
 * stores. Drivers stay silent so offers hang; once the matcher holds live
 * claims with offers pending, it is SIGKILLed. A janitor-ONLY process (not a
 * matcher) then proves the sweep needs no matcher alive: every claimed
 * driver returns to available, every orphaned OFFERED trip reverts to
 * MATCHING, every request re-enqueues. A fresh matcher then completes the
 * matching. The one-active-trip-per-driver invariant is polled throughout
 * and must never be violated.
 */

const DRIVERS = 10;
const REQUESTS = 6;
const CONSUMERS = 4; // in-flight cascades when the axe falls
const OFFER_TTL_MS = 4_000;
const CLAIM_TTL_MS = 6_000;

const ROOT = fileURLToPath(new URL('..', import.meta.url));

const ORIGIN = cellFor(37.7749, -122.4194);
const CENTER = { lat: 37.7749, lng: -122.4194 };

async function waitFor(cond: () => boolean | Promise<boolean>, timeoutMs: number, what: string): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!(await cond())) {
    if (Date.now() > deadline) throw new Error(`waitFor timed out: ${what}`);
    await new Promise((r) => setTimeout(r, 50));
  }
}

describe('SIGNATURE (crash variant): kill the matcher mid-cascade', () => {
  let redisContainer: StartedRedisContainer;
  let pgContainer: StartedPostgreSqlContainer;
  let redis: Redis;
  let fleet: Redis;
  let pool: pg.Pool;
  const children: ChildProcess[] = [];
  let accepting = false;

  beforeAll(async () => {
    [redisContainer, pgContainer] = await Promise.all([
      new RedisContainer('redis:7-alpine').start(),
      new PostgreSqlContainer('postgres:16-alpine').start(),
    ]);
    redis = new Redis(redisContainer.getConnectionUrl());
    await runMigrations(pgContainer.getConnectionUri());
    pool = createPool(pgContainer.getConnectionUri());

    // The fake fleet: silent while `accepting` is false (offers hang until
    // the offer TTL), accepts instantly once flipped.
    fleet = redis.duplicate();
    fleet.on('pmessage', (_p: string, _c: string, message: string) => {
      if (!accepting) return;
      const msg = JSON.parse(message) as { type: string; offerId?: string };
      if (msg.type === 'offer' && msg.offerId !== undefined) {
        void redis.lpush(offerReplyKey(msg.offerId), JSON.stringify({ accept: true }));
      }
    });
    await fleet.psubscribe('driver:*:msg');
  }, 240_000);

  afterAll(async () => {
    for (const child of children) child.kill('SIGKILL');
    await pool.end();
    fleet.disconnect();
    redis.disconnect();
    await Promise.all([redisContainer.stop(), pgContainer.stop()]);
  });

  // node --import tsx, NOT the tsx CLI: the CLI is a wrapper that re-spawns
  // node, so SIGKILLing it would orphan the real service instead of killing
  // it — and this test exists to kill the real service.
  function spawnService(script: string, env: Record<string, string>): ChildProcess {
    const child = spawn(process.execPath, ['--import', 'tsx', script], {
      cwd: ROOT,
      env: {
        ...process.env,
        REDIS_URL: redisContainer.getConnectionUrl(),
        DATABASE_URL: pgContainer.getConnectionUri(),
        ...env,
      },
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    child.stdout?.on('data', (chunk: Buffer) => process.stdout.write(`[${script}] ${String(chunk)}`));
    child.stderr?.on('data', (chunk: Buffer) => process.stdout.write(`[${script}!] ${String(chunk)}`));
    children.push(child);
    return child;
  }

  function spawnMatcher(port: number): ChildProcess {
    return spawnService('apps/matcher/src/main.ts', {
      PORT: String(port),
      MATCHER_CONSUMERS: String(CONSUMERS),
      OFFER_TTL_MS: String(OFFER_TTL_MS),
      CLAIM_TTL_MS: String(CLAIM_TTL_MS),
      JANITOR_SWEEP_MS: '500',
    });
  }

  async function activeTripViolations(): Promise<number> {
    const res = await pool.query(
      `SELECT driver_id FROM trips WHERE status IN ('matched', 'en_route', 'in_trip')
       GROUP BY driver_id HAVING count(*) > 1`,
    );
    return res.rowCount ?? 0;
  }

  it(
    'claims released, OFFERED trips re-enqueued, restarted matcher completes — invariant never violated',
    async () => {
      // Live fleet: seeded once, heartbeats refreshed continuously (a claimed
      // driver keeps pinging — that must never resurrect it into available).
      const geo = new GeoIndex(redis);
      const pings: GeoPing[] = Array.from({ length: DRIVERS }, (_, i) => ({ driverId: `d${i}`, ...CENTER }));
      await geo.applyPings(pings, Date.now());
      const heartbeats = setInterval(() => void geo.applyPings(pings, Date.now()), 2_000);

      // The invariant monitor: polled the entire run.
      let violations = 0;
      const monitor = setInterval(() => {
        void activeTripViolations().then((n) => {
          violations += n;
        });
      }, 100);

      try {
        const ids = Array.from({ length: REQUESTS }, (_, i) => `req-${i}`);
        await pool.query(
          `INSERT INTO ride_requests (id, lat, lng, dest_lat, dest_lng, status)
           SELECT unnest($1::text[]), $2, $3, $2, $3, 'pending'`,
          [ids, CENTER.lat, CENTER.lng],
        );
        await redis.lpush(REQUESTS_QUEUE, ...ids);

        // Phase 1: real matcher, silent drivers → live claims, offers pending.
        const matcher = spawnMatcher(18_090);
        await waitFor(async () => (await redis.keys('claim:*')).length === CONSUMERS, 30_000, 'claims held');
        // A consumer takes the Redis claim BEFORE it commits the 'offered' trip
        // row, so wait for Postgres to catch up too — otherwise the kill can fire
        // while some claims still lack their committed row and the precondition
        // below counts fewer than CONSUMERS (a setup race, not an invariant break).
        await waitFor(
          async () =>
            (
              await pool.query<{ n: number }>(
                `SELECT count(*)::int AS n FROM trips WHERE status = 'offered'`,
              )
            ).rows[0]?.n === CONSUMERS,
          30_000,
          'offered trips committed',
        );

        // The axe. No shutdown hooks, no cleanup — the process is just gone.
        matcher.kill('SIGKILL');
        await new Promise((r) => setTimeout(r, 200));

        // The wreckage is exactly what "TTL in the data" is for: claims and
        // OFFERED trips survive their owner.
        expect((await redis.keys('claim:*')).length).toBe(CONSUMERS);
        expect(await redis.zcard(CLAIMS_BY_EXPIRY)).toBe(CONSUMERS);
        const offered = await pool.query<{ n: number }>(
          `SELECT count(*)::int AS n FROM trips WHERE status = 'offered'`,
        );
        expect(offered.rows[0]?.n).toBe(CONSUMERS);

        // Phase 2: a janitor-ONLY process sweeps — no matcher exists at all.
        const janitor = spawnService('apps/matcher/src/janitor-main.ts', { JANITOR_SWEEP_MS: '500' });
        await waitFor(
          async () =>
            (await redis.keys('claim:*')).length === 0 && (await redis.zcard(CLAIMS_BY_EXPIRY)) === 0,
          CLAIM_TTL_MS + 15_000,
          'janitor released all claims',
        );

        // Every claimed driver is back available in its cell; no orphaned
        // OFFERED trips remain (reverted to matching); every request is
        // pending again and back on the queue.
        for (let i = 0; i < DRIVERS; i++) {
          expect(await redis.hget(driverKey(`d${i}`), 'status')).toBe('available');
        }
        expect((await redis.smembers(cellKey(ORIGIN))).sort()).toHaveLength(DRIVERS);
        expect(
          (await pool.query(`SELECT count(*)::int AS n FROM trips WHERE status = 'offered'`)).rows[0],
        ).toEqual({ n: 0 });
        expect(
          (await pool.query(`SELECT count(*)::int AS n FROM trips WHERE status = 'matching'`)).rows[0],
        ).toEqual({ n: CONSUMERS });
        expect(
          (await pool.query(`SELECT count(*)::int AS n FROM ride_requests WHERE status = 'pending'`)).rows[0],
        ).toEqual({ n: REQUESTS });
        expect((await redis.llen(REQUESTS_QUEUE)) >= REQUESTS - CONSUMERS).toBe(true);
        janitor.kill('SIGTERM');

        // Phase 3: drivers now answer; a fresh matcher finishes the job —
        // including the requests the dead one left in requests:processing.
        accepting = true;
        const restarted = spawnMatcher(18_091);
        // Gate on the TERMINAL trip state the assertions below check, not on the
        // upstream ride_requests='matched' write: the match commits in the order
        // request→matched, confirmClaim (claim deleted, driver on_trip), then
        // trip matched→en_route LAST. Waiting on 'matched' can fire before the
        // claim/on_trip/en_route writes land, flaking the en_route assertion.
        await waitFor(
          async () => {
            const res = await pool.query<{ n: number }>(
              `SELECT count(*)::int AS n FROM trips WHERE status = 'en_route'`,
            );
            return res.rows[0]?.n === REQUESTS;
          },
          30_000,
          'restarted matcher drove every trip en_route',
        );

        // Final state: every request matched to a unique driver, full outbox
        // chains, Redis and Postgres agreeing, defense-in-depth index silent.
        const trips = await pool.query<{ driver_id: string; status: string }>(
          `SELECT driver_id, status FROM trips WHERE status != 'cancelled'`,
        );
        expect(trips.rows).toHaveLength(REQUESTS);
        expect(trips.rows.every((t) => t.status === 'en_route')).toBe(true);
        expect(new Set(trips.rows.map((t) => t.driver_id)).size).toBe(REQUESTS);
        for (const { driver_id } of trips.rows) {
          expect(await redis.hget(driverKey(driver_id), 'status')).toBe('on_trip');
        }
        expect(await redis.keys('claim:*')).toEqual([]);
        expect(await redis.zcard(CLAIMS_BY_EXPIRY)).toBe(0);

        const metrics = await (await fetch('http://127.0.0.1:18091/metrics')).text();
        expect(metrics).toContain('pg_unique_violations_total 0');
        restarted.kill('SIGTERM');
      } finally {
        clearInterval(heartbeats);
        clearInterval(monitor);
      }

      expect(violations, 'one-active-trip-per-driver was violated during the run').toBe(0);
      expect(await activeTripViolations()).toBe(0);
    },
    180_000,
  );
});
