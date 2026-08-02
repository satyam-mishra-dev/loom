import { spawn, type ChildProcess } from 'node:child_process';
import type { AddressInfo } from 'node:net';
import { fileURLToPath } from 'node:url';
import { PostgreSqlContainer, type StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import { RedisContainer, type StartedRedisContainer } from '@testcontainers/redis';
import { Redis } from 'ioredis';
import type pg from 'pg';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { CLAIMS_BY_EXPIRY } from '@fleetline/core';
import { createPool, runMigrations } from '@fleetline/db';
import { Janitor, MatcherCore } from '@fleetline/matcher';
// Gateway and simulator are apps, not packages — reach into their sources the
// way their own tests do.
import { buildGateway, type Gateway } from '../apps/gateway/src/server.js';
import { signToken } from '../apps/gateway/src/auth.js';

/**
 * E2E: the whole system — real simulator process (seeded fleet that answers
 * offers and drives trips), real gateway (WS + pub/sub + reply lists), real
 * matcher + janitor — every request runs the full lifecycle to a terminal
 * state, every completed trip has its complete outbox chain, every driver is
 * freed.
 *
 * The city is small (800 m) and time compressed (speedup 25) so trips
 * complete in seconds; requests stop after 60 simulated seconds so the run
 * can drain.
 */

const ROOT = fileURLToPath(new URL('..', import.meta.url));
const SECRET = 'e2e-secret';
const DRIVERS = 200;

const COMPLETED_CHAIN = /^requested,matching,offered(,matching,offered)*,matched,en_route,in_trip,completed$/;
const CANCELLED_CHAIN = /^requested,matching(,offered,matching)+,cancelled$/;

async function waitFor(cond: () => Promise<boolean>, timeoutMs: number, what: string): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!(await cond())) {
    if (Date.now() > deadline) throw new Error(`waitFor timed out: ${what}`);
    await new Promise((r) => setTimeout(r, 200));
  }
}

describe('E2E lifecycle: simulator + gateway + matcher + janitor', () => {
  let redisContainer: StartedRedisContainer;
  let pgContainer: StartedPostgreSqlContainer;
  let redis: Redis;
  let pool: pg.Pool;
  let gateway: Gateway;
  let core: MatcherCore;
  let janitor: Janitor;
  let sim: ChildProcess | null = null;

  beforeAll(async () => {
    [redisContainer, pgContainer] = await Promise.all([
      new RedisContainer('redis:7-alpine').start(),
      new PostgreSqlContainer('postgres:16-alpine').start(),
    ]);
    redis = new Redis(redisContainer.getConnectionUrl());
    await runMigrations(pgContainer.getConnectionUri());
    pool = createPool(pgContainer.getConnectionUri());

    gateway = buildGateway({ redis, secret: SECRET, pool });
    await gateway.app.listen({ port: 0, host: '127.0.0.1' });
    core = new MatcherCore({ redis, pool });
    await core.start(4);
    janitor = new Janitor({ redis, pool, sweepIntervalMs: 500 });
    janitor.start();
  }, 240_000);

  afterAll(async () => {
    sim?.kill('SIGKILL');
    await janitor.stop();
    await core.stop();
    await gateway.app.close();
    await pool.end();
    redis.disconnect();
    await Promise.all([redisContainer.stop(), pgContainer.stop()]);
  });

  it(
    `${DRIVERS} seeded drivers: every request reaches a terminal state, every completed trip has a full outbox chain`,
    async () => {
      const port = (gateway.app.server.address() as AddressInfo).port;
      const url = `ws://127.0.0.1:${port}/ws?token=${signToken('sim', SECRET)}`;
      let stderr = '';
      sim = spawn(
        process.execPath,
        // prettier-ignore
        [
          '--import', 'tsx', 'apps/simulator/src/main.ts',
          '--sink', 'ws', '--gateway', url,
          '--drivers', String(DRIVERS), '--rps', '2', '--hotspots', '2',
          '--seed', '42', '--speedup', '25', '--city-m', '800',
          '--accept-prob', '0.8', '--request-ticks', '60', '--ticks', '0',
        ],
        { cwd: ROOT, stdio: ['ignore', 'ignore', 'pipe'] },
      );
      sim.stderr?.on('data', (chunk: Buffer) => (stderr += String(chunk)));

      // Requests flow in for the first 60 simulated seconds…
      await waitFor(
        async () => {
          const res = await pool.query<{ n: number }>('SELECT count(*)::int AS n FROM ride_requests');
          return (res.rows[0]?.n ?? 0) >= 50;
        },
        60_000,
        'requests arriving',
      );

      // …then the whole system drains: every request terminal, every trip
      // terminal (the simulator keeps ticking so in-flight trips finish).
      await waitFor(
        async () => {
          const res = await pool.query<{ open: number; trips_open: number }>(
            `SELECT
               count(*) FILTER (WHERE status NOT IN ('matched', 'unmatched'))::int AS open,
               (SELECT count(*) FROM trips WHERE status NOT IN ('completed', 'cancelled'))::int AS trips_open
             FROM ride_requests`,
          );
          return res.rows[0]?.open === 0 && res.rows[0]?.trips_open === 0;
        },
        120_000,
        'all requests and trips terminal',
      );

      const totals = await pool.query<{ status: string; n: number }>(
        'SELECT status, count(*)::int AS n FROM ride_requests GROUP BY status',
      );
      const requestCounts = Object.fromEntries(totals.rows.map((r) => [r.status, r.n]));
      const matched = requestCounts['matched'] ?? 0;
      expect(matched).toBeGreaterThan(0);

      // Outbox completeness: every terminal trip carries its entire history.
      const chains = await pool.query<{ id: string; status: string; chain: string }>(
        `SELECT t.id, t.status,
                (SELECT string_agg(e.type, ',' ORDER BY e.id) FROM trip_events e WHERE e.trip_id = t.id) AS chain
         FROM trips t`,
      );
      expect(chains.rows.length).toBeGreaterThan(0);
      for (const { id, status, chain } of chains.rows) {
        if (status === 'completed') expect(chain, `trip ${id}`).toMatch(COMPLETED_CHAIN);
        else expect(chain, `trip ${id}`).toMatch(CANCELLED_CHAIN);
      }
      const completed = chains.rows.filter((t) => t.status === 'completed').length;
      expect(completed).toBe(matched);

      // Every driver freed: nobody left claimed or on_trip, no claims, no
      // expiry-ZSET entries, and the never-happens counters never happened.
      const stuck: string[] = [];
      for (let i = 0; i < DRIVERS; i++) {
        const status = await redis.hget(`driver:d${i}`, 'status');
        if (status === 'claimed' || status === 'on_trip') stuck.push(`d${i}:${status ?? 'gone'}`);
      }
      expect(stuck).toEqual([]);
      expect(await redis.keys('claim:*')).toEqual([]);
      expect(await redis.zcard(CLAIMS_BY_EXPIRY)).toBe(0);
      expect(core.metrics.pgUniqueViolationsTotal).toBe(0);
      expect(core.metrics.confirmFailuresTotal).toBe(0);
      expect(core.metrics.tripsCompletedTotal).toBe(completed);

      // The invariant, one last time, in SQL.
      const dup = await pool.query(
        `SELECT driver_id FROM trips WHERE status IN ('matched', 'en_route', 'in_trip')
         GROUP BY driver_id HAVING count(*) > 1`,
      );
      expect(dup.rows).toEqual([]);

      // Shut the simulator down gracefully and pull its own accounting.
      const exited = new Promise<number | null>((resolve) => sim?.once('exit', resolve));
      sim?.kill('SIGTERM');
      expect(await exited).toBe(0);
      const done = /simulator done (\{.*\})/.exec(stderr);
      expect(done, stderr.slice(-2000)).not.toBeNull();
      const stats = JSON.parse(done![1]!) as Record<string, number>;
      expect(stats['offers']).toBeGreaterThan(0);
      expect(stats['tripsAssigned']).toBe(matched);
      expect(stats['tripsCompleted']).toBe(completed);
      console.log('e2e:', { requests: requestCounts, completed, sim: stats });
    },
    240_000,
  );
});
