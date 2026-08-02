import { spawn, type ChildProcess } from 'node:child_process';
import { writeFileSync } from 'node:fs';
import type { AddressInfo } from 'node:net';
import { performance } from 'node:perf_hooks';
import { fileURLToPath } from 'node:url';
import { PostgreSqlContainer, type StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import { RedisContainer, type StartedRedisContainer } from '@testcontainers/redis';
import { Redis } from 'ioredis';
import { GeoIndex, HEARTBEAT_ZSET, cellFor } from '@fleetline/core';
import { createPool, runMigrations } from '@fleetline/db';
import { Janitor, MatcherCore } from '@fleetline/matcher';
import { buildGateway } from '../apps/gateway/src/server.js';
import { signToken } from '../apps/gateway/src/auth.js';

/**
 * scripts/bench.ts — the load bench (§5.9). Boots the REAL stack (real Redis +
 * Postgres via testcontainers, in-process gateway + matcher + janitor) and
 * drives it with the REAL seeded simulator at increasing RPS. Latency is
 * request→match measured from Postgres event timestamps — ride_requests.
 * created_at (gateway insert) to the trip's `matched` outbox row created_at
 * (matcher commit), same DB clock, no synthetic stopwatch. Drivers auto-accept
 * with ~0 think time (--response-*-ms 0) so the number is the ENGINE's, not
 * simulated human delay.
 *
 * Also: one Redis-FLUSHALL-mid-stream self-heal measurement, and the geo
 * candidate-search timing. Output: JSON (scripts/bench-results.json) + a table.
 *
 * Reproducible: fixed seed. Run: `npm run bench`. Needs Docker (testcontainers).
 */

const ROOT = fileURLToPath(new URL('..', import.meta.url));
const SECRET = 'bench-secret';
const SEED = 42;
const DRIVERS = 5_000;
const RPS_LEVELS = [10, 25, 50, 100];
const DURATION_S = 12; // steady-state measurement window per level, wall seconds (speedup 1)
// Warm-up before measuring: the fleet must be fully indexed AND every driver
// channel subscribed on the gateway AND any cold-start offer-timeout backlog
// cleared (a driver whose channel isn't subscribed yet drops its offer and
// only recovers on the 8s offer TTL). Measuring before that captures the drain,
// not the engine. 12s comfortably exceeds the 8s offer TTL.
const WARMUP_MS = 12_000;
const CENTER = { lat: 37.7749, lng: -122.4194 };

interface LevelResult {
  rps: number;
  requests: number;
  matched: number;
  unmatched: number;
  unmatchedRate: number;
  matchesPerSec: number;
  p50Ms: number;
  p95Ms: number;
  p99Ms: number;
}

interface BenchOutput {
  meta: { seed: number; drivers: number; durationSec: number; at: string };
  levels: LevelResult[];
  geo: { candidateSearchP50Ms: number; candidateSearchP99Ms: number; samples: number };
  selfHeal: { flushallToRebuiltMs: number; drivers: number };
}

function percentile(sortedAsc: number[], q: number): number {
  if (sortedAsc.length === 0) return 0;
  return sortedAsc[Math.min(sortedAsc.length - 1, Math.floor(q * sortedAsc.length))] ?? 0;
}

async function waitFor(cond: () => Promise<boolean>, timeoutMs: number, what: string): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!(await cond())) {
    if (Date.now() > deadline) throw new Error(`bench waitFor timed out: ${what}`);
    await new Promise((r) => setTimeout(r, 200));
  }
}

function spawnSim(url: string, args: string[]): ChildProcess {
  return spawn(
    process.execPath,
    ['--import', 'tsx', 'apps/simulator/src/main.ts', '--sink', 'ws', '--gateway', url, ...args],
    { cwd: ROOT, stdio: ['ignore', 'ignore', 'pipe'] },
  );
}

async function main(): Promise<void> {
  process.stderr.write('bench: starting Postgres + Redis containers…\n');
  const [redisContainer, pgContainer] = await Promise.all<StartedRedisContainer | StartedPostgreSqlContainer>([
    new RedisContainer('redis:7-alpine').start(),
    new PostgreSqlContainer('postgres:16-alpine').start(),
  ]);
  const redis = new Redis((redisContainer as StartedRedisContainer).getConnectionUrl());
  await runMigrations((pgContainer as StartedPostgreSqlContainer).getConnectionUri());
  const pool = createPool((pgContainer as StartedPostgreSqlContainer).getConnectionUri());

  const gateway = buildGateway({
    redis,
    secret: SECRET,
    pool,
    // Generous so the limiter never colours the bench numbers.
    rateLimit: { limit: 1_000_000, windowMs: 1_000, burst: 2_000_000 },
  });
  await gateway.app.listen({ port: 0, host: '127.0.0.1' });
  const port = (gateway.app.server.address() as AddressInfo).port;
  const url = `ws://127.0.0.1:${port}/ws?token=${signToken('sim', SECRET)}`;

  const matcher = new MatcherCore({ redis, pool });
  await matcher.start(8);
  const janitor = new Janitor({ redis, pool, sweepIntervalMs: 1_000 });
  janitor.start();

  const levels: LevelResult[] = [];
  try {
    for (const rps of RPS_LEVELS) {
      process.stderr.write(`bench: RPS ${rps} …\n`);
      await redis.flushall();
      await pool.query('TRUNCATE trip_events, ride_requests, trips CASCADE');

      const sim = spawnSim(url, [
        '--drivers', String(DRIVERS),
        '--rps', String(rps),
        '--hotspots', '0',
        '--seed', String(SEED),
        '--speedup', '1',
        '--accept-prob', '1',
        '--response-min-ms', '0',
        '--response-max-ms', '0',
        '--request-ticks', '600', // requests flow through warm-up + window; sim stays up
        '--ticks', '0',
      ]);

      // Warm up: fleet fully indexed, channel subscriptions drained, cold-start
      // backlog cleared. Then measure a steady-state window bounded by the DB
      // clock (so warm-up requests are excluded, no truncation mid-run).
      await waitFor(
        async () => (await redis.zcard(HEARTBEAT_ZSET)) >= DRIVERS * 0.98,
        30_000,
        `fleet up at rps ${rps}`,
      );
      await new Promise((r) => setTimeout(r, WARMUP_MS));
      const measureStart = (await pool.query<{ t: Date }>('SELECT now() AS t')).rows[0]!.t;
      await new Promise((r) => setTimeout(r, DURATION_S * 1000));
      const measureEnd = (await pool.query<{ t: Date }>('SELECT now() AS t')).rows[0]!.t;

      // Let the window's own requests settle while the sim is still answering
      // offers (kill it only after — a dead sim would time out in-flight offers).
      await waitFor(
        async () => {
          const r = await pool.query<{ open: number }>(
            `SELECT count(*) FILTER (WHERE status NOT IN ('matched', 'unmatched'))::int AS open
             FROM ride_requests WHERE created_at BETWEEN $1 AND $2`,
            [measureStart, measureEnd],
          );
          return r.rows[0]?.open === 0;
        },
        60_000,
        `drain window at rps ${rps}`,
      );
      sim.kill('SIGKILL');

      const lat = await pool.query<{ ms: number }>(
        `SELECT EXTRACT(EPOCH FROM (e.created_at - r.created_at)) * 1000 AS ms
         FROM ride_requests r
         JOIN trips t ON t.request_id = r.id
         JOIN trip_events e ON e.trip_id = t.id AND e.type = 'matched'
         WHERE r.status = 'matched' AND r.created_at BETWEEN $1 AND $2`,
        [measureStart, measureEnd],
      );
      const counts = await pool.query<{ status: string; n: number }>(
        `SELECT status, count(*)::int AS n FROM ride_requests
         WHERE created_at BETWEEN $1 AND $2 GROUP BY status`,
        [measureStart, measureEnd],
      );
      const byStatus = Object.fromEntries(counts.rows.map((c) => [c.status, c.n]));
      const matched = byStatus['matched'] ?? 0;
      const unmatched = byStatus['unmatched'] ?? 0;
      const spanSec = DURATION_S;
      const samples = lat.rows.map((r) => Number(r.ms)).sort((a, b) => a - b);

      levels.push({
        rps,
        requests: matched + unmatched,
        matched,
        unmatched,
        unmatchedRate: matched + unmatched > 0 ? Number((unmatched / (matched + unmatched)).toFixed(3)) : 0,
        matchesPerSec: Number((matched / spanSec).toFixed(1)),
        p50Ms: Number(percentile(samples, 0.5).toFixed(2)),
        p95Ms: Number(percentile(samples, 0.95).toFixed(2)),
        p99Ms: Number(percentile(samples, 0.99).toFixed(2)),
      });
    }

    // ---- geo candidate-search timing + self-heal (fleet streaming, no requests) ----
    process.stderr.write('bench: geo candidate search + self-heal …\n');
    await redis.flushall();
    await pool.query('TRUNCATE trip_events, ride_requests, trips CASCADE');
    const geoSim = spawnSim(url, [
      '--drivers', String(DRIVERS),
      '--rps', '0',
      '--seed', String(SEED),
      '--speedup', '4',
      '--ticks', '0',
    ]);
    await waitFor(
      async () => (await redis.zcard(HEARTBEAT_ZSET)) >= DRIVERS,
      60_000,
      'fleet fully indexed',
    );

    const geoIndex = new GeoIndex(redis);
    const searchMs: number[] = [];
    for (let i = 0; i < 500; i++) {
      const lat = CENTER.lat + (Math.random() * 2 - 1) * 0.03;
      const lng = CENTER.lng + (Math.random() * 2 - 1) * 0.03;
      const t = performance.now();
      await geoIndex.findCandidates(lat, lng, { need: 8, maxK: 3, nowMs: Date.now() });
      searchMs.push(performance.now() - t);
    }
    searchMs.sort((a, b) => a - b);
    void cellFor(CENTER.lat, CENTER.lng); // (imported for parity with the intake path)

    // Self-heal: wipe the whole keyspace mid-stream, time the rebuild from the
    // continuing pings back to a whole index.
    const t0 = Date.now();
    await redis.flushall();
    await waitFor(async () => (await redis.zcard(HEARTBEAT_ZSET)) >= DRIVERS, 30_000, 'index self-heal');
    const selfHealMs = Date.now() - t0;
    geoSim.kill('SIGKILL');

    const out: BenchOutput = {
      meta: { seed: SEED, drivers: DRIVERS, durationSec: DURATION_S, at: new Date().toISOString() },
      levels,
      geo: {
        candidateSearchP50Ms: Number(percentile(searchMs, 0.5).toFixed(3)),
        candidateSearchP99Ms: Number(percentile(searchMs, 0.99).toFixed(3)),
        samples: searchMs.length,
      },
      selfHeal: { flushallToRebuiltMs: selfHealMs, drivers: DRIVERS },
    };

    writeFileSync(new URL('./bench-results.json', import.meta.url), JSON.stringify(out, null, 2));
    printTable(out);
  } finally {
    await janitor.stop();
    await matcher.stop();
    await gateway.app.close();
    await pool.end();
    redis.disconnect();
    await Promise.all([
      (redisContainer as StartedRedisContainer).stop(),
      (pgContainer as StartedPostgreSqlContainer).stop(),
    ]);
  }
}

function printTable(out: BenchOutput): void {
  const rows = out.levels.map((l) => ({
    rps: l.rps,
    reqs: l.requests,
    matched: l.matched,
    'matches/s': l.matchesPerSec,
    'unmatched%': (l.unmatchedRate * 100).toFixed(1),
    p50ms: l.p50Ms,
    p95ms: l.p95Ms,
    p99ms: l.p99Ms,
  }));
  process.stdout.write('\n=== Fleetline bench — request→match latency (real stack, seeded) ===\n');
  console.table(rows);
  process.stdout.write(
    `geo candidate search: p50 ${out.geo.candidateSearchP50Ms}ms  p99 ${out.geo.candidateSearchP99Ms}ms (${out.geo.samples} searches)\n`,
  );
  process.stdout.write(
    `index self-heal after FLUSHALL: ${out.selfHeal.flushallToRebuiltMs}ms (${out.selfHeal.drivers} drivers from live pings)\n`,
  );
  process.stdout.write('written: scripts/bench-results.json\n');
}

main().then(
  () => process.exit(0),
  (err: unknown) => {
    process.stderr.write(`bench failed: ${String(err)}\n`);
    process.exit(1);
  },
);
