import http from 'node:http';
import type { AddressInfo } from 'node:net';
import { PostgreSqlContainer, type StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import { RedisContainer, type StartedRedisContainer } from '@testcontainers/redis';
import { cellToLatLng } from 'h3-js';
import { Redis } from 'ioredis';
import type pg from 'pg';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { GeoIndex, SurgeStore, cellFor, type GeoPing } from '@fleetline/core';
import { createPool, runMigrations } from '@fleetline/db';
import { buildReadModel, type Snapshot } from '../src/server.js';

// Real read model against testcontainers Redis + Postgres: the whole snapshot
// fan-in (Redis positions, Postgres trips, surge) plus the SSE stream, the
// /spawn control, and the matcher-metrics scrape.

const CELL = cellFor(37.7749, -122.4194);
const [LAT, LNG] = cellToLatLng(CELL);

async function collectSse(url: string, wantFrames: number, timeoutMs = 4000): Promise<Snapshot[]> {
  return new Promise((resolve, reject) => {
    const frames: Snapshot[] = [];
    const req = http.get(url, (res) => {
      let buf = '';
      res.on('data', (chunk: Buffer) => {
        buf += chunk.toString();
        let idx: number;
        while ((idx = buf.indexOf('\n\n')) >= 0) {
          const raw = buf.slice(0, idx);
          buf = buf.slice(idx + 2);
          if (raw.startsWith('data: ')) {
            frames.push(JSON.parse(raw.slice('data: '.length)) as Snapshot);
            if (frames.length >= wantFrames) {
              req.destroy();
              resolve(frames);
              return;
            }
          }
        }
      });
    });
    req.on('error', (err) => {
      if (frames.length >= wantFrames) resolve(frames);
      else reject(err);
    });
    setTimeout(() => {
      req.destroy();
      if (frames.length >= wantFrames) resolve(frames);
      else reject(new Error('sse timeout'));
    }, timeoutMs).unref();
  });
}

describe('read model (testcontainers redis + postgres)', () => {
  let redisContainer: StartedRedisContainer;
  let pgContainer: StartedPostgreSqlContainer;
  let redis: Redis;
  let pool: pg.Pool;
  let geo: GeoIndex;
  let surge: SurgeStore;

  beforeAll(async () => {
    [redisContainer, pgContainer] = await Promise.all([
      new RedisContainer('redis:7-alpine').start(),
      new PostgreSqlContainer('postgres:16-alpine').start(),
    ]);
    redis = new Redis(redisContainer.getConnectionUrl());
    await runMigrations(pgContainer.getConnectionUri());
    pool = createPool(pgContainer.getConnectionUri());
    geo = new GeoIndex(redis);
    surge = new SurgeStore(redis);
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

  async function seedFleet(n: number): Promise<void> {
    const pings: GeoPing[] = Array.from({ length: n }, (_, i) => ({
      driverId: `d${i}`,
      lat: LAT + (i % 3) * 1e-4,
      lng: LNG + Math.floor(i / 3) * 1e-4,
    }));
    for (const p of pings) expect(cellFor(p.lat, p.lng)).toBe(CELL);
    await geo.applyPings(pings, Date.now());
  }

  async function seedEnRouteTrip(): Promise<void> {
    await pool.query(
      `INSERT INTO ride_requests (id, lat, lng, dest_lat, dest_lng, status)
       VALUES ('rq1', $1, $2, $3, $4, 'matched')`,
      [LAT, LNG, LAT + 0.01, LNG + 0.01],
    );
    await pool.query(
      `INSERT INTO trips (id, request_id, driver_id, rider_lat, rider_lng, status, claim_token)
       VALUES ('tr1', 'rq1', 'd0', $1, $2, 'en_route', 'tok')`,
      [LAT, LNG],
    );
  }

  it('buildSnapshot aggregates drivers, trip arcs and surge with bounded driver dots', async () => {
    await seedFleet(4);
    await seedEnRouteTrip();
    for (let i = 0; i < 8; i++) await surge.recordDemand(CELL, `r${i}`, Date.now());

    const rm = buildReadModel({ redis, pool, driverCap: 2, matcherMetricsUrl: 'http://127.0.0.1:1/none' });
    const snap = await rm.buildSnapshot();

    // Drivers bounded to the cap; the true total still reported.
    expect(snap.driversTotal).toBe(4);
    expect(snap.driversShown).toBe(2);
    expect(snap.drivers).toHaveLength(2);
    expect(snap.drivers[0]?.s).toBe('available');

    // The en_route trip becomes an arc pickup→dest.
    expect(snap.trips).toHaveLength(1);
    expect(snap.trips[0]).toMatchObject({ id: 'tr1', s: 'en_route' });
    expect(snap.counters.activeTrips).toBe(1);

    // 8 demand / 4 supply = 2.0 surge in the cell.
    const cell = snap.surge.find((s) => s.cell === CELL);
    expect(cell?.m).toBe(2);
    expect(snap.counters.surgeMax).toBe(2);
  });

  it('SSE /events streams periodic snapshots', async () => {
    await seedFleet(3);
    const rm = buildReadModel({ redis, pool, tickMs: 300, matcherMetricsUrl: 'http://127.0.0.1:1/none' });
    await rm.app.listen({ port: 0, host: '127.0.0.1' });
    rm.start();
    try {
      const port = (rm.app.server.address() as AddressInfo).port;
      const frames = await collectSse(`http://127.0.0.1:${port}/events`, 2);
      expect(frames.length).toBeGreaterThanOrEqual(2);
      expect(frames[0]?.driversTotal).toBe(3);
      expect(frames[0]?.t).toBeGreaterThan(0);
    } finally {
      await rm.stop();
    }
  });

  it('POST /spawn injects real ride requests into intake (row + queue + demand)', async () => {
    const rm = buildReadModel({ redis, pool, matcherMetricsUrl: 'http://127.0.0.1:1/none' });
    const res = await rm.app.inject({ method: 'POST', url: '/spawn', payload: { n: 10, hotspot: true } });
    expect(res.statusCode).toBe(200);
    expect(res.json()).toMatchObject({ spawned: 10, hotspot: true });

    const rows = await pool.query<{ n: number }>(
      `SELECT count(*)::int AS n FROM ride_requests WHERE status = 'pending'`,
    );
    expect(rows.rows[0]?.n).toBe(10);
    expect(await redis.llen('requests:queue')).toBe(10);
  });

  it('scrapes matcher counters (matches/sec, p50/p99) from /metrics', async () => {
    let matches = 100;
    const fake = http.createServer((_req, res) => {
      res.writeHead(200, { 'content-type': 'text/plain' });
      res.end(
        [
          `matches_total ${matches}`,
          'unmatched_total 5',
          'match_latency_ms{quantile="0.5"} 4.5',
          'match_latency_ms{quantile="0.99"} 16.8',
          '',
        ].join('\n'),
      );
      matches += 10; // each scrape sees 10 more matches
    });
    await new Promise<void>((r) => fake.listen(0, '127.0.0.1', r));
    try {
      const fport = (fake.address() as AddressInfo).port;
      let clock = 1_700_000_000_000;
      const rm = buildReadModel({
        redis,
        pool,
        matcherMetricsUrl: `http://127.0.0.1:${fport}/metrics`,
        now: () => clock,
      });

      const first = await rm.buildSnapshot();
      expect(first.counters.p50Ms).toBe(4.5);
      expect(first.counters.p99Ms).toBe(16.8);
      expect(first.counters.matchesTotal).toBe(100);
      expect(first.counters.matchesPerSec).toBe(0); // no prior sample yet

      clock += 1000; // one second later
      const second = await rm.buildSnapshot();
      expect(second.counters.matchesTotal).toBe(110);
      expect(second.counters.matchesPerSec).toBe(10); // +10 over 1s
    } finally {
      await new Promise<void>((r) => fake.close(() => r()));
    }
  });
});
