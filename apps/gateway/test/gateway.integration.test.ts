import { spawn } from 'node:child_process';
import type { AddressInfo } from 'node:net';
import { fileURLToPath } from 'node:url';
import { join } from 'node:path';
import { PostgreSqlContainer, type StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import { RedisContainer, type StartedRedisContainer } from '@testcontainers/redis';
import { Redis } from 'ioredis';
import type pg from 'pg';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import WebSocket from 'ws';
import { HEARTBEAT_ZSET, REQUESTS_QUEUE, cellKey, cellFor, driverKey } from '@fleetline/core';
import { createPool, runMigrations } from '@fleetline/db';
import { signToken } from '../src/auth.js';
import { buildGateway, type Gateway, type GatewayOptions } from '../src/server.js';

// Real gateway process behavior on an ephemeral port, real WS clients, real
// Redis via Testcontainers. The E2E case at the bottom drives the real
// simulator binary against it.

const SECRET = 'integration-secret';
const CENTER = { lat: 37.7749, lng: -122.4194 };

async function waitFor(cond: () => boolean | Promise<boolean>, timeoutMs = 5000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!(await cond())) {
    if (Date.now() > deadline) throw new Error('waitFor timed out');
    await new Promise((r) => setTimeout(r, 10));
  }
}

function pauseClient(ws: WebSocket): void {
  // Reach into ws's underlying net.Socket to simulate a reader that stalls:
  // the kernel receive buffer fills, then the server's send queue backs up.
  (ws as unknown as { _socket: { pause(): void } })._socket.pause();
}

describe('gateway (testcontainers redis, real sockets)', () => {
  let container: StartedRedisContainer;
  let redis: Redis;
  const gateways: Gateway[] = [];
  const clients: WebSocket[] = [];

  beforeAll(async () => {
    container = await new RedisContainer('redis:7-alpine').start();
    redis = new Redis(container.getConnectionUrl());
  }, 120_000);

  afterAll(async () => {
    redis.disconnect();
    await container.stop();
  });

  beforeEach(async () => {
    await redis.flushall();
  });

  afterEach(async () => {
    for (const ws of clients.splice(0)) ws.terminate();
    for (const gw of gateways.splice(0)) await gw.app.close();
  });

  async function startGateway(opts: Partial<GatewayOptions> = {}): Promise<{ gw: Gateway; port: number }> {
    const gw = buildGateway({ redis, secret: SECRET, ...opts });
    gateways.push(gw);
    await gw.app.listen({ port: 0, host: '127.0.0.1' });
    const port = (gw.app.server.address() as AddressInfo).port;
    return { gw, port };
  }

  async function connect(port: number, token: string): Promise<WebSocket> {
    const ws = new WebSocket(`ws://127.0.0.1:${port}/ws?token=${token}`);
    clients.push(ws);
    await new Promise<void>((resolve, reject) => {
      ws.once('open', resolve);
      ws.once('error', reject);
    });
    return ws;
  }

  function ping(ws: WebSocket, driverId: string, lat = CENTER.lat, lng = CENTER.lng): void {
    ws.send(JSON.stringify({ type: 'driver_ping', driverId, lat, lng, ts: 0 }));
  }

  it('serves /healthz and /metrics', async () => {
    const { gw } = await startGateway();
    const health = await gw.app.inject({ method: 'GET', url: '/healthz' });
    expect(health.statusCode).toBe(200);
    expect(health.json()).toEqual({ status: 'ok' });

    const metrics = await gw.app.inject({ method: 'GET', url: '/metrics' });
    expect(metrics.statusCode).toBe(200);
    expect(metrics.headers['content-type']).toContain('text/plain');
    expect(metrics.body).toContain('pings_received_total 0');
    expect(metrics.body).toContain('ping_batch_size_count 0');
  });

  it('rejects upgrades on paths other than /ws', async () => {
    const { port } = await startGateway();
    const ws = new WebSocket(`ws://127.0.0.1:${port}/nope`);
    clients.push(ws);
    const err = await new Promise<Error>((resolve) => ws.once('error', resolve));
    expect(err).toBeInstanceOf(Error);
  });

  it('auth: bad or missing token closes with 4001; good token is accepted', async () => {
    const { gw, port } = await startGateway();

    for (const qs of ['', '?token=d1.deadbeef', '?token=garbage']) {
      const ws = new WebSocket(`ws://127.0.0.1:${port}/ws${qs}`);
      clients.push(ws);
      const code = await new Promise<number>((resolve) => ws.once('close', resolve));
      expect(code).toBe(4001);
    }
    expect(gw.metrics.authRejectsTotal).toBe(3);
    expect(gw.metrics.wsConnectionsTotal).toBe(0);

    const ws = await connect(port, signToken('d1', SECRET));
    ping(ws, 'd1');
    await waitFor(() => gw.metrics.pingsReceivedTotal === 1);
    expect(gw.metrics.wsConnectionsTotal).toBe(1);
  });

  it('pings land in the index; the socket registry tracks multiplexed drivers', async () => {
    const { gw, port } = await startGateway();
    const ws = await connect(port, signToken('sim', SECRET));
    ping(ws, 'd1');
    ping(ws, 'd2');
    ping(ws, 'd3');

    await waitFor(async () => (await redis.zcard(HEARTBEAT_ZSET)) === 3);
    const cell = cellFor(CENTER.lat, CENTER.lng);
    expect((await redis.smembers(cellKey(cell))).sort()).toEqual(['d1', 'd2', 'd3']);
    expect(await redis.hget(driverKey('d1'), 'status')).toBe('available');

    // Registry: token principal + every driver seen on the socket.
    expect([...gw.driverSockets.keys()].sort()).toEqual(['d1', 'd2', 'd3', 'sim']);
    ws.close();
    await waitFor(() => gw.driverSockets.size === 0);
    expect(gw.metrics.wsConnectionsActive).toBe(0);
  });

  it('counts invalid payloads without crashing, and ride_requests separately', async () => {
    const { gw, port } = await startGateway();
    const ws = await connect(port, signToken('d1', SECRET));
    ws.send('not json');
    ws.send(JSON.stringify({ type: 'driver_ping', driverId: 'd1', lat: 'NaN', lng: 0 }));
    ws.send(JSON.stringify({ type: 'nonsense' }));
    ws.send(JSON.stringify({ type: 'ride_request', requestId: 'r1', ...CENTER, ts: 0 }));
    ping(ws, 'd1');
    await waitFor(() => gw.metrics.pingsReceivedTotal === 1);
    expect(gw.metrics.invalidMessagesTotal).toBe(3);
    expect(gw.metrics.rideRequestsReceivedTotal).toBe(1);
    expect(gw.metrics.wsMessagesReceivedTotal).toBe(5);
  });

  it('flushes on FLUSH_MAX without waiting for the timer', async () => {
    const { gw, port } = await startGateway({ flushMs: 60_000, flushMax: 10 });
    const ws = await connect(port, signToken('sim', SECRET));
    for (let i = 0; i < 10; i++) ping(ws, `m${i}`);

    // The 60s timer cannot have fired — only the size trigger explains this.
    await waitFor(async () => (await redis.zcard(HEARTBEAT_ZSET)) === 10);
    expect(gw.metrics.batchesFlushedTotal).toBe(1);
    expect(gw.metrics.batchSize.sum).toBe(10);
  });

  it('sweep loop offlines silent drivers and counts them', async () => {
    const { gw, port } = await startGateway({ staleMs: 200, sweepIntervalMs: 50 });
    const ws = await connect(port, signToken('d1', SECRET));
    ping(ws, 'd1');
    await waitFor(async () => (await redis.hget(driverKey('d1'), 'status')) === 'available');

    // Go silent; the gateway's own loop must sweep within ~staleMs + interval.
    await waitFor(() => gw.metrics.driversSweptTotal >= 1, 2000);
    expect(await redis.hget(driverKey('d1'), 'status')).toBe('offline');
    expect(await redis.zscore(HEARTBEAT_ZSET, 'd1')).toBeNull();
  });

  it('terminates sockets that miss the pong deadline', async () => {
    const { gw, port } = await startGateway({ pingIntervalMs: 100, pongTimeoutMs: 200 });
    const ws = await connect(port, signToken('d1', SECRET));
    pauseClient(ws); // never processes the server ping ⇒ never pongs
    await waitFor(() => gw.metrics.pongTimeoutsTotal >= 1 && gw.metrics.wsConnectionsActive === 0, 3000);
    expect(gw.driverSockets.size).toBe(0);
  });

  it('backpressure: a slow reader with a full outbound queue is disconnected', async () => {
    const { gw, port } = await startGateway({ maxQueueBytes: 128 * 1024 });
    const ws = await connect(port, signToken('slow', SECRET));
    pauseClient(ws);

    const payload = 'x'.repeat(32 * 1024);
    let refused = false;
    for (let i = 0; i < 500 && !refused; i++) {
      refused = !gw.sendToDriver('slow', payload);
    }
    expect(refused).toBe(true);
    expect(gw.metrics.backpressureDisconnectsTotal).toBe(1);
    await waitFor(() => gw.metrics.wsConnectionsActive === 0 && gw.driverSockets.size === 0);
  });

  describe('ride request intake (adds testcontainers postgres)', () => {
    let pgContainer: StartedPostgreSqlContainer;
    let pool: pg.Pool;

    beforeAll(async () => {
      pgContainer = await new PostgreSqlContainer('postgres:16-alpine').start();
      await runMigrations(pgContainer.getConnectionUri());
      pool = createPool(pgContainer.getConnectionUri());
    }, 180_000);

    afterAll(async () => {
      await pool.end();
      await pgContainer.stop();
    });

    beforeEach(async () => {
      await pool.query('TRUNCATE ride_requests, trips CASCADE');
    });

    function request(ws: WebSocket, requestId: string): void {
      ws.send(JSON.stringify({ type: 'ride_request', requestId, ...CENTER, ts: 0 }));
    }

    it('persists a pending row, then enqueues the id; duplicates dedupe on the row, not the queue', async () => {
      const { gw, port } = await startGateway({ pool });
      const ws = await connect(port, signToken('rider', SECRET));
      request(ws, 'r1');
      request(ws, 'r1'); // duplicate delivery
      request(ws, 'r2');

      await waitFor(() => gw.metrics.rideRequestsEnqueuedTotal === 3);
      expect(gw.metrics.rideRequestsReceivedTotal).toBe(3);
      expect(gw.metrics.rideRequestErrorsTotal).toBe(0);

      // Exactly one row per request id, pending, at the request's coordinates.
      const rows = await pool.query<{ id: string; status: string; lat: number }>(
        'SELECT id, status, lat FROM ride_requests ORDER BY id',
      );
      expect(rows.rows).toEqual([
        { id: 'r1', status: 'pending', lat: CENTER.lat },
        { id: 'r2', status: 'pending', lat: CENTER.lat },
      ]);

      // The queue carries every delivery (at-least-once); the matcher's row
      // guard is what collapses the duplicate.
      const queued = await redis.lrange(REQUESTS_QUEUE, 0, -1);
      expect(queued.sort()).toEqual(['r1', 'r1', 'r2']);
    });

    it('malformed ride_requests are invalid, not intake errors', async () => {
      const { gw, port } = await startGateway({ pool });
      const ws = await connect(port, signToken('rider', SECRET));
      ws.send(JSON.stringify({ type: 'ride_request', lat: CENTER.lat, lng: CENTER.lng })); // no id
      ws.send(JSON.stringify({ type: 'ride_request', requestId: 'r9', lat: 'x', lng: 0 }));
      request(ws, 'ok');

      await waitFor(() => gw.metrics.rideRequestsEnqueuedTotal === 1);
      expect(gw.metrics.invalidMessagesTotal).toBe(2);
      expect(await redis.lrange(REQUESTS_QUEUE, 0, -1)).toEqual(['ok']);
    });
  });

  it('E2E: real simulator over --sink ws indexes the whole fleet with coherent metrics', async () => {
    const { gw, port } = await startGateway();
    const drivers = 300;
    const ticks = 4;
    const root = fileURLToPath(new URL('../../..', import.meta.url));
    const url = `ws://127.0.0.1:${port}/ws?token=${signToken('sim', SECRET)}`;

    const sim = spawn(
      join(root, 'node_modules', '.bin', 'tsx'),
      // prettier-ignore
      [
        'apps/simulator/src/main.ts',
        '--sink', 'ws', '--gateway', url,
        '--drivers', String(drivers), '--ticks', String(ticks),
        '--rps', '0', '--seed', '7', '--speedup', '5',
      ],
      { cwd: root, stdio: ['ignore', 'ignore', 'pipe'] },
    );
    let stderr = '';
    sim.stderr.on('data', (chunk: Buffer) => (stderr += chunk.toString()));
    const exitCode = await new Promise<number | null>((resolve) => sim.once('exit', resolve));
    expect(exitCode, stderr).toBe(0);

    // The simulator reports its own send counters on exit — nothing dropped.
    const done = /simulator done (\{.*\})/.exec(stderr);
    expect(done, stderr).not.toBeNull();
    const simStats = JSON.parse(done![1]!) as { sent: number; dropped: number };
    expect(simStats.sent).toBe(drivers * ticks);
    expect(simStats.dropped).toBe(0);

    // All pings received and flushed (received == sum of batch sizes).
    const expected = drivers * ticks;
    // (batchSize is observed when a flush starts, batchesFlushedTotal when its
    // MULTI resolves — wait for both to settle before asserting coherence.)
    await waitFor(
      () =>
        gw.metrics.pingsReceivedTotal === expected &&
        gw.metrics.batchSize.sum === expected &&
        gw.metrics.batchesFlushedTotal + gw.metrics.flushErrorsTotal === gw.metrics.batchSize.count,
      10_000,
    );
    expect(gw.metrics.flushErrorsTotal).toBe(0);
    expect(gw.metrics.batchesFlushedTotal).toBe(gw.metrics.batchSize.count);
    expect(gw.metrics.invalidMessagesTotal).toBe(0);
    expect(gw.metrics.rideRequestsReceivedTotal).toBe(0);

    // Whole fleet indexed, positions fresh.
    expect(await redis.zcard(HEARTBEAT_ZSET)).toBe(drivers);
    for (const id of ['d0', 'd150', 'd299']) {
      const hash = await redis.hgetall(driverKey(id));
      expect(hash['status']).toBe('available');
      expect(Date.now() - Number(hash['heartbeatMs'])).toBeLessThan(15_000);
    }
  }, 120_000);
});
