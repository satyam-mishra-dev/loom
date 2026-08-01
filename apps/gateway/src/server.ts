import type { IncomingMessage } from 'node:http';
import type { Duplex } from 'node:stream';
import Fastify, { type FastifyInstance } from 'fastify';
import type { Redis } from 'ioredis';
import type pg from 'pg';
import { WebSocket, WebSocketServer } from 'ws';
import { GeoIndex, REQUESTS_QUEUE, type GeoPing } from '@fleetline/core';
import { verifyToken } from './auth.js';
import { createMetrics, renderMetrics, type GatewayMetrics } from './metrics.js';

export interface GatewayOptions {
  redis: Redis;
  /** HMAC secret for the auth-lite token scheme — see auth.ts. */
  secret: string;
  /**
   * Postgres pool for ride-request intake — the gateway owns intake
   * persistence (row first, then queue). Optional so a drivers-only gateway
   * (and the phase-A/B tests) can run without Postgres; without it,
   * ride_requests are counted and dropped.
   */
  pool?: pg.Pool;
  logger?: boolean;
  /** Flush the ping buffer after this many ms… (default 50) */
  flushMs?: number;
  /** …or once it holds this many pings — whichever comes first (default 500). */
  flushMax?: number;
  /** Server-initiated WS ping interval (default 25s, per Centrifugo's numbers). */
  pingIntervalMs?: number;
  /** Missing pong after this long ⇒ half-open socket, terminate (default 8s). */
  pongTimeoutMs?: number;
  /** Per-socket outbound queue bound in BYTES; overflow ⇒ disconnect (default 1 MiB). */
  maxQueueBytes?: number;
  /** How often the stale-driver sweep runs (default 2s). */
  sweepIntervalMs?: number;
  /** App-level heartbeat staleness threshold (default 10s). */
  staleMs?: number;
  /** Injectable clock for the Redis heartbeat/sweep (tests). */
  now?: () => number;
}

export interface Gateway {
  app: FastifyInstance;
  metrics: GatewayMetrics;
  geoIndex: GeoIndex;
  /**
   * driverId → socket, for the matcher (phase C/D) to push offers through.
   * One socket per driver in production; the simulator multiplexes many
   * drivers over one socket, so bindings are learned from observed pings
   * (plus the token principal), and all of a socket's bindings are removed
   * when it closes.
   */
  driverSockets: ReadonlyMap<string, WebSocket>;
  /** Bounded send to a driver's socket. False if unknown, closed, or over the queue bound. */
  sendToDriver(driverId: string, data: string): boolean;
}

type InboundMessage =
  | { kind: 'ping'; ping: GeoPing }
  | { kind: 'ride_request'; requestId: string; lat: number; lng: number };

/** Shape-check an incoming message. Trust boundary: never assume valid JSON shape. */
function parseMessage(raw: unknown): InboundMessage | null {
  if (typeof raw !== 'object' || raw === null) return null;
  const msg = raw as Record<string, unknown>;
  const { lat, lng } = msg;
  if (typeof lat !== 'number' || !Number.isFinite(lat)) return null;
  if (typeof lng !== 'number' || !Number.isFinite(lng)) return null;
  if (msg['type'] === 'driver_ping') {
    const driverId = msg['driverId'];
    if (typeof driverId !== 'string' || driverId.length === 0) return null;
    return { kind: 'ping', ping: { driverId, lat, lng } };
  }
  if (msg['type'] === 'ride_request') {
    const requestId = msg['requestId'];
    if (typeof requestId !== 'string' || requestId.length === 0) return null;
    return { kind: 'ride_request', requestId, lat, lng };
  }
  return null;
}

/**
 * Gateway: authenticated WS ingestion. Pings from ALL sockets accumulate in
 * one buffer flushed to GeoIndex.applyPings every flushMs or flushMax pings,
 * whichever comes first. The gateway owns liveness: transport-level ws
 * ping/pong AND the app-level heartbeat sweep both run here.
 */
export function buildGateway(opts: GatewayOptions): Gateway {
  const {
    redis,
    secret,
    pool,
    flushMs = 50,
    flushMax = 500,
    pingIntervalMs = 25_000,
    pongTimeoutMs = 8_000,
    maxQueueBytes = 1024 * 1024,
    sweepIntervalMs = 2_000,
    staleMs = 10_000,
    now = Date.now,
  } = opts;

  const metrics = createMetrics();
  const geoIndex = new GeoIndex(redis);
  const app = Fastify({ logger: opts.logger ?? false });

  app.get('/healthz', () => ({ status: 'ok' }));
  app.get('/metrics', (_req, reply) => {
    void reply.type('text/plain; version=0.0.4');
    return renderMetrics(metrics);
  });

  // ---- batched ingestion: one buffer across all sockets ----
  let buffer: GeoPing[] = [];
  let flushTimer: NodeJS.Timeout | null = null;
  let inFlight: Promise<void> = Promise.resolve();

  function flush(): Promise<void> {
    if (flushTimer !== null) {
      clearTimeout(flushTimer);
      flushTimer = null;
    }
    if (buffer.length === 0) return inFlight;
    const batch = buffer;
    buffer = [];
    metrics.batchSize.observe(batch.length);
    const started = performance.now();
    inFlight = geoIndex
      .applyPings(batch, now())
      .then(
        () => {
          metrics.batchesFlushedTotal++;
        },
        (err: unknown) => {
          metrics.flushErrorsTotal++;
          app.log.error({ err }, 'ping flush failed');
        },
      )
      .finally(() => metrics.flushLatencyMs.observe(performance.now() - started));
    return inFlight;
  }

  function enqueue(ping: GeoPing): void {
    buffer.push(ping);
    if (buffer.length >= flushMax) void flush();
    else if (flushTimer === null) flushTimer = setTimeout(() => void flush(), flushMs);
  }

  // ---- ride-request intake: gateway owns persistence, matcher owns matching ----
  // Row first (committed, status pending), THEN the id onto requests:queue —
  // the matcher loads the row by id, so it must exist before the id is
  // poppable. A duplicate WS delivery re-pushes the id but ON CONFLICT keeps
  // one row, and the matcher's pending→matching row guard makes the second
  // pop a no-op: at-least-once intake, exactly-once matching.
  async function intakeRequest(requestId: string, lat: number, lng: number): Promise<void> {
    if (pool === undefined) return; // drivers-only gateway: counted, not consumed
    try {
      await pool.query(
        `INSERT INTO ride_requests (id, lat, lng, status) VALUES ($1, $2, $3, 'pending')
         ON CONFLICT (id) DO NOTHING`,
        [requestId, lat, lng],
      );
      await redis.lpush(REQUESTS_QUEUE, requestId);
      metrics.rideRequestsEnqueuedTotal++;
    } catch (err) {
      metrics.rideRequestErrorsTotal++;
      app.log.error({ err, requestId }, 'ride request intake failed');
    }
  }

  // ---- socket registry + bounded outbound path ----
  const driverSockets = new Map<string, WebSocket>();

  // The ONLY way application data is written to a socket. Slow-client posture:
  // the outbound queue is bounded in bytes (ws's own bufferedAmount is the
  // queue); a client that can't keep up is disconnected rather than buffered
  // without bound — one stalled reader must not eat gateway memory. Clients
  // re-sync by reconnecting (positions are live state, a backlog is worthless).
  function sendOn(socket: WebSocket, data: string): boolean {
    if (socket.readyState !== WebSocket.OPEN) return false;
    if (socket.bufferedAmount + Buffer.byteLength(data) > maxQueueBytes) {
      metrics.backpressureDisconnectsTotal++;
      socket.terminate();
      return false;
    }
    socket.send(data);
    return true;
  }

  function sendToDriver(driverId: string, data: string): boolean {
    const socket = driverSockets.get(driverId);
    return socket !== undefined ? sendOn(socket, data) : false;
  }

  const wss = new WebSocketServer({ noServer: true });

  wss.on('connection', (socket, req: IncomingMessage) => {
    const url = new URL(req.url ?? '/', 'http://localhost');
    const principal = verifyToken(url.searchParams.get('token'), secret);
    if (principal === null) {
      metrics.authRejectsTotal++;
      socket.close(4001, 'invalid token');
      return;
    }

    metrics.wsConnectionsTotal++;
    metrics.wsConnectionsActive++;

    const boundIds = new Set<string>([principal]);
    driverSockets.set(principal, socket);

    // Transport liveness (notes §8): server pings every pingIntervalMs; no
    // pong within pongTimeoutMs ⇒ half-open TCP (NAT/mobile), terminate.
    // Deliberately independent of the app-level heartbeatMs in Redis: the
    // socket check catches dead LINKS, the Redis heartbeat catches dead
    // DRIVERS (a zombie TCP session that pings nothing must not stay
    // matchable, and a swept driver on a live socket can come back by simply
    // pinging again). Two layers, two failure classes.
    let pongTimer: NodeJS.Timeout | null = null;
    const pinger = setInterval(() => {
      if (pongTimer !== null) return; // previous ping unanswered; its timer decides
      socket.ping();
      pongTimer = setTimeout(() => {
        metrics.pongTimeoutsTotal++;
        socket.terminate();
      }, pongTimeoutMs);
    }, pingIntervalMs);
    socket.on('pong', () => {
      if (pongTimer !== null) {
        clearTimeout(pongTimer);
        pongTimer = null;
      }
    });

    socket.on('message', (data: Buffer | ArrayBuffer | Buffer[]) => {
      metrics.wsMessagesReceivedTotal++;
      let parsed: unknown;
      try {
        parsed = JSON.parse(String(data));
      } catch {
        metrics.invalidMessagesTotal++;
        return;
      }
      const msg = parseMessage(parsed);
      if (msg === null) {
        metrics.invalidMessagesTotal++;
        return;
      }
      if (msg.kind === 'ride_request') {
        metrics.rideRequestsReceivedTotal++;
        void intakeRequest(msg.requestId, msg.lat, msg.lng);
        return;
      }
      metrics.pingsReceivedTotal++;
      if (!boundIds.has(msg.ping.driverId)) {
        boundIds.add(msg.ping.driverId);
        driverSockets.set(msg.ping.driverId, socket);
      }
      enqueue(msg.ping);
    });

    socket.on('close', () => {
      metrics.wsConnectionsActive--;
      clearInterval(pinger);
      if (pongTimer !== null) clearTimeout(pongTimer);
      for (const id of boundIds) {
        if (driverSockets.get(id) === socket) driverSockets.delete(id);
      }
    });
  });

  app.server.on('upgrade', (req: IncomingMessage, socket: Duplex, head: Buffer) => {
    const { pathname } = new URL(req.url ?? '/', 'http://localhost');
    if (pathname !== '/ws') {
      socket.destroy();
      return;
    }
    wss.handleUpgrade(req, socket, head, (ws) => wss.emit('connection', ws, req));
  });

  // App-level liveness: the gateway owns the sweep loop. Drivers silent past
  // staleMs leave the available sets and go offline until they ping again.
  const sweeper = setInterval(() => {
    geoIndex.sweepStale(now(), staleMs).then(
      (swept) => {
        metrics.driversSweptTotal += swept.length;
      },
      (err: unknown) => app.log.error({ err }, 'stale sweep failed'),
    );
  }, sweepIntervalMs);

  app.addHook('onClose', async () => {
    clearInterval(sweeper);
    for (const client of wss.clients) client.terminate();
    await flush(); // drain buffered pings
    await inFlight;
    await new Promise<void>((resolve) => wss.close(() => resolve()));
  });

  return { app, metrics, geoIndex, driverSockets, sendToDriver };
}
