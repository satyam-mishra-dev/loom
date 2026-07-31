import type { IncomingMessage } from 'node:http';
import type { Duplex } from 'node:stream';
import Fastify, { type FastifyInstance } from 'fastify';
import type { Redis } from 'ioredis';
import { WebSocket, WebSocketServer } from 'ws';
import { GeoIndex, type GeoPing } from '@fleetline/core';
import { verifyToken } from './auth.js';
import { createMetrics, renderMetrics, type GatewayMetrics } from './metrics.js';

export interface GatewayOptions {
  redis: Redis;
  /** HMAC secret for the auth-lite token scheme — see auth.ts. */
  secret: string;
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

/** Shape-check an incoming message. Trust boundary: never assume valid JSON shape. */
function parseMessage(raw: unknown): GeoPing | 'ride_request' | null {
  if (typeof raw !== 'object' || raw === null) return null;
  const msg = raw as Record<string, unknown>;
  if (msg['type'] === 'ride_request') return 'ride_request';
  if (msg['type'] !== 'driver_ping') return null;
  const { driverId, lat, lng } = msg;
  if (typeof driverId !== 'string' || driverId.length === 0) return null;
  if (typeof lat !== 'number' || !Number.isFinite(lat)) return null;
  if (typeof lng !== 'number' || !Number.isFinite(lng)) return null;
  return { driverId, lat, lng };
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
      if (msg === 'ride_request') {
        // Counted but not consumed — the matcher picks these up in phase C.
        metrics.rideRequestsReceivedTotal++;
        return;
      }
      metrics.pingsReceivedTotal++;
      if (!boundIds.has(msg.driverId)) {
        boundIds.add(msg.driverId);
        driverSockets.set(msg.driverId, socket);
      }
      enqueue(msg);
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
