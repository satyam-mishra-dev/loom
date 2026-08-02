import type { IncomingMessage } from 'node:http';
import type { Duplex } from 'node:stream';
import Fastify, { type FastifyInstance } from 'fastify';
import type { Redis } from 'ioredis';
import type pg from 'pg';
import { WebSocket, WebSocketServer } from 'ws';
import {
  DegradingLimiter,
  GeoIndex,
  OFFER_REPLY_TTL_MS,
  REQUESTS_QUEUE,
  SurgeStore,
  TRIP_EVENTS_QUEUE,
  cellFor,
  driverChannel,
  offerReplyKey,
  type GeoPing,
} from '@fleetline/core';
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
  /**
   * Per-source GCRA on the ride-request intake path (§5.7). Keyed by the
   * socket's authenticated principal (the rider/source). Defaults are generous
   * so a normal fleet never trips them; the demo/tests tighten them to show
   * rejection. Over-limit requests are rejected + counted, never silently
   * dropped. Degrades fail-open by default (see DegradingLimiter).
   */
  rateLimit?: {
    limit?: number;
    windowMs?: number;
    burst?: number;
    failClosed?: boolean;
    redisTimeoutMs?: number;
  };
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
  /** The intake rate limiter — exposed so /metrics can render its degradation counters. */
  limiter: DegradingLimiter;
}

type InboundMessage =
  | { kind: 'ping'; ping: GeoPing }
  | {
      kind: 'ride_request';
      requestId: string;
      lat: number;
      lng: number;
      destLat: number | null;
      destLng: number | null;
    }
  | { kind: 'offer_reply'; offerId: string; driverId: string; accept: boolean }
  | { kind: 'trip_progress'; tripId: string; driverId: string; event: 'arrived_pickup' | 'trip_done' };

function isFiniteNumber(v: unknown): v is number {
  return typeof v === 'number' && Number.isFinite(v);
}

function nonEmptyString(v: unknown): v is string {
  return typeof v === 'string' && v.length > 0;
}

/** Shape-check an incoming message. Trust boundary: never assume valid JSON shape. */
function parseMessage(raw: unknown): InboundMessage | null {
  if (typeof raw !== 'object' || raw === null) return null;
  const msg = raw as Record<string, unknown>;
  switch (msg['type']) {
    case 'driver_ping': {
      const driverId = msg['driverId'];
      const { lat, lng } = msg;
      if (!nonEmptyString(driverId) || !isFiniteNumber(lat) || !isFiniteNumber(lng)) return null;
      return { kind: 'ping', ping: { driverId, lat, lng } };
    }
    case 'ride_request': {
      const requestId = msg['requestId'];
      const { lat, lng } = msg;
      if (!nonEmptyString(requestId) || !isFiniteNumber(lat) || !isFiniteNumber(lng)) return null;
      // Destination is optional on the wire; a missing one degenerates to a
      // zero-length trip rather than rejecting the request.
      const destLat = isFiniteNumber(msg['destLat']) ? msg['destLat'] : null;
      const destLng = isFiniteNumber(msg['destLng']) ? msg['destLng'] : null;
      return { kind: 'ride_request', requestId, lat, lng, destLat, destLng };
    }
    case 'offer_reply': {
      const { offerId, driverId, accept } = msg;
      if (!nonEmptyString(offerId) || !nonEmptyString(driverId) || typeof accept !== 'boolean') return null;
      return { kind: 'offer_reply', offerId, driverId, accept };
    }
    case 'trip_progress': {
      const { tripId, driverId, event } = msg;
      if (!nonEmptyString(tripId) || !nonEmptyString(driverId)) return null;
      if (event !== 'arrived_pickup' && event !== 'trip_done') return null;
      return { kind: 'trip_progress', tripId, driverId, event };
    }
    default:
      return null;
  }
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
    rateLimit,
    now = Date.now,
  } = opts;

  const metrics = createMetrics();
  const geoIndex = new GeoIndex(redis);
  const surge = new SurgeStore(redis);
  const limiter = new DegradingLimiter(redis, {
    config: {
      limit: rateLimit?.limit ?? 200,
      windowMs: rateLimit?.windowMs ?? 1_000,
      burst: rateLimit?.burst ?? 400,
    },
    failClosed: rateLimit?.failClosed ?? false,
    redisTimeoutMs: rateLimit?.redisTimeoutMs ?? 50,
    now,
  });
  const app = Fastify({ logger: opts.logger ?? false });

  app.get('/healthz', () => ({ status: 'ok' }));
  app.get('/metrics', (_req, reply) => {
    void reply.type('text/plain; version=0.0.4');
    return renderMetrics(metrics, limiter.metrics);
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
  async function intakeRequest(
    requestId: string,
    lat: number,
    lng: number,
    destLat: number | null,
    destLng: number | null,
  ): Promise<void> {
    if (pool === undefined) return; // drivers-only gateway: counted, not consumed
    try {
      await pool.query(
        `INSERT INTO ride_requests (id, lat, lng, dest_lat, dest_lng, status)
         VALUES ($1, $2, $3, $4, $5, 'pending')
         ON CONFLICT (id) DO NOTHING`,
        [requestId, lat, lng, destLat, destLng],
      );
      await redis.lpush(REQUESTS_QUEUE, requestId);
      // Feed the surge demand window for this request's cell (§5.7). Best
      // effort — a surge miss must never fail intake, so it rides its own
      // catch and the enqueue above is already committed.
      surge.recordDemand(cellFor(lat, lng), requestId, now()).catch((err: unknown) => {
        app.log.error({ err, requestId }, 'surge demand record failed');
      });
      metrics.rideRequestsEnqueuedTotal++;
    } catch (err) {
      metrics.rideRequestErrorsTotal++;
      app.log.error({ err, requestId }, 'ride request intake failed');
    }
  }

  // Rate-limited intake: check the per-source GCRA first, reject over-limit
  // requests with a 429-equivalent back to the socket (counted, not dropped),
  // otherwise persist + enqueue. `source` is the socket's authenticated
  // principal — the rider/API identity the limit is keyed to.
  async function rateLimitedIntake(
    source: string,
    socket: WebSocket,
    msg: { requestId: string; lat: number; lng: number; destLat: number | null; destLng: number | null },
  ): Promise<void> {
    const decision = await limiter.limit(`intake:${source}`);
    if (!decision.allowed) {
      metrics.rideRequestsRateLimitedTotal++;
      sendOn(
        socket,
        JSON.stringify({
          type: 'ride_rejected',
          requestId: msg.requestId,
          reason: 'rate_limited',
          retryAfterMs: Math.ceil(decision.retryAfterMs),
        }),
      );
      return;
    }
    await intakeRequest(msg.requestId, msg.lat, msg.lng, msg.destLat, msg.destLng);
  }

  // ---- driver replies + trip progress: WS in, Redis lists out ----
  // The reply list buffers the race where the driver answers before the
  // matcher blocks; PEXPIRE GCs replies nobody ever collects (offer already
  // timed out). Trip progress rides the same LPUSH/BLMOVE pattern as request
  // intake. See @fleetline/core messages.ts for the full transport rationale.
  async function forwardOfferReply(offerId: string, accept: boolean): Promise<void> {
    try {
      await redis
        .multi()
        .lpush(offerReplyKey(offerId), JSON.stringify({ accept }))
        .pexpire(offerReplyKey(offerId), OFFER_REPLY_TTL_MS)
        .exec();
      metrics.offerRepliesTotal++;
    } catch (err) {
      metrics.replyErrorsTotal++;
      app.log.error({ err, offerId }, 'offer reply forward failed');
    }
  }

  async function forwardTripProgress(tripId: string, driverId: string, event: string): Promise<void> {
    try {
      await redis.lpush(TRIP_EVENTS_QUEUE, JSON.stringify({ tripId, driverId, event }));
      metrics.tripProgressTotal++;
    } catch (err) {
      metrics.replyErrorsTotal++;
      app.log.error({ err, tripId, driverId }, 'trip progress forward failed');
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

  // ---- matcher → driver fan-out (offer transport, downstream half) ----
  // Each gateway SUBSCRIBEs to `driver:{id}:msg` for exactly its connected
  // drivers — subscribe when a driver binds to a socket (token principal at
  // auth, plus ids learned from pings on a multiplexed socket), unsubscribe
  // when the socket closes. A matcher publishing an offer therefore reaches
  // the one gateway that can deliver it, with no routing table anywhere.
  // Delivery reuses the bounded sendToDriver; a miss (driver gone, queue
  // full) is dropped — the matcher's offer timeout is the recovery path.
  const sub = redis.duplicate();
  sub.on('message', (channel: string, message: string) => {
    const driverId = channel.slice('driver:'.length, -':msg'.length);
    if (sendToDriver(driverId, message)) metrics.messagesForwardedTotal++;
    else metrics.forwardDropsTotal++;
  });

  // ioredis gotcha: duplicate() starts in 'wait', and a SUBSCRIBE issued
  // while the lazy connect is mid-handshake gets written BEFORE the ready
  // check's INFO, which then fails ("subscriber mode") and poisons the
  // connection. So: connect eagerly, and serialize every (un)subscribe
  // behind 'ready' — the chain also keeps bind/unbind ordering per driver.
  if (sub.status === 'wait') {
    sub.connect().catch((err: unknown) => app.log.error({ err }, 'subscriber connect failed'));
  }
  let subOps: Promise<unknown> = new Promise<void>((resolve) => {
    if ((sub.status as string) === 'ready') resolve();
    else sub.once('ready', resolve);
  });
  function queueSubOp(op: () => Promise<unknown>, driverId: string): void {
    subOps = subOps.then(op).catch((err: unknown) => {
      app.log.error({ err, driverId }, 'driver channel (un)subscribe failed');
    });
  }

  function bindDriver(driverId: string, socket: WebSocket): void {
    driverSockets.set(driverId, socket);
    queueSubOp(() => sub.subscribe(driverChannel(driverId)), driverId);
  }

  function unbindDriver(driverId: string): void {
    driverSockets.delete(driverId);
    queueSubOp(() => sub.unsubscribe(driverChannel(driverId)), driverId);
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
    bindDriver(principal, socket);

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
        void rateLimitedIntake(principal, socket, msg);
        return;
      }
      if (msg.kind === 'offer_reply') {
        void forwardOfferReply(msg.offerId, msg.accept);
        return;
      }
      if (msg.kind === 'trip_progress') {
        void forwardTripProgress(msg.tripId, msg.driverId, msg.event);
        return;
      }
      metrics.pingsReceivedTotal++;
      if (!boundIds.has(msg.ping.driverId)) {
        boundIds.add(msg.ping.driverId);
        bindDriver(msg.ping.driverId, socket);
      }
      enqueue(msg.ping);
    });

    socket.on('close', () => {
      metrics.wsConnectionsActive--;
      clearInterval(pinger);
      if (pongTimer !== null) clearTimeout(pongTimer);
      for (const id of boundIds) {
        if (driverSockets.get(id) === socket) unbindDriver(id);
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
    sub.disconnect();
    await new Promise<void>((resolve) => wss.close(() => resolve()));
  });

  return { app, metrics, geoIndex, driverSockets, sendToDriver, limiter };
}
