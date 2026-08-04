import type { ServerResponse } from 'node:http';
import Fastify, { type FastifyInstance } from 'fastify';
import type { Redis } from 'ioredis';
import type pg from 'pg';
import {
  HEARTBEAT_ZSET,
  REQUESTS_QUEUE,
  SurgeStore,
  cellFor,
  driverKey,
  type CellSurge,
} from '@fleetline/core';
import { runCrashDemo, runProof, tripHistory, type Deps } from './demo.js';

/**
 * The read model (CQRS read side) behind the dashboard. It is deliberately a
 * SEPARATE service from the gateway, not a route bolted onto it:
 *
 *  - The gateway's job is the write hot path — WS ingest, batched pings, offer
 *    transport. Fanning a periodic map snapshot to browsers, running Postgres
 *    trip queries, and scraping matcher metrics on a timer are read concerns
 *    with a completely different load profile; keeping them off the ingest
 *    event loop means a browser refresh storm can't touch driver ingestion.
 *  - The snapshot aggregates THREE sources — Redis (positions, surge), Postgres
 *    (active trips), the matcher's /metrics (match counters/latency). That
 *    fan-in is exactly a read model's job; no single write-path service owns
 *    all three.
 *  - It can be scaled or restarted independently, and the dashboard never
 *    touches a write-path service.
 *
 * Transport is SSE, not WebSocket (documented in DECISIONS.md): the dashboard
 * feed is strictly one-way server→browser periodic state, EventSource gives us
 * auto-reconnect and plain HTTP for free, and there is no client→server channel
 * to justify a duplex socket. The interactive controls (/spawn) are ordinary
 * POSTs, which is the honest shape for a command.
 */

export interface DriverDot {
  id: string;
  lat: number;
  lng: number;
  s: 'available' | 'claimed' | 'on_trip';
}

export interface TripArc {
  id: string;
  plat: number;
  plng: number;
  dlat: number;
  dlng: number;
  s: string;
}

export interface SurgeCellView {
  cell: string;
  m: number;
  demand: number;
  supply: number;
}

export interface Counters {
  matchesPerSec: number;
  p50Ms: number;
  p99Ms: number;
  activeTrips: number;
  unmatchedRate: number;
  matchesTotal: number;
  unmatchedTotal: number;
  surgeMax: number;
  driversTotal: number;
}

/**
 * The SSE payload. Bounded on purpose: `drivers` is capped (the rest are
 * summarized by count and by the surge heatmap's spatial density) so a
 * 5000-driver fleet never blows the frame — `driversTotal` reports the true
 * size, `driversShown` how many dots are in this frame.
 */
export interface Snapshot {
  t: number;
  drivers: DriverDot[];
  driversTotal: number;
  driversShown: number;
  trips: TripArc[];
  surge: SurgeCellView[];
  counters: Counters;
}

export interface ReadModelOptions {
  redis: Redis;
  pool: pg.Pool;
  /** Matcher /metrics URL to scrape counters from (default localhost:8090). */
  matcherMetricsUrl?: string;
  /** Snapshot cadence (default 1000ms). */
  tickMs?: number;
  /** Max driver dots per frame — the rest are summarized (default 1500). */
  driverCap?: number;
  /** Max trip arcs per frame (default 500). */
  tripCap?: number;
  /** Center for /spawn-generated requests (defaults to the simulator's city center). */
  center?: { lat: number; lng: number };
  /**
   * Demo-grade shared token gating the MUTATING /spawn and STREAMING /events
   * endpoints (G3). When set, both require `?token=<it>`; unset leaves them open
   * (bare local dev) with a startup warning. The dashboard is served the token
   * at build time, so it is a scan/curl deterrent, not user auth — a session or
   * signed cookie is the production upgrade (see DECISIONS.md).
   */
  authToken?: string | undefined;
  logger?: boolean;
  now?: () => number;
}

export interface ReadModel {
  app: FastifyInstance;
  /** Build one snapshot on demand (also the unit under test). */
  buildSnapshot(): Promise<Snapshot>;
  /** Begin the tick loop: recompute surge, build + broadcast a snapshot. */
  start(): void;
  stop(): Promise<void>;
  readonly clients: number;
}

/** One prometheus text-format value by exact metric key (`name` or `name{labels}`). */
function metricValue(text: string, key: string): number | null {
  for (const line of text.split('\n')) {
    if (line.length === 0 || line.startsWith('#')) continue;
    const sp = line.lastIndexOf(' ');
    if (sp < 0) continue;
    if (line.slice(0, sp) === key) {
      const n = Number(line.slice(sp + 1));
      return Number.isFinite(n) ? n : null;
    }
  }
  return null;
}

const SPAWN_MAX = 2_000;

export function buildReadModel(opts: ReadModelOptions): ReadModel {
  const {
    redis,
    pool,
    matcherMetricsUrl = 'http://127.0.0.1:8090/metrics',
    tickMs = 1_000,
    driverCap = 1_500,
    tripCap = 500,
    center = { lat: 37.7749, lng: -122.4194 },
    authToken,
    now = Date.now,
  } = opts;

  const app = Fastify({ logger: opts.logger ?? false });
  if (authToken === undefined) {
    app.log.warn('read-model auth disabled — /spawn and /events are OPEN (set READ_MODEL_TOKEN; demo/local only)');
  }
  const surge = new SurgeStore(redis);
  const sseClients = new Set<ServerResponse>();

  const metrics = { ticksTotal: 0, snapshotErrorsTotal: 0, scrapeErrorsTotal: 0, spawnedTotal: 0 };
  let latest: Snapshot | null = null;
  let timer: NodeJS.Timeout | null = null;
  // Previous scrape sample, for per-second rates over the tick interval.
  let prev: { matches: number; unmatched: number; t: number } | null = null;

  // ---- driver dots (bounded) ----
  async function driverDots(): Promise<{ dots: DriverDot[]; total: number }> {
    const total = await redis.zcard(HEARTBEAT_ZSET);
    // Most-recent heartbeats first, capped — the freshest positions win a slot.
    const ids = await redis.zrevrange(HEARTBEAT_ZSET, 0, driverCap - 1);
    if (ids.length === 0) return { dots: [], total };
    const p = redis.pipeline();
    for (const id of ids) p.hmget(driverKey(id), 'lat', 'lng', 'status');
    const rows = (await p.exec()) ?? [];
    const dots: DriverDot[] = [];
    ids.forEach((id, i) => {
      const row = (rows[i]?.[1] ?? []) as (string | null)[];
      const lat = Number(row[0]);
      const lng = Number(row[1]);
      const status = row[2];
      if (!Number.isFinite(lat) || !Number.isFinite(lng)) return;
      if (status !== 'available' && status !== 'claimed' && status !== 'on_trip') return;
      dots.push({ id, lat, lng, s: status });
    });
    return { dots, total };
  }

  // ---- active trip arcs (bounded) + active count ----
  async function tripArcs(): Promise<{ arcs: TripArc[]; active: number }> {
    const [arcRes, countRes] = await Promise.all([
      pool.query<{ id: string; plat: number; plng: number; dlat: number | null; dlng: number | null; status: string }>(
        `SELECT t.id, t.rider_lat AS plat, t.rider_lng AS plng,
                r.dest_lat AS dlat, r.dest_lng AS dlng, t.status
         FROM trips t JOIN ride_requests r ON r.id = t.request_id
         WHERE t.status IN ('en_route', 'in_trip')
         LIMIT $1`,
        [tripCap],
      ),
      pool.query<{ n: number }>(
        `SELECT count(*)::int AS n FROM trips WHERE status IN ('en_route', 'in_trip')`,
      ),
    ]);
    const arcs: TripArc[] = [];
    for (const r of arcRes.rows) {
      if (r.dlat === null || r.dlng === null) continue; // no destination → no arc
      arcs.push({ id: r.id, plat: r.plat, plng: r.plng, dlat: r.dlat, dlng: r.dlng, s: r.status });
    }
    return { arcs, active: countRes.rows[0]?.n ?? 0 };
  }

  // ---- counters: scrape the matcher, derive rates over the tick ----
  async function counters(surges: readonly CellSurge[], activeTrips: number, driversTotal: number): Promise<Counters> {
    let matchesTotal = prev?.matches ?? 0;
    let unmatchedTotal = prev?.unmatched ?? 0;
    let p50Ms = 0;
    let p99Ms = 0;
    try {
      const res = await fetch(matcherMetricsUrl, { signal: AbortSignal.timeout(500) });
      if (res.ok) {
        const text = await res.text();
        matchesTotal = metricValue(text, 'matches_total') ?? matchesTotal;
        unmatchedTotal = metricValue(text, 'unmatched_total') ?? unmatchedTotal;
        p50Ms = metricValue(text, 'match_latency_ms{quantile="0.5"}') ?? 0;
        p99Ms = metricValue(text, 'match_latency_ms{quantile="0.99"}') ?? 0;
      } else {
        metrics.scrapeErrorsTotal++;
      }
    } catch {
      metrics.scrapeErrorsTotal++;
    }

    const nowT = now();
    let matchesPerSec = 0;
    let unmatchedRate = 0;
    if (prev !== null) {
      const dt = (nowT - prev.t) / 1000;
      const dM = matchesTotal - prev.matches;
      const dU = unmatchedTotal - prev.unmatched;
      if (dt > 0) matchesPerSec = Math.max(0, dM / dt);
      if (dM + dU > 0) unmatchedRate = dU / (dM + dU);
    }
    prev = { matches: matchesTotal, unmatched: unmatchedTotal, t: nowT };

    const surgeMax = surges.reduce((mx, s) => Math.max(mx, s.multiplier), 1);
    return {
      matchesPerSec: Number(matchesPerSec.toFixed(2)),
      p50Ms: Number(p50Ms.toFixed(2)),
      p99Ms: Number(p99Ms.toFixed(2)),
      activeTrips,
      unmatchedRate: Number(unmatchedRate.toFixed(3)),
      matchesTotal,
      unmatchedTotal,
      surgeMax: Number(surgeMax.toFixed(2)),
      driversTotal,
    };
  }

  async function buildSnapshot(): Promise<Snapshot> {
    const nowMs = now();
    // The read model OWNS surge recompute on the tick: prune the windows,
    // derive multipliers, publish to the cell:surge hash, keep them for SSE.
    const surges = await surge.recompute(nowMs);
    await surge.publish(surges);

    const [{ dots, total }, { arcs, active }] = await Promise.all([driverDots(), tripArcs()]);
    const c = await counters(surges, active, total);

    return {
      t: nowMs,
      drivers: dots,
      driversTotal: total,
      driversShown: dots.length,
      trips: arcs,
      surge: surges
        .filter((s) => s.multiplier > 1)
        .map((s) => ({ cell: s.cell, m: Number(s.multiplier.toFixed(3)), demand: s.demand, supply: s.supply })),
      counters: c,
    };
  }

  function broadcast(snap: Snapshot): void {
    const frame = `data: ${JSON.stringify(snap)}\n\n`;
    for (const res of sseClients) res.write(frame);
  }

  // ---- routes ----
  app.addHook('onRequest', (req, reply, done) => {
    // The dashboard is served from a different origin (its own port); allow it.
    void reply.header('access-control-allow-origin', '*');
    void reply.header('access-control-allow-headers', 'content-type');
    void reply.header('access-control-allow-methods', 'GET,POST,OPTIONS');
    if (req.method === 'OPTIONS') {
      void reply.code(204).send();
      return;
    }
    // G3: gate the mutating (/spawn, /proof, /fault) and reading (/events,
    // /trip) demo endpoints behind the shared token when one is configured.
    // /healthz and /metrics stay open for the compose healthcheck and scraping.
    const gated = ['/spawn', '/events', '/proof', '/fault', '/trip'].some((p) => req.url.startsWith(p));
    if (authToken !== undefined && gated) {
      const provided = new URL(req.url, 'http://localhost').searchParams.get('token');
      if (provided !== authToken) {
        void reply.code(401).send({ error: 'unauthorized' });
        return;
      }
    }
    done();
  });

  app.get('/healthz', () => ({ status: 'ok' }));

  app.get('/metrics', (_req, reply) => {
    void reply.type('text/plain; version=0.0.4');
    return [
      `# TYPE read_model_ticks_total counter`,
      `read_model_ticks_total ${metrics.ticksTotal}`,
      `# TYPE read_model_sse_clients gauge`,
      `read_model_sse_clients ${sseClients.size}`,
      `# TYPE read_model_snapshot_errors_total counter`,
      `read_model_snapshot_errors_total ${metrics.snapshotErrorsTotal}`,
      `# TYPE read_model_scrape_errors_total counter`,
      `read_model_scrape_errors_total ${metrics.scrapeErrorsTotal}`,
      `# TYPE read_model_spawned_total counter`,
      `read_model_spawned_total ${metrics.spawnedTotal}`,
      '',
    ].join('\n');
  });

  // SSE: periodic snapshot fan-out. Hijack the socket so Fastify doesn't try
  // to finalize a normal response; write the latest frame immediately so a
  // fresh tab paints without waiting a whole tick.
  app.get('/events', (req, reply) => {
    reply.hijack();
    const res = reply.raw;
    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      Connection: 'keep-alive',
      'Access-Control-Allow-Origin': '*',
    });
    res.write(': connected\n\n');
    if (latest !== null) res.write(`data: ${JSON.stringify(latest)}\n\n`);
    sseClients.add(res);
    req.raw.on('close', () => sseClients.delete(res));
  });

  // Interactive control: spawn a burst of ride requests directly into intake
  // (row + queue + surge demand — the same path the gateway uses), so the
  // dashboard button drives real load. `hotspot` clusters them tightly so the
  // demo can force a cell to surge on demand.
  app.post<{ Body: { n?: number; hotspot?: boolean } }>('/spawn', async (req, reply) => {
    const n = Math.max(1, Math.min(SPAWN_MAX, Math.floor(Number(req.body?.n ?? 50))));
    const hotspot = req.body?.hotspot === true;
    const spread = hotspot ? 0.0015 : 0.03; // ~150m cluster vs ~3km spread
    const nowMs = now();
    let spawned = 0;
    for (let i = 0; i < n; i++) {
      const id = `spawn-${nowMs}-${i}`;
      const lat = center.lat + (Math.random() * 2 - 1) * spread;
      const lng = center.lng + (Math.random() * 2 - 1) * spread;
      const destLat = center.lat + (Math.random() * 2 - 1) * 0.03;
      const destLng = center.lng + (Math.random() * 2 - 1) * 0.03;
      try {
        await pool.query(
          `INSERT INTO ride_requests (id, lat, lng, dest_lat, dest_lng, status)
           VALUES ($1, $2, $3, $4, $5, 'pending') ON CONFLICT (id) DO NOTHING`,
          [id, lat, lng, destLat, destLng],
        );
        await redis.lpush(REQUESTS_QUEUE, id);
        await surge.recordDemand(cellFor(lat, lng), id, nowMs);
        spawned++;
      } catch (err) {
        app.log.error({ err, id }, 'spawn intake failed');
      }
    }
    metrics.spawnedTotal += spawned;
    return reply.send({ spawned, hotspot });
  });

  // The demo controls share the real stores; both /proof and /fault run a heavy
  // real scenario, so guard against overlapping runs (a double-click DoS).
  const deps: Deps = { redis, pool, center };
  let demoBusy = false;

  // Proof Mode: seed 20 drivers into one cell, fire 200 concurrent requests
  // through the real matcher, return the true outcome (the signature test, live).
  app.post('/proof', async (_req, reply) => {
    if (demoBusy) return reply.code(409).send({ error: 'a demo run is already in flight' });
    demoBusy = true;
    try {
      return reply.send(await runProof(deps));
    } catch (err) {
      app.log.error({ err }, 'proof run failed');
      return reply.code(500).send({ error: 'proof run failed' });
    } finally {
      demoBusy = false;
    }
  });

  // Crash the matcher: inject a real abandoned claim (expired lease + orphaned
  // offered trip) and watch the live janitor recover it — no fake recovery.
  app.post('/fault/abandon-claim', async (_req, reply) => {
    if (demoBusy) return reply.code(409).send({ error: 'a demo run is already in flight' });
    demoBusy = true;
    try {
      return reply.send(await runCrashDemo(deps));
    } catch (err) {
      app.log.error({ err }, 'crash demo failed');
      return reply.code(500).send({ error: 'crash demo failed' });
    } finally {
      demoBusy = false;
    }
  });

  // Trip inspector: the real event history + offer cascade + live surge.
  app.get<{ Params: { id: string } }>('/trip/:id/events', async (req, reply) => {
    const history = await tripHistory(deps, req.params.id);
    if (history === null) return reply.code(404).send({ error: 'trip not found' });
    return reply.send(history);
  });

  return {
    app,
    buildSnapshot,
    start(): void {
      if (timer !== null) return;
      const tick = (): void => {
        buildSnapshot().then(
          (snap) => {
            metrics.ticksTotal++;
            latest = snap;
            broadcast(snap);
          },
          (err: unknown) => {
            metrics.snapshotErrorsTotal++;
            app.log.error({ err }, 'snapshot build failed');
          },
        );
      };
      timer = setInterval(tick, tickMs);
      tick(); // paint immediately, don't wait a full interval
    },
    async stop(): Promise<void> {
      if (timer !== null) clearInterval(timer);
      timer = null;
      for (const res of sseClients) res.end();
      sseClients.clear();
      await app.close();
    },
    get clients(): number {
      return sseClients.size;
    },
  };
}
