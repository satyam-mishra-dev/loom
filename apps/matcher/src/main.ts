import Fastify from 'fastify';
import { Redis } from 'ioredis';
import { pino } from 'pino';
import { createPool } from '@fleetline/db';
import { Janitor } from './janitor.js';
import { MatcherCore } from './matcher.js';
import { renderMetrics } from './metrics.js';

function envInt(name: string, fallback: number): number {
  const raw = process.env[name];
  if (raw === undefined) return fallback;
  const n = Number(raw);
  if (!Number.isFinite(n) || n <= 0) throw new Error(`${name} must be a positive number, got "${raw}"`);
  return n;
}

const port = envInt('PORT', 8090);
const consumers = envInt('MATCHER_CONSUMERS', 4);
const redisUrl = process.env['REDIS_URL'] ?? 'redis://127.0.0.1:6381';
const databaseUrl =
  process.env['DATABASE_URL'] ?? 'postgres://fleetline:fleetline@127.0.0.1:5434/fleetline';

const log = pino({ name: 'matcher' });
const redis = new Redis(redisUrl);
const pool = createPool(databaseUrl);

const core = new MatcherCore({
  redis,
  pool,
  log,
  need: envInt('MATCH_NEED', 8),
  maxK: envInt('MATCH_MAX_K', 3),
  freshMs: envInt('FRESH_MS', 10_000),
  claimTtlMs: envInt('CLAIM_TTL_MS', 12_000),
  offerTtlMs: envInt('OFFER_TTL_MS', 8_000),
  maxOffers: envInt('MAX_OFFERS', 5),
});

// Every matcher embeds a janitor; janitor-main.ts runs the same loop alone.
const janitor = new Janitor({ redis, pool, log, sweepIntervalMs: envInt('JANITOR_SWEEP_MS', 1_000) });

const app = Fastify({ logger: false });
app.get('/healthz', () => ({ status: 'ok' }));
app.get('/metrics', (_req, reply) => {
  void reply.type('text/plain; version=0.0.4');
  return renderMetrics(core.metrics, janitor.metrics);
});

let shuttingDown = false;
const shutdown = async (): Promise<void> => {
  if (shuttingDown) return;
  shuttingDown = true;
  log.info('shutting down');
  await janitor.stop();
  await core.stop(); // finishes in-flight matches, ≤ ~1s
  await app.close();
  await pool.end();
  redis.disconnect();
  log.info('matcher stopped');
  process.exit(0);
};
process.on('SIGINT', () => void shutdown());
process.on('SIGTERM', () => void shutdown());

await app.listen({ port, host: '0.0.0.0' });
await core.start(consumers);
janitor.start();
log.info({ port, consumers }, 'matcher started');
