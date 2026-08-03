import { Redis } from 'ioredis';
import { createPool } from '@fleetline/db';
import { buildReadModel } from './server.js';

// Validate numeric env: a raw Number(env) that parses to NaN would feed
// setInterval(fn, NaN) — which fires as fast as the event loop can, DoS-ing the
// snapshot query against its own deps. envInt requires a positive number;
// envFinite allows any finite value (the map center may be negative).
function envInt(name: string, fallback: number): number {
  const raw = process.env[name];
  if (raw === undefined) return fallback;
  const n = Number(raw);
  if (!Number.isFinite(n) || n <= 0) throw new Error(`${name} must be a positive number, got "${raw}"`);
  return n;
}
function envFinite(name: string, fallback: number): number {
  const raw = process.env[name];
  if (raw === undefined) return fallback;
  const n = Number(raw);
  if (!Number.isFinite(n)) throw new Error(`${name} must be a finite number, got "${raw}"`);
  return n;
}

const port = envInt('PORT', 4600);
const redisUrl = process.env['REDIS_URL'] ?? 'redis://127.0.0.1:6381';
const databaseUrl =
  process.env['DATABASE_URL'] ?? 'postgres://fleetline:fleetline@127.0.0.1:5434/fleetline';
const matcherMetricsUrl = process.env['MATCHER_METRICS_URL'] ?? 'http://127.0.0.1:8090/metrics';

const redis = new Redis(redisUrl);
const pool = createPool(databaseUrl);
const rm = buildReadModel({
  redis,
  pool,
  matcherMetricsUrl,
  logger: true,
  tickMs: envInt('READ_MODEL_TICK_MS', 1_000),
  center: {
    lat: envFinite('CENTER_LAT', 37.7749),
    lng: envFinite('CENTER_LNG', -122.4194),
  },
  authToken: process.env['READ_MODEL_TOKEN'],
});

const shutdown = async (): Promise<void> => {
  await rm.stop();
  await pool.end();
  redis.disconnect();
  process.exit(0);
};
process.on('SIGINT', () => void shutdown());
process.on('SIGTERM', () => void shutdown());

await rm.app.listen({ port, host: '0.0.0.0' });
rm.start();
