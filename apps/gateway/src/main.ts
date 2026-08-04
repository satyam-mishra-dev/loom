import { Redis } from 'ioredis';
import { createPool } from '@loom/db';
import { buildGateway } from './server.js';

// Validate numeric env up front: a raw Number(env) that yields NaN would
// silently corrupt the port or the limiter (and NaN into any interval spins).
function envInt(name: string, fallback: number): number {
  const raw = process.env[name];
  if (raw === undefined) return fallback;
  const n = Number(raw);
  if (!Number.isFinite(n) || n <= 0)
    throw new Error(`${name} must be a positive number, got "${raw}"`);
  return n;
}

const port = envInt('PORT', 8080);
const redisUrl = process.env['REDIS_URL'] ?? 'redis://127.0.0.1:6381';
const databaseUrl = process.env['DATABASE_URL'] ?? 'postgres://loom:loom@127.0.0.1:5434/loom';
// Default keeps `docker compose up` + local demo one-command; set
// GATEWAY_SECRET for anything reachable beyond localhost.
const secret = process.env['GATEWAY_SECRET'] ?? 'loom-dev-secret';

const redis = new Redis(redisUrl);
const pool = createPool(databaseUrl);
const { app } = buildGateway({
  redis,
  secret,
  pool,
  logger: true,
  rateLimit: {
    limit: envInt('RATE_LIMIT_RPS', 200),
    windowMs: envInt('RATE_LIMIT_WINDOW_MS', 1_000),
    burst: envInt('RATE_LIMIT_BURST', 400),
    failClosed: process.env['RATE_LIMIT_FAIL_CLOSED'] === 'true',
  },
});

const shutdown = async (): Promise<void> => {
  await app.close();
  await pool.end();
  redis.disconnect();
  process.exit(0);
};
process.on('SIGINT', () => void shutdown());
process.on('SIGTERM', () => void shutdown());

await app.listen({ port, host: '0.0.0.0' });
