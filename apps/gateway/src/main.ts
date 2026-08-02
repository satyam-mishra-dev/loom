import { Redis } from 'ioredis';
import { createPool } from '@fleetline/db';
import { buildGateway } from './server.js';

const port = Number(process.env['PORT'] ?? 8080);
const redisUrl = process.env['REDIS_URL'] ?? 'redis://127.0.0.1:6381';
const databaseUrl =
  process.env['DATABASE_URL'] ?? 'postgres://fleetline:fleetline@127.0.0.1:5434/fleetline';
// Default keeps `docker compose up` + local demo one-command; set
// GATEWAY_SECRET for anything reachable beyond localhost.
const secret = process.env['GATEWAY_SECRET'] ?? 'fleetline-dev-secret';

const redis = new Redis(redisUrl);
const pool = createPool(databaseUrl);
const { app } = buildGateway({
  redis,
  secret,
  pool,
  logger: true,
  rateLimit: {
    limit: Number(process.env['RATE_LIMIT_RPS'] ?? 200),
    windowMs: Number(process.env['RATE_LIMIT_WINDOW_MS'] ?? 1_000),
    burst: Number(process.env['RATE_LIMIT_BURST'] ?? 400),
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
