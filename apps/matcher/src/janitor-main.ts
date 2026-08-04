import { Redis } from 'ioredis';
import { pino } from 'pino';
import { createPool } from '@loom/db';
import { Janitor } from './janitor.js';

// Standalone janitor: the same sweep loop every matcher embeds, runnable
// alone (`npm run janitor --workspace=@loom/matcher`) — proof that claim
// recovery needs no matcher alive, only the data.

// Validate numeric env: setInterval(fn, NaN) from a bad JANITOR_SWEEP_MS would
// spin as fast as the event loop, hammering Redis/Postgres. Require positive.
function envInt(name: string, fallback: number): number {
  const raw = process.env[name];
  if (raw === undefined) return fallback;
  const n = Number(raw);
  if (!Number.isFinite(n) || n <= 0)
    throw new Error(`${name} must be a positive number, got "${raw}"`);
  return n;
}

const redisUrl = process.env['REDIS_URL'] ?? 'redis://127.0.0.1:6381';
const databaseUrl = process.env['DATABASE_URL'] ?? 'postgres://loom:loom@127.0.0.1:5434/loom';
const sweepIntervalMs = envInt('JANITOR_SWEEP_MS', 1_000);

const log = pino({ name: 'janitor' });
const redis = new Redis(redisUrl);
const pool = createPool(databaseUrl);

const janitor = new Janitor({ redis, pool, log, sweepIntervalMs });
janitor.start();
log.info({ sweepIntervalMs }, 'janitor started');

const shutdown = async (): Promise<void> => {
  await janitor.stop();
  log.info(janitor.metrics, 'janitor stopped');
  await pool.end();
  redis.disconnect();
  process.exit(0);
};
process.on('SIGINT', () => void shutdown());
process.on('SIGTERM', () => void shutdown());
