import { Redis } from 'ioredis';
import { pino } from 'pino';
import { createPool } from '@fleetline/db';
import { Janitor } from './janitor.js';

// Standalone janitor: the same sweep loop every matcher embeds, runnable
// alone (`npm run janitor --workspace=@fleetline/matcher`) — proof that claim
// recovery needs no matcher alive, only the data.

const redisUrl = process.env['REDIS_URL'] ?? 'redis://127.0.0.1:6381';
const databaseUrl =
  process.env['DATABASE_URL'] ?? 'postgres://fleetline:fleetline@127.0.0.1:5434/fleetline';
const sweepIntervalMs = Number(process.env['JANITOR_SWEEP_MS'] ?? 1_000);

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
