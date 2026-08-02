import { Redis } from 'ioredis';
import { createPool } from '@fleetline/db';
import { buildReadModel } from './server.js';

const port = Number(process.env['PORT'] ?? 4600);
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
  tickMs: Number(process.env['READ_MODEL_TICK_MS'] ?? 1_000),
  center: {
    lat: Number(process.env['CENTER_LAT'] ?? 37.7749),
    lng: Number(process.env['CENTER_LNG'] ?? -122.4194),
  },
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
