import { Redis } from 'ioredis';
import { buildGateway } from './server.js';

const port = Number(process.env['PORT'] ?? 8080);
const redisUrl = process.env['REDIS_URL'] ?? 'redis://127.0.0.1:6381';
// Default keeps `docker compose up` + local demo one-command; set
// GATEWAY_SECRET for anything reachable beyond localhost.
const secret = process.env['GATEWAY_SECRET'] ?? 'fleetline-dev-secret';

const redis = new Redis(redisUrl);
const { app } = buildGateway({ redis, secret, logger: true });

const shutdown = async (): Promise<void> => {
  await app.close();
  redis.disconnect();
  process.exit(0);
};
process.on('SIGINT', () => void shutdown());
process.on('SIGTERM', () => void shutdown());

await app.listen({ port, host: '0.0.0.0' });
