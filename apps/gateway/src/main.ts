import { buildGateway } from './server.js';

const port = Number(process.env['PORT'] ?? 8080);
const { app } = buildGateway({ logger: true });

const shutdown = async (): Promise<void> => {
  await app.close();
  process.exit(0);
};
process.on('SIGINT', () => void shutdown());
process.on('SIGTERM', () => void shutdown());

await app.listen({ port, host: '0.0.0.0' });
