import type { IncomingMessage } from 'node:http';
import type { Duplex } from 'node:stream';
import Fastify, { type FastifyInstance } from 'fastify';
import { WebSocketServer } from 'ws';

export interface GatewayMetrics {
  wsConnectionsTotal: number;
  wsConnectionsActive: number;
  wsMessagesReceivedTotal: number;
}

export interface Gateway {
  app: FastifyInstance;
  metrics: GatewayMetrics;
}

// ponytail: homegrown counters rendered in Prometheus text format; prom-client
// only if histograms/labels are ever needed.
function renderMetrics(m: GatewayMetrics): string {
  return [
    '# TYPE ws_connections_total counter',
    `ws_connections_total ${m.wsConnectionsTotal}`,
    '# TYPE ws_connections_active gauge',
    `ws_connections_active ${m.wsConnectionsActive}`,
    '# TYPE ws_messages_received_total counter',
    `ws_messages_received_total ${m.wsMessagesReceivedTotal}`,
    '',
  ].join('\n');
}

/**
 * Gateway skeleton: /healthz, /metrics, and a /ws endpoint that accepts
 * connections and counts received messages. Parsing pings into the Redis geo
 * index is a later phase.
 */
export function buildGateway(opts: { logger?: boolean } = {}): Gateway {
  const metrics: GatewayMetrics = {
    wsConnectionsTotal: 0,
    wsConnectionsActive: 0,
    wsMessagesReceivedTotal: 0,
  };

  const app = Fastify({ logger: opts.logger ?? false });

  app.get('/healthz', () => ({ status: 'ok' }));
  app.get('/metrics', (_req, reply) => {
    void reply.type('text/plain; version=0.0.4');
    return renderMetrics(metrics);
  });

  const wss = new WebSocketServer({ noServer: true });
  wss.on('connection', (socket) => {
    metrics.wsConnectionsTotal++;
    metrics.wsConnectionsActive++;
    socket.on('message', () => {
      metrics.wsMessagesReceivedTotal++;
    });
    socket.on('close', () => {
      metrics.wsConnectionsActive--;
    });
  });

  app.server.on('upgrade', (req: IncomingMessage, socket: Duplex, head: Buffer) => {
    const { pathname } = new URL(req.url ?? '/', 'http://localhost');
    if (pathname !== '/ws') {
      socket.destroy();
      return;
    }
    wss.handleUpgrade(req, socket, head, (ws) => wss.emit('connection', ws, req));
  });

  app.addHook('onClose', (_instance, done) => {
    wss.close(() => done());
  });

  return { app, metrics };
}
