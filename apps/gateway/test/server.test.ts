import type { AddressInfo } from 'node:net';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import WebSocket from 'ws';
import { buildGateway, type Gateway } from '../src/server.js';

async function waitFor(cond: () => boolean, timeoutMs = 2000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!cond()) {
    if (Date.now() > deadline) throw new Error('waitFor timed out');
    await new Promise((r) => setTimeout(r, 10));
  }
}

describe('gateway', () => {
  let gw: Gateway;
  let port: number;

  beforeAll(async () => {
    gw = buildGateway();
    await gw.app.listen({ port: 0, host: '127.0.0.1' });
    port = (gw.app.server.address() as AddressInfo).port;
  });

  afterAll(async () => {
    await gw.app.close();
  });

  it('serves /healthz', async () => {
    const res = await gw.app.inject({ method: 'GET', url: '/healthz' });
    expect(res.statusCode).toBe(200);
    expect(res.json()).toEqual({ status: 'ok' });
  });

  it('accepts ws connections on /ws and counts received messages', async () => {
    const ws = new WebSocket(`ws://127.0.0.1:${port}/ws`);
    await new Promise<void>((resolve, reject) => {
      ws.once('open', resolve);
      ws.once('error', reject);
    });
    ws.send('one');
    ws.send('two');
    ws.send('three');
    await waitFor(() => gw.metrics.wsMessagesReceivedTotal === 3);

    const res = await gw.app.inject({ method: 'GET', url: '/metrics' });
    expect(res.statusCode).toBe(200);
    expect(res.headers['content-type']).toContain('text/plain');
    expect(res.body).toContain('ws_messages_received_total 3');
    expect(res.body).toContain('ws_connections_total 1');
    expect(res.body).toContain('ws_connections_active 1');

    ws.close();
    await waitFor(() => gw.metrics.wsConnectionsActive === 0);
  });

  it('rejects upgrades on paths other than /ws', async () => {
    const ws = new WebSocket(`ws://127.0.0.1:${port}/nope`);
    const err = await new Promise<Error>((resolve) => ws.once('error', resolve));
    expect(err).toBeInstanceOf(Error);
  });
});
