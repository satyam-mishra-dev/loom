import WebSocket from 'ws';
import type { ServerMessage, SimMessage } from '@loom/core';

export interface Sink {
  send(msg: SimMessage): void;
  close(): Promise<void>;
  readonly sent: number;
  readonly dropped: number;
}

/** Server → simulator messages (offers, trip assignments). Only the ws sink has an inbound side. */
export type ServerMessageHandler = (msg: ServerMessage) => void;

/** Shape-check an inbound server message — crashes are worse than drops. */
function parseServerMessage(raw: unknown): ServerMessage | null {
  if (typeof raw !== 'object' || raw === null) return null;
  const msg = raw as Record<string, unknown>;
  const point = (v: unknown): v is { lat: number; lng: number } =>
    typeof v === 'object' &&
    v !== null &&
    typeof (v as { lat: unknown }).lat === 'number' &&
    typeof (v as { lng: unknown }).lng === 'number';
  if (
    msg['type'] === 'offer' &&
    typeof msg['offerId'] === 'string' &&
    typeof msg['driverId'] === 'string' &&
    typeof msg['tripId'] === 'string' &&
    point(msg['pickup'])
  ) {
    return msg as unknown as ServerMessage;
  }
  if (
    msg['type'] === 'trip_assigned' &&
    typeof msg['tripId'] === 'string' &&
    typeof msg['driverId'] === 'string' &&
    point(msg['pickup']) &&
    point(msg['dest'])
  ) {
    return msg as unknown as ServerMessage;
  }
  return null;
}

export class NullSink implements Sink {
  sent = 0;
  readonly dropped = 0;
  send(): void {
    this.sent++;
  }
  async close(): Promise<void> {}
}

export class StdoutSink implements Sink {
  sent = 0;
  readonly dropped = 0;
  send(msg: SimMessage): void {
    process.stdout.write(JSON.stringify(msg) + '\n');
    this.sent++;
  }
  async close(): Promise<void> {}
}

export class WsSink implements Sink {
  sent = 0;
  dropped = 0;
  private readonly ws: WebSocket;
  private opened = false;
  private pending: string[] = [];
  private static readonly MAX_PENDING = 10_000;

  constructor(url: string, onMessage?: ServerMessageHandler) {
    this.ws = new WebSocket(url);
    this.ws.on('open', () => {
      this.opened = true;
      for (const data of this.pending) {
        this.ws.send(data);
        this.sent++;
      }
      this.pending = [];
    });
    this.ws.on('error', (err) => {
      process.stderr.write(`ws sink error: ${err.message}\n`);
    });
    if (onMessage !== undefined) {
      this.ws.on('message', (data: Buffer | ArrayBuffer | Buffer[]) => {
        let parsed: unknown;
        try {
          parsed = JSON.parse(String(data));
        } catch {
          return;
        }
        const msg = parseServerMessage(parsed);
        if (msg !== null) onMessage(msg);
      });
    }
  }

  // NOTE: drop-on-backpressure with counters; batching/acks arrive with gateway ingestion.
  send(msg: SimMessage): void {
    const data = JSON.stringify(msg);
    if (this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(data);
      this.sent++;
    } else if (!this.opened && this.pending.length < WsSink.MAX_PENDING) {
      this.pending.push(data);
    } else {
      this.dropped++;
    }
  }

  close(): Promise<void> {
    return new Promise((resolve) => {
      if (this.ws.readyState === WebSocket.CLOSED) {
        resolve();
        return;
      }
      this.ws.once('close', () => resolve());
      this.ws.close();
      setTimeout(resolve, 1000).unref();
    });
  }
}

export function createSink(
  kind: string,
  gatewayUrl: string,
  onMessage?: ServerMessageHandler,
): Sink {
  switch (kind) {
    case 'ws':
      return new WsSink(gatewayUrl, onMessage);
    case 'stdout':
      return new StdoutSink();
    case 'null':
      return new NullSink();
    default:
      throw new Error(`unknown sink "${kind}" (expected ws|stdout|null)`);
  }
}
