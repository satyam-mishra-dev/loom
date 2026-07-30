import WebSocket from 'ws';
import type { SimMessage } from '@fleetline/core';

export interface Sink {
  send(msg: SimMessage): void;
  close(): Promise<void>;
  readonly sent: number;
  readonly dropped: number;
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

  constructor(url: string) {
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
  }

  // ponytail: drop-on-backpressure with counters; batching/acks arrive with gateway ingestion.
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

export function createSink(kind: string, gatewayUrl: string): Sink {
  switch (kind) {
    case 'ws':
      return new WsSink(gatewayUrl);
    case 'stdout':
      return new StdoutSink();
    case 'null':
      return new NullSink();
    default:
      throw new Error(`unknown sink "${kind}" (expected ws|stdout|null)`);
  }
}
