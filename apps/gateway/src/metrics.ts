/**
 * Homegrown metrics, extending the phase-A counters. A histogram here is
 * exact count/sum plus quantiles over a bounded reservoir of recent samples —
 * enough for batch-size p50 and flush-latency p99 without prom-client.
 */
export class Hist {
  count = 0;
  sum = 0;
  private readonly samples: number[] = [];
  private idx = 0;

  constructor(private readonly cap = 10_000) {}

  observe(value: number): void {
    this.count++;
    this.sum += value;
    if (this.samples.length < this.cap) {
      this.samples.push(value);
    } else {
      this.samples[this.idx] = value;
      this.idx = (this.idx + 1) % this.cap;
    }
  }

  quantile(q: number): number {
    if (this.samples.length === 0) return 0;
    const sorted = [...this.samples].sort((a, b) => a - b);
    return sorted[Math.min(sorted.length - 1, Math.floor(q * sorted.length))] ?? 0;
  }
}

export interface GatewayMetrics {
  wsConnectionsTotal: number;
  wsConnectionsActive: number;
  wsMessagesReceivedTotal: number;
  pingsReceivedTotal: number;
  rideRequestsReceivedTotal: number;
  invalidMessagesTotal: number;
  authRejectsTotal: number;
  pongTimeoutsTotal: number;
  backpressureDisconnectsTotal: number;
  batchesFlushedTotal: number;
  flushErrorsTotal: number;
  driversSweptTotal: number;
  batchSize: Hist;
  flushLatencyMs: Hist;
}

export function createMetrics(): GatewayMetrics {
  return {
    wsConnectionsTotal: 0,
    wsConnectionsActive: 0,
    wsMessagesReceivedTotal: 0,
    pingsReceivedTotal: 0,
    rideRequestsReceivedTotal: 0,
    invalidMessagesTotal: 0,
    authRejectsTotal: 0,
    pongTimeoutsTotal: 0,
    backpressureDisconnectsTotal: 0,
    batchesFlushedTotal: 0,
    flushErrorsTotal: 0,
    driversSweptTotal: 0,
    batchSize: new Hist(),
    flushLatencyMs: new Hist(),
  };
}

function counter(name: string, value: number, type: 'counter' | 'gauge' = 'counter'): string[] {
  return [`# TYPE ${name} ${type}`, `${name} ${value}`];
}

function summary(name: string, h: Hist): string[] {
  return [
    `# TYPE ${name} summary`,
    `${name}_count ${h.count}`,
    `${name}_sum ${h.sum}`,
    `${name}{quantile="0.5"} ${h.quantile(0.5)}`,
    `${name}{quantile="0.99"} ${h.quantile(0.99)}`,
  ];
}

export function renderMetrics(m: GatewayMetrics): string {
  return [
    ...counter('ws_connections_total', m.wsConnectionsTotal),
    ...counter('ws_connections_active', m.wsConnectionsActive, 'gauge'),
    ...counter('ws_messages_received_total', m.wsMessagesReceivedTotal),
    ...counter('pings_received_total', m.pingsReceivedTotal),
    ...counter('ride_requests_received_total', m.rideRequestsReceivedTotal),
    ...counter('invalid_messages_total', m.invalidMessagesTotal),
    ...counter('auth_rejects_total', m.authRejectsTotal),
    ...counter('pong_timeouts_total', m.pongTimeoutsTotal),
    ...counter('backpressure_disconnects_total', m.backpressureDisconnectsTotal),
    ...counter('ping_batches_flushed_total', m.batchesFlushedTotal),
    ...counter('ping_flush_errors_total', m.flushErrorsTotal),
    ...counter('drivers_swept_total', m.driversSweptTotal),
    ...summary('ping_batch_size', m.batchSize),
    ...summary('ping_flush_latency_ms', m.flushLatencyMs),
    '',
  ].join('\n');
}
