import { Hist, renderCounter, renderSummary } from '@fleetline/core';

export interface GatewayMetrics {
  wsConnectionsTotal: number;
  wsConnectionsActive: number;
  wsMessagesReceivedTotal: number;
  pingsReceivedTotal: number;
  rideRequestsReceivedTotal: number;
  rideRequestsEnqueuedTotal: number;
  rideRequestErrorsTotal: number;
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
    rideRequestsEnqueuedTotal: 0,
    rideRequestErrorsTotal: 0,
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

export function renderMetrics(m: GatewayMetrics): string {
  return [
    ...renderCounter('ws_connections_total', m.wsConnectionsTotal),
    ...renderCounter('ws_connections_active', m.wsConnectionsActive, 'gauge'),
    ...renderCounter('ws_messages_received_total', m.wsMessagesReceivedTotal),
    ...renderCounter('pings_received_total', m.pingsReceivedTotal),
    ...renderCounter('ride_requests_received_total', m.rideRequestsReceivedTotal),
    ...renderCounter('ride_requests_enqueued_total', m.rideRequestsEnqueuedTotal),
    ...renderCounter('ride_request_errors_total', m.rideRequestErrorsTotal),
    ...renderCounter('invalid_messages_total', m.invalidMessagesTotal),
    ...renderCounter('auth_rejects_total', m.authRejectsTotal),
    ...renderCounter('pong_timeouts_total', m.pongTimeoutsTotal),
    ...renderCounter('backpressure_disconnects_total', m.backpressureDisconnectsTotal),
    ...renderCounter('ping_batches_flushed_total', m.batchesFlushedTotal),
    ...renderCounter('ping_flush_errors_total', m.flushErrorsTotal),
    ...renderCounter('drivers_swept_total', m.driversSweptTotal),
    ...renderSummary('ping_batch_size', m.batchSize),
    ...renderSummary('ping_flush_latency_ms', m.flushLatencyMs),
    '',
  ].join('\n');
}
