import { Hist, renderCounter, renderSummary, type LimiterMetrics } from '@loom/core';

export interface GatewayMetrics {
  wsConnectionsTotal: number;
  wsConnectionsActive: number;
  wsMessagesReceivedTotal: number;
  pingsReceivedTotal: number;
  rideRequestsReceivedTotal: number;
  rideRequestsEnqueuedTotal: number;
  /** Ride requests rejected by the intake rate limiter (429-equivalent). */
  rideRequestsRateLimitedTotal: number;
  rideRequestErrorsTotal: number;
  invalidMessagesTotal: number;
  authRejectsTotal: number;
  /** Pings/replies/trip-progress rejected because their driverId was outside the principal's scope (G1/G4). */
  scopeRejectsTotal: number;
  pongTimeoutsTotal: number;
  backpressureDisconnectsTotal: number;
  batchesFlushedTotal: number;
  flushErrorsTotal: number;
  driversSweptTotal: number;
  /** Pub/sub messages relayed to a connected driver's socket. */
  messagesForwardedTotal: number;
  /** Pub/sub messages for a driver this gateway could not deliver to. */
  forwardDropsTotal: number;
  offerRepliesTotal: number;
  tripProgressTotal: number;
  replyErrorsTotal: number;
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
    rideRequestsRateLimitedTotal: 0,
    rideRequestErrorsTotal: 0,
    invalidMessagesTotal: 0,
    authRejectsTotal: 0,
    scopeRejectsTotal: 0,
    pongTimeoutsTotal: 0,
    backpressureDisconnectsTotal: 0,
    batchesFlushedTotal: 0,
    flushErrorsTotal: 0,
    driversSweptTotal: 0,
    messagesForwardedTotal: 0,
    forwardDropsTotal: 0,
    offerRepliesTotal: 0,
    tripProgressTotal: 0,
    replyErrorsTotal: 0,
    batchSize: new Hist(),
    flushLatencyMs: new Hist(),
  };
}

export function renderMetrics(m: GatewayMetrics, limiter?: LimiterMetrics): string {
  const lines = [
    ...renderCounter('ws_connections_total', m.wsConnectionsTotal),
    ...renderCounter('ws_connections_active', m.wsConnectionsActive, 'gauge'),
    ...renderCounter('ws_messages_received_total', m.wsMessagesReceivedTotal),
    ...renderCounter('pings_received_total', m.pingsReceivedTotal),
    ...renderCounter('ride_requests_received_total', m.rideRequestsReceivedTotal),
    ...renderCounter('ride_requests_enqueued_total', m.rideRequestsEnqueuedTotal),
    ...renderCounter('ride_requests_rate_limited_total', m.rideRequestsRateLimitedTotal),
    ...renderCounter('ride_request_errors_total', m.rideRequestErrorsTotal),
    ...renderCounter('invalid_messages_total', m.invalidMessagesTotal),
    ...renderCounter('auth_rejects_total', m.authRejectsTotal),
    ...renderCounter('scope_rejects_total', m.scopeRejectsTotal),
    ...renderCounter('pong_timeouts_total', m.pongTimeoutsTotal),
    ...renderCounter('backpressure_disconnects_total', m.backpressureDisconnectsTotal),
    ...renderCounter('ping_batches_flushed_total', m.batchesFlushedTotal),
    ...renderCounter('ping_flush_errors_total', m.flushErrorsTotal),
    ...renderCounter('drivers_swept_total', m.driversSweptTotal),
    ...renderCounter('messages_forwarded_total', m.messagesForwardedTotal),
    ...renderCounter('forward_drops_total', m.forwardDropsTotal),
    ...renderCounter('offer_replies_total', m.offerRepliesTotal),
    ...renderCounter('trip_progress_total', m.tripProgressTotal),
    ...renderCounter('reply_errors_total', m.replyErrorsTotal),
    ...renderSummary('ping_batch_size', m.batchSize),
    ...renderSummary('ping_flush_latency_ms', m.flushLatencyMs),
  ];
  if (limiter !== undefined) {
    lines.push(
      ...renderCounter('intake_limiter_primary_total', limiter.primaryTotal),
      ...renderCounter('intake_limiter_fallback_total', limiter.fallbackTotal),
      ...renderCounter('intake_limiter_denied_total', limiter.deniedTotal),
      ...renderCounter('intake_limiter_fail_closed_rejects_total', limiter.failClosedRejectsTotal),
      ...renderCounter('intake_limiter_redis_timeouts_total', limiter.redisTimeoutsTotal),
      ...renderCounter('intake_limiter_redis_errors_total', limiter.redisErrorsTotal),
    );
  }
  lines.push('');
  return lines.join('\n');
}
