import { Hist, renderCounter, renderSummary } from '@fleetline/core';

export interface MatcherMetrics {
  /** Requests that ended matched / unmatched (terminal outcomes). */
  matchesTotal: number;
  unmatchedTotal: number;
  /** Redelivered ids skipped by the pending→matching row guard. */
  requestsSkippedTotal: number;
  /** claimDriver returned null — another matcher (or staleness) won. */
  claimConflictsTotal: number;
  /** THE never-happens counter: partial unique index rejected a trip write. */
  pgUniqueViolationsTotal: number;
  /** confirmClaim failed after the trip committed (claim expired mid-write). */
  confirmFailuresTotal: number;
  /** matchRequest threw (PG/Redis down); request left for the reaper. */
  matchErrorsTotal: number;
  /** Offers published to driver channels. */
  offersSentTotal: number;
  offerAcceptsTotal: number;
  offerDeclinesTotal: number;
  offerTimeoutsTotal: number;
  /** Mid-cascade guarded write missed — the janitor/a peer owns the request now. */
  cascadeLostTotal: number;
  /** Trips advanced to completed by the progression consumer. */
  tripsCompletedTotal: number;
  /** trip_progress messages the machine guards refused (duplicate/stale/out of order). */
  tripEventGuardFailuresTotal: number;
  /** Request pop → terminal status, ms. */
  matchLatencyMs: Hist;
}

export function createMetrics(): MatcherMetrics {
  return {
    matchesTotal: 0,
    unmatchedTotal: 0,
    requestsSkippedTotal: 0,
    claimConflictsTotal: 0,
    pgUniqueViolationsTotal: 0,
    confirmFailuresTotal: 0,
    matchErrorsTotal: 0,
    offersSentTotal: 0,
    offerAcceptsTotal: 0,
    offerDeclinesTotal: 0,
    offerTimeoutsTotal: 0,
    cascadeLostTotal: 0,
    tripsCompletedTotal: 0,
    tripEventGuardFailuresTotal: 0,
    matchLatencyMs: new Hist(),
  };
}

export interface JanitorMetrics {
  sweepsTotal: number;
  /** Expired claims atomically released (driver back to available). */
  janitorReleasedTotal: number;
  /** Requests re-enqueued onto requests:queue after a release. */
  janitorRequeuedTotal: number;
  /** Claim keys already erased by the PX net when the sweep arrived. */
  janitorGoneTotal: number;
  sweepErrorsTotal: number;
}

export function createJanitorMetrics(): JanitorMetrics {
  return {
    sweepsTotal: 0,
    janitorReleasedTotal: 0,
    janitorRequeuedTotal: 0,
    janitorGoneTotal: 0,
    sweepErrorsTotal: 0,
  };
}

export function renderMetrics(m: MatcherMetrics, j?: JanitorMetrics): string {
  const lines = [
    ...renderCounter('matches_total', m.matchesTotal),
    ...renderCounter('unmatched_total', m.unmatchedTotal),
    ...renderCounter('requests_skipped_total', m.requestsSkippedTotal),
    ...renderCounter('claim_conflicts_total', m.claimConflictsTotal),
    ...renderCounter('pg_unique_violations_total', m.pgUniqueViolationsTotal),
    ...renderCounter('confirm_failures_total', m.confirmFailuresTotal),
    ...renderCounter('match_errors_total', m.matchErrorsTotal),
    ...renderCounter('offers_sent_total', m.offersSentTotal),
    ...renderCounter('offer_accepts_total', m.offerAcceptsTotal),
    ...renderCounter('offer_declines_total', m.offerDeclinesTotal),
    ...renderCounter('offer_timeouts_total', m.offerTimeoutsTotal),
    ...renderCounter('cascade_lost_total', m.cascadeLostTotal),
    ...renderCounter('trips_completed_total', m.tripsCompletedTotal),
    ...renderCounter('trip_event_guard_failures_total', m.tripEventGuardFailuresTotal),
    ...renderSummary('match_latency_ms', m.matchLatencyMs),
  ];
  if (j !== undefined) {
    lines.push(
      ...renderCounter('janitor_sweeps_total', j.sweepsTotal),
      ...renderCounter('janitor_released_total', j.janitorReleasedTotal),
      ...renderCounter('janitor_requeued_total', j.janitorRequeuedTotal),
      ...renderCounter('janitor_gone_total', j.janitorGoneTotal),
      ...renderCounter('janitor_sweep_errors_total', j.sweepErrorsTotal),
    );
  }
  lines.push('');
  return lines.join('\n');
}
