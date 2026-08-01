import { Hist, renderCounter, renderSummary } from '@fleetline/core';

export interface MatcherMetrics {
  /** Requests that ended matched / unmatched (terminal outcomes). */
  matchesTotal: number;
  unmatchedTotal: number;
  /** Redelivered ids skipped by the pending→matching row guard. */
  requestsSkippedTotal: number;
  /** claimDriver returned null — another matcher (or staleness) won. */
  claimConflictsTotal: number;
  /** THE never-happens counter: partial unique index rejected a trip INSERT. */
  pgUniqueViolationsTotal: number;
  /** confirmClaim failed after the trip committed (claim expired mid-write). */
  confirmFailuresTotal: number;
  /** matchRequest threw (PG/Redis down); request left for the reaper. */
  matchErrorsTotal: number;
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
    matchLatencyMs: new Hist(),
  };
}

export function renderMetrics(m: MatcherMetrics): string {
  return [
    ...renderCounter('matches_total', m.matchesTotal),
    ...renderCounter('unmatched_total', m.unmatchedTotal),
    ...renderCounter('requests_skipped_total', m.requestsSkippedTotal),
    ...renderCounter('claim_conflicts_total', m.claimConflictsTotal),
    ...renderCounter('pg_unique_violations_total', m.pgUniqueViolationsTotal),
    ...renderCounter('confirm_failures_total', m.confirmFailuresTotal),
    ...renderCounter('match_errors_total', m.matchErrorsTotal),
    ...renderSummary('match_latency_ms', m.matchLatencyMs),
    '',
  ].join('\n');
}
