import { Braces } from 'lucide-react';
import type { ReactElement } from 'react';
import { Popover } from './ui/popover.js';

/**
 * Didactic affordance: a small {} on each major panel opens a plain-language
 * explanation of the mechanism + a deep link to the ARCHITECTURE.md section and
 * the test on disk.
 *
 * IMPORTANT: the sentences below are NEUTRAL,
 * FACTUAL technical descriptions written by the agent. They are NOT the author's
 * voice. Each is marked `TODO(voice)` for the author to rewrite as an interview
 * answer in miniature before the demo ships.
 */

// Optional GitHub base (e.g. https://github.com/you/loom/blob/main). When
// unset, the on-disk path is shown as a copyable monospace reference instead.
const REPO_URL = (import.meta.env['VITE_REPO_URL'] as string | undefined) ?? '';

export interface DidacticTopic {
  title: string;
  /** TODO(voice): replace with the author's own 2–3 sentences. */
  body: string;
  archAnchor: string; // ARCHITECTURE.md heading anchor
  testPath: string; // path on disk
}

export const TOPICS: Record<string, DidacticTopic> = {
  hud: {
    title: 'Live counters',
    // TODO(voice): rewrite in the author's voice.
    body: 'These figures are the real read model, not a mock. A separate CQRS service fans in three sources on a 1s tick — driver positions and surge from Redis, active trips from Postgres, match latency scraped from the matcher /metrics — and streams a bounded snapshot over SSE. p50/p99 are measured on the matched path only.',
    archAnchor: 'the-read-model-and-sse-why-sse-not-websocket',
    testPath: 'apps/read-model/test/read-model.integration.test.ts',
  },
  surge: {
    title: 'Surge heatmap',
    // TODO(voice): rewrite in the author's voice.
    body: 'Per H3 cell, surge is min(3, max(1, demand / supply)): a sliding 60s window of ride requests over the count of available drivers in that cell. The read model recomputes it each tick and publishes only surging cells. The math is bounded so a rider never sees a runaway price and zero supply can never divide by zero.',
    archAnchor: 'surge',
    testPath: 'packages/core/test/surge.test.ts',
  },
  cells: {
    title: 'H3 cell grid',
    // TODO(voice): rewrite in the author's voice.
    body: 'Every driver is bucketed into an H3 resolution-8 hexagon (~0.74 km²). Candidate search is an expanding gridDisk (k = 0, 1, 2…) that unions each ring’s available-driver set until it has enough fresh drivers. The grid you see is the actual spatial index the matcher queries.',
    archAnchor: 'data-flow-ping-index-match-claim-offer-trip-complete',
    testPath: 'packages/core/test/geo.test.ts',
  },
  proof: {
    title: 'Proof Mode',
    // TODO(voice): rewrite in the author's voice.
    body: '20 drivers are seeded into one cell and 200 ride requests are fired at it concurrently through the real matcher. Contention is serialized by an atomic Redis Lua claim (one SREM wins per driver); a Postgres partial-unique index is the backstop that would reject any double-assignment. The result here is the true outcome — exactly 20 match, 180 are honestly unmatched, zero double-assignments — the same scenario the CI signature test asserts.',
    archAnchor: 'the-atomic-claim-defense-in-depth-redis-and-postgres',
    testPath: 'test/no-double-assignment.test.ts',
  },
  crash: {
    title: 'Crash the matcher',
    // TODO(voice): rewrite in the author's voice.
    body: 'A claim’s expiry lives in the data (an expiresAt inside the claim value, indexed in a ZSET), not in any process. This injects the exact wreckage a killed matcher leaves — a held claim whose lease has lapsed and an orphaned offered trip — and the janitor, running inside every live matcher, sweeps it: the driver returns to available, the trip reverts to matching, the request re-enqueues. No matcher restart is needed and the one-active-trip-per-driver invariant never flips.',
    archAnchor: 'janitor-and-ttl-in-data-the-visibility-timeout',
    testPath: 'test/no-double-assignment-crash.test.ts',
  },
  inspector: {
    title: 'Trip inspector',
    // TODO(voice): rewrite in the author's voice.
    body: 'Every trip transition is written to a trip_events outbox in the same Postgres transaction as the status change, so the state and its history can never disagree. This timeline and the offer cascade below it are read straight from that outbox — real per-transition timestamps, real declined/timed-out/accepted offers.',
    archAnchor: 'data-flow-ping-index-match-claim-offer-trip-complete',
    testPath: 'apps/matcher/src/trip-store.ts',
  },
};

function archHref(anchor: string): string | null {
  return REPO_URL === '' ? null : `${REPO_URL}/ARCHITECTURE.md#${anchor}`;
}
function testHref(path: string): string | null {
  return REPO_URL === '' ? null : `${REPO_URL}/${path}`;
}

export function Didactic({ topic }: { topic: keyof typeof TOPICS }): ReactElement {
  const t = TOPICS[topic]!;
  const arch = archHref(t.archAnchor);
  const test = testHref(t.testPath);
  return (
    <Popover
      label={`How ${t.title} works`}
      trigger={
        <button
          type="button"
          className="grid h-5 w-5 place-items-center rounded text-muted transition-colors hover:bg-fg/10 hover:text-amber"
          title={`How ${t.title} works`}
        >
          <Braces size={13} />
        </button>
      }
    >
      <div className="font-hud text-[13px] font-700 uppercase tracking-wide text-fg">{t.title}</div>
      <p className="mt-2 font-body text-[12.5px] leading-relaxed text-fg/85">{t.body}</p>
      <div className="mt-3 flex flex-col gap-1 border-t border-line pt-2 font-mono text-[11px] text-muted">
        <LinkOrPath
          label="architecture"
          href={arch}
          text={`ARCHITECTURE.md § ${t.archAnchor.replace(/-/g, ' ')}`}
        />
        <LinkOrPath label="test" href={test} text={t.testPath} />
      </div>
    </Popover>
  );
}

function LinkOrPath({
  label,
  href,
  text,
}: {
  label: string;
  href: string | null;
  text: string;
}): ReactElement {
  return (
    <div className="flex items-baseline gap-2">
      <span className="microlabel text-[9px] text-muted/70">{label}</span>
      {href === null ? (
        <span className="truncate text-amber/80">{text}</span>
      ) : (
        <a
          href={href}
          target="_blank"
          rel="noreferrer"
          className="truncate text-amber hover:underline"
        >
          {text}
        </a>
      )}
    </div>
  );
}
