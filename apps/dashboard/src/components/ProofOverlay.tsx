import * as Dialog from '@radix-ui/react-dialog';
import { CircleCheck, ExternalLink, X } from 'lucide-react';
import type { ReactElement } from 'react';
import type { ProofResult } from '../types.js';
import { useCountUp, useReducedMotion } from '../lib/hooks.js';
import { Badge, Button } from './ui/base.js';
import { Didactic, TOPICS } from './Didactic.js';

export type ProofState =
  | { phase: 'idle' }
  | { phase: 'running' }
  | { phase: 'done'; result: ProofResult }
  | { phase: 'error'; message: string };

const REPO_URL = (import.meta.env['VITE_REPO_URL'] as string | undefined) ?? '';
const TEST_PATH = TOPICS['proof']!.testPath;
const DOTS = 200;
const MATCHED = 20;

// Deterministic outward offset per dot, for the converging-streak animation.
function offset(i: number): { fx: string; fy: string } {
  const a = (i * 137.508 * Math.PI) / 180;
  const r = 60 + ((i * 53) % 90);
  return { fx: `${(Math.cos(a) * r).toFixed(0)}px`, fy: `${(Math.sin(a) * r).toFixed(0)}px` };
}

/** Signature moment. Renders the REAL /proof outcome; reduced motion → card. */
export function ProofOverlay({
  state,
  onOpenChange,
}: {
  state: ProofState;
  onOpenChange: (open: boolean) => void;
}): ReactElement {
  const open = state.phase !== 'idle';
  const reduced = useReducedMotion();
  const done = state.phase === 'done';
  const result = state.phase === 'done' ? state.result : null;

  const matched = useCountUp(result?.matched ?? 0, 1100, done);
  const unmatched = useCountUp(result?.unmatched ?? 0, 1100, done);

  return (
    <Dialog.Root open={open} onOpenChange={onOpenChange}>
      <Dialog.Portal>
        <Dialog.Overlay className="fixed inset-0 z-50 bg-ground/80 backdrop-blur-sm" />
        <Dialog.Content className="panel fixed left-1/2 top-1/2 z-50 w-[calc(100vw-2rem)] max-w-[540px] -translate-x-1/2 -translate-y-1/2 p-6">
          <div className="mb-1 flex items-center justify-between">
            <div className="flex items-center gap-2">
              <Dialog.Title className="font-hud text-[18px] font-700 uppercase tracking-wide text-fg">
                Proof Mode
              </Dialog.Title>
              <Didactic topic="proof" />
            </div>
            <Dialog.Close
              aria-label="Close"
              className="rounded-md p-1 text-muted hover:bg-fg/8 hover:text-fg"
            >
              <X size={18} />
            </Dialog.Close>
          </div>
          <p className="mb-4 font-body text-[12.5px] leading-relaxed text-muted">
            200 concurrent requests converge on one cell holding 20 drivers, fired through the real
            matcher on the live sandbox. Exactly 20 lock, the rest go honestly unmatched, nobody
            double-books.
          </p>

          {state.phase === 'error' ? (
            <ErrorCard message={state.message} onRetry={() => onOpenChange(true)} />
          ) : (
            <>
              {!reduced && <DotField running={state.phase === 'running'} />}

              <div className="mt-4 grid grid-cols-3 gap-2">
                <Tally
                  value={done ? matched : 0}
                  total={MATCHED}
                  label="matched"
                  tone="matched"
                  pending={!done}
                />
                <Tally
                  value={done ? unmatched : 0}
                  total={DOTS - MATCHED}
                  label="unmatched"
                  tone="muted"
                  pending={!done}
                />
                <Tally
                  value={result?.doubleAssignments ?? 0}
                  total={0}
                  label="double-assignments"
                  tone={result && result.doubleAssignments === 0 ? 'matched' : 'alarm'}
                  pending={!done}
                  fixed
                />
              </div>

              {done && result !== null && (
                <div className="mt-4 flex flex-wrap items-center gap-2 border-t border-line pt-4">
                  <Badge tone={result.pgUniqueViolations === 0 ? 'matched' : 'alarm'}>
                    <CircleCheck size={12} /> pg_unique_violations {result.pgUniqueViolations}
                  </Badge>
                  <Badge tone="amber">claim conflicts {result.claimConflicts}</Badge>
                  <Badge tone="neutral">{result.elapsedMs} ms</Badge>
                  <Badge tone="neutral">cell {result.cell.slice(0, 7)}…</Badge>
                </div>
              )}

              <div className="mt-4 flex items-center justify-between">
                <span className="microlabel text-[9.5px]">
                  {state.phase === 'running'
                    ? 'firing 200 concurrent requests…'
                    : 'the same scenario CI asserts'}
                </span>
                <TestLink />
              </div>
            </>
          )}
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  );
}

function DotField({ running }: { running: boolean }): ReactElement {
  return (
    <div className="relative grid place-items-center rounded-[10px] border border-line bg-ground/40 py-5">
      <div className="grid grid-cols-[repeat(25,1fr)] gap-[3px]">
        {Array.from({ length: DOTS }, (_, i) => {
          const isMatched = i < MATCHED;
          const { fx, fy } = offset(i);
          return (
            <span
              key={i}
              className="h-[6px] w-[6px] rounded-full"
              style={{
                background: running ? '#FFB020' : isMatched ? '#7EE0A3' : 'rgba(154,168,191,0.35)',
                transition: 'background 220ms ease',
                ...(running
                  ? ({
                      '--fx': fx,
                      '--fy': fy,
                      animation: `fl-streak 900ms ease-out ${(i % 25) * 12}ms both`,
                    } as Record<string, string>)
                  : isMatched
                    ? {
                        boxShadow: '0 0 0 0 rgba(126,224,163,0.5)',
                        animation: 'fl-pulse 1.4s ease-out 1',
                      }
                    : {}),
              }}
            />
          );
        })}
      </div>
    </div>
  );
}

function Tally({
  value,
  total,
  label,
  tone,
  pending,
  fixed = false,
}: {
  value: number;
  total: number;
  label: string;
  tone: 'matched' | 'muted' | 'alarm';
  pending: boolean;
  fixed?: boolean;
}): ReactElement {
  const color =
    tone === 'matched' ? 'text-matched' : tone === 'alarm' ? 'text-alarm' : 'text-muted';
  return (
    <div className="rounded-[10px] border border-line bg-ground/40 px-3 py-2.5 text-center">
      <div
        className={`font-mono text-[26px] font-600 leading-none tnum ${pending ? 'text-fg/25' : color}`}
      >
        {pending ? (fixed ? '·' : '—') : value}
      </div>
      <div className="mt-1.5 microlabel text-[8.5px] leading-tight">{label}</div>
      {total > 0 && <div className="font-mono text-[9px] text-muted/60">of {total}</div>}
    </div>
  );
}

function ErrorCard({ message, onRetry }: { message: string; onRetry: () => void }): ReactElement {
  return (
    <div className="rounded-[10px] border border-alarm/40 bg-alarm/8 p-4">
      <div className="font-hud text-[13px] font-600 uppercase tracking-wide text-alarm">
        Proof didn’t run
      </div>
      <p className="mt-2 font-body text-[12.5px] text-fg/80">
        {message}. The read model may be starting up or the token may be wrong — check the stack is
        up, then run it again.
      </p>
      <Button className="mt-3" variant="primary" size="sm" onClick={onRetry}>
        Try again
      </Button>
    </div>
  );
}

function TestLink(): ReactElement {
  if (REPO_URL === '') {
    return <span className="font-mono text-[11px] text-amber/80">{TEST_PATH}</span>;
  }
  return (
    <a
      href={`${REPO_URL}/${TEST_PATH}`}
      target="_blank"
      rel="noreferrer"
      className="flex items-center gap-1 font-mono text-[11px] text-amber hover:underline"
    >
      {TEST_PATH} <ExternalLink size={11} />
    </a>
  );
}
