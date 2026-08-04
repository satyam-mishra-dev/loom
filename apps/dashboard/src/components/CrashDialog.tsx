import * as Dialog from '@radix-ui/react-dialog';
import { AlertTriangle, CircleCheck, Loader2, ShieldCheck, X } from 'lucide-react';
import type { ReactElement } from 'react';
import type { CrashResult } from '../types.js';
import { Badge, Button } from './ui/base.js';
import { Didactic, TOPICS } from './Didactic.js';

export type CrashState =
  | { phase: 'idle' }
  | { phase: 'confirm' }
  | { phase: 'running' }
  | { phase: 'done'; result: CrashResult }
  | { phase: 'error'; message: string };

const REPO_URL = (import.meta.env['VITE_REPO_URL'] as string | undefined) ?? '';
const TEST_PATH = TOPICS['crash']!.testPath;

/** §3.3 runner-up. A real abandoned-claim fault, recovered by the real janitor. */
export function CrashDialog({
  state,
  onConfirm,
  onOpenChange,
}: {
  state: CrashState;
  onConfirm: () => void;
  onOpenChange: (open: boolean) => void;
}): ReactElement {
  const open = state.phase !== 'idle';
  const result = state.phase === 'done' ? state.result : null;
  const running = state.phase === 'running';

  return (
    <Dialog.Root open={open} onOpenChange={onOpenChange}>
      <Dialog.Portal>
        <Dialog.Overlay className="fixed inset-0 z-50 bg-ground/80 backdrop-blur-sm" />
        <Dialog.Content className="panel fixed left-1/2 top-1/2 z-50 w-[calc(100vw-2rem)] max-w-[480px] -translate-x-1/2 -translate-y-1/2 p-6">
          <div className="mb-1 flex items-center justify-between">
            <div className="flex items-center gap-2">
              <AlertTriangle size={18} className="text-alarm" />
              <Dialog.Title className="font-hud text-[18px] font-700 uppercase tracking-wide text-fg">
                Crash the matcher
              </Dialog.Title>
              <Didactic topic="crash" />
            </div>
            <Dialog.Close
              aria-label="Close"
              className="rounded-md p-1 text-muted hover:bg-fg/8 hover:text-fg"
            >
              <X size={18} />
            </Dialog.Close>
          </div>

          {state.phase === 'confirm' && (
            <>
              <p className="mb-2 font-body text-[13px] leading-relaxed text-fg/85">
                This injects the exact wreckage a killed matcher leaves — a claim whose lease has
                lapsed and an orphaned offered trip — then watches the real janitor sweep it. No
                matcher restart; no faked recovery.
              </p>
              <p className="mb-5 font-body text-[12px] leading-relaxed text-muted">
                It runs on an isolated cell and cleans up after itself. Live traffic is untouched.
              </p>
              <div className="flex justify-end gap-2">
                <Button variant="ghost" onClick={() => onOpenChange(false)}>
                  Cancel
                </Button>
                <Button variant="alarm" onClick={onConfirm}>
                  Crash it
                </Button>
              </div>
            </>
          )}

          {(running || result !== null) && (
            <>
              <div className="mt-2 flex flex-col gap-2.5">
                <Step done label="Claim abandoned" detail="lease lapsed, driver still claimed" />
                <Step
                  done
                  label="Trip orphaned"
                  detail="offered trip stranded, request stuck matching"
                />
                <Step
                  done={result !== null && result.sweptClaim && result.driverFreed}
                  spinning={running}
                  label="Janitor sweeps"
                  detail={
                    result !== null
                      ? `claim released, driver available again${
                          result.recoveryMs !== null ? ` · ${result.recoveryMs} ms` : ''
                        }`
                      : 'reading the expired-claim ZSET…'
                  }
                />
                <Step
                  done={result !== null && result.orphanCleared}
                  spinning={running}
                  label="Offers re-cascade"
                  detail="orphaned trip reverted, request handed back to the queue"
                />
              </div>

              <div className="mt-5 flex items-center gap-2 rounded-[10px] border border-matched/30 bg-matched/8 px-3 py-2.5">
                <ShieldCheck size={16} className="text-matched" />
                <span className="font-hud text-[12px] font-600 uppercase tracking-wide text-matched">
                  no double-assignment
                </span>
                <span className="ml-auto font-mono text-[11px] tnum text-matched">
                  {result === null ? 'holding' : `${result.invariantBreaches} breaches`}
                </span>
              </div>

              {result !== null && (
                <div className="mt-4 flex items-center justify-between border-t border-line pt-4">
                  {result.recovered ? (
                    <Badge tone="matched">
                      <CircleCheck size={12} /> recovered, no restart
                    </Badge>
                  ) : (
                    <Badge tone="alarm">
                      janitor didn’t recover in window — is the matcher up?
                    </Badge>
                  )}
                  {REPO_URL === '' ? (
                    <span className="font-mono text-[11px] text-amber/80">{TEST_PATH}</span>
                  ) : (
                    <a
                      href={`${REPO_URL}/${TEST_PATH}`}
                      target="_blank"
                      rel="noreferrer"
                      className="font-mono text-[11px] text-amber hover:underline"
                    >
                      {TEST_PATH}
                    </a>
                  )}
                </div>
              )}
            </>
          )}

          {state.phase === 'error' && (
            <div className="mt-2 rounded-[10px] border border-alarm/40 bg-alarm/8 p-4">
              <div className="font-hud text-[13px] font-600 uppercase tracking-wide text-alarm">
                Couldn’t inject the fault
              </div>
              <p className="mt-2 font-body text-[12.5px] text-fg/80">
                {state.message}. Check the stack is up, then try again.
              </p>
              <Button className="mt-3" variant="alarm" size="sm" onClick={onConfirm}>
                Try again
              </Button>
            </div>
          )}
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  );
}

function Step({
  label,
  detail,
  done = false,
  spinning = false,
}: {
  label: string;
  detail: string;
  done?: boolean;
  spinning?: boolean;
}): ReactElement {
  return (
    <div className="flex items-start gap-3">
      <span className="mt-0.5 shrink-0">
        {done ? (
          <CircleCheck size={16} className="text-matched" />
        ) : spinning ? (
          <Loader2 size={16} className="animate-spin text-amber" />
        ) : (
          <span className="block h-4 w-4 rounded-full border border-line" />
        )}
      </span>
      <div className="min-w-0">
        <div className="font-hud text-[12.5px] font-600 uppercase tracking-wide text-fg">
          {label}
        </div>
        <div className="font-body text-[11.5px] text-muted">{detail}</div>
      </div>
    </div>
  );
}
