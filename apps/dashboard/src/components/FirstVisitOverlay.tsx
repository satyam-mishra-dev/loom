import { X } from 'lucide-react';
import { useState } from 'react';
import type { ReactElement } from 'react';
import { Button, Card } from './ui/base.js';

const SEEN_KEY = 'loom.seen-intro.v1';

/**
 * First-visit card (§3.4): what this is / what to press / link to the code,
 * then it gets out of the way. Dismissal persists in localStorage.
 *
 * TODO(voice): the three lines below are neutral factual placeholders written by
 * the agent — replace with the author's own words before the demo ships.
 */
export function FirstVisitOverlay(): ReactElement | null {
  const [dismissed, setDismissed] = useState<boolean>(() => {
    try {
      return localStorage.getItem(SEEN_KEY) === '1';
    } catch {
      return false;
    }
  });

  if (dismissed) return null;

  const close = (): void => {
    try {
      localStorage.setItem(SEEN_KEY, '1');
    } catch {
      /* private mode — just close for this session */
    }
    setDismissed(true);
  };

  return (
    <div className="pointer-events-none absolute inset-0 z-40 grid place-items-center">
      <Card className="pointer-events-auto relative w-[420px] max-w-[calc(100vw-2rem)] p-6">
        <button
          onClick={close}
          aria-label="Dismiss"
          className="absolute right-3 top-3 rounded-md p-1 text-muted transition-colors hover:bg-fg/8 hover:text-fg"
        >
          <X size={18} />
        </button>
        <div className="font-hud text-[20px] font-700 uppercase tracking-wide text-fg">Loom</div>
        <ul className="mt-4 flex flex-col gap-3 font-body text-[13.5px] leading-relaxed text-fg/85">
          {/* TODO(voice): line 1 — what this is. */}
          <li>
            <span className="microlabel mr-2 text-[9.5px] text-amber">what</span>A live
            ride-dispatch control room: a real-time map of drivers, trips and surge, streamed from
            the running engine.
          </li>
          {/* TODO(voice): line 2 — what to press. */}
          <li>
            <span className="microlabel mr-2 text-[9.5px] text-amber">press</span>
            <span className="text-amber">Run the proof</span> to fire 200 concurrent requests at 20
            drivers, or <span className="text-alarm">Crash the matcher</span> to watch it recover.
          </li>
          {/* TODO(voice): line 3 — link to the code. */}
          <li>
            <span className="microlabel mr-2 text-[9.5px] text-amber">code</span>Every panel’s{' '}
            <span className="font-mono text-amber">{'{}'}</span> opens the mechanism and links its
            test.
          </li>
        </ul>
        <div className="mt-5 flex justify-end">
          <Button variant="primary" onClick={close}>
            Enter the control room
          </Button>
        </div>
      </Card>
    </div>
  );
}
