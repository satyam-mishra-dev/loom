import { useEffect, useState } from 'react';
import type { ReactElement } from 'react';
import type { OfferAttempt, TripHistory } from '../types.js';
import { fetchTrip } from '../api.js';
import { Badge, Skeleton } from './ui/base.js';
import { Sheet } from './ui/overlay.js';
import { Didactic } from './Didactic.js';

const STEPS = [
  'requested',
  'matching',
  'offered',
  'matched',
  'en_route',
  'in_trip',
  'completed',
] as const;

type Fetch =
  { phase: 'loading' } | { phase: 'ok'; trip: TripHistory } | { phase: 'error'; message: string };

/** Right Sheet: the trip state machine + offer cascade, read from real data. */
export function TripInspector({
  tripId,
  onClose,
}: {
  tripId: string | null;
  onClose: () => void;
}): ReactElement {
  const [state, setState] = useState<Fetch>({ phase: 'loading' });

  useEffect(() => {
    if (tripId === null) return;
    let live = true;
    setState({ phase: 'loading' });
    fetchTrip(tripId).then(
      (trip) => live && setState({ phase: 'ok', trip }),
      (err: unknown) =>
        live &&
        setState({ phase: 'error', message: err instanceof Error ? err.message : 'failed' }),
    );
    return () => {
      live = false;
    };
  }, [tripId]);

  return (
    <Sheet open={tripId !== null} onOpenChange={(o) => !o && onClose()} title="Trip inspector">
      {state.phase === 'loading' && (
        <div className="flex flex-col gap-4">
          <Skeleton className="h-6 w-40" />
          <Skeleton className="h-24 w-full" />
          <Skeleton className="h-20 w-full" />
        </div>
      )}
      {state.phase === 'error' && (
        <div className="rounded-[10px] border border-alarm/40 bg-alarm/8 p-4">
          <div className="font-hud text-[13px] font-600 uppercase tracking-wide text-alarm">
            Couldn’t load the trip
          </div>
          <p className="mt-2 font-body text-[12.5px] text-fg/80">
            {state.message}. It may have completed and been archived — click another arc.
          </p>
        </div>
      )}
      {state.phase === 'ok' && <TripBody trip={state.trip} />}
    </Sheet>
  );
}

function TripBody({ trip }: { trip: TripHistory }): ReactElement {
  const firstAt = new Map<string, string>();
  for (const e of trip.events) if (!firstAt.has(e.type)) firstAt.set(e.type, e.at);
  const cancelled = trip.status === 'cancelled' || firstAt.has('cancelled');

  return (
    <div className="flex flex-col gap-5">
      <div className="flex flex-wrap items-center gap-2">
        <StatusBadge status={trip.status} />
        <SurgeBadge m={trip.surgeMultiplier} />
        <Badge tone="neutral">driver {trip.driverId}</Badge>
      </div>

      <div>
        <div className="mb-2 flex items-center gap-2">
          <span className="microlabel text-[10px]">State machine</span>
          <Didactic topic="inspector" />
        </div>
        <div className="overflow-x-auto pb-2">
          <div className="flex min-w-max items-start gap-0">
            {STEPS.map((step, i) => {
              const at = firstAt.get(step);
              const reached = at !== undefined;
              return (
                <div key={step} className="flex items-start">
                  <div className="flex w-[64px] flex-col items-center text-center">
                    <span
                      className="mb-1.5 h-2.5 w-2.5 rounded-full"
                      style={{ background: reached ? '#4FC3F7' : 'rgba(154,168,191,0.28)' }}
                    />
                    <span
                      className={`font-hud text-[9px] font-600 uppercase leading-tight tracking-wide ${
                        reached ? 'text-fg' : 'text-muted/50'
                      }`}
                    >
                      {step.replace('_', ' ')}
                    </span>
                    <span className="mt-0.5 font-mono text-[8.5px] tnum text-muted/70">
                      {reached ? clock(at) : '—'}
                    </span>
                  </div>
                  {i < STEPS.length - 1 && (
                    <span
                      className="mt-[4px] h-[2px] w-4 shrink-0"
                      style={{
                        background:
                          reached && firstAt.has(STEPS[i + 1]!) ? '#4FC3F7' : 'rgba(34,48,73,0.9)',
                      }}
                    />
                  )}
                </div>
              );
            })}
            {cancelled && (
              <div className="ml-1 flex w-[64px] flex-col items-center text-center">
                <span
                  className="mb-1.5 h-2.5 w-2.5 rounded-full"
                  style={{ background: '#FF5D5D' }}
                />
                <span className="font-hud text-[9px] font-600 uppercase leading-tight tracking-wide text-alarm">
                  cancelled
                </span>
              </div>
            )}
          </div>
        </div>
      </div>

      <div>
        <div className="mb-2 microlabel text-[10px]">Offer cascade</div>
        {trip.offers.length === 0 ? (
          <p className="font-body text-[12px] text-muted">No offers recorded on this trip yet.</p>
        ) : (
          <div className="flex flex-col gap-1.5">
            {trip.offers.map((o, i) => (
              <OfferChip key={o.offerId} attempt={o} index={i + 1} />
            ))}
          </div>
        )}
      </div>

      <div>
        <div className="mb-2 microlabel text-[10px]">Event log</div>
        <div className="flex flex-col gap-1 font-mono text-[11px]">
          {trip.events.map((e, i) => (
            <div
              key={i}
              className="flex items-baseline justify-between gap-3 border-b border-line/60 py-1"
            >
              <span className="text-fg/85">{e.type}</span>
              <span className="text-muted/70">{e.event ?? ''}</span>
              <span className="tnum text-muted/60">{clock(e.at)}</span>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

function OfferChip({ attempt, index }: { attempt: OfferAttempt; index: number }): ReactElement {
  const tone =
    attempt.result === 'accepted'
      ? 'matched'
      : attempt.result === 'timed_out'
        ? 'amber'
        : attempt.result === 'declined'
          ? 'neutral'
          : 'intrip';
  return (
    <div className="flex items-center gap-2 rounded-md border border-line bg-ground/40 px-2.5 py-1.5">
      <span className="microlabel text-[9px] text-muted/70">#{index}</span>
      <span className="font-mono text-[11.5px] text-fg/85">{attempt.driverId}</span>
      <Badge className="ml-auto" tone={tone}>
        {attempt.result.replace('_', ' ')}
      </Badge>
    </div>
  );
}

function StatusBadge({ status }: { status: string }): ReactElement {
  const tone = status === 'cancelled' ? 'alarm' : status === 'completed' ? 'matched' : 'intrip';
  return <Badge tone={tone}>{status.replace('_', ' ')}</Badge>;
}

function SurgeBadge({ m }: { m: number }): ReactElement {
  if (m <= 1) return <Badge tone="neutral">surge 1.0×</Badge>;
  const color = m >= 2.5 ? '#FF4D4D' : m >= 1.6 ? '#FF7A45' : '#FFB020';
  return (
    <span
      className="inline-flex items-center gap-1.5 rounded-md border px-2 py-0.5 font-mono text-[11px] font-500 tnum"
      style={{ color, borderColor: `${color}66`, background: `${color}1f` }}
    >
      surge {m.toFixed(2)}×
    </span>
  );
}

function clock(iso: string): string {
  const d = new Date(iso);
  return (
    d.toLocaleTimeString('en-GB', { hour12: false }) +
    '.' +
    String(d.getMilliseconds()).padStart(3, '0')
  );
}
