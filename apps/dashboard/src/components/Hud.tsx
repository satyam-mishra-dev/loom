import type { ReactElement, ReactNode } from 'react';
import type { Counters } from '../types.js';
import { Card, Skeleton } from './ui/base.js';
import { Sparkline } from './Sparkline.js';
import { Didactic } from './Didactic.js';

export interface Series {
  matchesPerSec: number[];
  p50: number[];
  p99: number[];
}

/** Top-left HUD: live counters + 60s sparklines (§3.4). */
export function Hud({
  counters,
  series,
  connected,
}: {
  counters: Counters | null;
  series: Series;
  connected: boolean;
}): ReactElement {
  return (
    <Card className="pointer-events-auto w-[248px] p-4">
      <div className="mb-3 flex items-center justify-between">
        <div className="flex items-baseline gap-2">
          <span className="font-hud text-[18px] font-700 tracking-wide text-fg">FLEETLINE</span>
          <span className="font-hud text-[11px] uppercase tracking-widest text-muted">control room</span>
        </div>
        <Didactic topic="hud" />
      </div>

      {counters === null ? (
        <div className="flex flex-col gap-3">
          {[0, 1, 2, 3].map((i) => (
            <Skeleton key={i} className="h-9 w-full" />
          ))}
        </div>
      ) : (
        <div className="flex flex-col gap-3">
          <Stat label="Fleet online" value={counters.driversTotal.toLocaleString()} />
          <Stat label="Active trips" value={counters.activeTrips.toLocaleString()} valueClass="text-intrip" />
          <Stat
            label="Matches / min"
            value={Math.round(counters.matchesPerSec * 60).toLocaleString()}
            valueClass="text-matched"
            spark={<Sparkline data={series.matchesPerSec} color="#7EE0A3" />}
          />
          <Stat
            label="Req → match p50"
            value={`${Math.round(counters.p50Ms)} ms`}
            spark={<Sparkline data={series.p50} color="#4FC3F7" />}
          />
          <Stat
            label="Req → match p99"
            value={`${Math.round(counters.p99Ms)} ms`}
            spark={<Sparkline data={series.p99} color="#FFB020" />}
          />
        </div>
      )}

      <div className="mt-3 flex items-center gap-2 border-t border-line pt-2">
        <span
          className="inline-block h-2 w-2 rounded-full"
          style={{ background: connected ? '#7EE0A3' : '#FF5D5D' }}
        />
        <span className="microlabel text-[10px]">{connected ? 'streaming' : 'reconnecting'}</span>
      </div>
    </Card>
  );
}

function Stat({
  label,
  value,
  valueClass = 'text-fg',
  spark,
}: {
  label: string;
  value: string;
  valueClass?: string;
  spark?: ReactNode;
}): ReactElement {
  return (
    <div className="flex items-end justify-between gap-2">
      <div className="min-w-0">
        <div className="microlabel text-[9.5px]">{label}</div>
        <div className={`font-mono text-[20px] font-600 leading-tight tnum ${valueClass}`}>{value}</div>
      </div>
      {spark !== undefined && <div className="shrink-0 pb-1">{spark}</div>}
    </div>
  );
}
