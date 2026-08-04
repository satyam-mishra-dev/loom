import { Layers as LayersIcon } from 'lucide-react';
import { useCallback, useEffect, useRef, useState } from 'react';
import type { ReactElement } from 'react';
import { toast } from 'sonner';
import { COMMIT_SHA, crashMatcher, readUrl, runProof, spawnRequests } from './api.js';
import type { Snapshot } from './types.js';
import { BottomDock } from './components/BottomDock.js';
import { CrashDialog, type CrashState } from './components/CrashDialog.js';
import { FirstVisitOverlay } from './components/FirstVisitOverlay.js';
import { FleetMap } from './components/FleetMap.js';
import { Hud, type Series } from './components/Hud.js';
import { LayerToggles, type LayerState } from './components/LayerToggles.js';
import { ProofOverlay, type ProofState } from './components/ProofOverlay.js';
import { TripInspector } from './components/TripInspector.js';
import { Card } from './components/ui/base.js';
import { Popover } from './components/ui/popover.js';

const SPARK_LEN = 60; // 60 samples ≈ 60s at the 1s tick.
const PULSE_MS = 1_600;

export function App(): ReactElement {
  const [snap, setSnap] = useState<Snapshot | null>(null);
  const [connected, setConnected] = useState(false);
  const [series, setSeries] = useState<Series>({ matchesPerSec: [], p50: [], p99: [] });
  const [matched, setMatched] = useState<Set<string>>(new Set());
  const [layers, setLayers] = useState<LayerState>({ surge: true, arcs: true, grid: false });
  const [hotspot, setHotspot] = useState(false);
  const [spawning, setSpawning] = useState(false);
  const [proof, setProof] = useState<ProofState>({ phase: 'idle' });
  const [crash, setCrash] = useState<CrashState>({ phase: 'idle' });
  const [tripId, setTripId] = useState<string | null>(null);

  // Previous per-driver status + pulse expiries, for the "fresh match" flash.
  const prevStatus = useRef<Map<string, string>>(new Map());
  const pulseUntil = useRef<Map<string, number>>(new Map());

  useEffect(() => {
    const es = new EventSource(readUrl('/events'));
    es.onopen = () => setConnected(true);
    es.onmessage = (ev) => {
      let s: Snapshot;
      try {
        s = JSON.parse(ev.data as string) as Snapshot;
      } catch {
        return; // malformed frame
      }
      setConnected(true);
      setSnap(s);

      // 60s sparkline buffers.
      setSeries((prev) => ({
        matchesPerSec: cap([...prev.matchesPerSec, s.counters.matchesPerSec]),
        p50: cap([...prev.p50, s.counters.p50Ms]),
        p99: cap([...prev.p99, s.counters.p99Ms]),
      }));

      // Fresh-match pulse: a driver that just entered on_trip flashes green.
      const now = Date.now();
      const prev = prevStatus.current;
      const next = new Map<string, string>();
      for (const d of s.drivers) {
        const was = prev.get(d.id);
        if (d.s === 'on_trip' && was !== undefined && was !== 'on_trip') {
          pulseUntil.current.set(d.id, now + PULSE_MS);
        }
        next.set(d.id, d.s);
      }
      prevStatus.current = next;
      for (const [id, until] of pulseUntil.current) if (until <= now) pulseUntil.current.delete(id);
      setMatched(new Set(pulseUntil.current.keys()));
    };
    es.onerror = () => setConnected(false); // EventSource auto-reconnects
    return () => es.close();
  }, []);

  const spawn = useCallback(
    (n: number) => {
      setSpawning(true);
      spawnRequests(n, hotspot)
        .then(
          (r) =>
            toast.success(`Spawned ${r.spawned} requests`, {
              description: hotspot
                ? 'clustered into one hotspot cell'
                : 'scattered across the city',
            }),
          () =>
            toast.error('Spawn failed', {
              description: 'the read model didn’t accept the request',
            }),
        )
        .finally(() => setSpawning(false));
    },
    [hotspot],
  );

  const doProof = useCallback(() => {
    setProof({ phase: 'running' });
    runProof().then(
      (result) => {
        setProof({ phase: 'done', result });
        toast.success(
          `${result.matched} matched · ${result.unmatched} unmatched · ${result.doubleAssignments} double-assignments`,
        );
      },
      (err: unknown) =>
        setProof({
          phase: 'error',
          message: err instanceof Error ? err.message : 'request failed',
        }),
    );
  }, []);

  const doCrash = useCallback(() => {
    setCrash({ phase: 'running' });
    crashMatcher().then(
      (result) => {
        setCrash({ phase: 'done', result });
        if (result.recovered)
          toast.success(`Janitor recovered in ${result.recoveryMs} ms`, {
            description: '0 double-assignments throughout',
          });
        else
          toast.error('Janitor did not recover in the window', {
            description: 'is the matcher running?',
          });
      },
      (err: unknown) =>
        setCrash({
          phase: 'error',
          message: err instanceof Error ? err.message : 'request failed',
        }),
    );
  }, []);

  return (
    <div className="absolute inset-0 overflow-hidden">
      <FleetMap snapshot={snap} layers={layers} matched={matched} onTripClick={setTripId} />

      {/* Top bar: HUD (left) + layers (right, collapses to a popover on phones). */}
      <div className="pointer-events-none absolute inset-x-0 top-0 flex items-start justify-between gap-2 p-3">
        <Hud counters={snap?.counters ?? null} series={series} connected={connected} />

        <div className="hidden min-[600px]:block">
          <LayerToggles layers={layers} onChange={setLayers} />
        </div>
        <div className="pointer-events-auto min-[600px]:hidden">
          <Popover
            label="Layers"
            trigger={
              <button className="panel grid h-10 w-10 place-items-center text-muted transition-colors hover:text-amber">
                <LayersIcon size={18} />
              </button>
            }
          >
            <LayerToggles layers={layers} onChange={setLayers} bare />
          </Popover>
        </div>
      </div>

      {/* Bottom dock. */}
      <div className="pointer-events-none absolute inset-x-0 bottom-0 flex justify-center p-3">
        <BottomDock
          driversTotal={snap?.counters.driversTotal ?? null}
          hotspot={hotspot}
          onHotspot={setHotspot}
          onSpawn={spawn}
          spawning={spawning}
          onRunProof={doProof}
          proofRunning={proof.phase === 'running'}
          onCrash={() => setCrash({ phase: 'confirm' })}
          crashRunning={crash.phase === 'running'}
        />
      </div>

      {COMMIT_SHA !== '' && (
        <div className="pointer-events-none absolute bottom-1 left-2 font-mono text-[9px] text-muted/40">
          {COMMIT_SHA.slice(0, 7)}
        </div>
      )}

      <FirstVisitOverlay />
      <ProofOverlay state={proof} onOpenChange={(o) => !o && setProof({ phase: 'idle' })} />
      <CrashDialog
        state={crash}
        onConfirm={doCrash}
        onOpenChange={(o) => !o && setCrash({ phase: 'idle' })}
      />
      <TripInspector tripId={tripId} onClose={() => setTripId(null)} />

      {/* Empty state: no stream yet. */}
      {snap === null && (
        <div className="pointer-events-none absolute inset-0 grid place-items-center">
          <Card className="p-5 text-center">
            <div className="font-hud text-[13px] font-600 uppercase tracking-wide text-muted">
              {connected ? 'waiting for the first snapshot…' : 'connecting to the read model…'}
            </div>
          </Card>
        </div>
      )}
    </div>
  );
}

function cap(arr: number[]): number[] {
  return arr.length > SPARK_LEN ? arr.slice(arr.length - SPARK_LEN) : arr;
}
