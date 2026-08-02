import { useEffect, useMemo, useState } from 'react';
import type { CSSProperties, ReactElement } from 'react';
import DeckGL from '@deck.gl/react';
import { ArcLayer, ScatterplotLayer } from '@deck.gl/layers';
import { H3HexagonLayer } from '@deck.gl/geo-layers';
import type { Layer } from '@deck.gl/core';
import type { DriverDot, Snapshot, SurgeCell, TripArc } from './types.js';

const READMODEL_URL = (import.meta.env['VITE_READMODEL_URL'] as string | undefined) ?? 'http://localhost:4600';
const CENTER = {
  lat: Number(import.meta.env['VITE_CENTER_LAT'] ?? 37.7749),
  lng: Number(import.meta.env['VITE_CENTER_LNG'] ?? -122.4194),
};

const INITIAL_VIEW = {
  longitude: CENTER.lng,
  latitude: CENTER.lat,
  zoom: 12.2,
  pitch: 45,
  bearing: 0,
};

const STATUS_COLOR: Record<DriverDot['s'], [number, number, number]> = {
  available: [46, 204, 113],
  claimed: [241, 196, 15],
  on_trip: [52, 152, 219],
};

/** m ∈ [1,3] → amber→red ramp; alpha rises with intensity. */
function surgeColor(m: number): [number, number, number, number] {
  const t = Math.min(1, Math.max(0, (m - 1) / 2));
  return [255, Math.round(190 * (1 - t)), 30, Math.round(90 + 110 * t)];
}

export function App(): ReactElement {
  const [snap, setSnap] = useState<Snapshot | null>(null);
  const [connected, setConnected] = useState(false);
  const [hotspot, setHotspot] = useState(false);
  const [busy, setBusy] = useState(false);

  useEffect(() => {
    const es = new EventSource(`${READMODEL_URL}/events`);
    es.onopen = () => setConnected(true);
    es.onmessage = (ev) => {
      try {
        setSnap(JSON.parse(ev.data as string) as Snapshot);
      } catch {
        /* ignore a malformed frame */
      }
    };
    es.onerror = () => setConnected(false); // EventSource auto-reconnects
    return () => es.close();
  }, []);

  async function spawn(n: number): Promise<void> {
    setBusy(true);
    try {
      await fetch(`${READMODEL_URL}/spawn`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ n, hotspot }),
      });
    } catch {
      /* the counters will show whether it landed */
    } finally {
      setBusy(false);
    }
  }

  const layers = useMemo<Layer[]>(() => {
    if (snap === null) return [];
    return [
      new H3HexagonLayer<SurgeCell>({
        id: 'surge',
        data: snap.surge,
        getHexagon: (d) => d.cell,
        extruded: true,
        elevationScale: 1,
        getElevation: (d) => (d.m - 1) * 400,
        getFillColor: (d) => surgeColor(d.m),
        pickable: true,
        opacity: 0.55,
      }),
      new ArcLayer<TripArc>({
        id: 'trips',
        data: snap.trips,
        getSourcePosition: (d) => [d.plng, d.plat],
        getTargetPosition: (d) => [d.dlng, d.dlat],
        getSourceColor: [52, 152, 219],
        getTargetColor: [155, 89, 182],
        getWidth: 1.5,
        greatCircle: false,
      }),
      new ScatterplotLayer<DriverDot>({
        id: 'drivers',
        data: snap.drivers,
        getPosition: (d) => [d.lng, d.lat],
        getFillColor: (d) => STATUS_COLOR[d.s],
        getRadius: 22,
        radiusMinPixels: 1.5,
        radiusMaxPixels: 4,
        pickable: false,
      }),
    ];
  }, [snap]);

  const c = snap?.counters;

  return (
    <div style={{ position: 'absolute', inset: 0 }}>
      <DeckGL initialViewState={INITIAL_VIEW} controller layers={layers} style={{ background: '#0a0e14' }} />

      {/* HUD */}
      <div style={panel('top', 'left')}>
        <div style={{ fontSize: 18, fontWeight: 700, letterSpacing: 0.5, marginBottom: 8 }}>
          FLEETLINE <span style={{ color: '#8aa0b6', fontWeight: 400 }}>live dispatch</span>
        </div>
        <Stat label="drivers" value={c ? c.driversTotal.toLocaleString() : '—'} sub={snap ? `${snap.driversShown} shown` : ''} />
        <Stat label="matches/sec" value={c ? c.matchesPerSec.toFixed(1) : '—'} accent="#2ecc71" />
        <Stat label="match p50 / p99" value={c ? `${c.p50Ms} / ${c.p99Ms} ms` : '—'} />
        <Stat label="active trips" value={c ? String(c.activeTrips) : '—'} accent="#3498db" />
        <Stat label="unmatched" value={c ? `${(c.unmatchedRate * 100).toFixed(1)}%` : '—'} />
        <Stat label="surge max" value={c ? `${c.surgeMax.toFixed(2)}×` : '—'} accent={c && c.surgeMax > 1 ? '#ff5a3c' : '#8aa0b6'} />
      </div>

      {/* Legend */}
      <div style={{ ...panel('top', 'right'), fontSize: 12 }}>
        <Swatch color="#2ecc71" label="available" />
        <Swatch color="#f1c40f" label="claimed" />
        <Swatch color="#3498db" label="on trip" />
        <div style={{ height: 8 }} />
        <Swatch color="#ff5a3c" label="surge cell" />
        <Swatch color="#9b59b6" label="trip arc → drop" />
      </div>

      {/* Controls */}
      <div style={{ ...panel('bottom', 'left'), display: 'flex', flexDirection: 'column', gap: 8, minWidth: 200 }}>
        <div style={{ display: 'flex', gap: 8 }}>
          <button style={btn} disabled={busy} onClick={() => void spawn(50)}>
            spawn 50
          </button>
          <button style={btn} disabled={busy} onClick={() => void spawn(300)}>
            spawn 300
          </button>
        </div>
        <label style={{ display: 'flex', alignItems: 'center', gap: 8, fontSize: 13, cursor: 'pointer' }}>
          <input type="checkbox" checked={hotspot} onChange={(e) => setHotspot(e.target.checked)} />
          hotspot cluster <span style={{ color: '#8aa0b6' }}>(forces a cell to surge)</span>
        </label>
        <div style={{ fontSize: 11, color: '#6b8098', lineHeight: 1.5 }}>
          chaos: <code>docker compose kill matcher</code> and watch the janitor release stranded claims —
          active trips keep completing, no driver double-booked.
        </div>
      </div>

      {/* Connection pill */}
      <div style={{ ...panel('bottom', 'right'), fontSize: 12, display: 'flex', alignItems: 'center', gap: 8 }}>
        <span style={{ width: 9, height: 9, borderRadius: 9, background: connected ? '#2ecc71' : '#e74c3c' }} />
        {connected ? 'streaming' : 'reconnecting…'}
      </div>
    </div>
  );
}

function Stat(props: { label: string; value: string; sub?: string; accent?: string }): ReactElement {
  return (
    <div style={{ marginBottom: 6 }}>
      <div style={{ fontSize: 11, color: '#8aa0b6', textTransform: 'uppercase', letterSpacing: 0.6 }}>
        {props.label}
      </div>
      <div style={{ fontSize: 20, fontWeight: 700, color: props.accent ?? '#e6edf3', fontVariantNumeric: 'tabular-nums' }}>
        {props.value}
        {props.sub !== undefined && props.sub !== '' && (
          <span style={{ fontSize: 11, fontWeight: 400, color: '#6b8098', marginLeft: 6 }}>{props.sub}</span>
        )}
      </div>
    </div>
  );
}

function Swatch(props: { color: string; label: string }): ReactElement {
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 4 }}>
      <span style={{ width: 10, height: 10, borderRadius: 10, background: props.color }} />
      {props.label}
    </div>
  );
}

function panel(v: 'top' | 'bottom', h: 'left' | 'right'): CSSProperties {
  return {
    position: 'absolute',
    ...(v === 'top' ? { top: 16 } : { bottom: 16 }),
    ...(h === 'left' ? { left: 16 } : { right: 16 }),
    padding: '14px 16px',
    background: 'rgba(14, 20, 28, 0.82)',
    border: '1px solid rgba(120, 150, 180, 0.18)',
    borderRadius: 12,
    color: '#e6edf3',
    backdropFilter: 'blur(8px)',
    boxShadow: '0 8px 30px rgba(0,0,0,0.45)',
  };
}

const btn: CSSProperties = {
  flex: 1,
  padding: '8px 10px',
  background: 'rgba(52, 152, 219, 0.18)',
  border: '1px solid rgba(52, 152, 219, 0.5)',
  borderRadius: 8,
  color: '#e6edf3',
  fontSize: 13,
  fontWeight: 600,
  cursor: 'pointer',
};
