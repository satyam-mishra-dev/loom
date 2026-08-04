import { ArcLayer, ScatterplotLayer } from '@deck.gl/layers';
import { H3HexagonLayer } from '@deck.gl/geo-layers';
import DeckGL from '@deck.gl/react';
import type { Layer, PickingInfo } from '@deck.gl/core';
import { latLngToCell } from 'h3-js';
import { useMemo } from 'react';
import type { ReactElement } from 'react';
import type { LayerState } from './LayerToggles.js';
import { CENTER } from '../api.js';
import type { DriverDot, Snapshot, SurgeCell, TripArc } from '../types.js';

const INITIAL_VIEW = {
  longitude: CENTER.lng,
  latitude: CENTER.lat,
  zoom: 12.2,
  pitch: 45,
  bearing: 0,
};

const AVAILABLE: [number, number, number] = [154, 168, 191]; // --available
const OFFERED: [number, number, number] = [255, 176, 32]; // --amber
const INTRIP: [number, number, number] = [79, 195, 247]; // --intrip
const MATCHED: [number, number, number] = [126, 224, 163]; // --matched

function driverColor(d: DriverDot, matched: Set<string>): [number, number, number, number] {
  if (matched.has(d.id)) return [...MATCHED, 255];
  switch (d.s) {
    case 'available':
      return [...AVAILABLE, 200];
    case 'claimed':
      return [...OFFERED, 255];
    case 'on_trip':
      return [...INTRIP, 235];
  }
}

// Surge ramp #FFB020 → #FF7A45 → #FF4D4D across m ∈ [1, 3].
const S1: [number, number, number] = [255, 176, 32];
const S2: [number, number, number] = [255, 122, 69];
const S3: [number, number, number] = [255, 77, 77];
function surgeColor(m: number): [number, number, number, number] {
  const t = Math.min(1, Math.max(0, (m - 1) / 2));
  const [a, b, k] = t < 0.5 ? [S1, S2, t / 0.5] : [S2, S3, (t - 0.5) / 0.5];
  const lerp = (i: number): number => Math.round(a[i]! + (b[i]! - a[i]!) * k);
  return [lerp(0), lerp(1), lerp(2), Math.round(90 + 130 * t)];
}

/** Full-bleed deck.gl over a plain colored ground — no external tile server. */
export function FleetMap({
  snapshot,
  layers,
  matched,
  onTripClick,
}: {
  snapshot: Snapshot | null;
  layers: LayerState;
  matched: Set<string>;
  onTripClick: (id: string) => void;
}): ReactElement {
  const deckLayers = useMemo<Layer[]>(() => {
    if (snapshot === null) return [];
    const out: Layer[] = [];

    if (layers.grid) {
      // The real spatial index: the H3 res-8 cells drivers actually occupy.
      const cells = new Set<string>();
      for (const d of snapshot.drivers) cells.add(latLngToCell(d.lat, d.lng, 8));
      out.push(
        new H3HexagonLayer<string>({
          id: 'grid',
          data: [...cells],
          getHexagon: (c) => c,
          extruded: false,
          filled: false,
          stroked: true,
          getLineColor: [34, 48, 73, 190], // --line
          lineWidthMinPixels: 1,
          pickable: false,
        }),
      );
    }

    if (layers.surge) {
      out.push(
        new H3HexagonLayer<SurgeCell>({
          id: 'surge',
          data: snapshot.surge,
          getHexagon: (d) => d.cell,
          extruded: true,
          elevationScale: 1,
          getElevation: (d) => (d.m - 1) * 400,
          getFillColor: (d) => surgeColor(d.m),
          pickable: false,
          opacity: 0.55,
        }),
      );
    }

    if (layers.arcs) {
      out.push(
        new ArcLayer<TripArc>({
          id: 'trips',
          data: snapshot.trips,
          getSourcePosition: (d) => [d.plng, d.plat],
          getTargetPosition: (d) => [d.dlng, d.dlat],
          getSourceColor: INTRIP,
          getTargetColor: MATCHED,
          getWidth: 1.6,
          widthMinPixels: 1.2,
          greatCircle: false,
          pickable: true,
          autoHighlight: true,
          highlightColor: [255, 176, 32, 220],
          onClick: (info: PickingInfo<TripArc>) => {
            if (info.object) onTripClick(info.object.id);
          },
        }),
      );
    }

    out.push(
      new ScatterplotLayer<DriverDot>({
        id: 'drivers',
        data: snapshot.drivers,
        getPosition: (d) => [d.lng, d.lat],
        getFillColor: (d) => driverColor(d, matched),
        getRadius: 22,
        radiusMinPixels: 1.6,
        radiusMaxPixels: 4.5,
        updateTriggers: { getFillColor: matched },
        pickable: false,
      }),
    );

    return out;
  }, [snapshot, layers, matched, onTripClick]);

  return (
    <DeckGL
      initialViewState={INITIAL_VIEW}
      controller
      layers={deckLayers}
      style={{ background: '#0C111C' }}
      getCursor={({ isHovering }) => (isHovering ? 'pointer' : 'grab')}
    />
  );
}
