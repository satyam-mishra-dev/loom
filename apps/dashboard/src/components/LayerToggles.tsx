import type { ReactElement } from 'react';
import { Card } from './ui/base.js';
import { Switch } from './ui/switch.js';
import { Didactic } from './Didactic.js';

export interface LayerState {
  surge: boolean;
  arcs: boolean;
  grid: boolean;
}

const LEGEND: { color: string; label: string }[] = [
  { color: '#9AA8BF', label: 'available' },
  { color: '#FFB020', label: 'offered' },
  { color: '#4FC3F7', label: 'en route / in trip' },
  { color: '#7EE0A3', label: 'fresh match' },
];

/** Top-right: layer toggles + the driver-state legend. `bare` drops the
 *  Card wrapper so the same content can live inside a mobile popover. */
export function LayerToggles({
  layers,
  onChange,
  bare = false,
}: {
  layers: LayerState;
  onChange: (next: LayerState) => void;
  bare?: boolean;
}): ReactElement {
  const Row = ({
    id,
    label,
    on,
    color,
    didactic,
  }: {
    id: keyof LayerState;
    label: string;
    on: boolean;
    color: string;
    didactic?: 'surge' | 'cells';
  }): ReactElement => (
    <div className="flex items-center justify-between gap-4 py-1">
      <label
        htmlFor={`layer-${id}`}
        className="flex cursor-pointer items-center gap-2 text-[13px] text-fg/90"
      >
        <span className="inline-block h-2.5 w-2.5 rounded-sm" style={{ background: color }} />
        {label}
      </label>
      <div className="flex items-center gap-1.5">
        {didactic !== undefined && <Didactic topic={didactic} />}
        <Switch
          id={`layer-${id}`}
          checked={on}
          color={color}
          onCheckedChange={(v) => onChange({ ...layers, [id]: v })}
        />
      </div>
    </div>
  );

  const inner = (
    <>
      <div className="mb-1.5 microlabel text-[10px]">Layers</div>
      <Row id="surge" label="Surge heatmap" on={layers.surge} color="#FF7A45" didactic="surge" />
      <Row id="arcs" label="Trip arcs" on={layers.arcs} color="#4FC3F7" />
      <Row id="grid" label="H3 cell grid" on={layers.grid} color="#223049" didactic="cells" />

      <div className="mt-3 border-t border-line pt-2">
        <div className="mb-1.5 microlabel text-[10px]">Driver state</div>
        <div className="grid grid-cols-2 gap-x-3 gap-y-1">
          {LEGEND.map((l) => (
            <div key={l.label} className="flex items-center gap-2 text-[11px] text-muted">
              <span className="inline-block h-2 w-2 rounded-full" style={{ background: l.color }} />
              {l.label}
            </div>
          ))}
        </div>
      </div>
    </>
  );

  if (bare) return <div className="w-[220px]">{inner}</div>;
  return <Card className="pointer-events-auto w-[228px] p-4">{inner}</Card>;
}
