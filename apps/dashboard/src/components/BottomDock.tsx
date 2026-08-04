import { AlertTriangle, Users, Zap } from 'lucide-react';
import type { ReactElement } from 'react';
import { Button, Card } from './ui/base.js';
import { Switch } from './ui/switch.js';

/** Bottom dock: spawn load, hotspot, driver count, Proof Mode, Crash (§3.4). */
export function BottomDock({
  driversTotal,
  hotspot,
  onHotspot,
  onSpawn,
  spawning,
  onRunProof,
  proofRunning,
  onCrash,
  crashRunning,
}: {
  driversTotal: number | null;
  hotspot: boolean;
  onHotspot: (v: boolean) => void;
  onSpawn: (n: number) => void;
  spawning: boolean;
  onRunProof: () => void;
  proofRunning: boolean;
  onCrash: () => void;
  crashRunning: boolean;
}): ReactElement {
  return (
    <Card className="pointer-events-auto flex flex-wrap items-center gap-x-5 gap-y-3 px-4 py-3">
      <div className="flex items-center gap-2">
        <span className="microlabel text-[9.5px]">Spawn requests</span>
        {[10, 50, 200].map((n) => (
          <Button key={n} size="sm" variant="ghost" disabled={spawning} onClick={() => onSpawn(n)}>
            {n}
          </Button>
        ))}
      </div>

      <label className="flex cursor-pointer items-center gap-2 text-[12px] text-fg/90">
        <Switch checked={hotspot} onCheckedChange={onHotspot} color="#FF7A45" />
        <span className="microlabel text-[9.5px]">Hotspot</span>
      </label>

      <div className="flex items-center gap-2 border-l border-line pl-4 text-muted">
        <Users size={14} />
        <span className="font-mono text-[13px] font-500 tnum text-fg">
          {driversTotal === null ? '—' : driversTotal.toLocaleString()}
        </span>
        <span className="microlabel text-[9.5px]">drivers</span>
      </div>

      <div className="ml-auto flex items-center gap-2">
        <Button variant="solid" onClick={onRunProof} disabled={proofRunning}>
          <Zap size={14} />
          {proofRunning ? 'Running…' : 'Run the proof'}
        </Button>
        <Button variant="alarm" onClick={onCrash} disabled={crashRunning}>
          <AlertTriangle size={14} />
          Crash the matcher
        </Button>
      </div>
    </Card>
  );
}
