import { parseArgs } from 'node:util';
import { Simulation } from './sim.js';
import { createSink } from './sinks.js';

const { values } = parseArgs({
  options: {
    drivers: { type: 'string', default: '100' },
    rps: { type: 'string', default: '5' },
    hotspots: { type: 'string', default: '3' },
    gateway: { type: 'string', default: 'ws://127.0.0.1:8080/ws' },
    sink: { type: 'string', default: 'stdout' },
    seed: { type: 'string', default: '42' },
    speedup: { type: 'string', default: '1' },
    ticks: { type: 'string', default: '0' },
  },
});

function num(name: string, raw: string, min: number): number {
  const n = Number(raw);
  if (!Number.isFinite(n) || n < min) {
    process.stderr.write(`--${name} must be a number >= ${min}, got "${raw}"\n`);
    process.exit(1);
  }
  return n;
}

const drivers = num('drivers', values.drivers, 1);
const rps = num('rps', values.rps, 0);
const hotspots = num('hotspots', values.hotspots, 0);
const seed = num('seed', values.seed, 0);
const speedup = num('speedup', values.speedup, 0.001);
const maxTicks = num('ticks', values.ticks, 0); // 0 = run until interrupted

const sim = new Simulation({ drivers, ratePerSec: rps, hotspots, seed });
const sink = createSink(values.sink, values.gateway);

const tickMs = 1000 / speedup; // one simulated second per tick
const startedAt = performance.now();
let ticks = 0;
let maxLagMs = 0;

const stats = (): Record<string, number> => {
  const elapsedMs = performance.now() - startedAt;
  return {
    ticks,
    elapsedMs: Math.round(elapsedMs),
    achievedTicksPerSec: Number((ticks / (elapsedMs / 1000)).toFixed(2)),
    targetTicksPerSec: Number(speedup.toFixed(2)),
    maxLagMs: Number(maxLagMs.toFixed(1)),
    sent: sink.sent,
    dropped: sink.dropped,
  };
};

async function finish(): Promise<never> {
  await sink.close();
  process.stderr.write(`simulator done ${JSON.stringify(stats())}\n`);
  process.exit(0);
}

process.on('SIGINT', () => void finish());
process.on('SIGTERM', () => void finish());

process.stderr.write(
  `simulator starting drivers=${drivers} rps=${rps} hotspots=${hotspots} seed=${seed} speedup=${speedup} sink=${values.sink}\n`,
);

// Drift-free loop: each tick is scheduled against absolute time, so transient
// slow ticks are absorbed instead of accumulating (lag is measured, not hidden).
function loop(): void {
  const target = startedAt + ticks * tickMs;
  const lag = performance.now() - target;
  if (lag > maxLagMs) maxLagMs = lag;

  for (const msg of sim.tick(1)) sink.send(msg);
  ticks++;

  if (ticks % 500 === 0) process.stderr.write(`progress ${JSON.stringify(stats())}\n`);
  if (maxTicks > 0 && ticks >= maxTicks) {
    void finish();
    return;
  }
  const delay = startedAt + ticks * tickMs - performance.now();
  setTimeout(loop, Math.max(0, delay));
}

loop();
