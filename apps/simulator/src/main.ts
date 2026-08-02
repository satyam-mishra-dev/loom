import { parseArgs } from 'node:util';
import type { ServerMessage } from '@fleetline/core';
import { Simulation } from './sim.js';
import { createSink } from './sinks.js';
import { DEFAULT_CITY } from './world.js';

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
    'accept-prob': { type: 'string', default: '0.8' },
    'city-m': { type: 'string', default: '0' },
    'request-ticks': { type: 'string', default: '0' },
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
const acceptProb = num('accept-prob', values['accept-prob'], 0);
const cityM = num('city-m', values['city-m'], 0); // 0 = default 10km city
const requestTicks = num('request-ticks', values['request-ticks'], 0); // 0 = every tick

const city = cityM > 0 ? { ...DEFAULT_CITY, widthM: cityM, heightM: cityM } : DEFAULT_CITY;
const sim = new Simulation({ drivers, ratePerSec: rps, hotspots, seed, city, acceptProb });

// Offer/trip counters for the exit stats.
let offers = 0;
let accepted = 0;
let declined = 0;
let tripsAssigned = 0;
let tripsCompleted = 0;

// Offers and assignments arrive over the sink's inbound side. Decisions are
// the sim's (seeded); the reply delay is simulated driver think time, mapped
// to real time through the speedup so it stays proportional to the ticks.
function onServerMessage(msg: ServerMessage): void {
  if (msg.type === 'offer') {
    offers++;
    const decision = sim.decideOffer(msg.driverId);
    if (decision.accept) accepted++;
    else declined++;
    setTimeout(
      () =>
        sink.send({
          type: 'offer_reply',
          offerId: msg.offerId,
          driverId: msg.driverId,
          accept: decision.accept,
        }),
      decision.delayMs / speedup,
    );
    return;
  }
  if (sim.assignTrip(msg.driverId, msg.tripId, msg.pickup, msg.dest)) tripsAssigned++;
}

const sink = createSink(values.sink, values.gateway, onServerMessage);

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
    offers,
    accepted,
    declined,
    tripsAssigned,
    tripsCompleted,
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
  `simulator starting drivers=${drivers} rps=${rps} hotspots=${hotspots} seed=${seed} speedup=${speedup} sink=${values.sink} acceptProb=${acceptProb}\n`,
);

// Drift-free loop: each tick is scheduled against absolute time, so transient
// slow ticks are absorbed instead of accumulating (lag is measured, not hidden).
function loop(): void {
  const target = startedAt + ticks * tickMs;
  const lag = performance.now() - target;
  if (lag > maxLagMs) maxLagMs = lag;

  for (const msg of sim.tick(1)) {
    if (msg.type === 'ride_request' && requestTicks > 0 && ticks >= requestTicks) continue;
    if (msg.type === 'trip_progress' && msg.event === 'trip_done') tripsCompleted++;
    sink.send(msg);
  }
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
