import { describe, expect, it } from 'vitest';
import { gcraDecide, type GcraConfig } from '../src/index.js';

// Pure GCRA arithmetic, hand-computed. Config: 10 units / 1000ms, burst 10.
//   emission_interval = (1000/1000) / 10 = 0.1s
//   burst_offset      = 0.1 * 10         = 1.0s
// nowSec is epoch-offset seconds; absolute origin is irrelevant, only deltas.
const CFG: GcraConfig = { limit: 10, windowMs: 1000 };

describe('gcraDecide — steady rate, paced', () => {
  it('cold key: first request allowed, consumes one unit of the burst (9 remaining)', () => {
    const d = gcraDecide(null, 100, CFG);
    expect(d.allowed).toBe(true);
    expect(d.remaining).toBe(9);
    expect(d.retryAfterSec).toBe(-1); // "not throttled" sentinel
    expect(d.newTatSec).toBeCloseTo(100.1, 9); // tat advanced by one emission interval
    expect(d.resetAfterSec).toBeCloseTo(0.1, 9); // one unit drains in one interval
  });

  it('once the burst is spent, one unit is admitted every emission interval (10/sec)', () => {
    // Drain the burst instantly at t=100: tat climbs to 100 + 10*0.1 = 101.
    let tat = 100;
    for (let i = 0; i < 10; i++) {
      const d = gcraDecide(tat, 100, CFG);
      expect(d.allowed).toBe(true);
      tat = d.newTatSec;
    }
    expect(tat).toBeCloseTo(101, 9);

    // Same instant, 11th request: denied.
    const denied = gcraDecide(tat, 100, CFG);
    expect(denied.allowed).toBe(false);

    // Advance exactly one interval — exactly one more unit conforms.
    const paced = gcraDecide(tat, 100.1, CFG);
    expect(paced.allowed).toBe(true);
    expect(paced.remaining).toBe(0);
  });
});

describe('gcraDecide — burst absorbed then paced', () => {
  it('admits a full burst of 10 instantly, then denies the 11th with correct retry/reset', () => {
    let tat = 100;
    let admitted = 0;
    for (let i = 0; i < 10; i++) {
      const d = gcraDecide(tat, 100, CFG);
      if (d.allowed) {
        admitted++;
        tat = d.newTatSec;
      }
    }
    expect(admitted).toBe(10);

    const denied = gcraDecide(tat, 100, CFG);
    expect(denied.allowed).toBe(false);
    expect(denied.remaining).toBe(0);
    // One interval until the next unit conforms; a full window until the bucket drains.
    expect(denied.retryAfterSec).toBeCloseTo(0.1, 9);
    expect(denied.resetAfterSec).toBeCloseTo(1.0, 9);
    expect(denied.newTatSec).toBe(tat); // DENY writes nothing — TAT unchanged
  });
});

describe('gcraDecide — burst = 1 is strictly paced', () => {
  it('one request instantly, the next in the same instant is denied', () => {
    const cfg: GcraConfig = { limit: 10, windowMs: 1000, burst: 1 };
    const first = gcraDecide(null, 100, cfg);
    expect(first.allowed).toBe(true);
    expect(first.remaining).toBe(0); // burst of 1 leaves nothing spare

    const second = gcraDecide(first.newTatSec, 100, cfg);
    expect(second.allowed).toBe(false);
    expect(second.retryAfterSec).toBeCloseTo(0.1, 9);
  });
});

describe('gcraDecide — idle key expiry (self-GC)', () => {
  it('reset_after equals drain time, so the key TTL expires exactly when it would refill', () => {
    // A single unit against a fresh key drains in one interval → TTL ceil(0.1s) = 1s.
    const d = gcraDecide(null, 500, CFG);
    expect(d.resetAfterSec).toBeCloseTo(0.1, 9);
    expect(Math.ceil(d.resetAfterSec)).toBe(1);
  });
});

describe('gcraDecide — cost > 1', () => {
  it('a cost-5 request consumes five units of the burst at once', () => {
    const d = gcraDecide(null, 100, CFG, 5);
    expect(d.allowed).toBe(true);
    expect(d.remaining).toBe(5); // 10 burst - 5 consumed
    expect(d.newTatSec).toBeCloseTo(100.5, 9);
  });

  it('a cost that exceeds the remaining burst is denied whole (all-or-nothing)', () => {
    const first = gcraDecide(null, 100, CFG, 8); // 8 of 10
    const second = gcraDecide(first.newTatSec, 100, CFG, 5); // needs 5, only 2 left
    expect(second.allowed).toBe(false);
  });
});
