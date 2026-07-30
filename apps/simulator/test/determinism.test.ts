import { describe, expect, it } from 'vitest';
import { Simulation } from '../src/sim.js';

function run(seed: number): string {
  const sim = new Simulation({ drivers: 20, ratePerSec: 5, hotspots: 3, seed });
  const out = [];
  for (let t = 0; t < 50; t++) out.push(...sim.tick());
  return JSON.stringify(out);
}

describe('seeded reproducibility', () => {
  it('identical seeds reproduce identical ping and request sequences', () => {
    expect(run(42)).toBe(run(42));
  });

  it('different seeds diverge', () => {
    expect(run(42)).not.toBe(run(43));
  });
});
