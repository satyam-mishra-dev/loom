/** Seeded RNG (mulberry32) + gaussian sampling. All simulator randomness flows through this. */

export interface Rng {
  /** Uniform in [0, 1). */
  next(): number;
  /** Uniform integer in [0, maxExclusive). */
  int(maxExclusive: number): number;
  /** Uniform in [min, max). */
  range(min: number, max: number): number;
  /** Normal distribution via Box-Muller. */
  gaussian(mean: number, stdDev: number): number;
}

export function createRng(seed: number): Rng {
  let state = seed >>> 0;
  let spare: number | null = null;

  const next = (): number => {
    state = (state + 0x6d2b79f5) >>> 0;
    let t = state;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };

  return {
    next,
    int: (maxExclusive) => Math.floor(next() * maxExclusive),
    range: (min, max) => min + next() * (max - min),
    gaussian: (mean, stdDev) => {
      if (spare !== null) {
        const value = spare;
        spare = null;
        return mean + stdDev * value;
      }
      let u1 = next();
      while (u1 === 0) u1 = next();
      const u2 = next();
      const mag = Math.sqrt(-2 * Math.log(u1));
      spare = mag * Math.sin(2 * Math.PI * u2);
      return mean + stdDev * mag * Math.cos(2 * Math.PI * u2);
    },
  };
}
