import { describe, expect, it } from 'vitest';
import { BASE_FARE, SURGE_MAX, SURGE_MIN, quoteFare, surgeMultiplier } from '../src/index.js';

describe('surgeMultiplier', () => {
  it('no demand → no surge (an idle cell never surges), whatever the supply', () => {
    expect(surgeMultiplier(0, 5)).toBe(SURGE_MIN);
    expect(surgeMultiplier(0, 0)).toBe(SURGE_MIN);
  });

  it('demand with zero supply → the cap (bounded, never Infinity/NaN)', () => {
    expect(surgeMultiplier(10, 0)).toBe(SURGE_MAX);
    expect(surgeMultiplier(1, 0)).toBe(SURGE_MAX);
  });

  it('demand below supply → clamped up to 1 (surge never discounts)', () => {
    expect(surgeMultiplier(5, 10)).toBe(SURGE_MIN);
    expect(surgeMultiplier(2, 100)).toBe(SURGE_MIN);
  });

  it('mid-range → the raw demand/supply ratio', () => {
    expect(surgeMultiplier(10, 5)).toBe(2.0);
    expect(surgeMultiplier(3, 2)).toBe(1.5);
    expect(surgeMultiplier(7, 4)).toBeCloseTo(1.75, 9);
  });

  it('demand far above supply → clamped down to the cap', () => {
    expect(surgeMultiplier(100, 5)).toBe(SURGE_MAX); // 20x → 3.0
    expect(surgeMultiplier(30, 10)).toBe(SURGE_MAX); // 3.0x exactly
    expect(surgeMultiplier(31, 10)).toBe(SURGE_MAX); // 3.1x → clamped
  });
});

describe('quoteFare', () => {
  it('surge of 1.0 leaves the base fare untouched', () => {
    expect(quoteFare(0, 1)).toBe(BASE_FARE);
    // A 2 km trip at no surge: base + per-km, deterministic to the cent.
    expect(quoteFare(2000, 1)).toBe(5.5);
  });

  it('a higher surge multiplier lifts the price proportionally', () => {
    const base = quoteFare(2000, 1);
    expect(quoteFare(2000, 2)).toBeGreaterThan(base);
    expect(quoteFare(2000, 2)).toBe(11); // 5.5 × 2
    expect(quoteFare(2000, 1.5)).toBeCloseTo(8.25, 9);
  });

  it('is deterministic and rounded to cents', () => {
    expect(quoteFare(1234, 1.7)).toBe(quoteFare(1234, 1.7));
    expect(Number.isInteger(quoteFare(1234, 1.7) * 100)).toBe(true);
  });
});
