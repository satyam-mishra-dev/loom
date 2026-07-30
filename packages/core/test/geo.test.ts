import { describe, expect, it } from 'vitest';
import { cellFor, H3_RESOLUTION, kRing } from '../src/geo.js';

describe('geo helpers', () => {
  it('maps a known coordinate to a stable resolution-8 cell', () => {
    expect(H3_RESOLUTION).toBe(8);
    // Downtown San Francisco; value pinned so an h3-js upgrade that changes
    // indexing (it must not) fails loudly.
    expect(cellFor(37.7749, -122.4194)).toBe('8828308281fffff');
  });

  it('kRing k=0 returns exactly the origin cell', () => {
    const cell = cellFor(37.7749, -122.4194);
    expect(kRing(cell, 0)).toEqual([cell]);
  });

  it('kRing sizes are 1, 7, 19 for k=0,1,2', () => {
    const cell = cellFor(37.7749, -122.4194);
    expect(kRing(cell, 0)).toHaveLength(1);
    expect(kRing(cell, 1)).toHaveLength(7);
    expect(kRing(cell, 2)).toHaveLength(19);
  });

  it('kRing includes the origin and only unique cells', () => {
    const cell = cellFor(37.7749, -122.4194);
    const ring = kRing(cell, 2);
    expect(ring).toContain(cell);
    expect(new Set(ring).size).toBe(ring.length);
  });
});
