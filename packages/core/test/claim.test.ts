import { describe, expect, it } from 'vitest';
import { buildClaimValue, claimKey, newClaimToken, parseClaimValue } from '../src/index.js';

describe('claim token', () => {
  it('is 32 lowercase hex chars (128 bits)', () => {
    const token = newClaimToken();
    expect(token).toMatch(/^[0-9a-f]{32}$/);
  });

  it('never repeats across many draws', () => {
    const seen = new Set<string>();
    for (let i = 0; i < 1000; i++) seen.add(newClaimToken());
    expect(seen.size).toBe(1000);
  });
});

describe('claim value', () => {
  it('expiry math: expiresAt is exactly nowMs + ttlMs, TTL in the data', () => {
    const value = buildClaimValue('trip-1', 'tok', 1_000_000, 8_000);
    expect(JSON.parse(value)).toEqual({ tripId: 'trip-1', token: 'tok', expiresAt: 1_008_000 });
  });

  it('round-trips through parseClaimValue; absent key parses to null', () => {
    const value = buildClaimValue('t', 'k', 5, 10);
    expect(parseClaimValue(value)).toEqual({ tripId: 't', token: 'k', expiresAt: 15 });
    expect(parseClaimValue(null)).toBeNull();
  });
});

describe('claim key', () => {
  it('namespaces by driver id', () => {
    expect(claimKey('d1')).toBe('claim:d1');
  });
});
