import { describe, expect, it } from 'vitest';
import { signToken, verifyToken } from '../src/auth.js';

const SECRET = 'test-secret';

describe('auth-lite tokens', () => {
  it('round-trips a signed token', () => {
    expect(verifyToken(signToken('d42', SECRET), SECRET)).toBe('d42');
  });

  it('supports driver ids containing dots (mac is after the last dot)', () => {
    expect(verifyToken(signToken('fleet.d42', SECRET), SECRET)).toBe('fleet.d42');
  });

  it('rejects a tampered mac', () => {
    const token = signToken('d42', SECRET);
    const flipped = token.slice(0, -1) + (token.endsWith('0') ? '1' : '0');
    expect(verifyToken(flipped, SECRET)).toBeNull();
  });

  it('rejects a token signed with a different secret', () => {
    expect(verifyToken(signToken('d42', 'other'), SECRET)).toBeNull();
  });

  it('rejects malformed and missing tokens', () => {
    expect(verifyToken(null, SECRET)).toBeNull();
    expect(verifyToken('', SECRET)).toBeNull();
    expect(verifyToken('no-dot', SECRET)).toBeNull();
    expect(verifyToken('.maconly', SECRET)).toBeNull();
    expect(verifyToken('d42.', SECRET)).toBeNull();
    expect(verifyToken('d42.shortmac', SECRET)).toBeNull();
  });
});
