import { createHmac, timingSafeEqual } from 'node:crypto';

/**
 * Auth-lite token scheme (documented here on purpose):
 *
 *   token = "<driverId>.<hex hmac-sha256(driverId, GATEWAY_SECRET)>"
 *
 * passed as `?token=` on the WS upgrade URL. The gateway recomputes the HMAC
 * and compares in constant time; a mismatch closes the socket with code 4001.
 * This proves the connector holds a token minted by someone who knows the
 * secret — no expiry, no scopes, no revocation. Real auth (JWT with expiry)
 * is out of scope for a simulated fleet; the shape of the check (verify at
 * upgrade, reject before any message handling) is what matters.
 *
 * The id may contain dots — the MAC is everything after the LAST dot.
 */
export function signToken(driverId: string, secret: string): string {
  return `${driverId}.${hmacHex(driverId, secret)}`;
}

/** Returns the authenticated driverId, or null for a missing/invalid token. */
export function verifyToken(token: string | null, secret: string): string | null {
  if (token === null) return null;
  const dot = token.lastIndexOf('.');
  if (dot <= 0) return null;
  const driverId = token.slice(0, dot);
  const mac = Buffer.from(token.slice(dot + 1));
  const expected = Buffer.from(hmacHex(driverId, secret));
  if (mac.length !== expected.length) return null;
  return timingSafeEqual(mac, expected) ? driverId : null;
}

function hmacHex(data: string, secret: string): string {
  return createHmac('sha256', secret).update(data).digest('hex');
}
