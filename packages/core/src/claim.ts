import { randomBytes } from 'node:crypto';
import type { Redis } from 'ioredis';
import { driverKey } from './geo-index.js';

/**
 * The atomic driver claim — the project's soul.
 *
 * Everything here runs as Redis Lua via defineCommand, because the claim is a
 * multi-key read-check-write (driver hash + cell available-set + claim key)
 * and only a script makes that one uninterruptible step: N concurrent
 * matchers race, Redis executes their scripts one at a time, and exactly one
 * observes the driver still available. The decisive write is the SREM from
 * the cell's available set — it is the linearization point, and it doubles as
 * a consistency check (a driver whose hash says available but who is missing
 * from its set is refused, not repaired, here).
 *
 * TTL lives in the DATA, not the process (pattern notes §3 / asynq's lease):
 * the claim value carries `expiresAt`, and confirmClaim checks it against the
 * caller's clock. The PX on the key is only a garbage-collection safety net —
 * if every matcher and the phase-D janitor die, the key still erases itself.
 * A claim is observably dead the moment its deadline passes, with zero
 * processes running; no in-memory timer is ever the authority.
 *
 * Phase D inserts the offer step between claimDriver and confirmClaim with no
 * rework: the claim already carries {tripId, token, expiresAt}, which is
 * exactly an offer's identity, proof-of-ownership, and deadline.
 *
 * Cell keys are derived from the driver hash inside the scripts (single-node
 * Redis; a cluster deployment would need hash-tagged keys per driver).
 */

export interface ClaimRecord {
  tripId: string;
  token: string;
  /** Epoch ms after which this claim is dead. Authoritative — the PX is only a GC net. */
  expiresAt: number;
}

export const DEFAULT_CLAIM_TTL_MS = 8_000;

export function claimKey(driverId: string): string {
  return `claim:${driverId}`;
}

/** 128-bit random hex token: proof of claim ownership, unguessable, one per attempt. */
export function newClaimToken(): string {
  return randomBytes(16).toString('hex');
}

/** The exact JSON stored at claim:{driverId}; expiry math in one place: nowMs + ttlMs. */
export function buildClaimValue(tripId: string, token: string, nowMs: number, ttlMs: number): string {
  const record: ClaimRecord = { tripId, token, expiresAt: nowMs + ttlMs };
  return JSON.stringify(record);
}

// KEYS[1] driver:{id}  KEYS[2] claim:{id}
// ARGV[1] driverId  ARGV[2] token  ARGV[3] claim JSON  ARGV[4] nowMs
// ARGV[5] freshMs  ARGV[6] ttlMs
//
// All checks precede all writes; any failed check returns nil having changed
// nothing (SREM of a non-member is a no-op, so even the decisive check cannot
// leave a partial state behind).
const CLAIM_LUA = `
local status = redis.call('HGET', KEYS[1], 'status')
if status ~= 'available' then return nil end
local hb = tonumber(redis.call('HGET', KEYS[1], 'heartbeatMs'))
if not hb then return nil end
if tonumber(ARGV[4]) - hb >= tonumber(ARGV[5]) then return nil end
local cell = redis.call('HGET', KEYS[1], 'cell')
if not cell then return nil end
if redis.call('SREM', 'cell:' .. cell .. ':available', ARGV[1]) == 0 then return nil end
redis.call('HSET', KEYS[1], 'status', 'claimed')
redis.call('SET', KEYS[2], ARGV[3], 'PX', ARGV[6])
return ARGV[2]
`;

// KEYS[1] claim:{id}  KEYS[2] driver:{id}
// ARGV[1] token  ARGV[2] nowMs
// Fail-and-change-nothing unless the claim exists, the token matches, and the
// in-value expiresAt has not passed (the caller's clock decides — see header).
const CONFIRM_LUA = `
local raw = redis.call('GET', KEYS[1])
if not raw then return 0 end
local claim = cjson.decode(raw)
if claim.token ~= ARGV[1] then return 0 end
if tonumber(ARGV[2]) >= tonumber(claim.expiresAt) then return 0 end
redis.call('DEL', KEYS[1])
redis.call('HSET', KEYS[2], 'status', 'on_trip')
return 1
`;

// KEYS[1] claim:{id}  KEYS[2] driver:{id}
// ARGV[1] driverId  ARGV[2] token
// Token must match; the driver returns to the available set of its CURRENT
// cell (the hash — the driver kept pinging position while claimed).
const RELEASE_LUA = `
local raw = redis.call('GET', KEYS[1])
if not raw then return 0 end
local claim = cjson.decode(raw)
if claim.token ~= ARGV[2] then return 0 end
redis.call('DEL', KEYS[1])
redis.call('HSET', KEYS[2], 'status', 'available')
local cell = redis.call('HGET', KEYS[2], 'cell')
if cell then redis.call('SADD', 'cell:' .. cell .. ':available', ARGV[1]) end
return 1
`;

/** Methods defineCommand installs on the client — typed here, zero `any`. */
interface ClaimCommands {
  flClaimDriver(
    driverKey: string,
    claimKey: string,
    driverId: string,
    token: string,
    claimValue: string,
    nowMs: number,
    freshMs: number,
    ttlMs: number,
  ): Promise<string | null>;
  flConfirmClaim(claimKey: string, driverKey: string, token: string, nowMs: number): Promise<number>;
  flReleaseClaim(claimKey: string, driverKey: string, driverId: string, token: string): Promise<number>;
}

export class ClaimStore {
  private readonly redis: Redis & ClaimCommands;

  constructor(redis: Redis) {
    redis.defineCommand('flClaimDriver', { numberOfKeys: 2, lua: CLAIM_LUA });
    redis.defineCommand('flConfirmClaim', { numberOfKeys: 2, lua: CONFIRM_LUA });
    redis.defineCommand('flReleaseClaim', { numberOfKeys: 2, lua: RELEASE_LUA });
    this.redis = redis as Redis & ClaimCommands;
  }

  /**
   * Atomically claim a driver for a trip. Succeeds only if the driver hash
   * exists with status 'available', the heartbeat is fresher than freshMs,
   * and the driver was actually in its cell's available set. Returns the
   * claim token, or null with nothing changed.
   */
  async claimDriver(
    driverId: string,
    tripId: string,
    nowMs: number,
    freshMs: number,
    ttlMs: number = DEFAULT_CLAIM_TTL_MS,
  ): Promise<string | null> {
    const token = newClaimToken();
    const value = buildClaimValue(tripId, token, nowMs, ttlMs);
    return this.redis.flClaimDriver(
      driverKey(driverId),
      claimKey(driverId),
      driverId,
      token,
      value,
      nowMs,
      freshMs,
      ttlMs,
    );
  }

  /**
   * Consume a live claim: token must match and expiresAt must not have
   * passed. Deletes the claim and sets the driver to on_trip (v1 goes
   * straight to trip; phase D interposes the offer/accept before this call).
   */
  async confirmClaim(driverId: string, token: string, nowMs: number = Date.now()): Promise<boolean> {
    return (await this.redis.flConfirmClaim(claimKey(driverId), driverKey(driverId), token, nowMs)) === 1;
  }

  /** Undo a claim (candidate walk moving on, PG refusing the trip): back to available, current cell. */
  async releaseClaim(driverId: string, token: string): Promise<boolean> {
    return (await this.redis.flReleaseClaim(claimKey(driverId), driverKey(driverId), driverId, token)) === 1;
  }
}

/** Convenience for tests/ops: parse a raw claim value, if present. */
export function parseClaimValue(raw: string | null): ClaimRecord | null {
  if (raw === null) return null;
  return JSON.parse(raw) as ClaimRecord;
}
