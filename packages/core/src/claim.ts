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
 * caller's clock. Every claim is ALSO indexed in the `claims:by-expiry` ZSET
 * (scored by expiresAt, written in the same script) so the janitor's sweep is
 * a ZRANGEBYSCORE, not a keyspace scan — and the expiry stays observable even
 * with zero matchers alive. The PX on the key is a garbage-collection safety
 * net set to ttl + a grace window: the grace exists because the JANITOR needs
 * to read the claim VALUE (it carries the tripId to re-enqueue) shortly AFTER
 * expiry; only if every matcher and every janitor stay dead past the grace
 * does Redis erase the key itself. The in-value expiresAt is authoritative
 * everywhere; the PX never is.
 *
 * The offer sits between claimDriver and confirmClaim: the claim already
 * carries {tripId, token, expiresAt}, which is exactly an offer's identity,
 * proof-of-ownership, and deadline.
 *
 * Cell keys and the expiry ZSET are addressed by literal name inside the
 * scripts (single-node Redis; a cluster deployment would need hash-tagged
 * keys per driver).
 */

export interface ClaimRecord {
  tripId: string;
  token: string;
  /** Epoch ms after which this claim is dead. Authoritative — the PX is only a GC net. */
  expiresAt: number;
}

/**
 * Claim TTL (12s) deliberately exceeds the offer TTL (8s) by a wide margin.
 * The claim must outlive the offer it guards: between claim creation and the
 * offer-timeout verdict sit the trip-row write, the publish, the gateway hop,
 * the driver's think time, and BLPOP's wakeup — if the claim could expire
 * while an accept is still in flight, the janitor could free the driver at
 * the exact moment the matcher confirms it, handing one driver to two
 * requests. The 4s margin makes that race unreachable in practice (and the
 * Postgres partial-unique index still defends the invariant if it ever
 * isn't).
 */
export const DEFAULT_CLAIM_TTL_MS = 12_000;
export const DEFAULT_OFFER_TTL_MS = 8_000;

/** ZSET of driverId scored by claim expiresAt — the janitor's worklist. */
export const CLAIMS_BY_EXPIRY = 'claims:by-expiry';

/** Extra life on the claim key's PX beyond expiresAt — the janitor's window to read the value. */
export const CLAIM_PX_GRACE_MS = 60_000;

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
// ARGV[5] freshMs  ARGV[6] ttlMs  ARGV[7] pxMs
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
redis.call('SET', KEYS[2], ARGV[3], 'PX', ARGV[7])
redis.call('ZADD', 'claims:by-expiry', tonumber(ARGV[4]) + tonumber(ARGV[6]), ARGV[1])
return ARGV[2]
`;

// KEYS[1] claim:{id}  KEYS[2] driver:{id}
// ARGV[1] token  ARGV[2] nowMs  ARGV[3] driverId
// Fail-and-change-nothing unless the claim exists, the token matches, and the
// in-value expiresAt has not passed (the caller's clock decides — see header).
const CONFIRM_LUA = `
local raw = redis.call('GET', KEYS[1])
if not raw then return 0 end
local claim = cjson.decode(raw)
if claim.token ~= ARGV[1] then return 0 end
if tonumber(ARGV[2]) >= tonumber(claim.expiresAt) then return 0 end
redis.call('DEL', KEYS[1])
redis.call('ZREM', 'claims:by-expiry', ARGV[3])
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
redis.call('ZREM', 'claims:by-expiry', ARGV[1])
redis.call('HSET', KEYS[2], 'status', 'available')
local cell = redis.call('HGET', KEYS[2], 'cell')
if cell then redis.call('SADD', 'cell:' .. cell .. ':available', ARGV[1]) end
return 1
`;

// KEYS[1] driver:{id}
// ARGV[1] driverId
// Trip completion: on_trip → available in the driver's CURRENT cell. Guarded
// on status so a duplicate trip_done (at-least-once queue) is a no-op.
const FREE_LUA = `
local status = redis.call('HGET', KEYS[1], 'status')
if status ~= 'on_trip' then return 0 end
redis.call('HSET', KEYS[1], 'status', 'available')
local cell = redis.call('HGET', KEYS[1], 'cell')
if cell then redis.call('SADD', 'cell:' .. cell .. ':available', ARGV[1]) end
return 1
`;

// KEYS[1] claim:{id}  KEYS[2] driver:{id}
// ARGV[1] driverId  ARGV[2] nowMs
// The janitor's atomic release: re-read the claim UNDER the script (it may
// have been confirmed/released/rewritten since the ZSET scan) and only if it
// is genuinely past its in-value expiresAt: delete it, de-index it, and put
// the driver back — but ONLY a driver still in 'claimed'. An 'offline' driver
// (swept for silence) must not re-enter an available set, and an 'on_trip'
// driver belongs to a live trip. Returns:
//   {'live'}                 claim exists and is not yet expired — no-op
//   {'released', tripId}     expired claim removed; tripId for the PG revert
//   {'gone', repaired}       key already erased by the PX net; ZSET cleaned,
//                            driver repaired if it was stuck 'claimed'
const JANITOR_LUA = `
local raw = redis.call('GET', KEYS[1])
if not raw then
  redis.call('ZREM', 'claims:by-expiry', ARGV[1])
  local status = redis.call('HGET', KEYS[2], 'status')
  if status == 'claimed' then
    redis.call('HSET', KEYS[2], 'status', 'available')
    local cell = redis.call('HGET', KEYS[2], 'cell')
    if cell then redis.call('SADD', 'cell:' .. cell .. ':available', ARGV[1]) end
    return {'gone', 1}
  end
  return {'gone', 0}
end
local claim = cjson.decode(raw)
if tonumber(ARGV[2]) < tonumber(claim.expiresAt) then return {'live'} end
redis.call('DEL', KEYS[1])
redis.call('ZREM', 'claims:by-expiry', ARGV[1])
local status = redis.call('HGET', KEYS[2], 'status')
if status == 'claimed' then
  redis.call('HSET', KEYS[2], 'status', 'available')
  local cell = redis.call('HGET', KEYS[2], 'cell')
  if cell then redis.call('SADD', 'cell:' .. cell .. ':available', ARGV[1]) end
end
return {'released', claim.tripId}
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
    pxMs: number,
  ): Promise<string | null>;
  flConfirmClaim(claimKey: string, driverKey: string, token: string, nowMs: number, driverId: string): Promise<number>;
  flReleaseClaim(claimKey: string, driverKey: string, driverId: string, token: string): Promise<number>;
  flFreeDriver(driverKey: string, driverId: string): Promise<number>;
  flJanitorRelease(
    claimKey: string,
    driverKey: string,
    driverId: string,
    nowMs: number,
  ): Promise<(string | number)[]>;
}

/** Outcome of a janitor sweep attempt on one driver's claim. */
export type JanitorSweepResult =
  | { kind: 'live' }
  | { kind: 'released'; tripId: string }
  | { kind: 'gone'; repaired: boolean };

export class ClaimStore {
  private readonly redis: Redis & ClaimCommands;

  constructor(redis: Redis) {
    redis.defineCommand('flClaimDriver', { numberOfKeys: 2, lua: CLAIM_LUA });
    redis.defineCommand('flConfirmClaim', { numberOfKeys: 2, lua: CONFIRM_LUA });
    redis.defineCommand('flReleaseClaim', { numberOfKeys: 2, lua: RELEASE_LUA });
    redis.defineCommand('flFreeDriver', { numberOfKeys: 1, lua: FREE_LUA });
    redis.defineCommand('flJanitorRelease', { numberOfKeys: 2, lua: JANITOR_LUA });
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
      ttlMs + CLAIM_PX_GRACE_MS,
    );
  }

  /**
   * Consume a live claim: token must match and expiresAt must not have
   * passed. Deletes the claim (and its expiry-ZSET entry) and sets the
   * driver to on_trip. Called after the driver accepted and the trip row
   * committed.
   */
  async confirmClaim(driverId: string, token: string, nowMs: number = Date.now()): Promise<boolean> {
    return (
      (await this.redis.flConfirmClaim(claimKey(driverId), driverKey(driverId), token, nowMs, driverId)) === 1
    );
  }

  /** Undo a claim (decline, timeout, PG refusing the trip): back to available, current cell. */
  async releaseClaim(driverId: string, token: string): Promise<boolean> {
    return (await this.redis.flReleaseClaim(claimKey(driverId), driverKey(driverId), driverId, token)) === 1;
  }

  /** Trip completed: on_trip → available in the driver's current cell. No-op unless on_trip. */
  async freeDriver(driverId: string): Promise<boolean> {
    return (await this.redis.flFreeDriver(driverKey(driverId), driverId)) === 1;
  }

  /** Drivers whose claims have expired per the ZSET — the janitor's worklist. */
  async expiredClaims(nowMs: number): Promise<string[]> {
    return this.redis.zrangebyscore(CLAIMS_BY_EXPIRY, '-inf', nowMs);
  }

  /** Atomically release ONE expired claim (janitor). See JANITOR_LUA for semantics. */
  async janitorRelease(driverId: string, nowMs: number): Promise<JanitorSweepResult> {
    const res = await this.redis.flJanitorRelease(claimKey(driverId), driverKey(driverId), driverId, nowMs);
    switch (res[0]) {
      case 'released':
        return { kind: 'released', tripId: String(res[1] ?? '') };
      case 'gone':
        return { kind: 'gone', repaired: res[1] === '1' || Number(res[1]) === 1 };
      default:
        return { kind: 'live' };
    }
  }
}

/** Convenience for tests/ops: parse a raw claim value, if present. */
export function parseClaimValue(raw: string | null): ClaimRecord | null {
  if (raw === null) return null;
  return JSON.parse(raw) as ClaimRecord;
}
