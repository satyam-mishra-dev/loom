import type { Redis } from 'ioredis';
import { cellFor, kRing } from './geo.js';

/**
 * Driver lifecycle on the index. Pings and the sweep only drive the
 * available/offline transitions; claimed/on_trip belong to the matcher
 * (phase C/D) and must never be clobbered from here.
 */
export type DriverStatus = 'available' | 'claimed' | 'on_trip' | 'offline';

/** One driver position report (transport envelope stripped by the gateway). */
export interface GeoPing {
  driverId: string;
  lat: number;
  lng: number;
}

export interface DriverRecord {
  driverId: string;
  cell: string;
  status: DriverStatus;
  lat: number;
  lng: number;
  heartbeatMs: number;
}

export interface CandidateResult {
  /** The gridDisk radius that satisfied the search (or the maxK cap). */
  k: number;
  /** H3 guarantees no ordering inside a disk — callers sort/score. */
  candidates: DriverRecord[];
}

/** ZSET of driverId scored by heartbeatMs, so the stale sweep is a ZRANGEBYSCORE, not a scan. */
export const HEARTBEAT_ZSET = 'drivers:by-heartbeat';
export const DEFAULT_STALE_MS = 10_000;

export function driverKey(driverId: string): string {
  return `driver:${driverId}`;
}

export function cellKey(cell: string): string {
  return `cell:${cell}:available`;
}

/**
 * Redis geo index over H3 res-8 cells: one hash per driver
 * (`driver:{id}` → cell,status,lat,lng,heartbeatMs), one SET of available
 * driver ids per cell (`cell:{h3}:available`), and a heartbeat ZSET for
 * O(log n) staleness sweeps. All clocks are injected (`nowMs`) — the index
 * never reads wall time, which is what makes staleness testable.
 */
export class GeoIndex {
  constructor(private readonly redis: Redis) {}

  /**
   * Apply a batch of pings: hash + set + ZSET updates for the whole batch in
   * ONE pipelined MULTI (batching is the point — one round trip per flush).
   *
   * A MULTI cannot branch on reads, so current cell/status are read in a
   * pipeline first. The window between read and write can race a matcher
   * claim; the atomic claim (phase C) re-verifies membership + status +
   * heartbeat in Lua, so a spuriously re-added driver is caught at claim time.
   */
  async applyPings(pings: readonly GeoPing[], nowMs: number): Promise<void> {
    if (pings.length === 0) return;

    // Newest ping per driver wins within a batch.
    const latest = new Map<string, GeoPing>();
    for (const ping of pings) latest.set(ping.driverId, ping);
    const ids = [...latest.keys()];

    const reads = this.redis.pipeline();
    for (const id of ids) reads.hmget(driverKey(id), 'cell', 'status');
    const prev = (await reads.exec()) ?? [];

    const multi = this.redis.multi();
    ids.forEach((id, i) => {
      const ping = latest.get(id)!;
      const row = (prev[i]?.[1] ?? [null, null]) as (string | null)[];
      const prevCell = row[0] ?? null;
      const prevStatus = row[1] ?? null;
      const cell = cellFor(ping.lat, ping.lng);

      const ours = prevStatus === null || prevStatus === 'available' || prevStatus === 'offline';
      if (ours) {
        // New, available, or offline driver: a ping makes it available in its
        // (possibly new) cell. SADD is idempotent, so always re-adding also
        // self-heals a lost set after a Redis wipe.
        if (prevStatus === 'available' && prevCell !== null && prevCell !== cell) {
          multi.srem(cellKey(prevCell), id);
        }
        multi.sadd(cellKey(cell), id);
        multi.hset(driverKey(id), {
          cell,
          status: 'available' satisfies DriverStatus,
          lat: ping.lat,
          lng: ping.lng,
          heartbeatMs: nowMs,
        });
      } else {
        // claimed/on_trip: position + heartbeat only. Status and set
        // membership belong to the matcher from here — never re-add.
        multi.hset(driverKey(id), { cell, lat: ping.lat, lng: ping.lng, heartbeatMs: nowMs });
      }
      multi.zadd(HEARTBEAT_ZSET, nowMs, id);
    });
    await multi.exec();
  }

  /**
   * Drivers whose heartbeat is older than staleMs: remove from their available
   * set, mark offline, drop from the heartbeat ZSET (a returning ping re-adds
   * them). A stale claimed/on_trip driver is also marked offline — a silent
   * driver cannot serve a trip; releasing its claim is the janitor's job
   * (phase D). Returns the swept driver ids.
   */
  async sweepStale(nowMs: number, staleMs: number = DEFAULT_STALE_MS): Promise<string[]> {
    const cutoff = nowMs - staleMs;
    const ids = await this.redis.zrangebyscore(HEARTBEAT_ZSET, '-inf', cutoff);
    if (ids.length === 0) return [];

    const reads = this.redis.pipeline();
    for (const id of ids) reads.hmget(driverKey(id), 'cell', 'status', 'heartbeatMs');
    const rows = (await reads.exec()) ?? [];

    const swept: string[] = [];
    const multi = this.redis.multi();
    ids.forEach((id, i) => {
      const row = (rows[i]?.[1] ?? [null, null, null]) as (string | null)[];
      const cell = row[0] ?? null;
      const status = row[1] ?? null;
      const heartbeatMs = row[2] === null || row[2] === undefined ? null : Number(row[2]);
      // A ping may land between the ZSET scan and this read — recheck against
      // the hash (the authority) before declaring the driver dead.
      if (heartbeatMs !== null && heartbeatMs > cutoff) return;
      if (status === 'available' && cell !== null) multi.srem(cellKey(cell), id);
      if (status !== null) multi.hset(driverKey(id), { status: 'offline' satisfies DriverStatus });
      multi.zrem(HEARTBEAT_ZSET, id);
      swept.push(id);
    });
    if (swept.length > 0) await multi.exec();
    return swept;
  }

  /**
   * Expanding gridDisk candidate search: k = 0,1,2,… SUNION the disk's
   * available sets each expansion until ≥need fresh candidates or k hits maxK.
   * Stale or non-available drivers are filtered against their hash (the sets
   * can briefly lag the hash between sweep passes).
   */
  async findCandidates(
    lat: number,
    lng: number,
    opts: { need: number; maxK: number; nowMs?: number; staleMs?: number },
  ): Promise<CandidateResult> {
    const nowMs = opts.nowMs ?? Date.now();
    const staleMs = opts.staleMs ?? DEFAULT_STALE_MS;
    const origin = cellFor(lat, lng);

    let k = 0;
    for (;;) {
      const ids = await this.redis.sunion(...kRing(origin, k).map(cellKey));
      const candidates = await this.loadFresh(ids, nowMs, staleMs);
      if (candidates.length >= opts.need || k >= opts.maxK) return { k, candidates };
      k++;
    }
  }

  private async loadFresh(ids: string[], nowMs: number, staleMs: number): Promise<DriverRecord[]> {
    if (ids.length === 0) return [];
    const reads = this.redis.pipeline();
    for (const id of ids) reads.hgetall(driverKey(id));
    const rows = (await reads.exec()) ?? [];

    const out: DriverRecord[] = [];
    ids.forEach((id, i) => {
      const hash = (rows[i]?.[1] ?? {}) as Record<string, string>;
      if (hash['status'] !== 'available') return;
      const heartbeatMs = Number(hash['heartbeatMs']);
      if (!Number.isFinite(heartbeatMs) || nowMs - heartbeatMs >= staleMs) return;
      out.push({
        driverId: id,
        cell: hash['cell'] ?? '',
        status: 'available',
        lat: Number(hash['lat']),
        lng: Number(hash['lng']),
        heartbeatMs,
      });
    });
    return out;
  }
}
