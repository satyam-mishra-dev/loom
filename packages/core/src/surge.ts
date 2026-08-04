import type { Redis } from 'ioredis';
import { cellKey } from './geo-index.js';

/**
 * Per-cell surge pricing.
 *
 * multiplier = min(3.0, max(1.0, demand / supply)), where demand is the count
 * of ride requests in a cell over a sliding 60s window and supply is the count
 * of drivers currently available in that cell. Recomputed on a tick by the
 * read model and published two ways: a Redis hash `cell:surge` (durable read
 * model, survives a read-model restart, readable by any consumer) and an
 * in-memory snapshot the SSE stream serves.
 *
 * Split, as with GCRA: `surgeMultiplier` is the PURE math (unit-tested against
 * the edge cases — no supply, no demand, mid-range ratio); `SurgeStore` is the
 * Redis plumbing (demand recorded by the gateway on intake, recomputed by the
 * read model). The demand window lives in Redis (a per-cell ZSET scored by
 * arrival time) so it survives a read-model restart and needs no cross-process
 * feed — the gateway records, the read model reads and prunes.
 */

export const SURGE_MIN = 1.0;
export const SURGE_MAX = 3.0;
export const SURGE_WINDOW_MS = 60_000;

/** The published read-model hash: cell → multiplier (only surging cells, mult > 1). */
export const SURGE_HASH = 'cell:surge';
/** Set of cells with demand in the window — the recompute worklist (avoids scanning all keys). */
export const SURGE_CELLS = 'surge:cells';

function demandKey(cell: string): string {
  return `surge:demand:${cell}`;
}

/**
 * The multiplier. demand/supply clamped to [1, 3]. No demand → 1 (never surge
 * an idle cell). Demand with zero supply → the cap (3): infinite pressure, but
 * the multiplier is bounded so a rider never sees a runaway price. Guards make
 * the division total — no NaN, no Infinity leaks to the dashboard.
 */
export function surgeMultiplier(demand: number, supply: number): number {
  if (demand <= 0) return SURGE_MIN;
  if (supply <= 0) return SURGE_MAX;
  return Math.min(SURGE_MAX, Math.max(SURGE_MIN, demand / supply));
}

/** Flat base fare + per-km rate, in the same abstract currency as the offer price. */
export const BASE_FARE = 2.5;
export const PER_KM_FARE = 1.5;

/**
 * The fare shown in a driver offer: (base + per-km × trip km) × surge, rounded
 * to cents. Pure and deterministic. surge = 1 leaves the base fare untouched;
 * a higher surge multiplier lifts the price proportionally. NOTE: naive
 * flat-rate model — real pricing folds in time, wait, tolls, and vehicle tier.
 */
export function quoteFare(tripMeters: number, surge: number): number {
  const base = BASE_FARE + PER_KM_FARE * (tripMeters / 1000);
  return Math.round(base * surge * 100) / 100;
}

export interface CellSurge {
  cell: string;
  demand: number;
  supply: number;
  multiplier: number;
}

export class SurgeStore {
  constructor(private readonly redis: Redis) {}

  /**
   * Record one ride request against its cell's demand window. Called by the
   * gateway on intake (it already computed the cell). The ZSET is scored by
   * arrival time so recompute can prune by score; a PX net twice the window
   * long GCs the whole cell if the read model ever stops pruning it.
   */
  async recordDemand(cell: string, requestId: string, nowMs: number): Promise<void> {
    await this.redis
      .multi()
      .zadd(demandKey(cell), nowMs, requestId)
      .sadd(SURGE_CELLS, cell)
      .pexpire(demandKey(cell), SURGE_WINDOW_MS * 2)
      .exec();
  }

  /**
   * Recompute every demand cell: prune the window, count demand (ZCARD) vs
   * supply (SCARD of the cell's available set), derive the multiplier. Cells
   * whose demand aged out entirely are dropped from the worklist. Returns
   * every demand cell's numbers (the read model keeps them for the SSE
   * snapshot); `publish` writes the surging ones to Redis.
   */
  async recompute(nowMs: number, windowMs: number = SURGE_WINDOW_MS): Promise<CellSurge[]> {
    const cells = await this.redis.smembers(SURGE_CELLS);
    if (cells.length === 0) return [];
    const cutoff = nowMs - windowMs;

    const prune = this.redis.pipeline();
    for (const cell of cells) {
      prune.zremrangebyscore(demandKey(cell), '-inf', cutoff);
      prune.zcard(demandKey(cell));
      prune.scard(cellKey(cell));
    }
    const res = (await prune.exec()) ?? [];

    const out: CellSurge[] = [];
    const emptied: string[] = [];
    cells.forEach((cell, i) => {
      const demand = Number(res[i * 3 + 1]?.[1] ?? 0);
      const supply = Number(res[i * 3 + 2]?.[1] ?? 0);
      if (demand === 0) {
        emptied.push(cell);
        return;
      }
      out.push({ cell, demand, supply, multiplier: surgeMultiplier(demand, supply) });
    });
    if (emptied.length > 0) await this.redis.srem(SURGE_CELLS, ...emptied);
    return out;
  }

  /**
   * Publish the surging cells (multiplier > 1) to the `cell:surge` hash,
   * replacing the previous snapshot atomically so a cell that stopped surging
   * disappears rather than lingering at a stale multiplier.
   */
  async publish(surges: readonly CellSurge[]): Promise<void> {
    const surging = surges.filter((s) => s.multiplier > SURGE_MIN);
    const multi = this.redis.multi().del(SURGE_HASH);
    if (surging.length > 0) {
      const pairs: string[] = [];
      for (const s of surging) pairs.push(s.cell, s.multiplier.toFixed(3));
      multi.hset(SURGE_HASH, ...pairs);
    }
    await multi.exec();
  }

  /** Read the published multipliers (for consumers that aren't the read model). */
  async published(): Promise<Record<string, number>> {
    const hash = await this.redis.hgetall(SURGE_HASH);
    const out: Record<string, number> = {};
    for (const [cell, mult] of Object.entries(hash)) out[cell] = Number(mult);
    return out;
  }

  /**
   * The current multiplier for a single cell, from the published hash. Defaults
   * to SURGE_MIN (no surge) for a cell that isn't surging or reads back garbage
   * — the matcher prices every offer, so this must never throw or return NaN.
   */
  async multiplierFor(cell: string): Promise<number> {
    const raw = await this.redis.hget(SURGE_HASH, cell);
    const m = raw === null ? SURGE_MIN : Number(raw);
    return Number.isFinite(m) && m >= SURGE_MIN ? m : SURGE_MIN;
  }
}
