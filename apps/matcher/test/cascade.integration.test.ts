import { PostgreSqlContainer, type StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import { RedisContainer, type StartedRedisContainer } from '@testcontainers/redis';
import { Redis } from 'ioredis';
import type pg from 'pg';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import {
  CLAIMS_BY_EXPIRY,
  ClaimStore,
  GeoIndex,
  REQUESTS_QUEUE,
  TRIP_EVENTS_QUEUE,
  cellFor,
  cellKey,
  claimKey,
  driverKey,
  offerReplyKey,
  type OfferMessage,
  type TripAssigned,
} from '@fleetline/core';
import { createPool, runMigrations } from '@fleetline/db';
import { Janitor } from '../src/janitor.js';
import { MatcherCore } from '../src/matcher.js';
import { TripStore } from '../src/trip-store.js';

// The offer cascade against real Redis + Postgres: fake drivers live on the
// real transport (psubscribe on the driver channels, replies LPUSHed to the
// offer reply lists) — exactly what the gateway does on their behalf.

const CENTER = { lat: 37.7749, lng: -122.4194 };
const DEST = { lat: 37.7849, lng: -122.4094 };
const C0 = cellFor(CENTER.lat, CENTER.lng);

async function waitFor(cond: () => boolean | Promise<boolean>, timeoutMs = 10_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!(await cond())) {
    if (Date.now() > deadline) throw new Error('waitFor timed out');
    await new Promise((r) => setTimeout(r, 25));
  }
}

/** accept / decline reply now, or stay silent (offer times out). */
type FleetBehavior = (offer: OfferMessage) => 'accept' | 'decline' | 'silent';

class FakeFleet {
  readonly offers: OfferMessage[] = [];
  readonly assigned: TripAssigned[] = [];
  behavior: FleetBehavior = () => 'accept';

  private constructor(
    private readonly sub: Redis,
    private readonly redis: Redis,
  ) {}

  static async start(redis: Redis): Promise<FakeFleet> {
    const sub = redis.duplicate();
    const fleet = new FakeFleet(sub, redis);
    sub.on('pmessage', (_pattern: string, _channel: string, message: string) => {
      const msg = JSON.parse(message) as OfferMessage | TripAssigned;
      if (msg.type === 'trip_assigned') {
        fleet.assigned.push(msg);
        return;
      }
      fleet.offers.push(msg);
      const verdict = fleet.behavior(msg);
      if (verdict === 'silent') return;
      void fleet.redis.lpush(offerReplyKey(msg.offerId), JSON.stringify({ accept: verdict === 'accept' }));
    });
    await sub.psubscribe('driver:*:msg');
    return fleet;
  }

  stop(): void {
    this.sub.disconnect();
  }
}

describe('offer cascade (testcontainers redis + postgres)', () => {
  let redisContainer: StartedRedisContainer;
  let pgContainer: StartedPostgreSqlContainer;
  let redis: Redis;
  let pool: pg.Pool;
  let geo: GeoIndex;
  let fleet: FakeFleet;
  const cores: MatcherCore[] = [];

  beforeAll(async () => {
    [redisContainer, pgContainer] = await Promise.all([
      new RedisContainer('redis:7-alpine').start(),
      new PostgreSqlContainer('postgres:16-alpine').start(),
    ]);
    redis = new Redis(redisContainer.getConnectionUrl());
    await runMigrations(pgContainer.getConnectionUri());
    pool = createPool(pgContainer.getConnectionUri());
    geo = new GeoIndex(redis);
  }, 240_000);

  afterAll(async () => {
    await pool.end();
    redis.disconnect();
    await Promise.all([redisContainer.stop(), pgContainer.stop()]);
  });

  beforeEach(async () => {
    await redis.flushall();
    await pool.query('TRUNCATE trip_events, ride_requests, trips CASCADE');
    fleet = await FakeFleet.start(redis);
  });

  afterEach(async () => {
    for (const core of cores.splice(0)) await core.stop();
    fleet.stop();
  });

  function newCore(opts: { offerTtlMs?: number; claimTtlMs?: number } = {}): MatcherCore {
    const core = new MatcherCore({ redis, pool, offerTtlMs: 2_000, claimTtlMs: 4_000, ...opts });
    cores.push(core);
    return core;
  }

  async function seedDrivers(n: number): Promise<void> {
    await geo.applyPings(
      Array.from({ length: n }, (_, i) => ({ driverId: `d${i}`, ...CENTER })),
      Date.now(),
    );
  }

  async function insertRequest(id: string): Promise<void> {
    await pool.query(
      `INSERT INTO ride_requests (id, lat, lng, dest_lat, dest_lng, status)
       VALUES ($1, $2, $3, $4, $5, 'pending')`,
      [id, CENTER.lat, CENTER.lng, DEST.lat, DEST.lng],
    );
  }

  async function eventChain(tripId: string): Promise<string[]> {
    const res = await pool.query<{ type: string }>(
      'SELECT type FROM trip_events WHERE trip_id = $1 ORDER BY id',
      [tripId],
    );
    return res.rows.map((r) => r.type);
  }

  it('full lifecycle through the running service: accept → matched → en_route → in_trip → completed, outbox complete', async () => {
    await seedDrivers(1);
    await insertRequest('r1');
    await redis.lpush(REQUESTS_QUEUE, 'r1');

    const core = newCore();
    await core.start(1);

    // Cascade: offer → accept → matched → trip_assigned → en_route.
    await waitFor(async () => {
      const res = await pool.query<{ status: string }>(`SELECT status FROM trips WHERE request_id = 'r1'`);
      return res.rows[0]?.status === 'en_route';
    });
    const trip = await pool.query<{ id: string; driver_id: string; status: string }>(
      `SELECT id, driver_id, status FROM trips WHERE request_id = 'r1'`,
    );
    const { id: tripId, driver_id: driverId } = trip.rows[0]!;
    expect(driverId).toBe('d0');
    expect(fleet.assigned).toHaveLength(1);
    expect(fleet.assigned[0]).toMatchObject({ tripId, driverId, dest: DEST });
    expect((await pool.query(`SELECT status FROM ride_requests WHERE id = 'r1'`)).rows[0]).toEqual({
      status: 'matched',
    });
    // Claim consumed: driver on_trip, nothing left to janitor.
    expect(await redis.hget(driverKey('d0'), 'status')).toBe('on_trip');
    expect(await redis.keys('claim:*')).toEqual([]);
    expect(await redis.zcard(CLAIMS_BY_EXPIRY)).toBe(0);

    // Progression: the driver reports milestones the way the gateway would.
    await redis.lpush(TRIP_EVENTS_QUEUE, JSON.stringify({ tripId, driverId, event: 'arrived_pickup' }));
    await waitFor(async () => {
      const res = await pool.query<{ status: string }>('SELECT status FROM trips WHERE id = $1', [tripId]);
      return res.rows[0]?.status === 'in_trip';
    });
    await redis.lpush(TRIP_EVENTS_QUEUE, JSON.stringify({ tripId, driverId, event: 'trip_done' }));
    await waitFor(async () => {
      const res = await pool.query<{ status: string }>('SELECT status FROM trips WHERE id = $1', [tripId]);
      return res.rows[0]?.status === 'completed';
    });

    // Driver freed back into its cell; outbox has EVERY hop, in order.
    await waitFor(async () => (await redis.hget(driverKey('d0'), 'status')) === 'available');
    expect(await redis.smembers(cellKey(C0))).toEqual(['d0']);
    expect(await eventChain(tripId)).toEqual([
      'requested',
      'matching',
      'offered',
      'matched',
      'en_route',
      'in_trip',
      'completed',
    ]);
    expect(core.metrics.offersSentTotal).toBe(1);
    expect(core.metrics.offerAcceptsTotal).toBe(1);
    expect(core.metrics.tripsCompletedTotal).toBe(1);
    expect(core.metrics.pgUniqueViolationsTotal).toBe(0);
  });

  it('decline → the NEXT candidate gets the offer; the decliner is released', async () => {
    await seedDrivers(2);
    await insertRequest('r1');
    fleet.behavior = (offer) => (offer.driverId === 'd0' ? 'decline' : 'accept');

    const core = newCore();
    expect(await core.matchRequest('r1')).toBe('matched');

    const trip = await pool.query<{ id: string; driver_id: string; status: string }>(
      'SELECT id, driver_id, status FROM trips',
    );
    expect(trip.rows[0]).toMatchObject({ driver_id: 'd1', status: 'en_route' });
    // The decliner went back to available, in its cell, claim gone.
    expect(await redis.hget(driverKey('d0'), 'status')).toBe('available');
    expect((await redis.smembers(cellKey(C0))).sort()).toEqual(['d0']);
    expect(await redis.exists(claimKey('d0'))).toBe(0);
    expect(fleet.offers.map((o) => o.driverId)).toEqual(['d0', 'd1']);
    expect(core.metrics.offerDeclinesTotal).toBe(1);
    expect(core.metrics.offerAcceptsTotal).toBe(1);
    expect(await eventChain(trip.rows[0]!.id)).toEqual([
      'requested',
      'matching',
      'offered',
      'matching', // d0 declined
      'offered',
      'matched',
      'en_route',
    ]);
  });

  it('all decline: exactly 5 offers, then an honest unmatched with a cancelled trip', async () => {
    await seedDrivers(8);
    await insertRequest('r1');
    fleet.behavior = () => 'decline';

    const core = newCore();
    expect(await core.matchRequest('r1')).toBe('unmatched');

    expect(core.metrics.offersSentTotal).toBe(5);
    expect(core.metrics.offerDeclinesTotal).toBe(5);
    expect(fleet.offers).toHaveLength(5);
    expect(new Set(fleet.offers.map((o) => o.driverId)).size).toBe(5); // never the same driver twice

    expect((await pool.query(`SELECT status FROM ride_requests WHERE id = 'r1'`)).rows[0]).toEqual({
      status: 'unmatched',
    });
    const trip = await pool.query<{ id: string; status: string }>('SELECT id, status FROM trips');
    expect(trip.rows[0]?.status).toBe('cancelled');
    const chain = await eventChain(trip.rows[0]!.id);
    expect(chain[chain.length - 1]).toBe('cancelled');
    expect(chain.filter((t) => t === 'offered')).toHaveLength(5);

    // Every driver is available again; no claims or ZSET entries survive.
    expect((await redis.smembers(cellKey(C0))).sort()).toHaveLength(8);
    expect(await redis.keys('claim:*')).toEqual([]);
    expect(await redis.zcard(CLAIMS_BY_EXPIRY)).toBe(0);
  });

  it('offer timeout (no reply): claim released, cascade moves to the next candidate', async () => {
    await seedDrivers(2);
    await insertRequest('r1');
    fleet.behavior = (offer) => (offer.driverId === 'd0' ? 'silent' : 'accept');

    const core = newCore({ offerTtlMs: 300, claimTtlMs: 1_500 });
    expect(await core.matchRequest('r1')).toBe('matched');

    expect(core.metrics.offerTimeoutsTotal).toBe(1);
    expect(core.metrics.offerAcceptsTotal).toBe(1);
    const trip = await pool.query<{ driver_id: string }>('SELECT driver_id FROM trips');
    expect(trip.rows[0]).toEqual({ driver_id: 'd1' });
    expect(await redis.hget(driverKey('d0'), 'status')).toBe('available');
    expect(await redis.exists(claimKey('d0'))).toBe(0);
  });

  it('JANITOR: a matcher dead between claim and reply strands nobody — release, revert, re-enqueue, re-match', async () => {
    await seedDrivers(1);
    await insertRequest('r1');

    // Stage the dead matcher by hand: it owned the request, claimed the
    // driver, created the OFFERED trip row… and vanished.
    const claims = new ClaimStore(redis);
    const trips = new TripStore(pool);
    await pool.query(`UPDATE ride_requests SET status = 'matching' WHERE id = 'r1'`);
    const token = await claims.claimDriver('d0', 'trip-1', Date.now(), 10_000, 500);
    expect(token).not.toBeNull();
    expect(
      await trips.offerTrip({
        tripId: 'trip-1',
        requestId: 'r1',
        driverId: 'd0',
        offerId: 'offer-1',
        claimToken: token!,
        rider: CENTER,
      }),
    ).toBe(true);

    // TTL lives in the data: once expiresAt passes, ANY janitor can act.
    const janitor = new Janitor({ redis, pool });
    expect(await janitor.sweepOnce()).toBe(0); // not yet expired: no-op
    await new Promise((r) => setTimeout(r, 600));
    expect(await janitor.sweepOnce()).toBe(1);

    // Driver back and claimable; trip reverted with an outbox row; request
    // re-enqueued and pending again.
    expect(await redis.hget(driverKey('d0'), 'status')).toBe('available');
    expect(await redis.smembers(cellKey(C0))).toEqual(['d0']);
    expect(await redis.exists(claimKey('d0'))).toBe(0);
    expect(await redis.zcard(CLAIMS_BY_EXPIRY)).toBe(0);
    expect((await pool.query(`SELECT status FROM trips WHERE id = 'trip-1'`)).rows[0]).toEqual({
      status: 'matching',
    });
    expect((await pool.query(`SELECT status FROM ride_requests WHERE id = 'r1'`)).rows[0]).toEqual({
      status: 'pending',
    });
    expect(await redis.lrange(REQUESTS_QUEUE, 0, -1)).toEqual(['r1']);
    expect(await eventChain('trip-1')).toEqual(['requested', 'matching', 'offered', 'matching']);
    expect(janitor.metrics.janitorReleasedTotal).toBe(1);
    expect(janitor.metrics.janitorRequeuedTotal).toBe(1);

    // A live matcher completes the re-cascade on the SAME trip row.
    expect(await newCore().matchRequest('r1')).toBe('matched');
    expect((await pool.query(`SELECT status, driver_id FROM trips WHERE id = 'trip-1'`)).rows[0]).toEqual({
      status: 'en_route',
      driver_id: 'd0',
    });
    expect(await eventChain('trip-1')).toEqual([
      'requested',
      'matching',
      'offered',
      'matching',
      'offered',
      'matched',
      'en_route',
    ]);
  });

  it('janitor leaves live claims and live trips alone', async () => {
    await seedDrivers(2);
    const claims = new ClaimStore(redis);
    await claims.claimDriver('d0', 'trip-x', Date.now(), 10_000, 60_000);
    const janitor = new Janitor({ redis, pool });
    expect(await janitor.sweepOnce()).toBe(0);
    expect(await redis.hget(driverKey('d0'), 'status')).toBe('claimed');
    expect(await redis.exists(claimKey('d0'))).toBe(1);
  });

  it('C1: a 23505 conflict reverts the trip and the cascade matches the next candidate (no strand)', async () => {
    // d0 is available in Redis but ALREADY holds an active trip in Postgres —
    // exactly "Redis let a double-claim through". d0 sits on the rider so it is
    // offered first; d1 is a touch farther, so the cascade falls to it.
    await geo.applyPings(
      [
        { driverId: 'd0', ...CENTER },
        { driverId: 'd1', lat: CENTER.lat + 0.001, lng: CENTER.lng },
      ],
      Date.now(),
    );
    // The pre-existing active trip that pins d0 in the partial-unique index.
    await pool.query(`INSERT INTO ride_requests (id, lat, lng, status) VALUES ('r0', $1, $2, 'matched')`, [
      CENTER.lat,
      CENTER.lng,
    ]);
    await pool.query(
      `INSERT INTO trips (id, request_id, driver_id, rider_lat, rider_lng, status, claim_token)
       VALUES ('trip-a', 'r0', 'd0', $1, $2, 'matched', 'tok')`,
      [CENTER.lat, CENTER.lng],
    );
    await insertRequest('r1');

    const core = newCore();
    expect(await core.matchRequest('r1')).toBe('matched');

    // The 23505 fired exactly once, and the cascade recovered onto d1.
    expect(core.metrics.pgUniqueViolationsTotal).toBe(1);
    const trip = await pool.query<{ id: string; driver_id: string; status: string }>(
      `SELECT id, driver_id, status FROM trips WHERE request_id = 'r1'`,
    );
    expect(trip.rows[0]).toMatchObject({ driver_id: 'd1', status: 'en_route' });
    expect((await pool.query(`SELECT status FROM ride_requests WHERE id = 'r1'`)).rows[0]).toEqual({
      status: 'matched',
    });

    // Neither driver stranded: d0 back available (its own trip untouched), d1 on_trip.
    expect(await redis.hget(driverKey('d0'), 'status')).toBe('available');
    expect(await redis.hget(driverKey('d1'), 'status')).toBe('on_trip');
    expect((await pool.query(`SELECT status FROM trips WHERE id = 'trip-a'`)).rows[0]).toEqual({
      status: 'matched',
    });
    expect(await redis.keys('claim:*')).toEqual([]);
    expect(await redis.zcard(CLAIMS_BY_EXPIRY)).toBe(0);

    // The outbox shows the conflict revert (offered→matching) then the re-offer.
    expect(await eventChain(trip.rows[0]!.id)).toEqual([
      'requested',
      'matching',
      'offered', // d0 offered
      'matching', // 23505 → reverted
      'offered', // d1 offered
      'matched',
      'en_route',
    ]);
  });

  it('C1/O2 backstop: a request stuck matching with an offered trip and no live claim is recovered by the reaper', async () => {
    await seedDrivers(1);
    // The residual state: request 'matching' (past the reaper grace), trip
    // 'offered', but no live claim (the driver was already repaired). Backdate
    // created_at so the reaper's grace window is satisfied without waiting.
    await pool.query(
      `INSERT INTO ride_requests (id, lat, lng, dest_lat, dest_lng, status, created_at)
       VALUES ('r1', $1, $2, $3, $4, 'matching', now() - interval '60 seconds')`,
      [CENTER.lat, CENTER.lng, DEST.lat, DEST.lng],
    );
    const claims = new ClaimStore(redis);
    const trips = new TripStore(pool);
    const token = await claims.claimDriver('d0', 'trip-b', Date.now(), 10_000, 4_000);
    await trips.offerTrip({
      tripId: 'trip-b',
      requestId: 'r1',
      driverId: 'd0',
      offerId: 'off-1',
      claimToken: token!,
      rider: CENTER,
    });
    // Erase the claim and repair the driver by hand — trip stays orphaned 'offered'.
    await redis.del(claimKey('d0'));
    await redis.zrem(CLAIMS_BY_EXPIRY, 'd0');
    await redis.hset(driverKey('d0'), { status: 'available' });
    await redis.sadd(cellKey(C0), 'd0');

    const core = newCore();
    expect(await core.recoverProcessing()).toBeGreaterThanOrEqual(1);

    // Reaper reverted the trip and handed the request back, re-enqueued.
    expect((await pool.query(`SELECT status FROM trips WHERE id = 'trip-b'`)).rows[0]).toEqual({
      status: 'matching',
    });
    expect((await pool.query(`SELECT status FROM ride_requests WHERE id = 'r1'`)).rows[0]).toEqual({
      status: 'pending',
    });
    expect(await redis.lrange(REQUESTS_QUEUE, 0, -1)).toContain('r1');

    // A live matcher finishes the re-cascade on the SAME trip row.
    expect(await core.matchRequest('r1')).toBe('matched');
    expect((await pool.query(`SELECT status, driver_id FROM trips WHERE id = 'trip-b'`)).rows[0]).toEqual({
      status: 'en_route',
      driver_id: 'd0',
    });
  });

  it('O2: janitor gone-branch reverts the orphaned offered trip and re-enqueues (past-grace strand)', async () => {
    await seedDrivers(1);
    await insertRequest('r1');
    const claims = new ClaimStore(redis);
    const trips = new TripStore(pool);
    await pool.query(`UPDATE ride_requests SET status = 'matching' WHERE id = 'r1'`);
    const token = await claims.claimDriver('d0', 'trip-b', Date.now(), 10_000, 500);
    await trips.offerTrip({
      tripId: 'trip-b',
      requestId: 'r1',
      driverId: 'd0',
      offerId: 'off-1',
      claimToken: token!,
      rider: CENTER,
    });
    // Past-grace: the PX net erased the claim VALUE; the ZSET member and the
    // stuck-'claimed' driver survive — the 'gone' state.
    await redis.del(claimKey('d0'));

    // Sweep with a clock past the claim's expiry so it is on the janitor's worklist.
    const janitor = new Janitor({ redis, pool, now: () => Date.now() + 10_000 });
    await janitor.sweepOnce();

    // The gone branch repaired the driver AND reverted the orphaned trip + re-enqueued.
    expect(await redis.hget(driverKey('d0'), 'status')).toBe('available');
    expect((await pool.query(`SELECT status FROM trips WHERE id = 'trip-b'`)).rows[0]).toEqual({
      status: 'matching',
    });
    expect((await pool.query(`SELECT status FROM ride_requests WHERE id = 'r1'`)).rows[0]).toEqual({
      status: 'pending',
    });
    expect(await redis.lrange(REQUESTS_QUEUE, 0, -1)).toContain('r1');
    expect(janitor.metrics.janitorGoneTotal).toBe(1);
    expect(janitor.metrics.janitorRequeuedTotal).toBe(1);
  });

  it('C2: two instances, trip_done consumed before arrived_pickup — driver ends freed, trip completed', async () => {
    // Stage a live en_route trip for d0 (as if just matched), d0 on_trip.
    await seedDrivers(1);
    await pool.query(
      `INSERT INTO ride_requests (id, lat, lng, dest_lat, dest_lng, status) VALUES ('r1', $1, $2, $3, $4, 'matched')`,
      [CENTER.lat, CENTER.lng, DEST.lat, DEST.lng],
    );
    await pool.query(
      `INSERT INTO trips (id, request_id, driver_id, rider_lat, rider_lng, status, claim_token, offer_id)
       VALUES ('trip-1', 'r1', 'd0', $1, $2, 'en_route', 'tok', 'off-1')`,
      [CENTER.lat, CENTER.lng],
    );
    await redis.hset(driverKey('d0'), { status: 'on_trip' });
    await redis.srem(cellKey(C0), 'd0');

    // Two matcher instances sharing the stores — two trip-progress consumers.
    const redisB = new Redis(redisContainer.getConnectionUrl());
    const poolB = createPool(pgContainer.getConnectionUri());
    const a = new MatcherCore({ redis, pool });
    const b = new MatcherCore({ redis: redisB, pool: poolB });
    try {
      await a.start(1);
      await b.start(1);

      // Force the reorder: trip_done lands FIRST, while the trip is still en_route.
      await redis.lpush(TRIP_EVENTS_QUEUE, JSON.stringify({ tripId: 'trip-1', driverId: 'd0', event: 'trip_done' }));
      await waitFor(() => a.metrics.tripEventPrematureTotal + b.metrics.tripEventPrematureTotal >= 1);

      // Now the predecessor arrives; the requeued trip_done retries and applies.
      await redis.lpush(
        TRIP_EVENTS_QUEUE,
        JSON.stringify({ tripId: 'trip-1', driverId: 'd0', event: 'arrived_pickup' }),
      );

      await waitFor(
        async () => (await pool.query(`SELECT status FROM trips WHERE id = 'trip-1'`)).rows[0]?.status === 'completed',
      );
      await waitFor(async () => (await redis.hget(driverKey('d0'), 'status')) === 'available');

      // The trip completed and the driver is back in its cell — nothing stranded.
      // (The trip was staged directly at en_route, so the outbox records only
      // the two progress hops applied here.)
      expect(await redis.smembers(cellKey(C0))).toEqual(['d0']);
      const chain = await eventChain('trip-1');
      expect(chain.slice(-2)).toEqual(['in_trip', 'completed']);
      expect(a.metrics.tripEventPrematureTotal + b.metrics.tripEventPrematureTotal).toBeGreaterThanOrEqual(1);
    } finally {
      await a.stop();
      await b.stop();
      await poolB.end();
      redisB.disconnect();
    }
  });
});
