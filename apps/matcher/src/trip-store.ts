import pg from 'pg';
import {
  IllegalTransitionError,
  initialTripState,
  transition,
  type TripEvent,
  type TripState,
} from '@fleetline/core';

/**
 * Persistence for the trip machine. Every method is ONE Postgres transaction
 * that (a) locks the trip row, (b) runs the pure machine to prove the edge is
 * legal, (c) writes the new status, and (d) INSERTs the outbox row into
 * trip_events — so a transition and its event commit or vanish together
 * (transactional outbox). Concurrency guards are the row lock plus explicit
 * status/offer_id checks: a caller that lost ownership (janitor raced it, a
 * peer stole the request) gets a false/'lost' back, never a partial write.
 */

export type AcceptResult = 'matched' | 'conflict' | 'lost';

interface TripRow {
  id: string;
  status: string;
  driver_id: string;
  offer_id: string | null;
  request_id: string;
}

/** Rehydrate the machine state from a trips row. */
function rowState(row: TripRow): TripState {
  switch (row.status) {
    case 'matching':
      return { status: 'matching' };
    case 'offered':
      return { status: 'offered', driverId: row.driver_id, offerId: row.offer_id ?? '' };
    case 'matched':
    case 'en_route':
    case 'in_trip':
    case 'completed':
      return { status: row.status, driverId: row.driver_id };
    case 'cancelled':
      return { status: 'cancelled', reason: 'unmatched' };
    default:
      throw new Error(`unknown trip status in row: ${row.status}`);
  }
}

export class TripStore {
  constructor(private readonly pool: pg.Pool) {}

  private async inTx<T>(fn: (client: pg.PoolClient) => Promise<T>): Promise<T> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const result = await fn(client);
      await client.query('COMMIT');
      return result;
    } catch (err) {
      await client.query('ROLLBACK');
      throw err;
    } finally {
      client.release();
    }
  }

  private async lockTrip(client: pg.PoolClient, tripId: string): Promise<TripRow | null> {
    const res = await client.query<TripRow>(
      'SELECT id, status, driver_id, offer_id, request_id FROM trips WHERE id = $1 FOR UPDATE',
      [tripId],
    );
    return res.rows[0] ?? null;
  }

  private async insertEvent(
    client: pg.PoolClient,
    tripId: string,
    type: string,
    payload: Record<string, unknown>,
  ): Promise<void> {
    await client.query('INSERT INTO trip_events (trip_id, type, payload) VALUES ($1, $2, $3)', [
      tripId,
      type,
      JSON.stringify(payload),
    ]);
  }

  /**
   * Put the trip in front of a claimed driver. First offer INSERTs the row
   * (born 'offered', with outbox rows for the requested→matching→offered
   * provenance the row itself skips); re-offers run matching→offered on the
   * existing row. Returns false, nothing written, if the row exists but is
   * not ours to offer (janitor or a peer owns it now) or if the INSERT lost a
   * request_id race.
   */
  async offerTrip(args: {
    tripId: string;
    requestId: string;
    driverId: string;
    offerId: string;
    claimToken: string;
    rider: { lat: number; lng: number };
  }): Promise<boolean> {
    try {
      return await this.inTx(async (client) => {
        const row = await this.lockTrip(client, args.tripId);
        if (row === null) {
          // Birth: drive the machine through its provenance so the outbox
          // chain starts at requested even though the row starts at offered.
          let state = initialTripState();
          state = transition(state, { type: 'MATCHING_STARTED' });
          state = transition(state, { type: 'OFFER_SENT', driverId: args.driverId, offerId: args.offerId });
          await client.query(
            `INSERT INTO trips (id, request_id, driver_id, rider_lat, rider_lng, status, claim_token, offer_id)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
            [
              args.tripId,
              args.requestId,
              args.driverId,
              args.rider.lat,
              args.rider.lng,
              state.status,
              args.claimToken,
              args.offerId,
            ],
          );
          await this.insertEvent(client, args.tripId, 'requested', { requestId: args.requestId });
          await this.insertEvent(client, args.tripId, 'matching', { event: 'MATCHING_STARTED' });
          await this.insertEvent(client, args.tripId, 'offered', {
            event: 'OFFER_SENT',
            driverId: args.driverId,
            offerId: args.offerId,
          });
          return true;
        }
        const next = transition(rowState(row), {
          type: 'OFFER_SENT',
          driverId: args.driverId,
          offerId: args.offerId,
        });
        await client.query(
          'UPDATE trips SET status = $2, driver_id = $3, offer_id = $4, claim_token = $5 WHERE id = $1',
          [args.tripId, next.status, args.driverId, args.offerId, args.claimToken],
        );
        await this.insertEvent(client, args.tripId, 'offered', {
          event: 'OFFER_SENT',
          driverId: args.driverId,
          offerId: args.offerId,
        });
        return true;
      });
    } catch (err) {
      if (err instanceof IllegalTransitionError) return false;
      if (err instanceof pg.DatabaseError && err.code === '23505') return false;
      throw err;
    }
  }

  /**
   * Driver accepted: offered → matched, request → matched, outbox row — one
   * TX. 'conflict' is the never-happens partial-unique-index rejection (the
   * driver already has an active trip); 'lost' means the offer or request is
   * no longer ours.
   */
  async acceptOffer(args: { tripId: string; offerId: string; requestId: string }): Promise<AcceptResult> {
    try {
      return await this.inTx(async (client) => {
        const row = await this.lockTrip(client, args.tripId);
        if (row === null || row.status !== 'offered' || row.offer_id !== args.offerId) {
          throw new LostOwnership();
        }
        const next = transition(rowState(row), { type: 'OFFER_ACCEPTED' });
        await client.query('UPDATE trips SET status = $2 WHERE id = $1', [args.tripId, next.status]);
        const req = await client.query(
          `UPDATE ride_requests SET status = 'matched', matched_trip_id = $2
           WHERE id = $1 AND status = 'matching'`,
          [args.requestId, args.tripId],
        );
        if (req.rowCount !== 1) throw new LostOwnership();
        await this.insertEvent(client, args.tripId, 'matched', {
          event: 'OFFER_ACCEPTED',
          driverId: row.driver_id,
          offerId: args.offerId,
        });
        return 'matched' as const;
      });
    } catch (err) {
      if (err instanceof LostOwnership || err instanceof IllegalTransitionError) return 'lost';
      if (err instanceof pg.DatabaseError && err.code === '23505') return 'conflict';
      throw err;
    }
  }

  /** After trip_assigned goes out: matched → en_route (the driver is driving to pickup). */
  async startEnRoute(tripId: string): Promise<boolean> {
    return this.applyGuarded(tripId, { type: 'DRIVER_EN_ROUTE' }, (row) => ({
      event: 'DRIVER_EN_ROUTE',
      driverId: row.driver_id,
    }));
  }

  /** Decline or timeout: offered → matching so the cascade can try the next candidate. */
  async revertOffer(
    tripId: string,
    offerId: string,
    cause: 'OFFER_DECLINED' | 'OFFER_TIMED_OUT',
    by: 'matcher' | 'janitor' = 'matcher',
  ): Promise<boolean> {
    try {
      return await this.inTx(async (client) => {
        const row = await this.lockTrip(client, tripId);
        if (row === null || row.status !== 'offered' || row.offer_id !== offerId) return false;
        const next = transition(rowState(row), { type: cause });
        await client.query('UPDATE trips SET status = $2 WHERE id = $1', [tripId, next.status]);
        await this.insertEvent(client, tripId, next.status, {
          event: cause,
          driverId: row.driver_id,
          offerId,
          by,
        });
        return true;
      });
    } catch (err) {
      if (err instanceof IllegalTransitionError) return false;
      throw err;
    }
  }

  /**
   * Cascade exhausted. Request ownership decides everything: if the
   * matching→unmatched UPDATE misses, someone else took the request over
   * (janitor re-enqueue) and the trip is theirs to drive — touch nothing.
   */
  async finishUnmatched(tripId: string, requestId: string): Promise<boolean> {
    return this.inTx(async (client) => {
      const row = await this.lockTrip(client, tripId);
      const req = await client.query(
        `UPDATE ride_requests SET status = 'unmatched' WHERE id = $1 AND status = 'matching'`,
        [requestId],
      );
      if (req.rowCount !== 1) throw new LostOwnership();
      if (row !== null && row.status === 'matching') {
        const next = transition(rowState(row), { type: 'UNMATCHED' });
        await client.query('UPDATE trips SET status = $2 WHERE id = $1', [tripId, next.status]);
        await this.insertEvent(client, tripId, next.status, { event: 'UNMATCHED' });
      }
      return true;
    }).catch((err: unknown) => {
      if (err instanceof LostOwnership) return false;
      throw err;
    });
  }

  /** Driver-reported milestones: en_route → in_trip → completed, driver-checked. */
  async progress(tripId: string, driverId: string, event: 'arrived_pickup' | 'trip_done'): Promise<boolean> {
    const machineEvent: TripEvent =
      event === 'arrived_pickup' ? { type: 'ARRIVED_PICKUP' } : { type: 'TRIP_DONE' };
    return this.applyGuarded(
      tripId,
      machineEvent,
      (row) => ({ event: machineEvent.type, driverId: row.driver_id }),
      driverId,
    );
  }

  /**
   * Janitor revert after an expired claim: offered → matching (+outbox) and
   * the request back to 'pending' so a matcher can pop it again. Also handles
   * the crash window where the dead matcher already reverted the trip but
   * died before finishing the cascade (trip 'matching', request 'matching').
   * Returns the requestId to re-enqueue, or null if there is nothing to do.
   */
  async janitorRevert(tripId: string): Promise<string | null> {
    return this.inTx(async (client) => {
      const row = await this.lockTrip(client, tripId);
      if (row === null) return null;
      const req = await client.query<{ status: string }>(
        'SELECT status FROM ride_requests WHERE id = $1 FOR UPDATE',
        [row.request_id],
      );
      const requestStatus = req.rows[0]?.status;
      if (row.status === 'offered') {
        const next = transition(rowState(row), { type: 'OFFER_TIMED_OUT' });
        await client.query('UPDATE trips SET status = $2 WHERE id = $1', [tripId, next.status]);
        await this.insertEvent(client, tripId, next.status, {
          event: 'OFFER_TIMED_OUT',
          driverId: row.driver_id,
          offerId: row.offer_id,
          by: 'janitor',
        });
        if (requestStatus === 'matching') {
          await client.query(`UPDATE ride_requests SET status = 'pending' WHERE id = $1`, [row.request_id]);
        }
        return row.request_id;
      }
      if (row.status === 'matching' && requestStatus === 'matching') {
        // Dead matcher crashed between reverting the trip and continuing the
        // cascade; nobody owns the request anymore — hand it back.
        await client.query(`UPDATE ride_requests SET status = 'pending' WHERE id = $1`, [row.request_id]);
        return row.request_id;
      }
      return null; // matched/en_route/in_trip/…: the trip is live, leave it alone
    });
  }

  /** Shared guarded single-edge apply: lock, machine-check, write status + outbox. */
  private async applyGuarded(
    tripId: string,
    event: TripEvent,
    payload: (row: TripRow) => Record<string, unknown>,
    requireDriverId?: string,
  ): Promise<boolean> {
    try {
      return await this.inTx(async (client) => {
        const row = await this.lockTrip(client, tripId);
        if (row === null) return false;
        if (requireDriverId !== undefined && row.driver_id !== requireDriverId) return false;
        const next = transition(rowState(row), event);
        await client.query('UPDATE trips SET status = $2 WHERE id = $1', [tripId, next.status]);
        await this.insertEvent(client, tripId, next.status, payload(row));
        return true;
      });
    } catch (err) {
      if (err instanceof IllegalTransitionError) return false;
      throw err;
    }
  }
}

/** Internal control-flow marker: the row we meant to own belongs to someone else now. */
class LostOwnership extends Error {
  constructor() {
    super('lost ownership');
  }
}
