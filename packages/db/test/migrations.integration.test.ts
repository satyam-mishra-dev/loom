import { PostgreSqlContainer, type StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import pg from 'pg';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { createPool, runMigrations } from '../src/index.js';

// The schema itself under test — especially THE partial unique index: it is
// the defense-in-depth layer, so it gets its own proof independent of the
// matcher (which is built never to trigger it).
describe('migrations (testcontainers postgres)', () => {
  let container: StartedPostgreSqlContainer;
  let pool: pg.Pool;

  beforeAll(async () => {
    container = await new PostgreSqlContainer('postgres:16-alpine').start();
    await runMigrations(container.getConnectionUri());
    pool = createPool(container.getConnectionUri());
  }, 120_000);

  afterAll(async () => {
    await pool.end();
    await container.stop();
  });

  beforeEach(async () => {
    await pool.query('TRUNCATE ride_requests, trips CASCADE');
  });

  async function insertRequest(id: string): Promise<void> {
    await pool.query(`INSERT INTO ride_requests (id, lat, lng) VALUES ($1, 37.77, -122.42)`, [id]);
  }

  async function insertTrip(id: string, requestId: string, driverId: string, status = 'matched'): Promise<void> {
    await pool.query(
      `INSERT INTO trips (id, request_id, driver_id, rider_lat, rider_lng, status, claim_token)
       VALUES ($1, $2, $3, 37.77, -122.42, $4, 'tok')`,
      [id, requestId, driverId, status],
    );
  }

  it('applies idempotently and creates both tables with expected defaults', async () => {
    await runMigrations(container.getConnectionUri()); // second run is a no-op
    await insertRequest('r1');
    const req = await pool.query(`SELECT status, matched_trip_id FROM ride_requests WHERE id = 'r1'`);
    expect(req.rows[0]).toEqual({ status: 'pending', matched_trip_id: null });

    await insertTrip('t1', 'r1', 'd1');
    const trip = await pool.query(`SELECT status FROM trips WHERE id = 't1'`);
    expect(trip.rows[0]).toEqual({ status: 'matched' });
  });

  it('partial unique index: a driver cannot hold two live trips', async () => {
    await insertRequest('r1');
    await insertRequest('r2');
    await insertTrip('t1', 'r1', 'd1');

    await expect(insertTrip('t2', 'r2', 'd1')).rejects.toMatchObject({
      code: '23505',
      constraint: 'trips_one_active_per_driver',
    });
    // en_route / in_trip are also "live" — the same driver is still blocked.
    await pool.query(`UPDATE trips SET status = 'en_route' WHERE id = 't1'`);
    await expect(insertTrip('t2', 'r2', 'd1')).rejects.toMatchObject({ code: '23505' });
  });

  it('partial unique index: a completed or cancelled trip frees the driver', async () => {
    await insertRequest('r1');
    await insertRequest('r2');
    await insertTrip('t1', 'r1', 'd1');
    await pool.query(`UPDATE trips SET status = 'completed' WHERE id = 't1'`);
    await insertTrip('t2', 'r2', 'd1'); // does not throw
    const count = await pool.query(`SELECT count(*)::int AS n FROM trips WHERE driver_id = 'd1'`);
    expect(count.rows[0]).toEqual({ n: 2 });
  });

  it('one trip per request: request_id is unique and status values are checked', async () => {
    await insertRequest('r1');
    await insertTrip('t1', 'r1', 'd1');
    await expect(insertTrip('t2', 'r1', 'd2')).rejects.toMatchObject({ code: '23505' });
    await expect(insertTrip('t3', 'r1', 'd3', 'teleporting')).rejects.toMatchObject({ code: '23514' });
    await expect(
      pool.query(`UPDATE ride_requests SET status = 'nonsense' WHERE id = 'r1'`),
    ).rejects.toMatchObject({ code: '23514' });
  });
});
