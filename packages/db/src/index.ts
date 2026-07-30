import pg from 'pg';

/** Connection pool over node-postgres. Raw SQL on hot paths is deliberate — see project brief. */
export function createPool(connectionString: string): pg.Pool {
  return new pg.Pool({ connectionString });
}
