import { fileURLToPath } from 'node:url';
import pg from 'pg';
import { runner } from 'node-pg-migrate';

/** Connection pool over node-postgres. Raw SQL on hot paths is deliberate — see project brief. */
export function createPool(connectionString: string): pg.Pool {
  return new pg.Pool({ connectionString });
}

const MIGRATIONS_DIR = fileURLToPath(new URL('../migrations', import.meta.url));

/**
 * Apply all pending migrations programmatically — the same files the
 * `npm run migrate` CLI uses, so tests and services share one schema source.
 */
export async function runMigrations(databaseUrl: string): Promise<void> {
  await runner({
    databaseUrl,
    dir: MIGRATIONS_DIR,
    direction: 'up',
    migrationsTable: 'pgmigrations',
    log: () => {},
  });
}
