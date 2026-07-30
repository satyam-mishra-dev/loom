import { PostgreSqlContainer } from '@testcontainers/postgresql';
import { RedisContainer } from '@testcontainers/redis';
import { Redis } from 'ioredis';
import { describe, expect, it } from 'vitest';
import { createPool } from '../src/index.js';

// Phase-0 gate: real Postgres and Redis via Testcontainers, no mocks.
// Trip schema migrations land in a later phase and will extend this suite.
describe('infrastructure (testcontainers)', () => {
  it('postgres 16 accepts connections and runs queries', async () => {
    const container = await new PostgreSqlContainer('postgres:16-alpine').start();
    try {
      const pool = createPool(container.getConnectionUri());
      const res = await pool.query<{ one: number }>('SELECT 1 AS one');
      expect(res.rows[0]?.one).toBe(1);
      await pool.end();
    } finally {
      await container.stop();
    }
  }, 120_000);

  it('redis 7 responds to PING', async () => {
    const container = await new RedisContainer('redis:7-alpine').start();
    try {
      const redis = new Redis(container.getConnectionUrl());
      expect(await redis.ping()).toBe('PONG');
      redis.disconnect();
    } finally {
      await container.stop();
    }
  }, 120_000);
});
