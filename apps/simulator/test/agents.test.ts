import { describe, expect, it } from 'vitest';
import { createAgents, noisyPing, tickAgent } from '../src/agents.js';
import { createRng } from '../src/rng.js';
import { DEFAULT_CITY, distance, inBounds } from '../src/world.js';

describe('driver agents', () => {
  it('true positions and noisy pings stay within city bounds over a long run', () => {
    const rng = createRng(7);
    const agents = createAgents(DEFAULT_CITY, 50, rng);
    let violations = 0;
    for (let t = 0; t < 2000; t++) {
      for (const agent of agents) {
        tickAgent(DEFAULT_CITY, agent, rng, 1);
        if (!inBounds(DEFAULT_CITY, agent.pos)) violations++;
        if (!inBounds(DEFAULT_CITY, noisyPing(DEFAULT_CITY, agent, rng))) violations++;
      }
    }
    expect(violations).toBe(0);
  });

  it('every tick strictly reduces distance to the current waypoint', () => {
    const rng = createRng(11);
    const agent = createAgents(DEFAULT_CITY, 1, rng)[0]!;
    for (let t = 0; t < 500; t++) {
      const waypoint = { ...agent.waypoint };
      const before = distance(agent.pos, waypoint);
      tickAgent(DEFAULT_CITY, agent, rng, 1);
      // On arrival pos === old waypoint, so distance drops to 0 — still strict.
      expect(distance(agent.pos, waypoint)).toBeLessThan(before);
    }
  });

  it('agents arrive and pick fresh waypoints (movement never stalls)', () => {
    const rng = createRng(13);
    const agent = createAgents(DEFAULT_CITY, 1, rng)[0]!;
    const seen = new Set<string>();
    for (let t = 0; t < 5000; t++) {
      tickAgent(DEFAULT_CITY, agent, rng, 1);
      seen.add(`${agent.waypoint.x},${agent.waypoint.y}`);
    }
    expect(seen.size).toBeGreaterThan(3);
  });
});
