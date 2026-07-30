import type { Rng } from './rng.js';
import {
  clampToCity,
  distance,
  randomIntersection,
  type City,
  type Point,
} from './world.js';

export interface DriverAgent {
  id: string;
  pos: Point;
  waypoint: Point;
  speedMps: number;
}

const MIN_SPEED_MPS = 5; // 18 km/h
const MAX_SPEED_MPS = 15; // 54 km/h

export function createAgents(city: City, count: number, rng: Rng): DriverAgent[] {
  return Array.from({ length: count }, (_, i) => {
    const pos = randomIntersection(city, rng);
    return {
      id: `d${i}`,
      pos,
      waypoint: pickWaypoint(city, rng, pos),
      speedMps: rng.range(MIN_SPEED_MPS, MAX_SPEED_MPS),
    };
  });
}

function pickWaypoint(city: City, rng: Rng, notAt: Point): Point {
  for (;;) {
    const wp = randomIntersection(city, rng);
    if (wp.x !== notAt.x || wp.y !== notAt.y) return wp;
  }
}

/**
 * Advance one tick: move straight toward the waypoint at the agent's speed;
 * on arrival, snap to it and pick a new one. Positions stay in bounds because
 * both endpoints are in-bounds grid intersections.
 */
export function tickAgent(city: City, agent: DriverAgent, rng: Rng, dtSec: number): void {
  const d = distance(agent.pos, agent.waypoint);
  const step = agent.speedMps * dtSec;
  if (d <= step) {
    agent.pos = { ...agent.waypoint };
    agent.waypoint = pickWaypoint(city, rng, agent.pos);
  } else {
    const f = step / d;
    agent.pos = {
      x: agent.pos.x + (agent.waypoint.x - agent.pos.x) * f,
      y: agent.pos.y + (agent.waypoint.y - agent.pos.y) * f,
    };
  }
}

/** Reported GPS position: true position + gaussian noise, clamped to the city. */
export function noisyPing(city: City, agent: DriverAgent, rng: Rng, stdDevM = 10): Point {
  return clampToCity(city, {
    x: agent.pos.x + rng.gaussian(0, stdDevM),
    y: agent.pos.y + rng.gaussian(0, stdDevM),
  });
}
