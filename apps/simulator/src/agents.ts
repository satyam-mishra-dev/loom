import type { Rng } from './rng.js';
import { clampToCity, distance, randomIntersection, type City, type Point } from './world.js';

/** roaming: random waypoints; to_pickup/to_dest: driving an assigned trip's legs. */
export type AgentMode = 'roaming' | 'to_pickup' | 'to_dest';

export interface ActiveTrip {
  tripId: string;
  pickup: Point;
  dest: Point;
}

export interface DriverAgent {
  id: string;
  pos: Point;
  waypoint: Point;
  speedMps: number;
  mode: AgentMode;
  trip: ActiveTrip | null;
}

/** Milestone reached this tick while driving a trip leg. */
export interface TripArrival {
  tripId: string;
  event: 'arrived_pickup' | 'trip_done';
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
      mode: 'roaming' as const,
      trip: null,
    };
  });
}

function pickWaypoint(city: City, rng: Rng, notAt: Point): Point {
  for (;;) {
    const wp = randomIntersection(city, rng);
    if (wp.x !== notAt.x || wp.y !== notAt.y) return wp;
  }
}

/** Put the agent on a trip: drive to pickup, then dest (waypoints override roaming). */
export function assignTrip(agent: DriverAgent, trip: ActiveTrip): void {
  agent.trip = trip;
  agent.mode = 'to_pickup';
  agent.waypoint = { ...trip.pickup };
}

/**
 * Advance one tick: move straight toward the waypoint at the agent's speed.
 * Roaming arrival picks a fresh random waypoint; trip-leg arrival returns the
 * milestone (arrived_pickup → head for dest; trip_done → back to roaming) so
 * the simulation can emit it. Positions stay in bounds because every
 * waypoint is an in-bounds point.
 */
export function tickAgent(
  city: City,
  agent: DriverAgent,
  rng: Rng,
  dtSec: number,
): TripArrival | null {
  const d = distance(agent.pos, agent.waypoint);
  const step = agent.speedMps * dtSec;
  if (d > step) {
    const f = step / d;
    agent.pos = {
      x: agent.pos.x + (agent.waypoint.x - agent.pos.x) * f,
      y: agent.pos.y + (agent.waypoint.y - agent.pos.y) * f,
    };
    return null;
  }
  // Arrived.
  agent.pos = { ...agent.waypoint };
  if (agent.mode === 'to_pickup' && agent.trip !== null) {
    agent.mode = 'to_dest';
    agent.waypoint = { ...agent.trip.dest };
    return { tripId: agent.trip.tripId, event: 'arrived_pickup' };
  }
  if (agent.mode === 'to_dest' && agent.trip !== null) {
    const tripId = agent.trip.tripId;
    agent.trip = null;
    agent.mode = 'roaming';
    agent.waypoint = pickWaypoint(city, rng, agent.pos);
    return { tripId, event: 'trip_done' };
  }
  agent.waypoint = pickWaypoint(city, rng, agent.pos);
  return null;
}

/** Reported GPS position: true position + gaussian noise, clamped to the city. */
export function noisyPing(city: City, agent: DriverAgent, rng: Rng, stdDevM = 10): Point {
  return clampToCity(city, {
    x: agent.pos.x + rng.gaussian(0, stdDevM),
    y: agent.pos.y + rng.gaussian(0, stdDevM),
  });
}
