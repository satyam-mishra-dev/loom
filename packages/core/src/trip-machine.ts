/**
 * The trip state machine — the single authority on what a trip may do next.
 *
 *   requested → matching → offered → matched → en_route → in_trip → completed
 *                  ↑  ↓        |
 *                  └──┘        └ (decline/timeout re-enters matching for the
 *               cancelled        next cascade attempt; matching → cancelled
 *                                when the cascade exhausts honestly)
 *
 * A state is a discriminated union carrying exactly the data that state is
 * entitled to: `offered` knows its driver AND offer, everything from `matched`
 * on knows only its driver. Guards are therefore structural — en_route can
 * only be produced from a matched state, and a matched state cannot exist
 * without a driverId, so "EN_ROUTE requires a driver" is enforced by the type
 * system, not by a runtime null check.
 *
 * `transition` is total: every (state, event) pair either returns the next
 * state or throws IllegalTransitionError. Persistence (trips.status +
 * ride_requests.status + the trip_events outbox) lives in the matcher's trip
 * store; this module never touches I/O.
 */

export type TripStatus =
  | 'requested'
  | 'matching'
  | 'offered'
  | 'matched'
  | 'en_route'
  | 'in_trip'
  | 'completed'
  | 'cancelled';

export type TripState =
  | { status: 'requested' }
  | { status: 'matching' }
  | { status: 'offered'; driverId: string; offerId: string }
  | { status: 'matched'; driverId: string }
  | { status: 'en_route'; driverId: string }
  | { status: 'in_trip'; driverId: string }
  | { status: 'completed'; driverId: string }
  | { status: 'cancelled'; reason: 'unmatched' | 'rider_cancelled' | 'abandoned' };

export type TripEvent =
  | { type: 'MATCHING_STARTED' }
  | { type: 'OFFER_SENT'; driverId: string; offerId: string }
  | { type: 'OFFER_ACCEPTED' }
  | { type: 'OFFER_DECLINED' }
  | { type: 'OFFER_TIMED_OUT' }
  | { type: 'DRIVER_EN_ROUTE' }
  | { type: 'ARRIVED_PICKUP' }
  | { type: 'TRIP_DONE' }
  | { type: 'UNMATCHED' }
  | { type: 'RIDER_CANCELLED' }
  | { type: 'TRIP_ABANDONED' };

export class IllegalTransitionError extends Error {
  constructor(
    readonly from: TripStatus,
    readonly event: TripEvent['type'],
  ) {
    super(`illegal trip transition: ${event} in state ${from}`);
    this.name = 'IllegalTransitionError';
  }
}

/** Every trip begins life as a bare request. */
export function initialTripState(): TripState {
  return { status: 'requested' };
}

/** Pure transition function. Returns the next state or throws IllegalTransitionError. */
export function transition(state: TripState, event: TripEvent): TripState {
  // The rider may cancel from any non-terminal state (before a match, mid-offer,
  // or mid-trip); the two terminal states refuse it. Handled up front so it
  // applies uniformly without threading a case through every status below.
  if (event.type === 'RIDER_CANCELLED') {
    if (state.status === 'completed' || state.status === 'cancelled') {
      throw new IllegalTransitionError(state.status, event.type);
    }
    return { status: 'cancelled', reason: 'rider_cancelled' };
  }
  // The janitor abandons a trip whose driver has gone silent mid-ride. Legal
  // only from the driver-committed driving states — a bound driver exists to
  // take out of service. Earlier states have no committed driver (the claim
  // janitor and cascade reclaim those), so abandonment there is illegal.
  if (event.type === 'TRIP_ABANDONED') {
    if (state.status === 'matched' || state.status === 'en_route' || state.status === 'in_trip') {
      return { status: 'cancelled', reason: 'abandoned' };
    }
    throw new IllegalTransitionError(state.status, event.type);
  }
  switch (state.status) {
    case 'requested':
      if (event.type === 'MATCHING_STARTED') return { status: 'matching' };
      break;
    case 'matching':
      if (event.type === 'OFFER_SENT') {
        return { status: 'offered', driverId: event.driverId, offerId: event.offerId };
      }
      if (event.type === 'UNMATCHED') return { status: 'cancelled', reason: 'unmatched' };
      break;
    case 'offered':
      if (event.type === 'OFFER_ACCEPTED') return { status: 'matched', driverId: state.driverId };
      if (event.type === 'OFFER_DECLINED' || event.type === 'OFFER_TIMED_OUT') {
        return { status: 'matching' };
      }
      break;
    case 'matched':
      if (event.type === 'DRIVER_EN_ROUTE') return { status: 'en_route', driverId: state.driverId };
      break;
    case 'en_route':
      if (event.type === 'ARRIVED_PICKUP') return { status: 'in_trip', driverId: state.driverId };
      break;
    case 'in_trip':
      if (event.type === 'TRIP_DONE') return { status: 'completed', driverId: state.driverId };
      break;
    case 'completed':
    case 'cancelled':
      break; // terminal — nothing is legal
  }
  throw new IllegalTransitionError(state.status, event.type);
}
