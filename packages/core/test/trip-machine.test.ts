import { describe, expect, it } from 'vitest';
import {
  IllegalTransitionError,
  initialTripState,
  transition,
  type TripEvent,
  type TripState,
} from '../src/index.js';

// The FULL matrix: every (state, event) pair is exercised. Legal edges assert
// the exact next state; every other pair must throw IllegalTransitionError.

const OFFER: TripEvent = { type: 'OFFER_SENT', driverId: 'd1', offerId: 'o1' };

const STATES: TripState[] = [
  { status: 'requested' },
  { status: 'matching' },
  { status: 'offered', driverId: 'd1', offerId: 'o1' },
  { status: 'matched', driverId: 'd1' },
  { status: 'en_route', driverId: 'd1' },
  { status: 'in_trip', driverId: 'd1' },
  { status: 'completed', driverId: 'd1' },
  { status: 'cancelled', reason: 'unmatched' },
];

const EVENTS: TripEvent[] = [
  { type: 'MATCHING_STARTED' },
  OFFER,
  { type: 'OFFER_ACCEPTED' },
  { type: 'OFFER_DECLINED' },
  { type: 'OFFER_TIMED_OUT' },
  { type: 'DRIVER_EN_ROUTE' },
  { type: 'ARRIVED_PICKUP' },
  { type: 'TRIP_DONE' },
  { type: 'UNMATCHED' },
];

// status → event type → expected next state. Everything absent is illegal.
const LEGAL: Record<string, Partial<Record<TripEvent['type'], TripState>>> = {
  requested: { MATCHING_STARTED: { status: 'matching' } },
  matching: {
    OFFER_SENT: { status: 'offered', driverId: 'd1', offerId: 'o1' },
    UNMATCHED: { status: 'cancelled', reason: 'unmatched' },
  },
  offered: {
    OFFER_ACCEPTED: { status: 'matched', driverId: 'd1' },
    OFFER_DECLINED: { status: 'matching' },
    OFFER_TIMED_OUT: { status: 'matching' },
  },
  matched: { DRIVER_EN_ROUTE: { status: 'en_route', driverId: 'd1' } },
  en_route: { ARRIVED_PICKUP: { status: 'in_trip', driverId: 'd1' } },
  in_trip: { TRIP_DONE: { status: 'completed', driverId: 'd1' } },
  completed: {},
  cancelled: {},
};

describe('trip machine: full transition matrix', () => {
  for (const state of STATES) {
    for (const event of EVENTS) {
      const expected = LEGAL[state.status]?.[event.type];
      if (expected !== undefined) {
        it(`${state.status} + ${event.type} → ${expected.status}`, () => {
          expect(transition(state, event)).toEqual(expected);
        });
      } else {
        it(`${state.status} + ${event.type} is illegal`, () => {
          expect(() => transition(state, event)).toThrowError(IllegalTransitionError);
          try {
            transition(state, event);
          } catch (err) {
            // The error names the exact pair — that is what gets logged.
            expect(err).toMatchObject({ from: state.status, event: event.type });
          }
        });
      }
    }
  }
});

describe('trip machine: shape', () => {
  it('starts at requested', () => {
    expect(initialTripState()).toEqual({ status: 'requested' });
  });

  it('the driver flows structurally from the offer into every later state', () => {
    let s = transition(initialTripState(), { type: 'MATCHING_STARTED' });
    s = transition(s, OFFER);
    s = transition(s, { type: 'OFFER_ACCEPTED' });
    s = transition(s, { type: 'DRIVER_EN_ROUTE' });
    s = transition(s, { type: 'ARRIVED_PICKUP' });
    s = transition(s, { type: 'TRIP_DONE' });
    expect(s).toEqual({ status: 'completed', driverId: 'd1' });
  });

  it('decline re-enters matching and a fresh offer carries the NEW driver', () => {
    let s: TripState = { status: 'offered', driverId: 'd1', offerId: 'o1' };
    s = transition(s, { type: 'OFFER_DECLINED' });
    s = transition(s, { type: 'OFFER_SENT', driverId: 'd2', offerId: 'o2' });
    expect(s).toEqual({ status: 'offered', driverId: 'd2', offerId: 'o2' });
  });
});
