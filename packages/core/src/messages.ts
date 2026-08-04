/** Wire messages shared by the simulator (producer) and gateway (consumer). */

export interface DriverPing {
  type: 'driver_ping';
  driverId: string;
  lat: number;
  lng: number;
  /** Simulated milliseconds since simulation start (deterministic under a fixed seed). */
  ts: number;
  /** Driver-reported status — simulator realism; the index derives truth from claims. */
  status?: 'available' | 'en_route' | 'in_trip';
}

export interface RideRequest {
  type: 'ride_request';
  requestId: string;
  lat: number;
  lng: number;
  destLat: number;
  destLng: number;
  ts: number;
}

/** Driver's answer to an offer, sent over its WS. */
export interface OfferReply {
  type: 'offer_reply';
  offerId: string;
  driverId: string;
  accept: boolean;
}

/** Driver-reported trip milestones, driven by its simulated movement. */
export interface TripProgress {
  type: 'trip_progress';
  tripId: string;
  driverId: string;
  event: 'arrived_pickup' | 'trip_done';
  ts: number;
}

export type SimMessage = DriverPing | RideRequest | OfferReply | TripProgress;

/** Matcher → driver: an offer with a deadline. TTL is in the payload — the driver sees it too. */
export interface OfferMessage {
  type: 'offer';
  offerId: string;
  tripId: string;
  driverId: string;
  pickup: { lat: number; lng: number };
  /** Displayed fare for the trip, surge already applied (see quoteFare). */
  price: number;
  /** The area's current surge multiplier that produced `price` (1.0 = no surge). */
  surge: number;
  /** Epoch ms; replies after this are ignored (the reply list just expires). */
  expiresAt: number;
}

/** Matcher → driver after a confirmed accept: drive to pickup, then dest. */
export interface TripAssigned {
  type: 'trip_assigned';
  tripId: string;
  driverId: string;
  pickup: { lat: number; lng: number };
  dest: { lat: number; lng: number };
}

export type ServerMessage = OfferMessage | TripAssigned;

/**
 * Request intake queue (gateway → matcher), Redis lists. The gateway LPUSHes
 * ride-request ids after persisting the row; matcher consumers BLMOVE from
 * the RIGHT into the processing list (FIFO, at-least-once).
 */
export const REQUESTS_QUEUE = 'requests:queue';
export const REQUESTS_PROCESSING = 'requests:processing';

/**
 * Offer transport (matcher ↔ gateway ↔ driver) — why pub/sub one way and a
 * list the other:
 *
 * DOWNSTREAM (matcher → driver) is Redis pub/sub on `driver:{id}:msg`. The
 * matcher has no idea which gateway instance holds a driver's socket; each
 * gateway SUBSCRIBEs to the channels of exactly the drivers connected to it
 * (subscribe at socket auth / first sighting, unsubscribe on disconnect), so
 * a PUBLISH fans out to precisely the driver's home gateway — zero routing
 * state anywhere else. Pub/sub's fire-and-forget semantics are the right
 * semantics here: a message for a disconnected driver SHOULD evaporate,
 * because the matcher already carries the recovery path (offer timeout →
 * release → next candidate).
 *
 * UPSTREAM (driver's reply → matcher) is a Redis LIST `offer:reply:{offerId}`
 * that the gateway LPUSHes and the awaiting matcher BLPOPs with a timeout.
 * Exactly one consumer is waiting and the reply must NOT evaporate if it
 * arrives a beat before the matcher blocks — a list buffers that race,
 * pub/sub would drop it. The list also gives the timeout for free: BLPOP's
 * deadline IS the offer TTL. Reply keys are PEXPIREd so unclaimed replies GC
 * themselves.
 */
export function driverChannel(driverId: string): string {
  return `driver:${driverId}:msg`;
}

export function offerReplyKey(offerId: string): string {
  return `offer:reply:${offerId}`;
}

/** How long an unconsumed offer reply may linger before Redis erases it. */
export const OFFER_REPLY_TTL_MS = 60_000;

/**
 * Trip progress queue (gateway → matcher), same list pattern as request
 * intake: LPUSH on arrival, BLMOVE into processing, LREM to ack. Duplicate
 * or stale deliveries are collapsed by the trip machine's guards.
 */
export const TRIP_EVENTS_QUEUE = 'trip:events';
export const TRIP_EVENTS_PROCESSING = 'trip:events:processing';

/**
 * Rider-cancellation queue (gateway → matcher), same LPUSH/BLMOVE/LREM list
 * pattern as request intake. The gateway enqueues the cancelled requestId; the
 * matcher runs the race-safe cancel against the trip machine + claim. cancelRide
 * is idempotent, so a duplicate delivery is a harmless no-op.
 */
export const CANCEL_QUEUE = 'cancel:queue';
export const CANCEL_PROCESSING = 'cancel:processing';
