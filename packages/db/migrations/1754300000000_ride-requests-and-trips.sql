-- Up Migration

-- Phase C schema: ride intake + matched trips (v1 goes request -> ATOMIC CLAIM
-- -> trip 'matched'; the offer cascade and trip state machine extend these
-- tables in phase D without altering them).

CREATE TABLE ride_requests (
  id text PRIMARY KEY,
  lat double precision NOT NULL,
  lng double precision NOT NULL,
  -- pending: persisted by the gateway, waiting in requests:queue.
  -- matching: a matcher consumer owns it (the pending->matching UPDATE is the
  --           idempotency guard that makes at-least-once delivery safe).
  -- matched/unmatched: terminal for v1.
  status text NOT NULL DEFAULT 'pending'
    CHECK (status IN ('pending', 'matching', 'matched', 'unmatched')),
  matched_trip_id text,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE trips (
  id text PRIMARY KEY,
  request_id text NOT NULL UNIQUE REFERENCES ride_requests (id),
  driver_id text NOT NULL,
  rider_lat double precision NOT NULL,
  rider_lng double precision NOT NULL,
  -- v1 inserts trips directly as 'matched'; en_route/in_trip/completed/
  -- cancelled arrive with the trip state machine (phase D).
  status text NOT NULL DEFAULT 'matched'
    CHECK (status IN ('matched', 'en_route', 'in_trip', 'completed', 'cancelled')),
  -- The Redis claim token that won this driver — kept for audit and so phase D
  -- can correlate offers/claims with the trip they produced.
  claim_token text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

ALTER TABLE ride_requests
  ADD CONSTRAINT ride_requests_matched_trip_fk
  FOREIGN KEY (matched_trip_id) REFERENCES trips (id);

-- THE defense-in-depth index: at most one live trip per driver, ever.
--
-- Redis prevents the race; Postgres makes the invariant unviolable. The atomic
-- claim (packages/core/src/claim.ts) serializes contention where it actually
-- happens — N concurrent matchers race on one Lua script and exactly one SREM
-- wins, at memory speed, before any SQL runs. But Redis is operational state,
-- not truth: it can be FLUSHALLed, restored from a stale snapshot, or brought
-- up empty and rebuilt from live pings (which is a feature — see the geo-index
-- self-heal test). If the race-prevention layer is ever wrong, this partial
-- unique index rejects the second INSERT and the bug becomes a counted,
-- logged 23505 instead of two cars arriving for two different riders. Neither
-- store can do the other's job: Postgres alone would put every claim attempt
-- through SQL round-trips and row locks on the hot path; Redis alone would
-- make a cache wipe capable of violating the business invariant.
CREATE UNIQUE INDEX trips_one_active_per_driver
  ON trips (driver_id)
  WHERE status IN ('matched', 'en_route', 'in_trip');

-- Down Migration

ALTER TABLE ride_requests DROP CONSTRAINT ride_requests_matched_trip_fk;
DROP TABLE trips;
DROP TABLE ride_requests;
