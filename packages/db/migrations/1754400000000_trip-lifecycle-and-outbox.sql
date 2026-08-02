-- Up Migration

-- Phase D: offer cascade + trip lifecycle.
--
-- trips gains the pre-acceptance statuses. A trip row is born 'offered' (the
-- first successful claim creates it); decline/timeout returns it to
-- 'matching' for the next cascade attempt; an exhausted cascade parks it at
-- 'cancelled'. The partial unique index deliberately keeps covering ONLY
-- matched/en_route/in_trip: an 'offered' trip is still exclusive via the
-- Redis claim, and a crash-orphaned 'offered' row must not block the driver's
-- next legitimate match while it waits for the janitor.

ALTER TABLE trips DROP CONSTRAINT trips_status_check;
ALTER TABLE trips ADD CONSTRAINT trips_status_check
  CHECK (status IN ('matching', 'offered', 'matched', 'en_route', 'in_trip', 'completed', 'cancelled'));

-- The offer currently (or last) attached to this trip — correlates the row
-- with offer:reply:{offerId} and guards accept/decline transitions against a
-- superseded offer.
ALTER TABLE trips ADD COLUMN offer_id text;

-- Ride requests carry a destination now (the driver needs somewhere to go).
-- Nullable: a request without one degenerates to a zero-length trip.
ALTER TABLE ride_requests ADD COLUMN dest_lat double precision;
ALTER TABLE ride_requests ADD COLUMN dest_lng double precision;

-- Transactional outbox for trip transitions: every applied transition INSERTs
-- one row here in the SAME transaction as the status write, so the event log
-- and the state can never disagree. `type` is the status the trip ENTERED;
-- payload carries the cause (machine event, driver, offer). dispatched_at is
-- for the (later) dispatcher — NULL means not yet delivered.
CREATE TABLE trip_events (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  trip_id text NOT NULL REFERENCES trips (id),
  type text NOT NULL,
  payload jsonb NOT NULL DEFAULT '{}',
  created_at timestamptz NOT NULL DEFAULT now(),
  dispatched_at timestamptz
);

CREATE INDEX trip_events_trip_id_idx ON trip_events (trip_id, id);

-- Down Migration

DROP TABLE trip_events;
ALTER TABLE ride_requests DROP COLUMN dest_lat;
ALTER TABLE ride_requests DROP COLUMN dest_lng;
ALTER TABLE trips DROP COLUMN offer_id;
ALTER TABLE trips DROP CONSTRAINT trips_status_check;
ALTER TABLE trips ADD CONSTRAINT trips_status_check
  CHECK (status IN ('matched', 'en_route', 'in_trip', 'completed', 'cancelled'));
