-- Up Migration

-- Rider cancellation: a request can now end 'cancelled' (the rider called it off
-- before or during the trip), alongside the existing terminal 'matched' and
-- 'unmatched'. The trips table already permits 'cancelled' (phase D), so only
-- ride_requests needs widening; the partial-unique index is untouched — a
-- cancelled trip is not 'matched'/'en_route'/'in_trip', so it frees the driver.

ALTER TABLE ride_requests DROP CONSTRAINT ride_requests_status_check;
ALTER TABLE ride_requests ADD CONSTRAINT ride_requests_status_check
  CHECK (status IN ('pending', 'matching', 'matched', 'unmatched', 'cancelled'));

-- Down Migration

ALTER TABLE ride_requests DROP CONSTRAINT ride_requests_status_check;
ALTER TABLE ride_requests ADD CONSTRAINT ride_requests_status_check
  CHECK (status IN ('pending', 'matching', 'matched', 'unmatched'));
