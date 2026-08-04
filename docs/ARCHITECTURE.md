# Architecture

This walks the system component by component, then follows one request from a
GPS ping to a completed trip. The design decisions behind these choices (and the
alternatives I rejected) are in [DECISIONS.md](DECISIONS.md); how each piece
fails and recovers is in [FAILURE-MODES.md](FAILURE-MODES.md).

## The monorepo

npm workspaces, TypeScript strict everywhere, zero `any`. Raw SQL on the hot
paths through `pg` — I want the SQL visible, not hidden behind an ORM.

- `packages/core` — domain logic with almost no I/O of its own: the H3 geo
  helpers and `GeoIndex`, the atomic `ClaimStore` (Lua), the `SurgeStore`, the
  GCRA `GcraLimiter` + `DegradingLimiter`, the trip state machine, the wire
  message contracts, the metrics primitives. This is where the unit tests live.
- `packages/db` — the connection pool and migrations (node-pg-migrate).
- `apps/gateway` — Fastify + `ws`: authenticated ingestion, batched ping flush,
  ride-request intake (rate-limited), and the offer transport relay.
- `apps/matcher` — the matching pipeline, the trip store (transactional outbox),
  and the janitor (embedded, or runnable standalone).
- `apps/simulator` — the seeded fleet + Poisson request generator; also the load
  generator behind `scripts/bench.ts`.
- `apps/read-model` — the CQRS read side: surge recompute, the SSE snapshot, and
  the `/spawn` control.
- `apps/dashboard` — React + deck.gl live map, fed by SSE.

## Data flow: ping → index → match → claim → offer → trip → complete

**Ping → index.** Each driver reports position at ~1 Hz over one WebSocket. The
gateway authenticates the socket at upgrade (auth-lite HMAC token), then buffers
pings across _all_ sockets into one batch flushed every 50 ms or every 500 pings,
whichever comes first. A flush is one pipelined flight of per-driver Lua scripts:
each ping is applied atomically — compute the H3 res-8 cell, move the driver's id
between `cell:{h3}:available` sets, write the `driver:{id}` hash
(`cell,status,lat,lng,heartbeatMs`), and score the driver in a
`drivers:by-heartbeat` ZSET so the staleness sweep is a `ZRANGEBYSCORE`, not a
scan. The per-ping atomicity matters: an earlier read-then-MULTI version had a
window where a matcher's claim landed between reading `status='available'` and
writing it back, resurrecting a just-claimed driver into the available set —
which is exactly the latent bug the [war story](FAILURE-MODES.md) describes.

**Match: candidates → score.** A ride request is persisted by the gateway (row
first, status `pending`, then its id `LPUSH`ed onto `requests:queue`) so the
matcher can load the row by id. A matcher consumer `BLMOVE`s the id into a
processing list and runs the pipeline. The `pending → matching` UPDATE is the
idempotency guard: whoever wins that row owns the request, every other delivery
is a counted no-op — at-least-once intake, exactly-once matching. Candidate
search is an expanding `gridDisk` (k = 0, 1, 2…) that unions the disk's available
sets until it has enough fresh drivers or hits the radius cap, then ranks them by
a naive ETA — great-circle distance over an assumed urban speed — with freshest
heartbeat and driver id as deterministic tiebreaks (`gridDisk` guarantees no
ordering, so something has to). ETA is monotonic in distance, so this is still
closest-first; framing it as arrival time is what gives the offer an ETA to show.

**Claim (the atomic core).** For each candidate in score order, the matcher runs
the claim as a single Redis Lua script: verify the driver hash says `available`
with a fresh heartbeat, then `SREM` the driver from its cell's available set. The
`SREM` is the linearization point — N concurrent matchers race, Redis runs their
scripts one at a time, and exactly one observes the driver still a member of the
set. That winner writes `status='claimed'`, stores `claim:{driver}` =
`{tripId, token, expiresAt}`, and adds the driver to a `claims:by-expiry` ZSET.
Everything else fails and changes nothing.

**Offer cascade.** The claim already carries an offer's identity, proof of
ownership, and deadline. The matcher writes the trip row (`offered`) and its
outbox events in one Postgres transaction, publishes the offer to
`driver:{id}:msg`, and `BLPOP`s the driver's reply list with the offer TTL as the
block timeout. Accept → confirm the claim, drive the trip to `en_route`, publish
`trip_assigned`. Decline or timeout → revert the trip (Postgres) then release the
claim (Redis), and offer the next candidate; after five offers, an honest
`unmatched`, logged. The write ordering is deliberate: on accept it is trip
commit → confirm claim (so every Postgres failure unwinds through the same atomic
release while the claim still exists); on decline/timeout it is trip revert →
claim release (so a crash between them leaves a _live_ claim the janitor can see,
release, and re-enqueue from — the other order would strand an `offered` row no
sweep could find).

**Trip → complete.** The driver's simulated movement emits `arrived_pickup` and
`trip_done`, which flow through the same list/`BLMOVE` pattern to the matcher's
trip-event loop and drive `en_route → in_trip → completed`. On completion the
driver is freed back to `available` in its current cell, and the trip's outbox
chain (`requested,matching,offered,matched,en_route,in_trip,completed`) is
complete.

## The atomic claim: defense in depth (Redis _and_ Postgres)

The claim is guarded twice, and I can say precisely why both are needed.

- **Redis prevents the race.** Contention happens where cars are scarce and
  requests concurrent; the Lua claim serializes exactly that contention at memory
  speed, before any SQL runs. Postgres alone would put every claim attempt
  through a SQL round trip and row locks on the hottest path in the system.
- **Postgres makes the invariant unviolable.** `trips` carries a partial unique
  index — `UNIQUE (driver_id) WHERE status IN ('matched','en_route','in_trip')`.
  Redis is operational state, not truth: it can be `FLUSHALL`ed, restored from a
  stale snapshot, or brought up empty and rebuilt from live pings (which is a
  feature). If the race-prevention layer is ever wrong, this index rejects the
  second INSERT and the bug becomes a counted, logged `23505` — a metric ticking
  up — instead of two cars arriving for two different riders. Redis alone would
  make a cache wipe capable of violating a business invariant.

Neither store can do the other's job. The signature test asserts that in the
normal path the index _never_ fires (`pgUniqueViolationsTotal === 0`) — it is the
backstop, not the mechanism. The index deliberately covers only
`matched/en_route/in_trip`: an `offered` trip is still exclusive via the Redis
claim, and a crash-orphaned `offered` row must not block a driver's next
legitimate match while it waits for the janitor.

## Janitor and TTL-in-data (the visibility timeout)

A matcher that dies between claim and offer must strand nobody. The claim's
expiry lives in the **data**, not in any process: `expiresAt` inside the claim
value, indexed in the `claims:by-expiry` ZSET written by the same Lua that
created the claim. The janitor `ZRANGEBYSCORE`s due claims and, for each, runs
one Lua script that re-reads the claim (it may have been confirmed or released
since the scan) and only if it is genuinely past `expiresAt` deletes it and
returns the driver to its cell's available set — then reverts the trip
(`offered → matching`, guarded, with an outbox row) and re-enqueues the request.
The sweep is idempotent and multi-janitor-safe: concurrent sweepers race
harmlessly (one releases, the rest see `live` or a missing key). Because the
expiry is observable in the data, a claim is _dead the moment its deadline
passes_ even with zero janitors alive — the janitor is how it gets _cleaned up_,
not how it becomes safe. Every matcher embeds the sweep; `janitor-main.ts` runs
the same loop alone, proving the point.

## Surge

Per cell, a sliding 60-second window of ride-request arrivals (a Redis ZSET
scored by arrival time, written by the gateway on intake) versus the current
count of available drivers in that cell. The multiplier is
`min(3.0, max(1.0, demand / supply))` — no surge on an idle cell, capped so a
rider never sees a runaway price, and total (no NaN/Infinity) at zero supply. The
read model recomputes on a tick, prunes the windows, publishes the surging cells
to a `cell:surge` Redis hash and an in-memory snapshot the SSE stream serves. The
matcher reads that hash to price each offer: `quoteFare` lifts a flat
base-plus-per-km fare by the pickup cell's live multiplier, priced once per
cascade so every candidate quotes the same fare, and the offer carries both the
price and the multiplier that produced it. Surge is a property of the ride's
area, not the driver, so it sets the displayed fare but never reorders
candidates. The multiplier and fare math are pure and unit-tested; the Redis
pipeline is integration-tested.

## Rate limiting (GCRA) and its degradation

Ride-request intake is rate-limited per source with a **GCRA** limiter I ported
from `redis_rate` — one value per key (the theoretical arrival time), an atomic
read-compute-write in Lua so N concurrent callers can't double-spend the
schedule, and a self-expiring key (TTL = drain time). The pure arithmetic is a TS
function the in-process fallback also runs; the Lua mirrors it and is proven
atomic by an integration test (200 concurrent calls, burst 20 → exactly 20 pass).

The limiter's own Redis is a dependency that can fail, so `DegradingLimiter`
wraps it: a short timeout around the Redis call routes both errors _and_ slow
Redis to an in-process approximate GCRA over a bounded Map (fail-open, the
default — the limiter exists to protect the service, and rejecting all traffic
because the _limiter's_ store died inverts the priority). A `failClosed` flag
flips it to reject-on-outage for the cases where the limit _is_ the product. Over-
limit requests get a 429-equivalent back on the socket and a counted metric —
never a silent drop.

## The read model and SSE (why SSE, not WebSocket)

The dashboard is fed by a **separate** read-model service, not a route on the
gateway. The gateway's job is the write hot path; fanning a periodic map snapshot
to browsers, running Postgres trip queries, and scraping the matcher's metrics on
a timer are read concerns with a different load profile, and the snapshot fans in
from three sources (Redis positions and surge, Postgres active trips, the
matcher's `/metrics`) — exactly a read model's job, owned by no single write-path
service. It can be scaled or restarted independently, and the dashboard never
touches a write-path service.

The transport is **Server-Sent Events, not WebSocket**, and the reasoning is
about fit. The feed is strictly one-way, server → browser, periodic state. SSE is
plain HTTP with a trivial framing (`data: …\n\n`), and `EventSource` gives
automatic reconnection with no code. A WebSocket is a duplex, stateful, framed
connection; I would be paying for a bidirectional channel I have no upstream
traffic for, and re-implementing the reconnect logic SSE hands me for free. The
one place the browser _does_ talk back — the "spawn requests" control — is an
ordinary `POST /spawn`, which is the honest shape for a command. WebSockets are
where Loom's _drivers_ live (bidirectional pings, offers, replies,
heartbeats); the dashboard's needs are the opposite, so the transport is too. The
snapshot is bounded — driver dots are capped and the true fleet size reported
separately — so a 5,000-driver fleet never blows the frame.
