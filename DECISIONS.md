# Decisions

Every non-obvious choice, what I rejected, and why. This is the interview script.

## H3 resolution 8 for the geo index

**Decided:** index drivers into H3 resolution-8 cells (~0.74 km² average, ~531 m
edge). **Rejected:** finer (res 9, ~0.1 km²) and coarser (res 7, ~5 km²). Res 9
cells are smaller than the typical pickup radius, so most searches would expand
several k-rings anyway — more cells to union per query for no selectivity gain.
Res 7 cells are big enough that a single cell holds a whole neighbourhood's
drivers, so the available-set scan per candidate search stops being cheap. Res 8
sits at neighbourhood scale, where k = 0…2 usually satisfies a search and each
set is small. H3's 12 pentagons could distort `gridDisk` near icosahedron
vertices, but they sit over ocean and a simulated inland city never touches one;
production code would still use the safe `gridDisk`/`gridDiskDistances` variants,
which I do.

## Redis H3 index vs native Redis GEO vs Tile38

**Decided:** hand-rolled H3 cells over Redis sets, in the same Redis that holds
claims and surge. **Rejected native Redis GEO** (`GEOADD`/`GEOSEARCH`): it hides
the cell arithmetic but inherits geohash's pole distortion and boundary problem
(neighbours across a cell edge share no prefix), and it gives a radius *scan*
rather than per-cell *available sets* — and O(1) set membership is exactly what
the atomic claim mutates. **Rejected Tile38**: it is a purpose-built geo server
with server-side geofencing (push an event when an object enters a region), which
is genuinely more than I built — but dispatch here is pull-based ("who is near me
now"), not a standing set of geofences, and keeping the index in the same Redis as
the claim keys means one round trip, one failure domain, one wipe-and-heal story.
If I needed "notify me when a driver enters the airport polygon", Tile38 would
earn the dependency.

## Atomic claim: Redis Lua vs Postgres `FOR UPDATE SKIP LOCKED`

**Decided:** the claim is a Redis Lua script; Postgres carries a partial unique
index as defense in depth. **Rejected** making Postgres the *primary* claim
mechanism (`SELECT … FOR UPDATE SKIP LOCKED` over an availability table).
Contention happens where drivers are scarce and requests concurrent — the hottest
path in the system — and I did not want every claim attempt to be a SQL round
trip and a row lock. The Lua claim serializes that contention at memory speed
before any SQL runs. But Redis is disposable operational state, so the invariant
cannot live only there: the partial unique index
(`UNIQUE (driver_id) WHERE status IN ('matched','en_route','in_trip')`) makes a
second live trip per driver impossible in the store of record, turning any escaped
race into a counted `23505`. Redis prevents the race; Postgres makes it
unviolable; neither can do the other's job. (I studied pg-boss's SKIP LOCKED
ergonomics for this and use the same pattern for the *queue*, just not the claim.)

## Transport: SSE for the dashboard, WebSocket for drivers

**Decided:** the dashboard read model streams over Server-Sent Events; drivers
speak WebSocket. **Rejected** WebSocket for the dashboard. The dashboard feed is
strictly one-way, server → browser, periodic state — SSE is plain HTTP with
trivial framing and `EventSource` gives automatic reconnection for free, whereas a
WebSocket is a duplex channel I have no upstream traffic for plus reconnect logic
I would have to write. Drivers are the opposite (bidirectional pings, offers,
replies, heartbeats), so they get a WebSocket. The dashboard's one command —
spawn requests — is an ordinary `POST`, the honest shape for a command.

## Rate limiter: GCRA vs token bucket vs sliding-window counter

**Decided:** GCRA (the arithmetic ported from `redis_rate`). **Rejected** a
fixed/sliding-window counter and a classic token bucket. A fixed window
double-spends across the boundary (2× burst at the seam); a sliding-window log is
more memory and more Redis ops per check. GCRA expresses sustained rate *and* a
bounded burst as capacity, stores a single value per key (the theoretical arrival
time), needs one atomic Lua read-compute-write, and self-garbage-collects (an
idle key's TTL equals its drain time). A token bucket is close, but GCRA's TAT
form makes "when may the next request conform" a subtraction, which is exactly the
`retry_after` the caller wants.

## Rate limiter degradation: fail-open vs fail-closed

**Decided:** fail-open by default (in-process approximate fallback when Redis errs
or is slow), with a `failClosed` flag. **Rejected** fail-closed as the default.
This limiter guards *capacity*; it exists to protect the service. If the limiter's
own Redis dies while the service is healthy, rejecting 100% of traffic inverts the
priority. So a short timeout routes errors and slowness alike to a local GCRA,
requests keep being limited approximately, and on recovery the local counts are
discarded (a brief over-admission window, accepted). Fail-closed is correct only
when the limit *is* the product — billing quotas, hard security throttles — hence
the flag, not a hard-coded choice. (The idea is `rate-limiter-flexible`'s
"insurance limiter".)

## Dashboard with a build step (Vite + deck.gl)

**Decided:** the dashboard is a real Vite + React + deck.gl build. **Rejected** a
no-build static page. This is the deliberate contrast with the sibling project
(Tally), whose dashboard is intentionally build-free — a single HTML file, no
toolchain — because Tally's read side is tables and numbers that vanilla DOM
renders fine, and the point there was to prove the *engine*, not to ship a
frontend. Loom's artifact *is* a live map: 5,000 moving dots, trip arcs, and
an H3 surge heatmap need WebGL, and deck.gl's `H3HexagonLayer` maps my surge cells
to coloured, extruded hexagons directly. That earns a build step. It stays
self-contained — Vite bundles everything, no CDN at runtime (the deployment CSP
forbids remote fetches) — and the map renders its layers over a dark canvas with
no basemap tiles, which also sidesteps the no-external-fetch constraint.

## npm workspaces

**Decided:** npm workspaces, one repo, `packages/*` + `apps/*`. **Rejected** pnpm
(which the original plan named) and separate repos per service. The domain logic
in `packages/core` is imported by every app and unit-tested in one place; a
monorepo keeps that a single symlinked dependency with one `npm install` and one
lockfile, and lets the integration tests wire real components together without
publishing anything. npm (not pnpm) because the toolchain here is already npm and
the delta buys nothing for a project this size.

## Trip machine written fresh, not shared with Tally's intent machine

**Decided:** the trip state machine is written from scratch as a discriminated
union with a total `transition` function, even though Tally has a structurally
similar payment-intent machine. **Rejected** extracting a shared state-machine
package. The two machines rhyme — both are pure `(state, event) → state`
functions that reject illegal transitions and emit a transactional-outbox row per
transition — but their states, events, and guards are entirely different
(`requested → matching → offered → matched → en_route → in_trip → completed`
here; a payment lifecycle there), and the shared surface would be a two-line
interface wrapping two unrelated bodies. Writing it fresh was cheap, it is
genuinely better the second time (the trip states carry exactly the data each
state is entitled to, so "en_route requires a driver" is a *type*, not a runtime
check), and the honest diff between the two implementations is a better talking
point than a forced abstraction. The *pattern* is credited; the code is its own.

## Driver identity: fleet-principal scope, not per-device auth

**Decided:** the WS token authorizes a *principal*, and the gateway scopes every
payload `driverId` against it — a per-driver principal may act only for its own
id, a `fleet:`-prefixed principal for its whole driver namespace. **Rejected**
trusting the payload `driverId` (the original hole: any valid token could ping,
reply, or report progress for *any* driver, hijacking its channel and offers) and
**rejected** minting a separate socket + token per simulated driver. The
simulator multiplexes 5,000 drivers over one socket by design; a real dispatch
edge does the same — it aggregates a fleet's GPS from a carrier's telematics box.
So the honest model is a **fleet principal** trusted to speak for its fleet, with
per-driver messages scoped to that namespace, not a pretend per-device identity I
don't actually have. The production upgrade is a signed per-driver credential
minted at device enrolment, so the gateway need not trust the aggregator blindly;
the scope check is the seam where that credential would plug in. `offer_reply`
and `trip_progress` ride the same scope check, so a token cannot answer another
driver's offer either.

## Read-model auth: a demo-grade shared token, honestly labelled

**Decided:** gate the read model's mutating `/spawn` and streaming `/events`
behind a shared token (`READ_MODEL_TOKEN`); compose sets it and bakes the same
value into the dashboard bundle so the map still streams one-command.
**Rejected** leaving them open (a `curl /spawn` injects thousands of real
requests; `/events` streams every driver's position) and **rejected** a real
login for a single-page demo dashboard with no user model. The token is visible
in the client bundle, so it is explicitly demo-grade — a deterrent against
anonymous internet scans and stray `curl`s, not user authentication. `/healthz`
and `/metrics` stay open for the compose healthcheck and scraping. A session or
signed cookie behind a real identity is the production upgrade; the token check
is where it would live.

## Migrations via node-pg-migrate; raw SQL on hot paths

**Decided:** node-pg-migrate for schema, raw `pg` for queries. **Rejected** an
ORM. The interesting parts of this system are the SQL — the partial unique index,
the `pending → matching` idempotency UPDATE, the outbox insert in the same
transaction as the status write — and I want them visible and reviewable, not
generated. node-pg-migrate gives ordered up/down migrations that the tests and
services share through one `runMigrations` entry point.
