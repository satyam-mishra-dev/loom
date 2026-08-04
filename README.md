# Loom

**Matches 5,000 drivers in milliseconds. Never double-books one.**

Real-time geo-dispatch over an H3 hexagonal index — atomic driver claims,
crash-safe offer timeouts, and per-cell surge, proven under 200-way concurrency.
The city is simulated; the engineering is not.

It is a TypeScript monorepo: a driver/traffic **simulator**, a WebSocket
**gateway** that ingests position pings and ride requests, a Redis **geo index**
over H3 cells, a **matcher** that runs an atomic driver claim and an offer
cascade, a **trip state machine** with a transactional outbox, a **janitor** that
recovers stranded claims, a GCRA **rate limiter**, a sliding-window **surge**
engine, a CQRS **read model** streaming over SSE, and a **deck.gl dashboard**.

---

## Architecture

```mermaid
flowchart TD
  SIM["Simulator<br/>5k driver agents (1Hz GPS) + Poisson requests"]
  GW["Gateway (Fastify + ws)<br/>auth-lite · batched ping ingest · GCRA intake limiter"]
  REDIS["Redis geo index<br/>driver:{id} hash · cell:{h3}:available sets · heartbeat ZSET"]
  MATCH["Matcher<br/>k-ring candidates → score → ATOMIC CLAIM → offer cascade"]
  JAN["Janitor<br/>sweeps expired claims (visibility timeout)"]
  PG["Postgres<br/>ride_requests · trips (partial-unique index) · trip_events outbox"]
  RM["Read model<br/>surge recompute · SSE snapshot · /spawn"]
  DASH["Dashboard (React + deck.gl)<br/>driver dots · trip arcs · surge heatmap · HUD"]

  SIM -- "WS pings + ride_requests" --> GW
  GW -- "pipelined MULTI" --> REDIS
  GW -- "row + queue" --> PG
  MATCH <-- "candidates / claim (Lua)" --> REDIS
  MATCH -- "trip TX + outbox" --> PG
  MATCH -- "offer (pub/sub)" --> GW
  GW -- "offer reply (list)" --> MATCH
  MATCH -- "trip_assigned" --> GW
  GW -- "assign / offer" --> SIM
  JAN <-- "expired claims" --> REDIS
  JAN -- "revert + re-enqueue" --> PG
  REDIS --> RM
  PG --> RM
  MATCH -. "/metrics scrape" .-> RM
  RM -- "SSE snapshot" --> DASH
  DASH -- "POST /spawn" --> RM
```

The offer transport is split on purpose: **downstream** (matcher → driver) is
Redis pub/sub — a message for a disconnected driver _should_ evaporate because
the matcher already owns the recovery path (offer timeout → release → next
candidate). **Upstream** (driver reply → matcher) is a Redis list the awaiting
matcher `BLPOP`s, so a reply that arrives a beat before the matcher blocks is
buffered rather than dropped, and the block's deadline _is_ the offer TTL.

---

## Guarantees

Every number below is reproduced by a committed script or test — nothing here is
hand-waved. See [`ARCHITECTURE.md`](ARCHITECTURE.md) for how each is enforced and
[`FAILURE-MODES.md`](FAILURE-MODES.md) for what breaks and how it recovers.

**No driver is ever double-assigned.** The signature test
([`test/no-double-assignment.test.ts`](test/no-double-assignment.test.ts)) seeds
20 available drivers in one H3 cell, fires **200 concurrent ride requests**
through the real matching path (real Redis, real Postgres, real Lua claim, real
offer round-trip — nothing mocked), and asserts exactly **20 trips**, **180
honest unmatched**, every assigned driver unique in Postgres _and_ consistent in
Redis, and **zero partial-unique-index violations**. It also runs across **two
independent matcher instances** to prove cross-process safety, and a **crash
variant** ([`test/no-double-assignment-crash.test.ts`](test/no-double-assignment-crash.test.ts))
kills the matcher mid-cascade and asserts the invariant still holds. Both run in
CI on every push.

**A wiped index heals itself from live pings.** The geo index holds only
operational state; after a `FLUSHALL` the continuing 1 Hz pings rebuild it. The
tight rebuild of a full batch measures in the tens of milliseconds (**~69 ms**,
65–79 ms across runs,
[`packages/core/test/geo-index.integration.test.ts`](packages/core/test/geo-index.integration.test.ts));
in the bench, wiping the whole keyspace under a live 5,000-driver stream and
timing the return to a whole index measures **203 ms**.

**Request → match latency (real stack, seeded simulator, `npm run bench`).**
5,000 drivers, drivers auto-accept with ~0 think time so the number is the
engine's, not simulated human delay. Latency is measured from Postgres event
timestamps: `ride_requests.created_at` (gateway insert) to the trip's `matched`
outbox row (matcher commit) — same DB clock, no synthetic stopwatch.

| requests/s | matches/s | p50 (ms) | p95 (ms) | p99 (ms) | unmatched |
| ---------: | --------: | -------: | -------: | -------: | --------: |
|         10 |       9.8 |     13.8 |     44.7 |     56.9 |        0% |
|         25 |      18.9 |     35.8 |    122.8 |    218.0 |        0% |
|         50 |      49.8 |     69.2 |    133.6 |    159.5 |        0% |
|        100 |      99.7 |    101.9 |    227.2 |    359.7 |        0% |

This sweep shows the **queuing slope** — latency rising with offered load on a
fixed 5,000-driver fleet — not a saturation point: it does not push RPS past the
fleet's capacity to find the knee (throughput stays matched to demand,
`unmatched 0%`, across all four levels). Finding where it first breaks is a
deliberate next experiment, not a hidden result. The table above was captured on
a quiet box; [`PERFORMANCE.md`](PERFORMANCE.md) profiles where that latency goes
(~90% is queue wait, not match logic), records a controlled before/after
optimization pass under heavier co-resident load, and is honest about what did
_not_ help (adding consumers).

Geo candidate search (k-ring expand, `need=8`, `maxK=3`) over the indexed fleet:
**p50 0.59 ms, p99 2.82 ms** across 500 searches (query points now seeded, so the
query _set_ is reproducible run to run). These sub-millisecond micro-numbers stay
**statistical** — single-digit-ms medians carry real scheduling jitter, so treat
them as a magnitude, not a fixed constant. Reproduce with `npm run bench` (needs
Docker; writes `scripts/bench-results.json`).

---

## Demo

A 90-second walkthrough script lives in
[`docs/demo.md`](docs/demo.md): spawn a hotspot burst, watch a cell surge past
1×, kill the matcher and watch the janitor recover, then cut to the green
signature test.

---

## Quickstart

**Whole stack, one command** (needs Docker):

```bash
docker compose up -d --wait
```

Then open the dashboard at **http://localhost:4620**. Other host ports:

| Service          | URL                           |
| ---------------- | ----------------------------- |
| Dashboard        | http://localhost:4620         |
| Read model / SSE | http://localhost:4600/events  |
| Gateway metrics  | http://localhost:4640/metrics |
| Matcher metrics  | http://localhost:4650/metrics |
| Postgres         | localhost:5434                |
| Redis            | localhost:6381                |

The simulator boots a 2,000-driver fleet by default; set `SIM_DRIVERS=5000` (and
`SIM_RPS`, `SIM_HOTSPOTS`) for the headline demo. Tear down with
`docker compose down -v`.

**Local dev.** Bring up just the infrastructure, migrate, then run services with
hot reload:

```bash
npm install
docker compose up -d postgres redis
DATABASE_URL=postgres://loom:loom@127.0.0.1:5434/loom \
  npm run migrate --workspace=@loom/db

npm run start --workspace=@loom/gateway      # :8080
npm run start --workspace=@loom/matcher      # :8090 (embeds the janitor)
npm run start --workspace=@loom/read-model   # :4600 (SSE)
npm run dev   --workspace=@loom/dashboard    # :4620 (Vite)
npm run start --workspace=@loom/simulator -- \
  --sink ws --gateway ws://127.0.0.1:8080/ws --auth-secret loom-dev-secret \
  --drivers 2000 --rps 20 --hotspots 3
```

**Checks.** `npm run typecheck`, `npm run lint`, `npm test`,
`npm run test:integration` (Testcontainers — real Postgres + Redis, no mocks),
`npm run test:signature`, `npm run bench`.

---

## Why H3, and not Redis GEO or Tile38

I index drivers into **resolution-8 H3 cells** (~0.74 km² average, ~531 m edge —
neighbourhood scale, the right bucket for city dispatch). A ping computes its
cell with `h3-js`, the driver's id moves between `cell:{h3}:available` sets, and
candidate search is an expanding `gridDisk` (k-ring): union the disk's
available-sets for k = 0, 1, 2… until there are enough fresh candidates or a
radius cap. Hexagons are the point — a hexagon's six neighbours all sit at _one_
centre-to-centre distance, so a k-ring is a clean radius query; a square grid has
two neighbour distances (edge vs diagonal, ratio √2), so "k rings" over squares
approximate a circle badly and distance-by-ring needs case analysis.

**Versus Redis native GEO** (`GEOADD`/`GEOSEARCH`, a sorted set over geohash-52
scores): it would spare me the cell arithmetic and give radius search out of the
box, but geohash inherits lat/lng rectangle distortion (cells shrink toward the
poles) and the geohash **boundary problem** — two points metres apart across a
cell edge share no prefix. H3's explicit neighbour traversal is the honest fix
for that, with uniform geometry, and it lets me keep per-cell _available_ sets
that a claim mutates in O(1), which is what the atomic claim needs — not a
radius scan per request.

**Versus Tile38**: Tile38 is a purpose-built geospatial server with live object
tracking and, crucially, **server-side geofencing** — it will push an event when
an object enters/leaves a region. That is genuinely more than I built. I chose
not to take the dependency because dispatch here is pull-based (a request asks
"who is near me _now_"), not a standing set of geofences, and folding the geo
index into the same Redis that already holds the claim keys, offer reply lists,
and surge windows means the atomic claim and the index membership live in one
store — one round trip, one failure domain, one `FLUSHALL`-and-heal story. If I
needed "notify me when any driver enters the airport polygon," Tile38 would earn
its place. ([DECISIONS.md](DECISIONS.md) records the res-8 choice and the
rejected alternatives in full.)

---

## Design references

Studied as textbooks, not sources — the designs are credited here; the code is
my own.

- **[uber/h3](https://h3geo.org/)** + [h3-js](https://github.com/uber/h3-js) —
  hexagonal cells, resolutions, `gridDisk` neighbour traversal (geo index).
- **[go-redis/redis_rate](https://github.com/go-redis/redis_rate)** — the GCRA
  arithmetic (TAT, emission interval, burst offset) I ported to TS + Lua.
- **[hibiken/asynq](https://github.com/hibiken/asynq)** — the visibility-timeout
  / lease pattern behind the claim expiry and janitor.
- **[tidwall/tile38](https://github.com/tidwall/tile38)** — the geofencing
  design I compared against and chose not to adopt.
- **[animir/node-rate-limiter-flexible](https://github.com/animir/node-rate-limiter-flexible)**
  — the "insurance limiter" degradation strategy (fail-open with a local
  fallback).
- **[Centrifugo](https://centrifugal.dev/)** — WS heartbeat and backpressure
  posture (server ping, bounded outbound queue, disconnect-and-resync).
- **[timgit/pg-boss](https://github.com/timgit/pg-boss)** — `FOR UPDATE SKIP
LOCKED` claim ergonomics I weighed against the Redis Lua claim.
