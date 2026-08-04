# Performance

A disciplined optimization pass on the request→match hot path. Every number
here comes from `npm run bench` (the committed load bench) or from temporary
per-phase timers inside `matchRequest` used only to locate the hot spot and
removed before commit. No number is hand-waved; nothing was changed that a test
does not still cover.

**Headline:** the match *service* path is already tight (~12 ms p50 to a
committed match). At RPS 100 roughly **90 % of end-to-end latency is queue
wait**, not match logic. The kept optimizations shave ~3 Postgres round trips
per matched request off the two hottest deterministic service phases; the
median-of-3 sweep shows **RPS 100 p50 137 → 111 ms (-19 %)** and **RPS 10
19 → 16 ms (-15 %)**, with the mid-levels inside the noise floor. Adding
consumers — the obvious lever — was measured and **rejected**: it inflates
per-request Postgres time faster than it adds capacity.

---

## Methodology

- **Source of truth:** `npm run bench` — fixed RNG seed (42), 5 000 drivers,
  RPS 10/25/50/100, 12 s warm-up + 12 s steady-state window per level. Latency
  is `ride_requests.created_at` (gateway insert) → the trip's `matched` outbox
  row `created_at` (matcher commit), same DB clock — no synthetic stopwatch.
- **Median-of-3, back-to-back.** Each headline number below is the median of 3
  consecutive full sweeps. **p50 is the stable metric**; p95/p99 swing run to
  run on this box and are reported but not leaned on.
- **Co-resident-load caveat (important).** These runs share a laptop with 2–3
  other Docker compose stacks (Tally, a live Loom stack) *plus* the bench's
  own testcontainers Postgres+Redis. Absolute latencies carry real co-resident
  noise — treat them as a magnitude, and trust the *relative* before/after
  (measured back-to-back on the same loaded box) far more than any single
  absolute value. This is also why the numbers here read higher than the
  README's bench table, which was captured on a quieter box; the README stands
  as the lighter-load reference, this file is the controlled before/after.
- **Phase breakdown:** temporary `performance.now()` timers around each phase of
  `matchRequest` (env-gated, reverted before commit). Percentiles over the RPS
  100 window.

---

## Where the request→match time actually goes

Profiled phases at RPS 100 (8 consumers), p50 / mean in ms:

| phase (matchRequest) | what it is | p50 | mean |
| --- | --- | ---: | ---: |
| intake (`UPDATE`+`SELECT`) | claim request + resume lookup (2 PG round trips) | 1.28 | 2.24 |
| find_candidates | gridDisk SUNION + HGETALL pipeline (2 Redis) | 0.85 | 1.05 |
| claim | atomic Lua claim (1 Redis) | 0.35 | 0.46 |
| **offer_trip** | **birth TX: INSERT trip + 3 outbox INSERTs (7 PG round trips)** | **3.26** | **3.95** |
| publish | offer → driver channel (1 Redis) | 0.35 | 0.46 |
| blpop | offer round trip through the simulator | 2.29 | 27.71 |
| accept | accept TX (6 PG round trips) | 2.88 | 3.55 |
| **to_matched_total** | **request pop → `matched` committed (service time)** | **11.81** | **39.60** |

Two facts fall straight out:

1. **Service is tight, queue wait dominates.** `to_matched_total` p50 is
   **11.8 ms** while the end-to-end bench p50 at RPS 100 is **~120–137 ms**. The
   difference — ~90 % — is the request sitting in `requests:queue` waiting for a
   consumer. Shaving the match logic barely moves the headline directly; it
   moves it *indirectly*, by freeing consumers and cutting shared-Postgres load.
2. **The two Postgres transactions are the hottest deterministic phases.**
   `offer_trip` (birth, 7 round trips) and `accept` (6) dwarf every Redis phase.
   `blpop`'s mean (27.7) vastly exceeds its p50 (2.3): ~1 % of offers stall
   multiple seconds on the single multiplexed simulator socket — the variance
   source discussed under *Remaining bottleneck*.

The optimizations therefore target Postgres round trips (which both lowers
service time *and* reduces load on the shared Postgres), not Redis and not the
already-sub-millisecond geo search.

---

## Optimizations applied (kept)

### 1. Birth-path outbox: 3 `INSERT`s → 1 multi-row `INSERT`

`offerTrip`'s birth path wrote the `requested → matching → offered` provenance
as three separate `INSERT INTO trip_events` round trips inside the TX. Collapsed
into a single multi-row `INSERT` — same rows, same transaction, same order
(the `IDENTITY` sequence assigns ids in `VALUES` order, so the outbox chain is
byte-identical), **2 fewer round trips** on the hottest cascade path.

| `offer_trip` (RPS 100, profiled) | before | after | Δ |
| --- | ---: | ---: | ---: |
| p50 | 3.26 ms | 2.68 ms | **-18 %** |
| mean | 3.95 ms | 3.45 ms | **-13 %** |

*File:* `apps/matcher/src/trip-store.ts`.

### 2. Intake: `UPDATE` + separate resume `SELECT` → one query

Every request ran two Postgres statements up front: the `pending → matching`
claim `UPDATE`, then a separate `SELECT id FROM trips WHERE request_id = $1` for
crash-resume (which returns NULL on the ~always-taken fresh path). Folded the
resume lookup into the `UPDATE ... RETURNING` as a correlated subquery — the
`trips` table is not touched by the `UPDATE`, so the subquery reads it in the
same statement, **1 fewer round trip** per request, identical semantics
(`NULL existing_trip_id` ⇒ fresh cascade).

| intake (RPS 100, profiled) | before | after | Δ |
| --- | ---: | ---: | ---: |
| p50 | 1.28 ms | 1.05 ms | **-18 %** |
| mean | 2.24 ms | 1.93 ms | **-14 %** |

*File:* `apps/matcher/src/matcher.ts`.

**Combined:** ~3 fewer Postgres round trips per matched request; profiled
`to_matched_total` p50 11.81 → 11.23 ms (-5 %). Correctness unchanged — full
suite green including the signature + crash tests (see below).

### Headline effect — median of 3 sweeps

Request→match p50 (ms), baseline (original code) vs final (optimized), each the
median of 3 back-to-back sweeps on the same loaded box:

| RPS | baseline p50 | final p50 | Δ | note |
| ---: | ---: | ---: | ---: | --- |
| 10 | 18.8 | 15.9 | **-15 %** | clear |
| 25 | 28.1 | 26.6 | -5 % | small |
| 50 | 59.4 | 61.4 | +3 % | within noise |
| 100 | 136.9 | 111.4 | **-19 %** | clear, non-overlapping runs |

The RPS 100 result is the strongest signal: the three baseline runs
(133.7 / 136.9 / 143.1) and three optimized runs (109.9 / 111.4 / 113.4) have
**non-overlapping p50 distributions**. The optimized triplet was also measured
*first* (before the baseline triplet), so any co-resident drift over the session
would have penalised — not flattered — the optimized numbers, yet it still came
out lower. p95/p99 stay noise-dominated at every level (both directions across
levels) and no claim rests on them.

---

## What did NOT help (negative results)

### More consumers + a bigger Postgres pool

The obvious fix for a queue-wait bottleneck is more consumers. Measured it
(16 consumers, PG pool max 25, RPS 100, single run) — **rejected**:

| RPS 100 | 8 consumers, pool 10 | 16 consumers, pool 25 |
| --- | ---: | ---: |
| end-to-end p50 | 122 ms | 103 ms |
| **service `to_matched` p50** | **11.8 ms** | **21.1 ms** |
| `offer_trip` mean | 3.95 ms | 7.84 ms |
| `accept` mean | 3.55 ms | 6.58 ms |
| `blpop` mean | 27.7 ms | 54.6 ms |

Doubling consumers **doubled per-request Postgres time**: the 16 consumers
contend on the shared Postgres (and the single multiplexed simulator socket)
harder than they add throughput. The headline barely moved (~15 %) while the
service path degraded across the board — a wash bought with real contention.
This is why the kept optimizations go the *other* direction (fewer round trips,
less shared-store load) rather than adding workers. The `MATCHER_CONSUMERS` env
knob still exists for deployments where Postgres is not the co-resident
bottleneck; the bench's default of 8 is left unchanged.

### Postgres-pool-size knob, `accept`-TX CTE collapse

- Making `createPool`'s `max` configurable was tried alongside the consumer
  bump; with contention (not connection starvation) as the limiter it did
  nothing on its own and was reverted (no dead config).
- Collapsing `acceptOffer`'s `UPDATE trips` + `UPDATE ride_requests` +
  `INSERT event` into one CTE would save ~1 round trip, but the two updates
  mutually gate each other (`ride_requests` rowcount decides `LostOwnership`;
  the trip update carries the `offered`/`offer_id` guard and the 23505
  partial-unique-index `conflict` path — the correctness headline). A CTE can't
  express that mutual gating without risking those guards, so for ~1 ms it was
  **not** done. Correctness is non-negotiable.

---

## Final numbers (optimized, median of 3)

Request→match latency, real stack, seeded simulator:

| RPS | matches/s | p50 (ms) | p95 (ms) | p99 (ms) | unmatched |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 10 | 9.8 | 15.9 | 64.2 | 72.3 | 0 % |
| 25 | 20.6 | 26.6 | 62.1 | 87.1 | 0 % |
| 50 | 49.8 | 61.4 | 223.5 | 258.1 | 0 % |
| 100 | 99.7 | 111.4 | 302.4 | 442.6 | 0 % |

- **Geo candidate search** (gridDisk expand, `need=8`, `maxK=3`): p50 **0.61 ms**,
  p99 **2.71 ms** over 500 seeded searches — untouched by this pass, sub-ms as
  before.
- **Index self-heal** (FLUSHALL under a live 5 000-driver stream → whole index):
  **203 ms** — untouched, consistent across all six sweeps.

Throughput stays matched to demand (`unmatched 0 %`) at every level — the sweep
maps the queuing slope, it does not push past the fleet's capacity to find the
knee.

### Correctness (unchanged, re-verified on the final tree)

- Unit: **125 passed**.
- Integration (Postgres+Redis via testcontainers): **78 passed**.
- **Signature `no-double-assignment` (2) + crash variant (1): 3 passed.**
- E2E lifecycle: **1 passed**.

The atomic-claim no-double-assignment guarantee, the janitor recovery, the trip
invariants, and the audit fixes (conflict-revert, reorder-safe progress, driver
scoping, coordinate validation) are all intact — the optimizations only reorder
Postgres statements *within the existing transactions and guards*, never across
them.

---

## Remaining bottleneck and honest next step

**The bottleneck is queue wait, and its variance is set by the offer round
trip, not the match logic.** `blpop`'s mean (~28 ms) is ~12× its p50 (~2.3 ms):
about 1 % of offers get no reply and stall to the 8 s offer TTL, and a stalled
offer pins a consumer for that whole time, saturating the small consumer pool
and dragging the queue behind it. On this bench those stalls trace to the
*single multiplexed simulator socket* (one WS connection standing in for 5 000
drivers) and its busy event loop — a harness artifact, not an engine defect —
but the shape (a fat offer-latency tail saturating consumers) is exactly what a
real fleet would show under a slow/flaky driver link.

Honest next steps, in leverage order:

1. **Cut the offer-stall tail** — this is the real variance lever, worth far
   more than trimming the already-tight service path. Options: per-driver
   sockets in the load path instead of one multiplexed socket; a shorter
   *first* offer deadline with fast re-offer so one dead driver doesn't cost a
   full 8 s; or gateway backpressure/subscription-readiness gating so an offer
   is never published to a driver whose channel can't deliver.
2. **Leave the match service path alone.** At ~12 ms p50 over ~15 Postgres round
   trips + the Redis claim + a real WS offer round trip, it is tight; the
   remaining PG collapses (the `accept` CTE) trade correctness guards for ~1 ms
   and are not worth it.
3. **Then, and only then, revisit consumer count** — but per-consumer, with the
   contention measured, not blindly (see the negative result above).
