# Failure modes

What breaks, what happens, how it recovers, and which committed test or bench
proves it. This file is the point of the project as much as the happy path is.

---

## Redis restarts (or is wiped) mid-stream

**What breaks.** Redis holds the whole geo index — driver hashes, per-cell
available sets, the heartbeat ZSET — plus claim keys and offer reply lists. A
restart or `FLUSHALL` erases all of it.

**What happens.** Nothing that matters is lost, because the index is *operational
state derived from a live stream*, not a source of truth. Drivers keep pinging at
~1 Hz; each ping's atomic apply re-`SADD`s the driver into its cell and re-writes
its hash, so the index refills itself. `SADD`/`HSET` are idempotent, so the
rebuild needs no coordination. Trips already committed live in Postgres and are
untouched. In-flight claims and offers that were mid-air are lost, but that is
exactly an offer timeout, which the matcher already recovers from.

**Recovery.** Automatic, bounded by the ping cadence. The tight rebuild of a full
batch measures **~69 ms** (65–79 ms across runs,
`packages/core/test/geo-index.integration.test.ts`, which `FLUSHALL`s and asserts
the index returns whole). Under a live 5,000-driver stream, the bench wipes the
keyspace and times the return to a whole index at **203 ms**
(`npm run bench`). This "the index heals from the stream" property is *why* the
partial-unique index in Postgres exists — Redis being disposable is a feature, so
truth cannot live only there.

---

## Matcher crashes between claim and offer

**What breaks.** A matcher claims a driver (driver → `claimed` in Redis, trip row
`offered`, request `matching`) and then dies before the offer is answered — or
between reverting an offer and continuing the cascade. Without recovery, that
driver is stranded `claimed` forever and the request never completes.

**What happens.** The claim's expiry lives in the data — `expiresAt` in the claim
value, indexed in the `claims:by-expiry` ZSET — so the claim is *observably dead*
the moment its deadline passes, regardless of which process died. The janitor
(embedded in every matcher, or standalone) sweeps the ZSET, re-reads each due
claim under a Lua script, and for a genuinely expired one deletes it, returns the
driver to its cell's available set, reverts the trip `offered → matching` with an
outbox row, and re-enqueues the request so a surviving matcher runs the cascade
again.

**Recovery.** Automatic within claim TTL + one janitor interval. The claim TTL
(12 s) deliberately exceeds the offer TTL (8 s) by a wide margin so the janitor
can never free a driver while an accept is still in flight.

**Proof.** `test/no-double-assignment-crash.test.ts` runs the signature scenario
and kills the matcher mid-cascade; the invariant still holds (exactly the
available drivers matched, no double assignment). `apps/matcher/test/cascade.integration.test.ts`
covers the janitor releasing a stranded claim and the driver becoming claimable
again within TTL + ε.

---

## Driver socket drops mid-trip

**What breaks.** A driver's WebSocket dies (NAT rebind, mobile handoff, crash)
while a trip is in progress, or a zombie TCP session stays open but the driver
app is dead.

**What happens.** Two independent liveness layers catch it. At the transport
level the gateway sends a server ping on an interval and terminates the socket if
no pong arrives within the pong timeout — that catches dead *links*, including
half-open TCP that never delivers a FIN. At the application level, a driver that
stops sending position pings goes stale: the heartbeat sweep (a `ZRANGEBYSCORE`
over the heartbeat ZSET) removes it from its available set and marks it
`offline`, so it cannot be matched — that catches dead *drivers* whose socket is
somehow still open. The two are deliberately separate: a swept driver on a live
socket comes straight back by pinging again, and a zombie socket that pings
nothing cannot stay matchable. An in-flight trip's row is untouched by either; if
the driver never returns, the trip's progress events simply stop and it stays in
its last state until an operator or a future reconciler resolves it (an honest,
logged open item, not a silent corruption).

**Proof.** The gateway integration suite covers both: sockets that miss the pong
deadline are terminated and unbound, and the sweep offlines silent drivers and
drops them from the heartbeat ZSET.

---

## Thundering herd on one cell

**What breaks.** A hotspot (a concert letting out, an airport) concentrates many
concurrent requests on the few drivers in one H3 cell — the exact shape of the
signature test, at scale.

**What happens.** The atomic claim is the pressure valve. Every request races for
the same scarce drivers; the Lua claim serializes them so each driver is claimed
by exactly one request, the losers see `claimDriver` return null (a counted
`claimConflictsTotal`), expand their k-ring for the next candidate, and after five
offers fail, return an honest `unmatched` rather than hanging or double-booking.
Meanwhile the surge engine sees demand outrun supply in that cell and lifts the
multiplier toward its 3× cap, which is the economic pressure valve the visual
demo makes obvious.

**Recovery.** No recovery needed — this is steady-state correct behaviour under
overload, not a failure to heal from. Throughput stays bounded by the fleet, not
by contention.

**Proof.** `test/no-double-assignment.test.ts`: 200 concurrent requests onto 20
drivers in one cell yields exactly 20 trips, 180 unmatched, zero double
assignment, and asserts `claimConflictsTotal > 0` (the contention was real). The
bench's supply-vs-demand behaviour and the live demo's per-cell surge (up to 3×
in hotspot cells) show the same thing at fleet scale.

---

## The janitor itself dies

**What breaks.** The process responsible for cleaning up expired claims is gone.

**What happens.** Claims still expire — that is the whole point of putting the
expiry in the data. `expiresAt` in the claim value and the `claims:by-expiry`
ZSET score make a claim *dead the moment its deadline passes*, with no process
involved; a dead claim is observable by anyone who looks. Every matcher embeds
the same sweep loop, so as long as one matcher is alive, expired claims are
swept. If *every* sweeper is down past the claim key's grace TTL, Redis's own
`PX` net eventually erases the key; the next janitor to wake finds the key gone,
still cleans the ZSET entry, and repairs a driver stuck `claimed` — and the
request revives via the matcher's stale-`pending` reaper on startup.

**Recovery.** Automatic and process-independent; "TTL lives in the data, not the
process" is both the interview line and the correctness argument.

**Proof.** The cascade integration test exercises the standalone janitor path and
the "key gone before sweep" branch; the claim integration tests cover the Lua
re-check semantics (`live`/`released`/`gone`).

---

## The rate limiter's Redis is down

**What breaks.** The limiter's own store is unreachable or slow.

**What happens.** `DegradingLimiter` wraps the Redis GCRA call in a short timeout
and routes both errors *and* slowness to an in-process approximate GCRA over a
bounded Map — the same pure arithmetic, per-instance and approximate. By default
this is **fail-open**: the service was fine, and rejecting all traffic because the
*limiter's* dependency died would invert the priority. A `RATE_LIMIT_FAIL_CLOSED`
flag flips it to reject-on-outage for cases where the limit is the product
(billing, hard security throttles). On recovery the local counts are discarded,
not merged — a brief over-admission window accepted by design. The fallback's
memory is bounded (FIFO eviction past a cap) so a key-space attack during an
outage can't grow it without bound; and per-instance limits multiply by instance
count, so a fleet should set the fallback limit to roughly
`global_limit / instance_count`.

**Proof.** `packages/core/test/limiter.test.ts` covers all four paths: primary
healthy, Redis error → fallback keeps limiting, slow Redis → timeout → fallback,
and fail-closed → reject. `gateway.integration.test.ts` proves the intake path
rejects over-limit requests with a 429-equivalent and counts them.

---

## War story: the latent `applyPings` resurrection race (found and fixed in phase D)

This one is real, and it is the reason the two-layer claim defense exists in the
shape it does.

The geo index originally applied a batch of pings by *reading* each driver's
status in a pipeline, deciding in application code whether the driver was
`available`, and then writing the new position and set membership in a `MULTI`.
That read-then-write had a window. With the phase-D offer cascade holding a claim
for whole seconds while position pings streamed in continuously, a matcher's
claim (`SREM` from the available set, status → `claimed`) could land *between* the
ping's read of `status='available'` and its write — and the write would clobber
the just-claimed driver straight back to `available` and re-`SADD` it into the
cell. One driver, now claimable twice.

I did not catch it by reading the code. I caught it because the smoke run's
`pg_unique_violations` counter started ticking up — the Postgres partial-unique
index rejecting the second trip INSERT with a `23505`. That is precisely the job
of the defense-in-depth layer: turn a race that slipped past Redis into a loud,
counted, logged event instead of two cars dispatched to two riders. Without the
index, the bug would have been an invisible correctness violation in production;
with it, the bug was a metric.

The fix was to make one ping's apply a single atomic Lua script: the status check
and the writes it guards are one uninterruptible step, and a driver whose status
is `claimed` or `on_trip` gets a position/heartbeat update *only* — the script
can never move it back into an available set. The signature test now asserts
`pgUniqueViolationsTotal === 0` on every run, so the index stays the silent
backstop it is meant to be, and a regression that reintroduces the race would
turn the signature test red immediately.
