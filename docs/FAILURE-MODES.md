# Failure modes

What breaks, what happens, how it recovers, and which committed test or bench
proves it. This file is the point of the project as much as the happy path is.

---

## Redis restarts (or is wiped) mid-stream

**What breaks.** Redis holds the whole geo index — driver hashes, per-cell
available sets, the heartbeat ZSET — plus claim keys and offer reply lists. A
restart or `FLUSHALL` erases all of it.

**What happens.** Nothing that matters is lost, because the index is _operational
state derived from a live stream_, not a source of truth. Drivers keep pinging at
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
(`npm run bench`). This "the index heals from the stream" property is _why_ the
partial-unique index in Postgres exists — Redis being disposable is a feature, so
truth cannot live only there.

---

## Matcher crashes between claim and offer

**What breaks.** A matcher claims a driver (driver → `claimed` in Redis, trip row
`offered`, request `matching`) and then dies before the offer is answered — or
between reverting an offer and continuing the cascade. Without recovery, that
driver is stranded `claimed` forever and the request never completes.

**What happens.** The claim's expiry lives in the data — `expiresAt` in the claim
value, indexed in the `claims:by-expiry` ZSET — so the claim is _observably dead_
the moment its deadline passes, regardless of which process died. The janitor
(embedded in every matcher, or standalone) sweeps the ZSET, re-reads each due
claim under a Lua script, and for a genuinely expired one deletes it, returns the
driver to its cell's available set, reverts the trip `offered → matching` with an
outbox row, and re-enqueues the request so a surviving matcher runs the cascade
again.

**Recovery.** Automatic within claim TTL + one janitor interval. The claim TTL
(12 s) deliberately exceeds the offer TTL (8 s) by a wide margin so the janitor
can never free a driver while an accept is still in flight. Two backstops close
the residual holes: (a) if Redis lets a double-claim slip through, the accept's
`23505` is caught, the trip is reverted `offered → matching`, and the cascade
continues to the next candidate (never stranding on the conflicting driver);
(b) the startup reaper re-enqueues requests stuck past a grace window in _both_
`pending` **and** `matching` — for a `matching` row with a trip it first reverts
the trip and hands the request back to `pending`, so a request abandoned
mid-cascade by a dead matcher (or a past-grace janitor repair) is recovered even
if its claim is already gone.

**Proof.** `test/no-double-assignment-crash.test.ts` runs the signature scenario
and kills the matcher mid-cascade; the invariant still holds (exactly the
available drivers matched, no double assignment). `apps/matcher/test/cascade.integration.test.ts`
covers the janitor releasing a stranded claim and the driver becoming claimable
again within TTL + ε, the `23505`-conflict revert-and-continue, and the reaper
recovering a request stuck `matching` with an orphaned `offered` trip and no
live claim.

---

## Driver socket drops mid-trip

**What breaks.** A driver's WebSocket dies (NAT rebind, mobile handoff, crash)
while a trip is in progress, or a zombie TCP session stays open but the driver
app is dead.

**What happens.** Two independent liveness layers catch it. At the transport
level the gateway sends a server ping on an interval and terminates the socket if
no pong arrives within the pong timeout — that catches dead _links_, including
half-open TCP that never delivers a FIN. At the application level, a driver that
stops pinging goes stale: the heartbeat sweep (a `ZRANGEBYSCORE` over the
heartbeat ZSET) drops an idle driver from its available set and marks it
`offline`, so it cannot be matched — that catches dead _drivers_ on a socket that
somehow stays open. The two are deliberately separate: a swept idle driver on a
live socket comes straight back by pinging again, and a zombie socket that pings
nothing cannot stay matchable.

An _on-trip_ driver is the one case the sweep leaves alone — never offlined,
never dropped from the ZSET. Demoting it would be a correctness bug rather than
cleanup: its reconnect ping would resurrect it into an available set and make it
claimable for a _second_ trip while still on its first. An on-trip driver's
liveness is the matcher and janitor's concern, not the sweep's. So a driver whose
socket dies mid-trip keeps its trip: when a per-driver principal reconnects, the
gateway re-sends the active trip it still owns (`sessions_resumed_total`), the
driver picks up where it left off, and its resumed pings stay position-only — it
never re-enters the matchable pool. If the driver truly never comes back, the
janitor's abandonment reconciler closes it out: a trip with no progress event for
`tripMaxAgeMs` (default 30 min) is driven to `cancelled(abandoned)` and its
stranded driver retired `on_trip → offline` — never re-added to an available set,
so a driver we forcibly retire still cannot be double-assigned. Redis is retired
before the trip is cancelled, so a crash between the two writes self-heals on the
next pass (the driver is already unclaimable; the pass re-selects the still-live
trip and finishes the cancel). Staleness is measured by the trip's last event,
not its birth, so a legitimately long ride that keeps reporting is never touched.

**Proof.** The gateway integration suite covers all three: sockets that miss the
pong deadline are terminated and unbound; the sweep offlines silent idle drivers
and drops them from the heartbeat ZSET but leaves an on-trip driver `on_trip` and
tracked; and a driver that reconnects mid-trip gets its active trip re-sent,
stays `on_trip`, and never rejoins an available set even with an aggressive sweep
running underneath. The cascade suite adds the abandonment case: a driver stuck
`on_trip` on a trip idle past its max age is retired to `offline`, the trip goes
`cancelled(abandoned)`, and a direct `claimDriver` on the retired driver is
refused — the double-assignment invariant holds even for a driver we forcibly retire.

---

## A rider cancels mid-offer or mid-trip

**What breaks.** A rider cancels while the matcher is mid-cascade (a driver
claimed, an offer out) or mid-trip (a driver assigned and driving). Handled
carelessly this races the atomic claim, the offer cascade, and the janitor at
once — enough to double-free a driver, strand one `claimed`, or leave a cancelled
ride still holding a car.

**What happens.** The gateway forwards the cancel by `requestId` onto a
`cancel:queue` list; a matcher `cancelLoop` drains it with the same at-least-once
list pattern as ride intake. `cancelRide` is race-safe because the trips row is
the authority: one locked transaction — trip-then-request lock order, the same as
every other trip-store TX, so it can't deadlock against a concurrent accept or
janitor sweep — flips the trip and its request to `cancelled`. That write is what
_stops_ an in-flight cascade: the cascade's next guarded step sees a
non-`matching` request or non-`offered` trip and backs off, exactly as it does
when a peer steals the request. Only a trip that genuinely owns a live driver
(`offered`/`matched`/`en_route`/`in_trip`) hands back a driver id and claim token,
and that driver is then freed in Redis token-guarded, so a driver already
re-claimed for another trip is never touched. A trip parked at `matching` carries
a released-ghost driver id and frees nobody. It is idempotent throughout:
cancelling an already-terminal ride is a counted no-op, so a duplicate cancel or a
redelivered queue item changes nothing.

**Recovery.** None to heal from — cancellation is a legal terminal transition
(`RIDER_CANCELLED` from any non-terminal state; the two terminal states refuse
it), not a fault. A cancel racing a crash rides the same startup drain as the
rest of the in-flight work: `cancel:processing` items are moved back onto
`cancel:queue` on boot.

**Proof.** `apps/matcher/test/cascade.integration.test.ts` covers the mid-offer
cancel (cascade stops, claimed driver released, never assigned), the mid-trip
cancel (trip cancelled, on-trip driver returned to available, no strand), the
no-trip-yet cancel, the already-completed no-op, and the full queue path through
the running matcher. `apps/gateway/test/gateway.integration.test.ts` asserts the
gateway enqueues a `ride_cancel` and rejects a malformed one.

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
ZSET score make a claim _dead the moment its deadline passes_, with no process
involved; a dead claim is observable by anyone who looks. Every matcher embeds
the same sweep loop, so as long as one matcher is alive, expired claims are
swept. If _every_ sweeper is down past the claim key's grace TTL, Redis's own
`PX` net eventually erases the key; the next janitor to wake finds the key gone,
still cleans the ZSET entry, and repairs a driver stuck `claimed`. Because the
erased value took the `tripId` with it, that branch then looks the driver's
orphaned `offered` trip up _by driver id_, reverts it `offered → matching`, and
re-enqueues its request immediately — no wait for a process restart. (Earlier
this branch left the trip stuck `offered` and its request stuck `matching`
forever; the startup reaper broadening to `status IN ('pending','matching')` is
the second, restart-time backstop for the same strand.)

**Recovery.** Automatic and process-independent; "TTL lives in the data, not the
process" is both the interview line and the correctness argument.

**Proof.** The cascade integration test exercises the standalone janitor path, the
"key gone before sweep" branch reverting the orphaned trip and re-enqueuing, and
the reaper recovering a stuck-`matching` request; the claim integration tests
cover the Lua re-check semantics (`live`/`released`/`gone`).

---

## The rate limiter's Redis is down

**What breaks.** The limiter's own store is unreachable or slow.

**What happens.** `DegradingLimiter` wraps the Redis GCRA call in a short timeout
and routes both errors _and_ slowness to an in-process approximate GCRA over a
bounded Map — the same pure arithmetic, per-instance and approximate. By default
this is **fail-open**: the service was fine, and rejecting all traffic because the
_limiter's_ dependency died would invert the priority. A `RATE_LIMIT_FAIL_CLOSED`
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

## A token tries to act for a driver it doesn't own (channel hijack)

**What breaks.** A `driver_ping`, `offer_reply`, or `trip_progress` carries a
`driverId` in its payload. Without a check, any socket with a _valid_ token
could send messages for **another** driver's id — overwriting that driver's
position, binding its offer channel to the attacker's socket (stealing rider
pickup PII and its offers), or accepting/declining an offer that isn't theirs.

**What happens.** The gateway authorizes a **principal**, then scopes every
payload `driverId` against it. A per-driver principal may act only for its own
id; a **fleet** principal (id prefixed `fleet:`) may act for its whole driver
namespace. Anything out of scope is rejected and counted (`scope_rejects_total`)
before it can bind a channel or enqueue. The trust model is explicit: the
simulator connects as `fleet:sim` and multiplexes all drivers over one socket —
that is a **fleet edge aggregating a fleet's GPS**, exactly like a real dispatch
gateway ingesting from a carrier's telematics box. The fleet token is trusted to
speak for its fleet; it is _not_ a substitute for per-device driver auth, which
is the production upgrade path (a signed per-driver credential minted at device
enrolment, so the gateway need not trust the aggregator blindly). See
DECISIONS.md.

**Proof.** `apps/gateway/test/gateway.integration.test.ts` asserts a driver-scoped
token pinging/replying/reporting for another driver is rejected (no bind, no
index write, no reply forwarded), while a `fleet:` principal drives the whole
fleet through the E2E path.

---

## Two matcher instances reorder a driver's trip-progress events

**What breaks.** Trip progress is `en_route → in_trip → completed`, driven by two
driver-reported events (`arrived_pickup`, then `trip_done`). With more than one
matcher instance, the trip-progress consumers can split a single trip's two
events across instances and process `trip_done` _before_ `arrived_pickup` lands.
The `trip_done` transition is then illegal (the trip is still `en_route`).
Dropping it would strand the trip `in_trip` and the driver `on_trip` forever.

**What happens.** The store distinguishes a **premature** event (its predecessor
hasn't committed — the current status is _earlier_ than the transition's required
source) from a **terminal** one (a duplicate, late, spoofed, or genuinely-illegal
event). A premature event is **not** acked: it is re-queued after a short backoff
so the predecessor commits first, then retried until it applies. A terminal event
is dropped and counted. A separate periodic **reconciler** is the last backstop
for the crash sliver where `trip_done` committed but the process died before
freeing the driver: it frees any driver left `on_trip` whose latest trip is
terminal.

**Proof.** `apps/matcher/test/cascade.integration.test.ts` runs two instances,
forces `trip_done` to be consumed before `arrived_pickup`, and asserts the trip
still completes and the driver ends freed (`trip_event_premature_total > 0`).

---

## The read model is reachable from outside

**What breaks.** The read model serves a mutating control (`POST /spawn`, which
injects up to 2,000 real ride requests per call) and a streaming feed
(`/events`, every driver's live position). Left unauthenticated on a reachable
host these are an injection vector and a data leak.

**What happens.** Both endpoints sit behind a shared token when `READ_MODEL_TOKEN`
is set (compose sets it and bakes the same value into the dashboard bundle, so
the map streams end to end). `/healthz` and `/metrics` stay open for the compose
healthcheck and scraping. Unset (bare local dev) leaves them open with a startup
warning. This is **demo-grade**: the token is visible in the client bundle, so it
is a scan/curl deterrent, not user auth — a session or signed cookie is the
production upgrade (DECISIONS.md).

**Proof.** `apps/read-model/test/read-model.integration.test.ts` asserts `/spawn`
and `/events` reject a missing/wrong token (401) and accept the right one, while
`/healthz` stays open.

---

## War story: the latent `applyPings` resurrection race (found and fixed in phase D)

This one is real, and it is the reason the two-layer claim defense exists in the
shape it does.

The geo index originally applied a batch of pings by _reading_ each driver's
status in a pipeline, deciding in application code whether the driver was
`available`, and then writing the new position and set membership in a `MULTI`.
That read-then-write had a window. With the phase-D offer cascade holding a claim
for whole seconds while position pings streamed in continuously, a matcher's
claim (`SREM` from the available set, status → `claimed`) could land _between_ the
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
is `claimed` or `on_trip` gets a position/heartbeat update _only_ — the script
can never move it back into an available set. The signature test now asserts
`pgUniqueViolationsTotal === 0` on every run, so the index stays the silent
backstop it is meant to be, and a regression that reintroduces the race would
turn the signature test red immediately.
