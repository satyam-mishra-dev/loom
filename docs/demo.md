# 90-second demo script

The recording that headlines the README. Run against a booted stack
(`docker compose up -d --wait`, then open http://localhost:4620).

1. **The map (0:00–0:15).** Open the dashboard. Green dots are the available
   fleet; blue arcs are trips in progress (pickup → drop). The HUD shows the
   live fleet size, matches/sec, and match p50/p99. Let it breathe for a beat so
   the viewer sees it is genuinely live, not a screenshot.

2. **Spawn a burst (0:15–0:35).** Toggle **hotspot cluster** on and hit
   **spawn 300**. Requests land in one neighbourhood, demand outruns the drivers
   parked there, and that H3 cell lights up red as the surge multiplier climbs
   past 1× — call out the number in the HUD.

3. **Watch it clear (0:35–0:55).** Drivers claim the requests, arcs fan out to
   the hotspot, and the surge cell cools back toward 1× as supply catches up.
   matches/sec spikes with the burst.

4. **Chaos (0:55–1:20).** In a terminal: `docker compose kill matcher`. Point
   out that active trips keep completing and no driver is double-booked — the
   claim's expiry lives in Redis data, so even with the matcher dead the fleet
   is safe. `docker compose up -d matcher` and the janitor releases any stranded
   claims; matching resumes.

5. **The proof (1:20–1:30).** Cut to a terminal running `npm run test:signature`
   — 200 concurrent requests, 20 drivers, exactly 20 trips, zero double
   assignment, green. That is the guarantee behind the pretty map.
