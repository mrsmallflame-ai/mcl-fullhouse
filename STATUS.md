# MCL FULLHOUSE — Status

## Usage (now)

```bash
python3 fill_all.py            # dry-run: full programme plan + read-only probes
python3 fill_all.py --live     # fill every upcoming showtime of every movie
```

No link, no theatre name, no session id. The programme is discovered live.

## ✅ Round 1: FULLHOUSE build-out
- [x] `discover.py` — full-programme enumeration via the site's own JSON API
      (`MCLWebAPI2/GetNowShowingGrid` → `GetShowDays`), with house, price,
      remaining-seat count and HK-time filtering
- [x] Verified live: **1,697 upcoming sessions / 61 movies / 14 cinemas** in one pass
- [x] `fill_all.py` orchestrator — queue + N concurrent house-fillers reusing
      blaze2's engine; periodic rediscovery; per-movie stats; dry-run by
      default, `--live` gated behind a 5s abort window
- [x] Dry-run verified against live site: probes report real free-seat counts,
      zero claims made

## ✅ Round 2: API-limited edition (perf/api-limited branch)
- [x] `governor.py` — global AIMD controller (additive increase / ×0.5 on
      pressure / full-jitter exponential cooldowns / Retry-After wins /
      fast recovery), shared by discovery, scanners and booking chains;
      per-host budgets (`FULLHOUSE_MAX_CONC_TIX/SEATPLAN/WWW`)
- [x] Global 429/server-busy backoff signals respected everywhere — all fixed
      sleeps removed from the booking path
- [x] `blaze2.py` v3 — persistent pre-warmed client pool, MovieSetId cache,
      ALL chunks drained per scan (no starvation), event-driven rounds
      (no gather barrier)
- [x] Pipelined concurrent watchdog scanner (replaces serial loop) with
      adaptive poll interval; visit time-budgets; headroom-driven rediscovery
- [x] Offline test rig: `mock_mcl.py`, `bench_old_vs_new.py` (A/B/C),
      `tests_e2e_chain.py`, `tests_live_queue.py`, `tests_watchdog.py`
- [x] Measured vs baseline under mock throttling (14 rps): old 0 seats @ 0%
      success → new 77+ seats @ **100% success**; parity when unthrottled

## 🔜 Round 3 candidates
- [ ] Live soak test across a single cinema first (`--limit-movies 1 --live`)
- [ ] Optional Telegram/webhook summary at end of each rediscovery cycle
- [ ] Session-end detection to stop revisiting finished screenings

