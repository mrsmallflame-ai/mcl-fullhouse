# mcl-fullhouse

**Every movie. Every showtime. One queue.**

Successor to [`mcl-filler`](https://github.com/mrsmallflame-ai/mcl-filler) with
zero target input: instead of pasting one theatre/session link, FULLHOUSE
discovers the **entire MCL Cinemas (HK) programme** and runs the same pure-HTTP
seat filler across all of it.

```
1,697 upcoming sessions · 61 movies · 14 cinemas  (discovered in one pass)
```

## How it works

```
discover.py                      fill_all.py
─────────────────────            ─────────────────────────────
MCLWebAPI2/GetNowShowingGrid     blaze2.fetch_seat_plan   (read seat map)
      ↓ all movies               blaze2.book_chunk        (claim 6 seats/worker)
MCLWebAPI2/GetShowDays                 ↑ unchanged engine,
      ↓ days → cinemas → sessions       reused as a library
(si, ci) pairs + house + price
```

Discovery talks to the **same JSON API the official website's JavaScript uses**
— no scraping of HTML pages, no browser rendering, no credentials. Each session
arrives with its cinema, house, start time, price and a rough free-seat count;
the live RealSeatPlan endpoint remains the ground truth at fill time.

The booking core is **blaze2.py, byte-for-byte identical** to mcl-filler's:
parallel workers each claim a disjoint chunk of ≤6 seats through
prime → iframe → nonmember → ticket type → pick seats → claim → payment page.
Unpaid claims expire on MCL's side; FULLHOUSE keeps rotating so reclaimed seats
get re-taken.

## Safety by default

```bash
python3 fill_all.py            # 🧪 DRY RUN — full programme plan + live
                               #             read-only seat probes. Claims nothing.
python3 fill_all.py --live     # 🔴 actually fills every upcoming showtime
```

`--live` prints a warning banner and waits 5 seconds (Ctrl+C aborts) before its
first claim. Bookings stop at the payment page — no payment details are ever
entered — exactly like the original engine. Be a decent creature: don't run
this against a screening other humans actually want to attend.

## Performance: the only limit is MCL

Every self-imposed throttle is gone. One shared **AIMD governor** paces all
traffic — discovery, seat-map scans, booking chains — ramping concurrency up
until MCL itself pushes back (`Server busy`, 429/5xx, `Retry-After`), backing
off with full jitter in milliseconds-scale cooldowns, and re-saturating within
seconds of clean air. Fixed sleeps are gone; seat maps are scanned
concurrently with priority for sessions that just freed seats; every free-seat
chunk in a scan is claimed (not just the first `workers`); booking clients are
persistent and pre-warmed; rediscovery fires early when the queue starves.

Measured offline against a faithful mock (see `bench_old_vs_new.py`,
`MOCK_README.md`):

| scenario | old engine | new engine |
|---|---|---|
| single 113-seat house, clean | 113 paid · 100% ok | 113 paid · 100% ok |
| three houses concurrently | 180 paid · 100% ok | 180 paid · 100% ok |
| **seat-plan throttled at 14 rps** | **0 paid · 0% ok** (busy-sleeps burn every round) | **77+ paid · 100% ok** (governor rides the limit) |

Under zero pressure both engines saturate the mock equally — localhost hides
the new engine's TLS/session-reuse wins, which show up on the real network.
Under MCL pressure the difference is existential.

New knobs (all optional): `--max-conc-tix N`, `--max-conc-seatplan N`,
env `FULLHOUSE_MAX_CONC_TIX=24`, `FULLHOUSE_MAX_CONC_SEATPLAN=8`,
`FULLHOUSE_MAX_CONC_WWW=8`, `FULLHOUSE_VISIT_BUDGET=90`,
`FULLHOUSE_STATS_INTERVAL=5`. Offline testing:
`MCL_WWW_BASE / MCL_INFO_BASE / MCL_TIX_BASE` point anywhere (e.g. the mock).

## Install & run

Requirements: Python **3.10+**, [`httpx`](https://www.python-httpx.org/) — nothing else.

```bash
git clone https://github.com/mrsmallflame-ai/mcl-fullhouse.git
cd mcl-fullhouse
pip install httpx

# see the whole programme (read-only)
python3 discover.py --upcoming
python3 discover.py --upcoming --json > programme.json

# rehearse, then unleash
python3 fill_all.py
python3 fill_all.py --live --houses 4 --workers 12
```

### One venue, one movie

See **[CINEMAS.md](CINEMAS.md)** for all 14 venue codes and the exact-matching
rules (including the version-variants gotcha). Quick forms:

```bash
python3 discover.py --cinemas                        # venue codes + names
python3 discover.py --movies                         # movie ids + titles
python3 fill_all.py --live --cinema 014 --movie "kung fu soccer" --poll 20
```

`--cinema` / `--movie` each accept a case-insensitive name substring or an
exact code/id. `--poll SECONDS` switches to watchdog mode: re-scan the target's
seat maps every N seconds and instantly re-claim seats freed by expired claims.
Add `--drain` to exit automatically once nothing is left.

### CLI

| flag | default | meaning |
|---|---|---|
| `--live` | off | actually claim seats (default = read-only plan) |
| `--houses N` | 3 | sessions filled concurrently |
| `--workers N` | 8 | parallel booking workers per session |
| `--max-rounds N` | 12 | seat-map rounds per visit before rotating |
| `--refresh MIN` | 15 | rediscover programme for new/reopened sessions |
| `--order empty\|soonest` | empty | most-free-seats first, or chronological |
| `--lang 1\|2` | 2 | 中文 / English movie titles |
| `--limit-movies N` | – | cap scan size (testing) |
| `--movie X` | – | one movie: title substring or MovieSetId |
| `--cinema X` | – | one venue: name substring or cinema code |
| `--poll SEC` | – | watchdog mode: re-scan + re-claim every SEC seconds |
| `--drain` | off | exit when no upcoming sessions match the filters |

`blaze2` environment variables carry over: `BLAZE_SEATS` (seats per worker,
default 6), `BLAZE_WORKERS`, `BLAZE_IDLE_POLL`.

## Files

| file | role |
|---|---|
| `fill_all.py` | orchestrator: discover → queue → concurrent house-fillers → rediscover (or `--poll` watchdog) |
| `discover.py` | full-programme enumeration via MCLWebAPI2 (read-only); `--cinemas` / `--movies` rosters |
| `governor.py` | global AIMD concurrency/rate controller — the "only MCL limits us" core |
| `mclhosts.py` | base-URL resolution (`MCL_WWW_BASE` / `MCL_INFO_BASE` / `MCL_TIX_BASE`) |
| `mock_mcl.py` | offline mock of every MCL endpoint (127.0.0.1 only) for testing/benchmarks |
| `bench_old_vs_new.py` | automated baseline-vs-optimized benchmark (scenarios A/B/C) |
| `tests_e2e_chain.py`, `tests_live_queue.py`, `tests_watchdog.py` | integration tests (all offline) |
| `DESIGN.md`, `MOCK_README.md` | architecture + mock/testing docs |
| `CINEMAS.md` | all venue codes + one-place-one-movie matching guide |
| `launch_shards.ps1` | spawn N shard terminals or a watchdog window (Windows) |
| `blaze2.py` | pure-HTTP booking engine v3: pooled clients, event-driven drains, zero sleeps |
| `mcl_filler.py`, `find_sessions.py`, … | original single-session tooling, kept for reference |

## Provenance

Fork-successor of [mrsmallflame-ai/mcl-filler](https://github.com/mrsmallflame-ai/mcl-filler)
(same author); history preserved. The delta is the discovery layer
(`discover.py`) and multi-session orchestrator (`fill_all.py`) — the original
fills *one* link you paste; FULLHOUSE needs no link at all.

## Benchmarks (single-house, from mcl-filler)

full 113-seat house in 41s · peak **514 seats/min** (16 workers) · three houses
simultaneously. FULLHOUSE multiplies that across every session in the
programme; expect site-side throttling to be your ceiling, not this code.

---

*For research and responsible-capacity-testing purposes. Respect the cinemas.*
