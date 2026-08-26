# DESIGN — FULLHOUSE max-throughput ("only MCL limits us")

Target branch: `perf/api-limited`. Python 3.14, stdlib + httpx only.
Goal: remove every self-imposed limit (B1–B7) so throughput is throttled only
by MCL's own pressure signals ("server busy" body, 429/5xx, timeouts).

## Bottlenecks addressed

| ID | Bottleneck | Fix |
|----|------------|-----|
| B1 | only `min(chunks, workers)` chunks run per round | semaphore-bounded pool drains ALL chunks |
| B2 | `asyncio.gather` round barrier | event-driven refetch; workers never idle on slowest peer |
| B3 | fresh AsyncClient+TLS per chunk; MovieSetId re-parsed every chunk | persistent pre-warmed client pool + MovieSetId cache |
| B4 | fixed sleeps 10s/20s/6s | governor-driven waits, jittered exp backoff, Retry-After |
| B5 | serial watchdog seat-map scan | pipelined concurrent scanner, governor-bound |
| B6 | no global rate coordination | one shared AIMD governor, per-host budgets |
| B7 | stalled visits hog slots; flat 15-min rediscovery | visit time budget; early refresh on starvation |

## 1. Base-URL contract (needed by mock + bench)

Module `mclhosts.py`:

```python
def hosts() -> tuple[str, str, str]:
    """(www, info, tix) bases, resolved once from env at first call."""
    # MCL_WWW_BASE  default https://www.mclcinema.com
    # MCL_INFO_BASE default https://info.mclcinema.com
    # MCL_TIX_BASE  default https://www4.mclcinema.com
```

All URL construction in `blaze2.py` / `discover.py` / `fill_all.py` uses these.
Real-host defaults mean zero behavior change for users.

## 2. governor.py — global AIMD controller

One instance shared by everything in the process (discovery, scanners,
booking chains). Per-host-class state:

```python
@dataclass
class HostBudget:
    name: str            # "seatplan" | "www" | "tix"
    max_conc: int        # hard ceiling
    init_conc: int       # startup level

class Governor:
    def __init__(self, budgets: dict[str, HostBudget]): ...
    async def acquire(self, host: str) -> None: ...   # blocks until slot granted
    def release(self, host: str) -> None: ...
    def report(self, host: str, signal: Signal) -> None: ...
    def limit(self, host: str) -> int: ...            # current effective cap
    def snapshot(self) -> dict: ...                   # for stats line
```

`Signal`: OK | BUSY (body contains "server busy") | RETRYABLE (429/503, timeout)
with optional `retry_after: float`.

Defaults (env-overridable):

| host | env ceiling | default | init |
|------|-------------|---------|------|
| seatplan | `FULLHOUSE_MAX_CONC_SEATPLAN` | 8 | 2 |
| www      | `FULLHOUSE_MAX_CONC_WWW`      | 8 | 4 |
| tix      | `FULLHOUSE_MAX_CONC_TIX`      | 24 | 6 |

AIMD rules (per host, evaluated on a rolling window):
- Window `CLEAN_WINDOW=2.0s`, `MIN_SAMPLES=8`.
- Zero pressure in window → `conc = min(max_conc, conc + 1)` every window.
- Any pressure → `conc = max(1, conc * 0.5)` **immediately**, plus global cooldown:
  `BACKOFF_BASE=0.5s`, doubles per successive pressure event, cap `BACKOFF_CAP=20s`,
  **full jitter** (`sleep = random.uniform(0, cooldown)`).
- `Retry-After` header wins if larger than computed cooldown.

- Fast recovery: cooldown halves per clean window (so we re-saturate in seconds,
  not minutes).
- `acquire()` grants slots up to current `conc`; waiters FIFO. Implementation:
  per-host counter + `asyncio.Condition`; NO polling loops.

Pressure classification helper used by all callers:

```python
def classify(status: int, body_head: str) -> Signal
```

## 3. Persistent client pool (in blaze2.py)

```python
class WorkerPool:
    def __init__(self, size: int, timeout: httpx.Timeout | None = None): ...
    async def start(self) -> None        # creates N AsyncClients (keep-alive,
                                         # follow_redirects, UA headers), then
                                         # pre-warms TLS to www/info/tix concurrently
    async def client(self, wid: int) -> httpx.AsyncClient
    async def aclose(self) -> None
```

- One client per virtual worker id; cookie jar persists across chunks and
  sessions (behaves like a returning customer, matches real usage).
- MovieSetId cache: `dict[(ci, si)] -> str`, TTL 1800s, filled lazily by prime
  step; concurrent fills coalesce via per-key lock.
- `GetPurchaseIFrameURL` remains per-booking (token freshness) — revisiting only
  if bench proves it cacheable.

## 4. blaze2.py booking-path rework

```python
async def book_chunk(ci, si, wid, seats, pool, governor, results, stats) -> int
```

- `governor.acquire("tix")` before starting; release in `finally`.
- Request sequence byte-compatible with baseline: prime → iframe-url → entry →
  nonmember POST → tickettype POST + submitTicketTypes AJAX → PickSeats token →
  SubmitSelectedSeat → payment POST. Prime uses MovieSetId cache when warm.
- ZERO sleeps inside. On BUSY/RETRYABLE: `report()` then raise
  `BookingBusy(retry_after)` — caller reschedules the chunk; the worker does not
  block its client.
- Chunk scheduling helper (replaces round loop in fill paths):

```python
async def drain_chunks(ci, si, label, seat_queue, inflight, pool, governor,
                       workers_sem, results, stats, deadline) -> int
```

Workers pull seats → build ≤SEATS_PER chunks → run `book_chunk` under
`workers_sem`; ALL chunks get processed (fixes B1); successful/unsuccessful
claims update `inflight`; refetch of the seat map is triggered by queue-low
watermark (`< 2` chunks) or inflight-drain event, never by gather (fixes B2).

`fetch_seat_plan(client, ci, si)` keeps its name/signature but internally takes
`governor.acquire("seatplan")`, honors Retry-After/busy classification, and
returns `(seats | [] | None)` as today. No sleep loops inside.

## 5. Session fill (fill_all.py)

```python
async def fill_session(sc_source, s: Session, stats, cfg, pool, governor) -> tuple[str, int]
```

- Soft visit budget `FULLHOUSE_VISIT_BUDGET` (default 90s) — a poisoned session
  rotates out instead of hogging a house slot (B7).
- Loop: scan seat plan → feed `drain_chunks` → rescan on watermark/drain events.
- Exit states preserved: "full" / "stalled" / "partial" + totals; miss-streak
  rule (2 consecutive no-progress scans → stalled) kept, but "miss" pauses are
  governor-aware micro-backoffs (jittered 0.25–1s), not fixed 6s.

## 6. Scanner for watchdog mode (B5)

```python
class SeatScanner:
    def __init__(self, pool, governor): ...
    async def scan(self, targets, *, on_free) -> None
```

- Priority order: sessions whose previous pass had free seats first (freed
  claims reappear there), then largest `remaining`.
- In-flight dedup set; concurrency bounded by governor("seatplan").
- `on_free(target)` callbacks launch `fill_session` tasks bounded by
  `--houses` semaphore; watchdog pass interval stays `--poll SECONDS` but
  shrinks adaptively (floor `max(1.0, poll*0.25)`) while reclaim-frees are
  being found, restoring upward when passes come back empty.

## 7. Orchestrator wiring

- House workers: `asyncio.Queue` of sessions, `--houses` consumers (unchanged
  semantics), each runs `fill_session`.
- Rediscovery: background task triggers early when (queue depth < houses AND
  governor mostly idle) OR `--refresh` minutes elapsed (refresh becomes a cap,
  not a metronome).
- Stats: one line per `FULLHOUSE_STATS_INTERVAL=5s`:
  `booked/min · active chains · gov[seatplan,www,tix] levels · busy% · queue`.

## 8. Compatibility guarantees

- CLI: all existing flags/env vars honored. NEW optional flags only:
  `--max-conc-tix`, `--max-conc-seatplan` (map to env defaults above).
- Dry-run default; `--live` 5-second abort gate untouched.
- Wheelchair spaces excluded; Vibrate seats remain bookable.
- No traffic leaves the process except to the configured base URLs.

## 9. Validation contract (validator/mock)

- Mock request-log diff: new engine's request sequence must equal baseline
  modulo (a) cached prime GETs and (b) timing compression.
- Scenarios A/B/C/D must show: ≥ old success rate, busy-rate under mock
  threshold, strictly higher throughput.

