#!/usr/bin/env python3
"""
MCL FULLHOUSE — API-limited edition.

Same premise: discover EVERY showtime of EVERY movie and fill them all.
Every self-imposed throttle is gone:

* one shared AIMD governor paces discovery, seat-plan scans and booking
  chains — MCL's own pressure signals ("server busy", 429/5xx, Retry-After)
  are the only ceiling
* pipelined concurrent seat-map scanning (no serial watchdog loop)
* ALL seat chunks per scan processed by bounded consumers (no starvation)
* visit time-budgets so stalled sessions rotate out; rediscovery fires early
  when the queue starves instead of on a flat timer

SAFETY DEFAULT: dry-run. Nothing is claimed until --live. Bookings stop at
the payment page. Wheelchair spaces are never touched.

Usage:
  python3 fill_all.py                       # plan everything (read-only)
  python3 fill_all.py --live                # FILL every upcoming showtime
  python3 fill_all.py --live --houses 4     # four sessions filled concurrently
"""

import argparse
import asyncio
import os
import random
import sys
import time
from collections import Counter
from datetime import datetime

import httpx

import blaze2
import discover
from blaze2 import UA, SEATS_PER, WorkerPool, enqueue_chunks, drain_chunks
from discover import Session, discover_all, filter_upcoming
from governor import Governor, HostBudget, set_shared_governor, shared_governor

HK_TZ = discover.HK_TZ

VISIT_BUDGET = float(os.environ.get("FULLHOUSE_VISIT_BUDGET", "90"))
STATS_INTERVAL = float(os.environ.get("FULLHOUSE_STATS_INTERVAL", "5"))

ACTIVE = 0          # sessions currently being filled


class Stats:
    def __init__(self):
        self.booked = 0
        self.visits = 0
        self.full_houses = 0
        self.per_movie = Counter()
        self.started = time.time()

    def line(self):
        mins = max(self.elapsed_min(), 0.01)
        return (f"Σ booked {self.booked} | houses visited {self.visits} "
                f"| full {self.full_houses} | {self.booked / mins:.0f} seats/min "
                f"| up {mins:.0f}m")

    def elapsed_min(self):
        return (time.time() - self.started) / 60


async def probe(sc: httpx.AsyncClient, s: Session) -> int | None:
    """Read-only seat count right now (dry-run helper)."""
    try:
        seats = await blaze2.fetch_seat_plan(sc, str(s.ci), str(s.si))
        return len(seats) if seats is not None else None
    except Exception:
        return None


async def dry_run(sc: httpx.AsyncClient, sessions: list[Session],
                  limit_probe: int) -> None:
    print(f"\n🧪 DRY RUN — probing live seat plans for first {limit_probe} "
          f"sessions (read-only, nothing is claimed)\n")
    sem = asyncio.Semaphore(6)

    async def one(i, s):
        async with sem:
            n = await probe(sc, s)
            tag = "?" if n is None else ("FULL" if n == 0 else f"{n} free")
            print(f"  #{i:<4} si={s.si:<8} {s.showdate} {s.start_time or '?':<5} "
                  f"r~{s.remaining if s.remaining is not None else '?':<3} "
                  f"probe={tag:<5} {s.cinema[:26]:<26} {s.movie[:36]}")

    await asyncio.gather(*(one(i, s) for i, s in enumerate(sessions[:limit_probe], 1)))

    movies = Counter(s.movie for s in sessions)
    cinemas = Counter(s.cinema for s in sessions)
    print(f"\n📋 PLAN SUMMARY")
    print(f"   sessions : {len(sessions)}")
    print(f"   movies   : {len(movies)}")
    print(f"   cinemas  : {len(cinemas)}")
    print("\n   top movies by session count:")
    for name, n in movies.most_common(10):
        print(f"     {n:>3}×  {name[:60]}")
    print("\n▶️  to actually claim these seats:  python3 fill_all.py --live")



async def fill_one(sc: httpx.AsyncClient, s: Session, stats: Stats,
                   workers: int, max_rounds: int, pool: WorkerPool,
                   wid_base: int = 0) -> tuple[str, int]:
    """Fill one session until full / stalled / time-budget, event-driven.

    scan -> drain ALL chunks (bounded consumers) -> rescan on demand.
    No fixed sleeps: waits are governor cooldowns or micro-backoffs."""
    global ACTIVE
    ACTIVE += 1
    gov = shared_governor()
    label = f"{s.movie[:24]} @ {s.cinema[:18]} {s.showdate} {s.start_time or ''}"
    deadline = time.monotonic() + VISIT_BUDGET
    total, misses, rnd = 0, 0, 0
    try:
        while rnd < max_rounds and time.monotonic() < deadline:
            rnd += 1
            try:
                seats = await blaze2.fetch_seat_plan(sc, str(s.ci), str(s.si))
            except Exception as e:
                print(f"    [{label}] seatplan error {str(e)[:50]}")
                await asyncio.sleep(random.uniform(0.5, 1.5))
                continue
            if seats is None:
                # pressure — the governor's cooldown is our wait
                snap = gov.snapshot()["seatplan"]
                await asyncio.sleep(max(snap["cooldown"], 0.25))
                continue
            if not seats:
                stats.full_houses += 1
                return "full", total

            q: asyncio.Queue = asyncio.Queue()
            inflight: set = set()
            enqueue_chunks(seats, q, inflight)
            results: dict = {}
            got = await drain_chunks(str(s.ci), str(s.si), label[:12], q,
                                     inflight, pool, asyncio.Semaphore(workers),
                                     results, stats, deadline,
                                     n_consumers=workers, wid_base=wid_base)
            total += got                      # drain_chunks updated stats.booked
            if got:
                stats.per_movie[s.movie] += got
                misses = 0
                print(f"    ✅ si={s.si} {label}: +{got} seats "
                      f"(visit total {total})")
            else:
                misses += 1
                if misses >= 2:
                    return ("stalled" if total == 0 else "partial"), total
                # contested seats — brief jittered pause, not a fixed 6s
                await asyncio.sleep(random.uniform(0.25, 1.0))
        return ("stalled" if total == 0 else "partial"), total
    finally:
        ACTIVE -= 1


class SeatScanner:
    """Pipelined concurrent seat-map prober (replaces the serial loop).

    Concurrency is bounded by a local semaphore AND the governor's seatplan
    budget; priority = sessions that had free seats most recently."""

    def __init__(self, sc: httpx.AsyncClient, probe_width: int = 8):
        self.sc = sc
        self.sem = asyncio.Semaphore(max(1, probe_width))
        self.last_free: dict[int, int] = {}

    def order(self, targets: list[Session]) -> list[Session]:
        return sorted(targets,
                      key=lambda t: -self.last_free.get(t.si, 0))

    async def scan(self, targets: list[Session]) -> list[tuple[Session, int]]:
        frees: list[tuple[Session, int]] = []

        async def one(t: Session):
            async with self.sem:
                try:
                    seats = await blaze2.fetch_seat_plan(self.sc,
                                                         str(t.ci), str(t.si))
                except Exception:
                    return
                if seats:
                    frees.append((t, len(seats)))
                    self.last_free[t.si] = len(seats)
                elif seats is not None:
                    self.last_free.pop(t.si, None)

        await asyncio.gather(*(one(t) for t in targets))
        frees.sort(key=lambda p: -self.last_free.get(p[0].si, p[1]))
        return frees


async def watch_targets(sc: httpx.AsyncClient, args, stats: Stats) -> None:
    """Watchdog mode (--poll N): concurrent re-scan of every target's seat
    map each pass; freed seats are claimed immediately, bounded by --houses.
    Pass interval adapts: shrinks while reclaim-frees keep appearing."""
    targets = await harvest_sessions(args)
    movies = {s.movie for s in targets}
    cinemas = {s.cinema for s in targets}
    bits = [b for b in (f"cinema~{args.cinema}" if args.cinema else None,
                        f"movie~{args.movie}" if args.movie else None) if b]
    print(f"🎬 watching {len(targets)} sessions ({len(movies)} movies | "
          f"{len(cinemas)} cinemas){' | ' + ', '.join(bits) if bits else ''}")
    if not targets:
        if args.drain:
            print("🏁 nothing matches these filters right now — done.")
            return
        print("(nothing yet — will keep re-checking)")

    if not args.live:
        await dry_run(sc, targets, min(args.probe, len(targets)))
        return

    print(f"\n⚠️  LIVE WATCH in 5s — every ~{args.poll:.0f}s the seat maps are "
          f"re-scanned concurrently;\n    freed seats are re-claimed. Ctrl+C to abort.\n")
    await asyncio.sleep(5)

    gov = shared_governor()
    scanner = SeatScanner(sc, probe_width=max(4, args.houses * 2))
    houses_sem = asyncio.Semaphore(max(1, args.houses))
    last_harvest = time.time()

    async def guarded_fill(s: Session):
        async with houses_sem:
            try:
                status, total = await fill_one(sc, s, stats, args.workers,
                                               args.max_rounds, POOL["pool"],
                                               wid_base=0)
                if total:
                    print(f"  👀 si={s.si} {status} (+{total}) | {stats.line()}")
            except Exception as e:
                print(f"  💥 H si={s.si}: {str(e)[:70]}")

    while True:
        t_pass = time.time()
        now_hk = datetime.now(HK_TZ)
        alive = []
        for s in targets:
            dt = s.start_dt()
            if dt is not None and datetime(dt.year, dt.month, dt.day,
                                           dt.hour, dt.minute,
                                           tzinfo=HK_TZ) < now_hk:
                continue                       # screening started -> drop
            alive.append(s)
        targets = alive

        frees = await scanner.scan(scanner.order(alive))
        if frees:
            print(f"  🔎 pass: free seats on {len(frees)} session(s) "
                  f"({sum(n for _, n in frees)} seats)")
        await asyncio.gather(*(guarded_fill(s) for s, _ in frees))

        if time.time() - last_harvest >= args.refresh * 60:
            try:
                fresh = await harvest_sessions(args)
                known = {s.si for s in targets}
                added = [s for s in fresh if s.si not in known]
                if added:
                    print(f"  ♻️  +{len(added)} new sessions now watched")
                targets.extend(added)
            except Exception as e:
                print(f"  ⚠️ target refresh failed: {str(e)[:70]}")
            last_harvest = time.time()
            if args.drain and not targets:
                print("🏁 drained: nothing left to watch — stopping.")
                return

        # adaptive interval: shrink while reclaim activity is hot
        waited = time.time() - t_pass
        poll_eff = max(1.0, args.poll * 0.25) if frees else args.poll
        await asyncio.sleep(max(0.05, poll_eff - waited))


POOL: dict = {"pool": None}       # process-wide WorkerPool, built in supervisor


async def harvest_sessions(args) -> list[Session]:
    """Discover + filter (movie/cinema/shard/order/upcoming)."""
    ss = await discover_all(lang=args.lang,
                            limit_movies=args.limit_movies,
                            concurrency=8, progress=lambda m: None)
    if args.movie:
        needle = args.movie.lower()
        ss = [s for s in ss
              if needle in s.movie.lower() or s.movie_id == args.movie]
        if not ss:
            print(f"🤷 no sessions matching movie filter: {args.movie!r}")
    if args.cinema:
        cneed = args.cinema.lower()
        ss = [s for s in ss
              if cneed in s.cinema.lower() or s.ci == args.cinema]
        if not ss:
            print(f"🤷 no sessions matching cinema filter: {args.cinema!r}")
    ss = filter_upcoming(ss)
    key = {
        "empty": lambda s: (-(s.remaining or 0), s.showdate),
        "soonest": lambda s: (s.start_dt() or datetime.max.replace(tzinfo=HK_TZ),),
    }[args.order]
    ss.sort(key=key)
    if getattr(args, "shard", None):
        si_, sn_ = (int(x) for x in args.shard.split("/", 1))
        ss = [s for k, s in enumerate(ss) if k % sn_ == si_ - 1]
    return ss


def build_governor(args) -> Governor:
    """Governor with optional CLI ceiling overrides."""
    budgets = {
        "seatplan": HostBudget("seatplan",
                               getattr(args, "max_conc_seatplan", None)
                               or int(os.environ.get("FULLHOUSE_MAX_CONC_SEATPLAN", "8")), 2),
        "www": HostBudget("www",
                          int(os.environ.get("FULLHOUSE_MAX_CONC_WWW", "8")), 4),
        "tix": HostBudget("tix",
                          getattr(args, "max_conc_tix", None)
                          or int(os.environ.get("FULLHOUSE_MAX_CONC_TIX", "24")), 6),
    }
    return Governor(budgets)


async def stats_ticker(stats: Stats, queue: asyncio.Queue | None) -> None:
    gov = shared_governor()
    while True:
        await asyncio.sleep(STATS_INTERVAL)
        q = f"q={queue.qsize()}" if queue is not None else "watch"
        print(f"  📊 {stats.line()} | active={ACTIVE} {q} | gov={gov.snapshot()}")


async def supervisor(args, stats: Stats) -> None:
    set_shared_governor(build_governor(args))
    gov = shared_governor()

    async with httpx.AsyncClient(follow_redirects=True,
                                 timeout=httpx.Timeout(12, connect=6)) as sc:
        sc.headers.update({"User-Agent": UA})

        # one process-wide pool for both modes (watchdog + queue)
        pool = WorkerPool(max(2, args.houses * args.workers))
        await pool.start()
        POOL["pool"] = pool

        if getattr(args, "poll", None):
            try:
                await watch_targets(sc, args, stats)
            finally:
                await pool.aclose()
            return

        shard = None
        if args.shard:
            try:
                si_, sn_ = (int(x) for x in args.shard.split("/", 1))
                if not (1 <= si_ <= sn_ and sn_ > 1):
                    raise ValueError
            except Exception:
                raise SystemExit(f"--shard must look like '1/3', got {args.shard!r}")
            shard = (si_, sn_)

        async def harvest() -> list[Session]:
            return await harvest_sessions(args)

        print("🔎 discovering the full programme ...")
        sessions = await harvest()
        movies = {s.movie for s in sessions}
        cinemas = {s.cinema for s in sessions}
        lbl = f" | shard {shard[0]}/{shard[1]}" if shard else ""
        print(f"🎬 {len(sessions)} upcoming sessions | {len(movies)} movies | "
              f"{len(cinemas)} cinemas{lbl}")

        if not args.live:
            await dry_run(sc, sessions, args.probe)
            return

        print(f"\n⚠️  LIVE FILL in 5s — every free seat of every upcoming "
              f"showtime will be claimed.\n    Ctrl+C to abort.\n")
        await asyncio.sleep(5)

        queue: asyncio.Queue = asyncio.Queue()
        for s in sessions:
            queue.put_nowait(s)
        stop = asyncio.Event()


        async def house(wid: int) -> None:
            wid_base = wid * args.workers
            while not stop.is_set():
                try:
                    s = queue.get_nowait()
                except asyncio.QueueEmpty:
                    await asyncio.sleep(0.5)
                    continue
                try:
                    status, total = await fill_one(sc, s, stats, args.workers,
                                                   args.max_rounds, pool,
                                                   wid_base=wid_base)
                    print(f"  🏠 H{wid} si={s.si} {status} (+{total}) "
                          f"| {stats.line()}")
                except Exception as e:
                    print(f"  💥 H{wid} si={s.si}: {str(e)[:70]}")

        async def refresher() -> None:
            """Rediscover when the queue starves (early) or --refresh elapses."""
            last = time.time()
            while not stop.is_set():
                starved = (queue.empty() and ACTIVE == 0)
                wait = 15.0 if starved else max(1.0, min(60.0,
                        last + args.refresh * 60 - time.time()))
                if not starved and time.time() - last < args.refresh * 60:
                    await asyncio.sleep(min(wait, 15.0))
                    continue
                try:
                    fresh = [s for s in await harvest()]
                    added = enqueue_fresh(queue, fresh)
                    print(f"  ♻️  rediscovered programme: +{added} "
                          f"new/reopened sessions queued")
                    if args.drain and not fresh and queue.empty() and ACTIVE == 0:
                        print("  🏁 drained — stopping.")
                        stop.set()
                        return
                except Exception as e:
                    print(f"  ⚠️ refresh failed: {str(e)[:70]}")
                last = time.time()

        def enqueue_fresh(queue: asyncio.Queue, fresh: list[Session]) -> int:
            inq = {getattr(it, "si", None) for it in list(queue._queue)}  # noqa: SLF001
            added = 0
            for s in fresh:
                if s.si not in inq:
                    queue.put_nowait(s)
                    added += 1
            return added

        tasks = [asyncio.create_task(house(w)) for w in range(max(1, args.houses))]
        tasks.append(asyncio.create_task(refresher()))
        tasks.append(asyncio.create_task(stats_ticker(stats, queue)))
        try:
            await asyncio.gather(*tasks)
        finally:
            stop.set()
            await pool.aclose()


def main():
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(line_buffering=True)
        except Exception:
            pass
    p = argparse.ArgumentParser(
        description="MCL FULLHOUSE — fill every showtime of every movie "
                    "(dry-run by default)")
    p.add_argument("--lang", type=int, default=2, choices=(1, 2))
    p.add_argument("--live", action="store_true",
                   help="actually claim seats (default is a read-only plan)")
    p.add_argument("--houses", type=int,
                   default=int(os.environ.get("FULLHOUSE_HOUSES", "3")),
                   help="sessions filled concurrently (default 3)")
    p.add_argument("--workers", type=int,
                   default=int(os.environ.get("BLAZE_WORKERS", "8")),
                   help="parallel booking workers per house (default 8)")
    p.add_argument("--max-rounds", type=int, default=12,
                   help="seat-map rounds per visit before rotating (default 12)")
    p.add_argument("--refresh", type=float, default=15,
                   help="rediscovery period cap, minutes (queue starvation "
                        "triggers earlier refresh)")
    p.add_argument("--order", choices=("empty", "soonest"), default="empty",
                   help="empty = most free seats first (default); soonest")
    p.add_argument("--limit-movies", type=int, help="cap scan size (testing)")
    p.add_argument("--movie", help="only this movie: name substring or MovieSetId")
    p.add_argument("--cinema", help="only this venue: name substring or code")
    p.add_argument("--poll", type=float, metavar="SECONDS",
                   help="watchdog mode: concurrent re-scan every N seconds; "
                        "freed seats re-claimed instantly")
    p.add_argument("--drain", action="store_true",
                   help="exit once nothing matches the filters anymore")
    p.add_argument("--shard", metavar="I/N",
                   help="run slice I of N (e.g. 1/3)")
    p.add_argument("--probe", type=int, default=25,
                   help="sessions to live-probe in dry-run")
    p.add_argument("--max-conc-tix", type=int,
                   help="hard ceiling on parallel booking chains "
                        "(default 24 / $FULLHOUSE_MAX_CONC_TIX)")
    p.add_argument("--max-conc-seatplan", type=int,
                   help="hard ceiling on parallel seat-map scans "
                        "(default 8 / $FULLHOUSE_MAX_CONC_SEATPLAN)")
    args = p.parse_args()

    stats = Stats()
    print("🍿 MCL FULLHOUSE — every movie · every showtime · one queue\n"
          f"   mode: {'🔴 LIVE' if args.live else '🧪 dry-run (use --live to claim seats)'}")
    try:
        asyncio.run(supervisor(args, stats))
    except KeyboardInterrupt:
        pass
    finally:
        print(f"\n🏁 {stats.line()}")
        if stats.per_movie:
            print("   per movie:")
            for name, n in stats.per_movie.most_common():
                print(f"     {n:>4} seats  {name[:60]}")


if __name__ == "__main__":
    main()




