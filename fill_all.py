#!/usr/bin/env python3
"""
MCL FULLHOUSE — the mcl-filler premise with zero target input:
discover EVERY showtime of EVERY movie and fill them all.

SAFETY DEFAULT: dry-run. It enumerates the whole programme, probes live seat
plans (read-only) and prints exactly what it would do. Nothing is claimed or
held until you pass --live.

Usage:
  python3 fill_all.py                       # plan everything (read-only)
  python3 fill_all.py --live                # FILL every upcoming showtime
  python3 fill_all.py --live --houses 4     # four cinemas filled concurrently

Engine: reuses blaze2's pure-HTTP booking core unchanged (fetch_seat_plan +
book_chunk). Discovery: see discover.py.
"""

import argparse
import asyncio
import os
import random
import time
from collections import Counter, defaultdict
from datetime import datetime

import httpx

import blaze2
import discover
from blaze2 import UA, SEATS_PER, fetch_seat_plan, book_chunk
from discover import Session, discover_all, filter_upcoming

HK_TZ = discover.HK_TZ


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
        seats = await fetch_seat_plan(sc, s.ci, str(s.si))
        return len(seats) if seats is not None else None
    except Exception:
        return None


async def dry_run(sc: httpx.AsyncClient, sessions: list[Session], limit_probe: int) -> None:
    print(f"\n🧪 DRY RUN — probing live seat plans for first {limit_probe} sessions "
          f"(read-only, nothing is claimed)\n")
    sem = asyncio.Semaphore(6)

    async def one(i, s):
        async with sem:
            n = await probe(sc, s)
            tag = "?" if n is None else ("FULL" if n == 0 else f"{n} free")
            print(f"  #{i:<4} si={s.si:<8} {s.showdate} {s.start_time or '?':<5} "
                  f"r~{s.remaining if s.remaining is not None else '?':<3} probe={tag:<5} "
                  f"{s.cinema[:26]:<26} {s.movie[:36]}")

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
                   workers: int, max_rounds: int) -> tuple[str, int]:
    """Run blaze-style rounds against one session until full / stalled."""
    stats.visits += 1
    label = f"{s.movie[:24]} @ {s.cinema[:18]} {s.showdate} {s.start_time or ''}"
    total, misses, rnd = 0, 0, 0
    while rnd < max_rounds:
        rnd += 1
        try:
            seats = await fetch_seat_plan(sc, s.ci, str(s.si))
        except Exception as e:
            print(f"    [{label}] seatplan error {str(e)[:50]} — retry")
            await asyncio.sleep(6)
            continue
        if seats is None:
            await asyncio.sleep(10)
            continue
        if not seats:
            stats.full_houses += 1
            return "full", total
        chunks = [seats[i:i + SEATS_PER] for i in range(0, len(seats), SEATS_PER)]
        sem = asyncio.Semaphore(workers)
        results = {}
        tasks = [book_chunk(str(s.ci), str(s.si), w, chunks[w], sem, results)
                 for w in range(min(len(chunks), workers))]
        got_list = await asyncio.gather(*tasks)
        got = sum(got_list)
        total += got
        stats.booked += got
        if got:
            stats.per_movie[s.movie] += got
            misses = 0
            print(f"    ✅ si={s.si} {label}: +{got} seats (visit total {total})")
        else:
            misses += 1
            if misses >= 2:
                break
            await asyncio.sleep(6)
    return "stalled" if total == 0 else "partial", total


async def watch_targets(sc: httpx.AsyncClient, args, stats: Stats) -> None:
    """Watchdog mode (--poll N): rescan the target's seat maps every N seconds;
    whenever freed seats appear (expired claims), claim them again."""
    targets = await harvest_sessions(args)
    movies = {s.movie for s in targets}
    cinemas = {s.cinema for s in targets}
    label_bits = []
    if args.cinema:
        label_bits.append(f"cinema~{args.cinema}")
    if args.movie:
        label_bits.append(f"movie~{args.movie}")
    print(f"🎬 watching {len(targets)} sessions ({len(movies)} movies | "
          f"{len(cinemas)} cinemas){' | ' + ', '.join(label_bits) if label_bits else ''}")
    if not targets:
        if args.drain:
            print("🏁 nothing matches these filters right now — done.")
            return
        print("(0 matches - auto-rechecking every ~60s)")

    if not args.live:
        await dry_run(sc, targets, min(args.probe, len(targets)))
        return

    print(f"\n⚠️  LIVE WATCH in 5s — every {args.poll:.0f}s the seat maps are re-scanned; "
          f"\n    freed seats are re-claimed. Ctrl+C to abort.\n")
    await asyncio.sleep(5)

    last_harvest = time.time()
    last_req = blaze2.REQ_COUNT
    pass_no = 0
    scan_sem = asyncio.Semaphore(max(1, getattr(args, "watch_concurrency", 6)))

    async def check_one(s: Session):
        """Probe one house; if seats are visible, fill it. Returns liveness."""
        dt = s.start_dt()
        if dt is not None and datetime(dt.year, dt.month, dt.day, dt.hour,
                                       dt.minute, tzinfo=HK_TZ) < datetime.now(HK_TZ):
            return None  # screening already started -> drop
        async with scan_sem:
            try:
                seats = await fetch_seat_plan(sc, s.ci, str(s.si))
            except Exception:
                return s
        if seats:                       # freed / unsold seats exist -> grab them now
            status, total = await fill_one(sc, s, stats, args.workers, args.max_rounds)
            if total:
                print(f"  👀 si={s.si} {status} (+{total}) | {stats.line()}")
        return s

    while True:
        t_pass = time.time()
        pass_no += 1
        checked = await asyncio.gather(*(check_one(s) for s in targets))
        targets = [s for s in checked if s is not None]
        scan_secs = time.time() - t_pass

        recheck = args.refresh * 60 if targets else max(args.poll, 60.0)
        if time.time() - last_harvest >= recheck:
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

        waited = time.time() - t_pass
        pause = max(0.5, args.poll - waited)
        req_delta = blaze2.REQ_COUNT - last_req
        last_req = blaze2.REQ_COUNT
        if pass_no % 10 == 1:
            rate = req_delta / max(waited + pause, 0.1) * 60
            print(f"  ⏱️  pass {pass_no}: {len(targets)} houses scanned in {scan_secs:.1f}s "
                  f"| ~{req_delta} req/pass (~{rate:.0f}/min) | next scan in {pause:.0f}s")
        await asyncio.sleep(pause)


async def harvest_sessions(args) -> list[Session]:
    """Discover + filter (movie/cinema/shard/order/upcoming). Shared by all modes."""
    def prog(m):
        if m.startswith("!"):   # surface fetch failures even in quiet modes
            print("  . " + m)
    ss = await discover_all(lang=args.lang,
                            limit_movies=args.limit_movies,
                            concurrency=8, progress=prog)
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
    if getattr(args, "date", None):
        want = args.date.strip()
        ss = [s for s in ss if s.showdate == want]
        if not ss:
            print("[!] no sessions found on {want} for these filters")
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


async def supervisor(args, stats: Stats) -> None:
    async with httpx.AsyncClient(follow_redirects=True, timeout=httpx.Timeout(12, connect=6)) as sc:
        sc.headers.update({"User-Agent": UA})

        if getattr(args, "poll", None):
            await watch_targets(sc, args, stats)
            return

        # optional deterministic sharding: --shard I/N takes every Nth session
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
        label = f" | shard {shard[0]}/{shard[1]}" if shard else ""
        print(f"🎬 {len(sessions)} upcoming sessions | {len(movies)} movies | "
              f"{len(cinemas)} cinemas{label}")

        if not args.live:
            await dry_run(sc, sessions, args.probe)
            return

        print(f"\n⚠️  LIVE MODE in 5s — claiming real seats at MCL (unpaid, like the original"
              f"\n    engine: holds expire). Ctrl+C to abort.\n")
        await asyncio.sleep(5)

        queue: asyncio.Queue[Session] = asyncio.Queue()
        active: set[int] = set()

        def enqueue(ss):
            n = 0
            for s in ss:
                if s.si not in active:
                    active.add(s.si)
                    queue.put_nowait(s)
                    n += 1
            return n

        enqueue(sessions)
        print(f"🚀 filling with {args.houses} concurrent houses × {args.workers} workers "
              f"(seats/worker {SEATS_PER})\n")

        stop = asyncio.Event()

        async def house(wid: int):
            while not stop.is_set():
                try:
                    s = queue.get_nowait()
                except asyncio.QueueEmpty:
                    await asyncio.sleep(2)
                    continue
                active.discard(s.si)
                try:
                    status, total = await fill_one(sc, s, stats, args.workers, args.max_rounds)
                    print(f"  🏠 H{wid} si={s.si} {status} (+{total}) | {stats.line()}")
                except Exception as e:
                    print(f"  💥 H{wid} si={s.si}: {str(e)[:70]}")

        async def refresher():
            while not stop.is_set():
                await asyncio.sleep(args.refresh * 60)
                try:
                    fresh = [s for s in await harvest()]
                    added = enqueue(fresh)
                    print(f"  ♻️  rediscovered programme: +{added} new/reopened sessions queued")
                    if args.drain and not fresh and queue.empty():
                        print("  🏁 drained: no upcoming sessions left for these filters — stopping.")
                        stop.set()
                except Exception as e:
                    print(f"  ⚠️ refresh failed: {str(e)[:70]}")

        tasks = [asyncio.create_task(house(w)) for w in range(max(1, args.houses))]
        tasks.append(asyncio.create_task(refresher()))
        await asyncio.gather(*tasks)


def main():
    p = argparse.ArgumentParser(
        description="MCL FULLHOUSE — fill every showtime of every movie (dry-run by default)")
    p.add_argument("--lang", type=int, default=2, choices=(1, 2))
    p.add_argument("--live", action="store_true",
                   help="actually claim seats (default is a read-only plan)")
    p.add_argument("--houses", type=int, default=int(os.environ.get("FULLHOUSE_HOUSES", "3")),
                   help="sessions filled concurrently (default 3)")
    p.add_argument("--workers", type=int, default=int(os.environ.get("BLAZE_WORKERS", "8")),
                   help="parallel booking workers per house (default 8)")
    p.add_argument("--max-rounds", type=int, default=12,
                   help="seat-plan rounds per visit before rotating (default 12)")
    p.add_argument("--refresh", type=float, default=15, help="rediscovery period, minutes")
    p.add_argument("--order", choices=("empty", "soonest"), default="empty",
                   help="empty = most free seats first (default); soonest = chronological")
    p.add_argument("--limit-movies", type=int, help="cap movies scanned (testing)")
    p.add_argument("--movie", help="only this movie: case-insensitive name substring or MovieSetId")
    p.add_argument("--cinema", help="only this location: case-insensitive cinema name substring or cinema code")
    p.add_argument("--date", help="only showtimes on this date: YYYY-MM-DD")
    p.add_argument("--poll", type=float, metavar="SECONDS",
                   help="watchdog mode: re-scan the target's seat maps every N seconds "
                        "(e.g. --poll 20) and instantly re-claim freed seats; "
                        "overrides queue/rotate mode")
    p.add_argument("--watch-concurrency", type=int, default=6, metavar="N",
                   help="houses probed simultaneously per watchdog pass (default 6)")
    p.add_argument("--drain", action="store_true",
                   help="exit once no upcoming sessions match the filters instead of running forever")
    p.add_argument("--shard", metavar="I/N",
                   help="run slice I of N (e.g. 1/3) — split the queue across terminals; "
                        "each terminal must use a different I")
    p.add_argument("--probe", type=int, default=25, help="sessions to live-probe in dry-run")
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
