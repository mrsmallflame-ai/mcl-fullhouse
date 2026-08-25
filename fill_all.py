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


async def supervisor(args, stats: Stats) -> None:
    async with httpx.AsyncClient(follow_redirects=True, timeout=httpx.Timeout(12, connect=6)) as sc:
        sc.headers.update({"User-Agent": UA})

        async def harvest() -> list[Session]:
            def prog(m):
                pass  # quiet during orchestration; discovery summary printed separately
            ss = await discover_all(lang=args.lang,
                                    limit_movies=args.limit_movies,
                                    concurrency=8, progress=prog)
            if args.movie:
                needle = args.movie.lower()
                ss = [s for s in ss
                      if needle in s.movie.lower() or s.movie_id == args.movie]
                if not ss:
                    print(f"🤷 no sessions matching movie filter: {args.movie!r}")
            ss = filter_upcoming(ss)
            key = {
                "empty": lambda s: (-(s.remaining or 0), s.showdate),
                "soonest": lambda s: (s.start_dt() or datetime.max.replace(tzinfo=HK_TZ),),
            }[args.order]
            ss.sort(key=key)
            return ss

        print("🔎 discovering the full programme ...")
        sessions = await harvest()
        movies = {s.movie for s in sessions}
        cinemas = {s.cinema for s in sessions}
        print(f"🎬 {len(sessions)} upcoming sessions | {len(movies)} movies | {len(cinemas)} cinemas")

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
