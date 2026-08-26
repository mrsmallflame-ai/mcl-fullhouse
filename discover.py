#!/usr/bin/env python3
"""
FULLHOUSE discovery — enumerate EVERY movie and EVERY session on MCL Cinemas (HK).

Pure HTTP against the same JSON API the official site's JavaScript uses:
  MCLWebAPI2/GetNowShowingGrid.aspx?l=<lang>          -> all now-showing movies
  MCLWebAPI2/GetShowDays.aspx?l=<lang>&t=s&id=<mid>   -> versions -> days -> cinemas -> sessions

Read-only: nothing here books, claims, or holds anything.

Usage:
  python3 discover.py                       # human table
  python3 discover.py --json                # machine-readable
  python3 discover.py --movie 14841         # one movie only
  python3 discover.py --upcoming            # drop sessions whose start time has passed
"""

import argparse
import asyncio
import json
import re
import sys
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from typing import Optional

import httpx

from mclhosts import hosts
from governor import Signal, classify, retry_after_seconds, shared_governor

if hasattr(sys.stdout, "reconfigure"):  # keep emoji/中文 intact on Windows consoles
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
HK_TZ = timezone(timedelta(hours=8))  # cinema local time


def _api_base() -> str:
    """MCLWebAPI2 root, resolved from env-overridable host (mclhosts)."""
    return f"{hosts()[0]}/MCLWebAPI2/"


# 'Thu, Aug 27, 10:00 AM, IMAX/House 12 $130'
_TIME_RE = re.compile(r"(\d{1,2}:\d{2})\s*([AaPp][Mm])")
_PRICE_RE = re.compile(r"\$(\d+(?:\.\d+)?)")


@dataclass
class Session:
    si: int                 # session id (filmSessionId)
    ci: str                 # cinema code id
    cinema: str             # cinema display name
    movie_id: str           # MovieSetId
    movie: str              # movie title
    version: str            # e.g. "English IMAX ..."
    showdate: str           # ISO date "2026-08-27"
    when: str               # raw session name from API (time, house, price)
    house: Optional[str]    # parsed house/screen name, best effort
    start_time: Optional[str]  # "HH:MM" 24h, best effort
    price: Optional[str]    # ticket price string, best effort
    remaining: Optional[int]  # seats reported free at discovery time

    def start_dt(self) -> Optional[datetime]:
        """Best-effort naive-HK datetime of session start."""
        if not self.showdate or not self.start_time:
            return None
        try:
            h, m = self.start_time.split(":")
            d = datetime.fromisoformat(self.showdate)
            return d.replace(hour=int(h), minute=int(m))
        except Exception:
            return None


def _parse_sn(sn: str):
    """Split an API session name into (24h time, house, price)."""
    t = _TIME_RE.search(sn)
    start_time = None
    if t:
        hh, mm = t.group(1).split(":")
        ap = t.group(2).upper()
        hh = int(hh) % 12 + (12 if ap == "PM" else 0)
        start_time = f"{hh:02d}:{mm}"
    price = _PRICE_RE.search(sn)
    house = None
    tail = sn.split("AM,", 1)[-1].split("PM,", 1)[-1].strip()
    if "," in tail:
        house = tail.rsplit("$", 1)[0].strip(" ,") or None
    elif "$" in tail:
        house = tail.split("$")[0].strip() or None
    return start_time, house, (price.group(1) if price else None)


async def get_movies(c: httpx.AsyncClient, lang: int = 2) -> list[dict]:
    """All now-showing movies: [{id, mn(name), t(type), ls}, ...]."""
    gov = shared_governor()
    await gov.acquire("www")
    try:
        r = await c.get(_api_base() + "GetNowShowingGrid.aspx", params={"l": lang})
        sig = classify(r.status_code, r.text[:200])
        gov.report("www", sig, retry_after_seconds(r.headers))
        if sig is not Signal.OK:
            raise RuntimeError(f"GetNowShowingGrid pressure ({sig.name})")
        data = r.json()
    finally:
        gov.release("www")
    return [
        {"id": str(m["id"]), "name": (m.get("mn") or "").strip(), "type": m.get("t", "S")}
        for m in data.get("movies", [])
        if str(m.get("id", "")) not in ("", "0")
    ]


async def get_movie_sessions(c: httpx.AsyncClient, movie: dict, lang: int = 2) -> list[Session]:
    """Flatten versions->days->cinemas->sessions for one movie."""
    out: list[Session] = []
    gov = shared_governor()
    await gov.acquire("www")
    try:
        r = await c.get(_api_base() + "GetShowDays.aspx",
                        params={"l": lang, "t": "s", "id": movie["id"]})
        sig = classify(r.status_code, r.text[:200])
        gov.report("www", sig, retry_after_seconds(r.headers))
        if sig is not Signal.OK:
            raise RuntimeError(f"GetShowDays pressure ({sig.name})")
        payload = r.json() or []
    finally:
        gov.release("www")
    for ver in payload:
        vname = ver.get("vn") or ver.get("v") or ""
        for day in ver.get("sd") or []:
            showdate = day.get("ShowDate") or ""
            for cin in day.get("c") or []:
                ci = str(cin.get("ci", ""))
                cname = (cin.get("cn") or "").strip()
                for s in cin.get("s") or []:
                    try:
                        si = int(s.get("si", 0))
                    except (TypeError, ValueError):
                        continue
                    if si <= 0:
                        continue
                    sn = s.get("sn") or ""
                    stime, house, price = _parse_sn(sn)
                    rem = s.get("r")
                    out.append(Session(
                        si=si, ci=ci, cinema=cname,
                        movie_id=movie["id"], movie=movie["name"], version=vname,
                        showdate=showdate, when=sn, house=house,
                        start_time=stime, price=price,
                        remaining=int(rem) if isinstance(rem, (int, float)) else None,
                    ))
    return out


async def discover_all(lang: int = 2,
                       only_movie: Optional[str] = None,
                       limit_movies: Optional[int] = None,
                       concurrency: int = 8,
                       progress=None) -> list[Session]:
    """Every session of every now-showing movie, de-duplicated by si."""
    async with httpx.AsyncClient(follow_redirects=True, timeout=20) as c:
        c.headers.update({"User-Agent": UA})
        movies = await get_movies(c, lang)
        if only_movie:
            movies = [m for m in movies if m["id"] == str(only_movie)]
        if limit_movies:
            movies = movies[:limit_movies]
        if progress:
            progress(f"{len(movies)} movies on release")

        sem = asyncio.Semaphore(max(1, concurrency))
        done = 0

        async def one(m):
            nonlocal done
            async with sem:
                try:
                    ss = await get_movie_sessions(c, m, lang)
                except Exception as e:
                    if progress:
                        progress(f"! {m['name'][:40]}: {str(e)[:60]}")
                    ss = []
                done += 1
                if progress:
                    progress(f"[{done}/{len(movies)}] {m['name'][:44]:<44} {len(ss)} sessions")
                return ss

        lists = await asyncio.gather(*(one(m) for m in movies))

    seen, uniq = set(), []
    for lst in lists:
        for s in lst:
            if s.si not in seen:
                seen.add(s.si)
                uniq.append(s)
    return uniq


def filter_upcoming(sessions: list[Session], now: Optional[datetime] = None) -> list[Session]:
    """Drop sessions we cannot place in time or that have already started."""
    now = now or datetime.now(HK_TZ)
    kept = []
    for s in sessions:
        dt = s.start_dt()
        if dt is None:
            kept.append(s)          # unparsable -> let the engine decide live
        elif datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute, tzinfo=HK_TZ) >= now:
            kept.append(s)
    return kept


def main():
    p = argparse.ArgumentParser(description="Enumerate every MCL movie/session (read-only)")
    p.add_argument("--lang", type=int, default=2, choices=(1, 2), help="1=中文 2=English")
    p.add_argument("--movie", help="only this MovieSetId")
    p.add_argument("--limit-movies", type=int, help="cap number of movies scanned")
    p.add_argument("--concurrency", type=int, default=8)
    p.add_argument("--upcoming", action="store_true", help="drop started/past sessions")
    p.add_argument("--sort", choices=("empty", "soonest", "movie"), default="empty")
    p.add_argument("--json", action="store_true", help="emit JSON instead of a table")
    p.add_argument("--cinemas", action="store_true",
                   help="list every cinema code + name, then exit")
    p.add_argument("--movies", action="store_true",
                   help="list every movie id + title (incl. version variants), then exit")
    args = p.parse_args()

    async def run():
        msgs = []

        def progress(m):
            msgs.append(m)
            if not args.json:
                print(f"  · {m}")

        ss = await discover_all(lang=args.lang, only_movie=args.movie,
                                limit_movies=args.limit_movies,
                                concurrency=args.concurrency, progress=progress)

        if args.cinemas or args.movies:
            if args.cinemas:
                cs = sorted({(s.ci, s.cinema) for s in ss})
                print(f"{len(cs)} cinemas")
                for ci, cn in cs:
                    print(f"{ci}\t{cn}")
            if args.movies:
                mv = sorted({(s.movie_id, s.movie) for s in ss})
                print(f"{len(mv)} movies")
                for mid, mn in mv:
                    print(f"{mid}\t{mn}")
            return

        dropped = 0
        if args.upcoming:
            before = len(ss)
            ss = filter_upcoming(ss)
            dropped = before - len(ss)

        key = {
            "empty": lambda s: (-(s.remaining or 0), s.showdate, s.ci),
            "soonest": lambda s: (s.start_dt() or datetime.max.replace(tzinfo=HK_TZ),),
            "movie": lambda s: (s.movie.lower(), s.showdate, s.start_time or ""),
        }[args.sort]
        ss.sort(key=key)

        if args.json:
            print(json.dumps([asdict(s) for s in ss], ensure_ascii=False, indent=2))
        else:
            print(f"\n🎬 {len(ss)} sessions (--upcoming dropped {dropped})\n")
            print(f"{'si':>8}  {'r':>4}  {'ci':<4}  {'date':<10} {'time':<5}  "
                  f"{'cinema':<34} {'house':<22} movie")
            for s in ss:
                print(f"{s.si:>8}  {s.remaining if s.remaining is not None else '?':>4}  "
                      f"{s.ci:<4}  {s.showdate:<10} {s.start_time or '?':<5}  "
                      f"{s.cinema[:34]:<34} {(s.house or '?')[:22]:<22} {s.movie[:40]}")

    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        print("\n👋 stopped")


if __name__ == "__main__":
    main()
