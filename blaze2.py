#!/usr/bin/env python3
"""BLAZE v3 engine: pure-HTTP MCL theatre filler — API-limited edition.

Same booking chain as v2 (prime -> iframe-url -> entry -> nonmember ->
tickettype -> pickseats -> claim -> payment page) with every self-imposed
limit removed:

* persistent pre-warmed client pool (no TLS churn per chunk)
* MovieSetId cached per (ci, si)
* ALL chunks processed per scan (semaphore-bounded consumers)
* zero fixed sleeps — waits come from the shared AIMD governor, which ramps
  concurrency until MCL itself pushes back ("server busy" / 429 / 5xx /
  Retry-After) and recovers fast afterwards

Usage: python3 blaze2.py <ci> <si> [workers] [--debug]
Env:   BLAZE_SEATS=6  BLAZE_ROUNDS=<cap>  BLAZE_IDLE_POLL=20
       FULLHOUSE_MAX_CONC_TIX / FULLHOUSE_MAX_CONC_SEATPLAN / FULLHOUSE_MAX_CONC_WWW
       MCL_WWW_BASE / MCL_INFO_BASE / MCL_TIX_BASE   (offline testing)
"""
import asyncio, json, os, re, sys, time
import html as H
import httpx

from mclhosts import hosts
from governor import (Signal, classify, retry_after_seconds, PressureBusy,
                      shared_governor)

BookingBusy = PressureBusy            # DESIGN.md name for the same abort

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
SEATS_PER = int(os.environ.get("BLAZE_SEATS", "6"))

# Bookable seat statuses. Wheelchair spaces are deliberately EXCLUDED —
# they are reserved for wheelchair users. Sofa/couple seats need the
# separate twoSeatCount flow and are not handled yet.
BOOKABLE_STATUSES = ("normal", "vibrate")


def parse_seats(html):
    """Extract bookable seats (Normal + Vibrate) with their real area codes."""
    seats, seen = [], set()
    for m in re.finditer(r'<img[^>]*seatnum="([A-Z]+\d+)"[^>]*>', html, re.I):
        tag = m.group(0)
        sn = m.group(1)
        if sn in seen:
            continue
        st = re.search(r'status="([^"]*)"', tag, re.I)
        if not st or st.group(1).lower() not in BOOKABLE_STATUSES:
            continue
        row = re.search(r'\srow="(\d+)"', tag, re.I)
        col = re.search(r'\scolumn="(\d+)"', tag, re.I)
        if row and col:
            seen.add(sn)
            ac = re.search(r'areacode="([^"]*)"', tag, re.I)
            ar = re.search(r'\sarea="([^"]*)"', tag, re.I)
            seats.append({
                "sn": sn,
                "row": row.group(1),
                "col": col.group(1),
                "status": st.group(1).lower(),
                "areacode": ac.group(1) if ac else "0000000001",
                "area": ar.group(1) if ar else "1",
            })
    return seats


async def fetch_seat_plan(c, ci, si):
    """Live seat map under the shared governor. [] full · None busy/error."""
    www, info, _tix = hosts()
    gov = shared_governor()
    await gov.acquire("seatplan")
    try:
        r = await c.get(f"{info}/RealSeatPlan/SeatPlan",
                        params={"cinemaCode": ci, "filmSessionId": si,
                                "language": "en-US",
                                "seatCount": SEATS_PER, "twoSeatCount": 0},
                        headers={"Referer": f"{www}/MCLSelectSeat.aspx?visLang=2&ci={ci}&si={si}"})
        sig = classify(r.status_code, r.text[:200])
        gov.report("seatplan", sig, retry_after_seconds(r.headers))
        if sig is not Signal.OK:
            return None
        return parse_seats(r.text)
    except httpx.HTTPError:
        gov.report("seatplan", Signal.RETRYABLE)
        raise
    finally:
        gov.release("seatplan")


# ------------------------------------------------------------ client pool ----

class WorkerPool:
    """Persistent per-worker httpx clients (keep-alive, cookie jars persist
    across chunks/sessions like a returning customer)."""

    def __init__(self, size: int, timeout: httpx.Timeout | None = None):
        self._size = max(1, int(size))
        self._timeout = timeout or httpx.Timeout(12, connect=6)
        self._clients: list[httpx.AsyncClient] = []

    async def start(self) -> None:
        www, _info, tix = hosts()
        for _ in range(self._size):
            c = httpx.AsyncClient(follow_redirects=True, timeout=self._timeout)
            c.headers.update({"User-Agent": UA,
                              "Accept-Language": "en-US,en;q=0.9"})
            self._clients.append(c)
        # pre-warm TLS/TCP concurrently; failures are non-fatal (first real
        # request will establish the connection instead)
        async def warm(c: httpx.AsyncClient, base: str):
            try:
                await c.head(base + "/", timeout=5)
            except Exception:
                pass
        await asyncio.gather(*(warm(c, b) for c in self._clients
                               for b in {www, tix}))

    def client(self, wid) -> httpx.AsyncClient:
        try:
            idx = int(wid)
        except (TypeError, ValueError):
            idx = abs(hash(str(wid)))
        return self._clients[idx % len(self._clients)]

    async def aclose(self) -> None:
        await asyncio.gather(*(c.aclose() for c in self._clients),
                             return_exceptions=True)
        self._clients.clear()

    def __len__(self) -> int:
        return len(self._clients)


# ------------------------------------------------------- MovieSetId cache ----

_MSET_TTL = 1800.0
_MSET_CACHE: dict[tuple[str, str], tuple[str, float]] = {}
_MSET_LOCKS: dict[tuple[str, str], asyncio.Lock] = {}


async def _movieset_for(c: httpx.AsyncClient, ci: str, si: str,
                        referer_url: str | None = None) -> str | None:
    """MovieSetId for (ci, si), cached with TTL; concurrent fills coalesce."""
    key = (str(ci), str(si))
    hit = _MSET_CACHE.get(key)
    if hit and time.monotonic() < hit[1]:
        return hit[0]
    lock = _MSET_LOCKS.setdefault(key, asyncio.Lock())
    async with lock:
        hit = _MSET_CACHE.get(key)
        if hit and time.monotonic() < hit[1]:
            return hit[0]
        www = hosts()[0]
        gov = shared_governor()
        await gov.acquire("www")
        try:
            r = await c.get(f"{www}/MCLSelectSeat.aspx",
                            params={"visLang": "2", "ci": ci, "si": si},
                            headers={"Referer": referer_url or f"{www}/NowShowing.aspx?visLang=2"})
            sig = classify(r.status_code, r.text[:200])
            gov.report("www", sig, retry_after_seconds(r.headers))
            if sig is not Signal.OK:
                raise BookingBusy(f"prime busy ({sig.name})",
                                  retry_after_seconds(r.headers))
            m = (re.search(r'MovieSetId.{0,3}?(\d+)', r.text)
                 or re.search(r'V-(\d+)\.(?:jpg|png)', r.text))
            if not m:
                return None
            _MSET_CACHE[key] = (m.group(1), time.monotonic() + _MSET_TTL)
            return m.group(1)
        except httpx.HTTPError:
            gov.report("www", Signal.RETRYABLE)
            raise
        finally:
            gov.release("www")


# ----------------------------------------------------------- booking chain ----

async def _book_chain(ci: str, si: str, wid, seats, pool: WorkerPool,
                      results: dict) -> int:
    """One full booking attempt on a pooled client. The request sequence is
    byte-compatible with the v2 engine; pressure raises BookingBusy instead
    of sleeping."""
    www, _info, tix = hosts()
    gov = shared_governor()
    n = len(seats)
    await gov.acquire("tix")
    t0 = time.time()
    try:
        c = pool.client(wid)

        # 1. prime page -> MovieSetId (cached per session)
        mset = await _movieset_for(c, ci, si)
        if mset is None:
            results[wid] = f"W{wid}: no MovieSetId"; return 0

        # 2. ticketing iframe URL w/ fresh token
        r = await c.get(f"{www}/GetPurchaseIFrameURL.aspx",
                        params={"CinemaCodeID": ci, "FilmSessionId": si,
                                "MovieSetId": mset, "Language": "en-US"},
                        headers={"Referer": f"{www}/MCLSelectSeat.aspx?visLang=2&ci={ci}&si={si}",
                                 "X-Requested-With": "XMLHttpRequest"})
        sig = classify(r.status_code, r.text[:200])
        gov.report("tix", sig, retry_after_seconds(r.headers))
        if sig is not Signal.OK:
            raise BookingBusy(f"iframe-url {sig.name}",
                              retry_after_seconds(r.headers))
        m = re.search(rf'{re.escape(tix)}/MCL\.Front\.Ticketing\?[^"\']+', r.text)
        if not m:
            results[wid] = f"W{wid}: no ticketing url"; return 0

        # 3. ticketing entry page (sets www4 session)
        r = await c.get(m.group(0).replace("&amp;", "&"))
        sig = classify(r.status_code, r.text[:200])
        gov.report("tix", sig, retry_after_seconds(r.headers))
        if sig is not Signal.OK:
            raise BookingBusy(f"entry {sig.name}",
                              retry_after_seconds(r.headers))

        # 4. non-member form POST
        forms = re.findall(r'<form[^>]*action="([^"]*)"[^>]*>(.*?)</form>', r.text, re.S)
        target = next((a for a, b in forms if "nonmember" in b.lower()), None)
        if not target:
            results[wid] = f"W{wid}: no nonmember form"; return 0
        body_html = next(b for a, b in forms if a == target)
        fields = dict(re.findall(r'<input[^>]*name="([^"]*)"[^>]*value="([^"]*)"', body_html))
        action = target if target.startswith("http") else tix + H.unescape(target)
        r = await c.post(action, data=fields)
        sig = classify(r.status_code, r.text[:200])
        gov.report("tix", sig, retry_after_seconds(r.headers))
        if sig is not Signal.OK:
            raise BookingBusy(f"nonmember {sig.name}",
                              retry_after_seconds(r.headers))


        # 5. ticket-type selection (AJAX, same payload as the site's JS)
        ttype_html = r.text
        m_stt = re.search(r'var\s+submitTicketTypes\s*=\s*["\']([^"\']+)', ttype_html)
        sel = re.search(r'<select[^>]*>(.*?)</select>', ttype_html, re.S)
        if not m_stt or not sel:
            results[wid] = f"W{wid}: no tickettype endpoint"; return 0
        opt = None
        for om in re.finditer(r'<option[^>]*>', sel.group(1)):
            tag = om.group(0)
            code = re.search(r'code="([0-9A-Za-z]+)"', tag)
            val = re.search(r'value="(\d+)"', tag)
            price = re.search(r'price="([\d.]+)"', tag)
            name = re.search(r'ticketTypeName="([^"]*)"', tag)
            if code and val and val.group(1) == str(n):
                opt = {"code": code.group(1),
                       "name": H.unescape(name.group(1)) if name else "",
                       "price": int(float(price.group(1))) if price else 0}
                break
        if not opt:
            results[wid] = f"W{wid}: no qty-{n} option"; return 0
        payload = {"selectedValues": json.dumps({
            "Tickets": [{"TicketTypeCode": opt["code"], "TicketTypeName": opt["name"],
                          "Quantity": n, "Price": opt["price"]}],
            "Vouchers": [], "Concessions": [],
            "TotalBookingFee": 0, "TotalPrice": opt["price"] * n,
            "TotalOccupySeatAmount": n, "TotalOccupyTwoSeatAmount": 0})}
        stt_url = m_stt.group(1)
        stt_url = stt_url if stt_url.startswith("http") else tix + stt_url
        r2 = await c.post(stt_url, data=payload,
                          headers={"X-Requested-With": "XMLHttpRequest",
                                   "Referer": str(r.url)})
        sig = classify(r2.status_code, r2.text[:200])
        gov.report("tix", sig, retry_after_seconds(r2.headers))
        if sig is not Signal.OK:
            raise BookingBusy(f"tickettypes {sig.name}",
                              retry_after_seconds(r2.headers))


        # 6. PickSeats page (fetches __RequestVerificationToken)
        forms = re.findall(r'<form[^>]*action="([^"]*)"[^>]*>(.*?)</form>', ttype_html, re.S)
        target = next((a for a, b in forms if "/PickSeats" in a), None)
        if not target:
            results[wid] = f"W{wid}: no pickseats form"; return 0
        body_html = next(b for a, b in forms if a == target)
        tok_m = re.search(r'name="__RequestVerificationToken"[^>]*value="([^"]*)"', body_html)
        post_url = target if target.startswith("http") else tix + H.unescape(target)
        r = await c.post(post_url,
                         data={"__RequestVerificationToken": tok_m.group(1) if tok_m else ""},
                         headers={"Referer": stt_url})
        sig = classify(r.status_code, r.text[:200])
        gov.report("tix", sig, retry_after_seconds(r.headers))
        if sig is not Signal.OK:
            raise BookingBusy(f"pickseats {sig.name}",
                              retry_after_seconds(r.headers))
        pickseats_html = r.text

        # 7. claim the seats (204 = held)
        data = {}
        for i, seat in enumerate(seats):
            data[f"selectedSeats[{i}][AreaCode]"] = seat["areacode"]
            data[f"selectedSeats[{i}][AreaNumber]"] = str(seat["area"])
            data[f"selectedSeats[{i}][RowIndex]"] = str(seat["row"])
            data[f"selectedSeats[{i}][ColumnIndex]"] = str(seat["col"])
            data[f"selectedSeats[{i}][SeatName]"] = seat["sn"]
        data["languageCulture"] = "en-US"
        data["platform"] = "DesktopWeb"
        r = await c.post(f"{tix}/MCL.Front.Ticketing/PickSeats/SubmitSelectedSeat",
                         data=data,
                         headers={"X-Requested-With": "XMLHttpRequest",
                                  "Referer": f"{tix}/MCL.Front.Ticketing/PickSeats?language=en-US&source=DesktopWeb"})
        sig = classify(r.status_code, r.text[:200])
        gov.report("tix", sig, retry_after_seconds(r.headers))
        if r.status_code != 204:
            if sig is not Signal.OK:
                raise BookingBusy(f"claim {sig.name}",
                                  retry_after_seconds(r.headers))
            try:
                j = r.json()
                msg = j[0].get("content", "?")[:40] if isinstance(j, list) and j else str(j)[:60]
                results[wid] = f"W{wid}: ❌ {msg} ({time.time()-t0:.1f}s)"
                return 0
            except Exception:
                results[wid] = f"W{wid}: ❌ http {r.status_code}"; return 0


        # 8. payment form from the ORIGINAL pickseats page -> booking held
        forms = re.findall(r'<form[^>]*action="([^"]*)"[^>]*>(.*?)</form>', pickseats_html, re.S)
        target = next((a for a, b in forms if "pickSeatSubmitButton" in b), None)
        if not target:
            results[wid] = f"W{wid}: no payment form"; return 0
        f3 = {}
        for inp in re.findall(r'<input[^>]*>', next(b for a, b in forms if a == target)):
            nm = re.search(r'name="([^"]*)"', inp)
            vl = re.search(r'value="([^"]*)"', inp)
            if nm:
                f3[H.unescape(nm.group(1))] = H.unescape(vl.group(1)) if vl else ""
        pay_url = target if target.startswith("http") else tix + H.unescape(target)
        r = await c.post(pay_url, data=f3)
        sig = classify(r.status_code, r.text[:200])
        gov.report("tix", sig, retry_after_seconds(r.headers))
        ok = "Payment" in str(r.url) or "payment" in r.text[:4000].lower()
        dt = time.time() - t0
        seats_str = ",".join(s["sn"] for s in seats)
        if ok:
            results[wid] = f"W{wid}: ✅ [{seats_str}] BOOKED {dt:.1f}s"
            return n
        results[wid] = f"W{wid}: post-payment fail {str(r.url)[:60]}"
        return 0
    except BookingBusy as e:
        results[wid] = f"W{wid}: ⏳ {e} ({time.time()-t0:.1f}s)"
        return 0
    except Exception as e:
        results[wid] = f"W{wid}: 💥 {str(e)[:70]}"
        return 0
    finally:
        gov.release("tix")


async def book_chunk(ci, si, wid, seats, arg4, arg5, arg6=None, arg7=None) -> int:
    """Booking entry point.

    New style:  book_chunk(ci, si, wid, seats, pool, governor, results)
    Legacy:     book_chunk(ci, si, wid, seats, sem, results)   — v2 behavior
                (fresh isolated client per call; still works for old callers)
    """
    if isinstance(arg4, WorkerPool):
        return await _book_chain(str(ci), str(si), wid, seats, arg4, arg6)
    # legacy path: (sem, results) — v2 semantics on a one-shot pooled client
    sem, results = arg4, arg5
    async with sem:
        pool = WorkerPool(1)
        await pool.start()
        try:
            return await _book_chain(str(ci), str(si), wid, seats, pool, results)
        finally:
            await pool.aclose()


# ------------------------------------------------------------- scheduling ----

async def drain_chunks(ci, si, label, seat_queue, inflight, pool: WorkerPool,
                       workers_sem, results, stats, deadline,
                       *, n_consumers: int, wid_base: int = 0) -> int:
    """Consume seat chunks with `n_consumers` workers bounded by workers_sem.

    ALL queued chunks get processed (fixes chunk starvation); the queue-low
    watermark is the caller's signal to refetch (no gather barrier). Returns
    seats booked."""
    gov = shared_governor()
    booked_total = 0

    async def consumer(wid: int):
        nonlocal booked_total
        while True:
            if deadline and time.monotonic() > deadline:
                return
            try:
                chunk = seat_queue.get_nowait()
            except asyncio.QueueEmpty:
                return                       # queue drained -> caller rescans
            try:
                got = await book_chunk(ci, si, wid, chunk,
                                       pool, gov, results)
                booked_total += got
                if stats is not None and got:
                    stats.booked += got
            except BookingBusy:
                pass                         # governor already backed off;
            finally:                          # next scan retries these seats
                for s in chunk:
                    inflight.discard(s["sn"])
                seat_queue.task_done()

    await asyncio.gather(*(consumer(wid_base + i + 1)
                           for i in range(max(1, n_consumers))))
    return booked_total


def enqueue_chunks(seats, seat_queue: asyncio.Queue, inflight: set) -> int:
    """Chunk fresh seats into SEATS_PER groups, dedup vs in-flight claims."""
    pushed = 0
    for i in range(0, len(seats), SEATS_PER):
        chunk = [s for s in seats[i:i + SEATS_PER] if s["sn"] not in inflight]
        if not chunk:
            continue
        for s in chunk:
            inflight.add(s["sn"])
        seat_queue.put_nowait(chunk)
        pushed += 1
    return pushed


# ------------------------------------------------------------------ main ----

async def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    debug = "--debug" in sys.argv
    ci = args[0] if args else "017"
    si = args[1] if len(args) > 1 else "127637"
    workers = int(args[2]) if len(args) > 2 else 8
    rounds = os.environ.get("BLAZE_ROUNDS")
    rounds = int(rounds) if rounds else None   # None = run forever
    idle_poll = float(os.environ.get("BLAZE_IDLE_POLL", "20"))

    print(f"🔥 BLAZE v3 | ci={ci} si={si} workers={workers} seats/worker={SEATS_PER}")
    print("   ♾️  running until you stop it (Ctrl+C). Re-claims freed seats.")

    gov = shared_governor()
    pool = WorkerPool(workers)
    await pool.start()
    total, rnd = 0, 0
    t_start = time.time()
    t_last_booking = t_start

    try:
        async with httpx.AsyncClient(follow_redirects=True,
                                     timeout=httpx.Timeout(12, connect=6)) as sc:
            sc.headers.update({"User-Agent": UA})
            while rounds is None or rnd < rounds:
                rnd += 1
                t_fetch = time.time()
                try:
                    seats = await fetch_seat_plan(sc, ci, si)
                except Exception as e:
                    print(f"  [round {rnd}] seatplan error: {str(e)[:60]}")
                    await asyncio.sleep(0.5 * (2 ** min(rnd % 5, 3)))   # jittered
                    continue
                if seats is None:
                    snap = gov.snapshot()["seatplan"]
                    wait = max(snap["cooldown"], 0.25)
                    print(f"  [round {rnd}] seatplan pressure — governor cooldown {wait:.1f}s "
                          f"(limit={snap['limit']})")
                    await asyncio.sleep(wait)
                    continue
                if not seats:
                    idle = time.time() - t_last_booking
                    print(f"  [round {rnd}] house full ({idle:.0f}s) — rescanning in {idle_poll:.0f}s")
                    await asyncio.sleep(idle_poll)
                    continue

                seat_queue: asyncio.Queue = asyncio.Queue()
                inflight: set = set()
                pushed = enqueue_chunks(seats, seat_queue, inflight)
                results: dict = {}
                got = await drain_chunks(ci, str(si), f"r{rnd}", seat_queue,
                                         inflight, pool,
                                         asyncio.Semaphore(workers), results,
                                         None, None, n_consumers=workers)
                if got:
                    t_last_booking = time.time()
                total += got
                for w in sorted(results):
                    if debug or "✅" in results[w] or "💥" in results[w]:
                        print(f"    {results[w]}")
                el = time.time() - t_start
                print(f"  ── [round {rnd}] +{got} | total {total} | {el:.0f}s | "
                      f"{total/el*60 if el else 0:.0f}/min | gov={gov.snapshot()}")
                if got == 0:
                    # visible seats all rejected/contested — micro-backoff, no fixed 6s
                    await asyncio.sleep(0.25 + 0.75 * (time.time() % 1))
    finally:
        await pool.aclose()
        print(f"\n🏁 TOTAL: {total} seats in {time.time()-t_start:.1f}s")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 stopped by user")







