#!/usr/bin/env python3
"""BLAZE v2: pure-HTTP MCL theatre filler. No browser rendering.

Per round:
  1. ONE seat-plan fetch (shared) -> all available Normal seats
  2. Chunk into groups of N seats -> one worker per group (parallel httpx)
  3. Each worker: prime -> iframe url -> nonmember -> tickettype -> pickseats -> claim -> payment
  4. Loop until no seats left or zero progress twice.

Usage: python3 blaze2.py <ci> <si> [workers] [--debug]
Env:   BLAZE_SEATS=6 (seats per worker), BLAZE_ROUNDS=30
"""
import asyncio, json, os, re, sys, time
import html as H
import httpx

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
BASE = "https://www4.mclcinema.com"
SEATS_PER = int(os.environ.get("BLAZE_SEATS", "6"))

# Bookable seat statuses. Wheelchair spaces are deliberately EXCLUDED —
# they are reserved for wheelchair users. Sofa/couple seats need the
# separate twoSeatCount flow and are not handled yet.
BOOKABLE_STATUSES = ("normal", "vibrate")


def parse_seats(html):
    """Extract bookable seats (Normal + Vibrate) with their real area codes.

    Returns dicts: {sn, row, col, status, areacode, area}. Legend/decoration
    <img> tags (no seatnum/row/column) are skipped by the regex requirements.
    """
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
    r = await c.get("https://info.mclcinema.com/RealSeatPlan/SeatPlan",
                    params={"cinemaCode": ci, "filmSessionId": si, "language": "en-US",
                            "seatCount": SEATS_PER, "twoSeatCount": 0},
                    headers={"Referer": f"https://www.mclcinema.com/MCLSelectSeat.aspx?visLang=2&ci={ci}&si={si}"})
    if "server busy" in r.text.lower():
        return None
    return parse_seats(r.text)

async def book_chunk(ci, si, wid, seats, sem, results):
    """One worker books its assigned chunk. Isolated cookie jar + connection."""
    n = len(seats)
    async with sem:
        t0 = time.time()
        try:
            async with httpx.AsyncClient(follow_redirects=True, timeout=httpx.Timeout(12, connect=6)) as c:
                c.headers.update({"User-Agent": UA, "Accept-Language": "en-US,en;q=0.9"})

                r = await c.get("https://www.mclcinema.com/MCLSelectSeat.aspx",
                                params={"visLang": "2", "ci": ci, "si": si})
                mset = re.search(r'MovieSetId.{0,3}?(\d+)', r.text) or re.search(r'V-(\d+)\.(?:jpg|png)', r.text)
                if not mset:
                    results[wid] = f"W{wid}: no MovieSetId"; return 0
                r = await c.get("https://www.mclcinema.com/GetPurchaseIFrameURL.aspx",
                                params={"CinemaCodeID": ci, "FilmSessionId": si,
                                        "MovieSetId": mset.group(1), "Language": "en-US"},
                                headers={"Referer": str(r.url), "X-Requested-With": "XMLHttpRequest"})
                m = re.search(r'https://www4\.mclcinema\.com/MCL\.Front\.Ticketing\?[^"\']+', r.text)
                if not m:
                    results[wid] = f"W{wid}: no ticketing url"; return 0

                r = await c.get(m.group(0).replace("&amp;", "&"))
                forms = re.findall(r'<form[^>]*action="([^"]*)"[^>]*>(.*?)</form>', r.text, re.S)
                target = next((a for a, b in forms if "nonmember" in b.lower()), None)
                if not target:
                    results[wid] = f"W{wid}: no nonmember form"; return 0
                body_html = next(b for a, b in forms if a == target)
                fields = dict(re.findall(r'<input[^>]*name="([^"]*)"[^>]*value="([^"]*)"', body_html))
                r = await c.post(BASE + H.unescape(target), data=fields)

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
                stt_url = stt_url if stt_url.startswith("http") else BASE + stt_url
                await c.post(stt_url, data=payload,
                             headers={"X-Requested-With": "XMLHttpRequest", "Referer": str(r.url)})

                forms = re.findall(r'<form[^>]*action="([^"]*)"[^>]*>(.*?)</form>', ttype_html, re.S)
                target = next((a for a, b in forms if "/PickSeats" in a), None)
                if not target:
                    results[wid] = f"W{wid}: no pickseats form"; return 0
                body_html = next(b for a, b in forms if a == target)
                tok_m = re.search(r'name="__RequestVerificationToken"[^>]*value="([^"]*)"', body_html)
                r = await c.post(BASE + H.unescape(target), data={"__RequestVerificationToken": tok_m.group(1) if tok_m else ""},
                                 headers={"Referer": stt_url})
                pickseats_html = r.text

                data = {}
                for i, seat in enumerate(seats):
                    data[f"selectedSeats[{i}][AreaCode]"] = seat["areacode"]
                    data[f"selectedSeats[{i}][AreaNumber]"] = str(seat["area"])
                    data[f"selectedSeats[{i}][RowIndex]"] = str(seat["row"])
                    data[f"selectedSeats[{i}][ColumnIndex]"] = str(seat["col"])
                    data[f"selectedSeats[{i}][SeatName]"] = seat["sn"]
                data["languageCulture"] = "en-US"
                data["platform"] = "DesktopWeb"
                r = await c.post(f"{BASE}/MCL.Front.Ticketing/PickSeats/SubmitSelectedSeat",
                                 data=data,
                                 headers={"X-Requested-With": "XMLHttpRequest",
                                          "Referer": f"{BASE}/MCL.Front.Ticketing/PickSeats?language=en-US&source=DesktopWeb"})
                if r.status_code != 204:
                    try:
                        j = r.json()
                        msg = j[0].get("content", "?")[:40] if isinstance(j, list) and j else str(j)[:60]
                        results[wid] = f"W{wid}: ❌ {msg} ({time.time()-t0:.1f}s)"
                        return 0
                    except Exception:
                        results[wid] = f"W{wid}: ❌ http {r.status_code}"; return 0

                # payment form from the ORIGINAL pickseats page
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
                r = await c.post(BASE + H.unescape(target), data=f3)
                ok = "Payment" in str(r.url) or "payment" in r.text[:4000].lower()
                dt = time.time() - t0
                seats_str = ",".join(s["sn"] for s in seats)
                if ok:
                    results[wid] = f"W{wid}: ✅ [{seats_str}] BOOKED {dt:.1f}s"
                    return n
                results[wid] = f"W{wid}: post-payment fail {str(r.url)[:60]}"
                return 0
        except Exception as e:
            results[wid] = f"W{wid}: 💥 {str(e)[:70]}"
            return 0

async def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    debug = "--debug" in sys.argv
    ci = args[0] if args else "017"
    si = args[1] if len(args) > 1 else "127637"
    workers = int(args[2]) if len(args) > 2 else 8
    rounds = os.environ.get("BLAZE_ROUNDS")
    rounds = int(rounds) if rounds else None   # None = run forever

    idle_poll = float(os.environ.get("BLAZE_IDLE_POLL", "20"))   # secs between scans when full

    print(f"🔥 BLAZE v2 | ci={ci} si={si} workers={workers} seats/worker={SEATS_PER}")
    print(f"   ♾️  running until you stop it (Ctrl+C). Re-claims seats as unpaid claims expire.")
    total = 0
    rnd = 0
    t_start = time.time()
    t_last_booking = t_start

    async with httpx.AsyncClient(follow_redirects=True, timeout=httpx.Timeout(12, connect=6)) as sc:
        sc.headers.update({"User-Agent": UA})
        while rounds is None or rnd < rounds:
            rnd += 1
            t_fetch = time.time()
            try:
                seats = await fetch_seat_plan(sc, ci, si)
            except Exception as e:
                print(f"  [round {rnd}] seatplan error: {str(e)[:60]} — retrying...")
                await asyncio.sleep(5)
                continue
            if seats is None:
                print(f"  [round {rnd}] seatplan busy, waiting...")
                await asyncio.sleep(10)
                continue
            if not seats:
                idle = time.time() - t_last_booking
                print(f"  [round {rnd}] house currently full "
                      f"(full for {idle:.0f}s) — rescanning in {idle_poll:.0f}s for expired claims...")
                await asyncio.sleep(idle_poll)
                continue

            # chunks are disjoint within a round; across rounds, previously
            # claimed seats MAY legitimately return (expired claims) so no filter.
            fresh = seats
            chunks = [fresh[i:i+SEATS_PER] for i in range(0, len(fresh), SEATS_PER)]
            chunks = [c for c in chunks if c]
            print(f"  [round {rnd}] {len(fresh)} claimable ({len(seats)} avail, {time.time()-t_fetch:.1f}s fetch) -> {len(chunks)} chunks")
            if not chunks:
                await asyncio.sleep(idle_poll)
                continue

            sem = asyncio.Semaphore(workers)
            results = {}
            tasks = [book_chunk(ci, si, w, chunks[w], sem, results) for w in range(min(len(chunks), workers))]
            booked_list = await asyncio.gather(*tasks)

            booked = sum(booked_list)
            if booked:
                t_last_booking = time.time()
            total += booked
            for w in sorted(results):
                print(f"    {results[w]}")
            elapsed = time.time() - t_start
            print(f"  ── +{booked} | total {total} | elapsed {elapsed:.0f}s | {total/elapsed*60 if elapsed else 0:.0f}/min")
            if booked == 0:
                # seats visible but all rejected — likely just-claimed by others; brief pause
                await asyncio.sleep(6)

    print(f"\n🏁 TOTAL: {total} seats in {time.time()-t_start:.1f}s")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 stopped by user")