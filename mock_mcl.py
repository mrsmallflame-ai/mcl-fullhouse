#!/usr/bin/env python3
"""mock_mcl.py — OFFLINE mock of the MCL cinema API for FULLHOUSE benchmarking.

Binds 127.0.0.1 ONLY. Never talks to any real mclcinema.com host. Stdlib-only.

Endpoints mirrored from what blaze2.py / discover.py / fill_all.py actually send:
  www  /MCLSelectSeat.aspx                      GET  prime page (MovieSetId)
  www  /GetPurchaseIFrameURL.aspx               GET  -> absolute ticketing URL
  www  /MCLWebAPI2/GetNowShowingGrid.aspx       GET  -> discovery JSON
  www  /MCLWebAPI2/GetShowDays.aspx             GET  -> discovery JSON (r=free)
  info /RealSeatPlan/SeatPlan                   GET  -> seat-map HTML
  tix  /MCL.Front.Ticketing                     GET  -> entry forms (nonmember)
  tix  /MCL.Front.Ticketing/Login/NonMemberLogin POST -> ticket-type page
  tix  /MCL.Front.Ticketing/PickSeats/SubmitTicketTypes POST -> AJAX ack
  tix  /MCL.Front.Ticketing/PickSeats           POST -> pickseats page (payment form)
  tix  /MCL.Front.Ticketing/PickSeats/SubmitSelectedSeat POST -> 204 | conflict JSON
  tix  /MCL.Front.Ticketing/PickSeats/Payment   POST -> 302 -> /Payment/Success
  any  /__stats                                 GET  -> JSON counters for harness
  any  /__reset                                 POST/GET -> zero counters (+?reseed=1)

Pressure model ("only MCL limits us"):
  --busy-threshold RPS   sliding 1s-window request cap; over it,
                         /RealSeatPlan/SeatPlan answers HTTP 200 "Server busy"
                         (what fetch_seat_plan() classifies as busy) and every
                         other endpoint answers 503 + Retry-After.
  --latency-ms           artificial per-request delay.
  --claim-expiry SEC     unpaid claim TTL (scenario C reclaim tests).
  --ghost-seats N        pre-claim N seats at startup with staggered expiries.

Run:  ./.venv/bin/python mock_mcl.py --port 8618 --seats 113 --claim-expiry 8
Smoke: curl 'http://127.0.0.1:8618/RealSeatPlan/SeatPlan?cinemaCode=017&filmSessionId=100001'
Stats: curl http://127.0.0.1:8618/__stats  (JSON; see /__stats handler for keys)
"""
import argparse
import json
import re
import threading
import time
import uuid
from collections import deque
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs, quote

# ---------------------------------------------------------------- state ----

LOCK = threading.RLock()
ARGS = None                       # populated in main()
START_TS = time.time()

BOOKABLE = ("normal", "vibrate")  # wheelchair deliberately NOT bookable


class Seat:
    __slots__ = ("sn", "row", "col", "status", "areacode", "area",
                 "owner", "expires_at")

    def __init__(self, sn, row, col, status):
        self.sn, self.row, self.col, self.status = sn, row, col, status
        self.areacode, self.area = "0000000001", "1"
        self.owner = None          # claim tag when held
        self.expires_at = 0.0      # <= now means free


class House:
    """One film session: a grid of seats plus per-session counters."""

    def __init__(self, ci, si, nseats):
        self.ci, self.si = str(ci), str(si)
        self.seats = _layout(nseats)
        self.payments = 0          # seats that reached Payment/Success
        self.claims_ok = 0
        self.conflicts = 0

    def bookable(self):
        return [s for s in self.seats if s.status.lower() in BOOKABLE]

    def snapshot(self):
        b = self.bookable()
        free = sum(1 for s in b if s.expires_at <= time.time())
        return {"total_bookable": len(b), "free": free,
                "claimed": len(b) - free, "payments": self.payments}


def _layout(nseats):
    """Row-major grid; every 17th seat Vibrate; +2 wheelchair (unbookable)."""
    seats, cols, r = [], 12, 0
    while len(seats) < nseats:
        row_letter = chr(ord("A") + r % 26) * (1 + r // 26)
        for c in range(1, cols + 1):
            if len(seats) >= nseats:
                break
            status = "Vibrate" if (len(seats) + 1) % 17 == 0 else "Normal"
            seats.append(Seat(f"{row_letter}{c}", r + 1, c, status))
        r += 1
    seats.append(Seat(f"W{r}1", r + 1, 1, "Wheelchair"))
    seats.append(Seat(f"W{r}2", r + 1, 2, "Wheelchair"))
    return seats


HOUSES: dict[tuple[str, str], House] = {}


def get_house(ci, si):
    key = (str(ci), str(si))
    with LOCK:
        h = HOUSES.get(key)
        if h is None:
            h = HOUSES[key] = House(ci, si, ARGS.seats)
        return h


# ------------------------------------------------------------- counters ----

class Counters:
    def __init__(self):
        self.total = 0
        self.busy = 0
        self.by_path: dict[str, int] = {}
        self.submit_attempts = 0
        self.submit_ok = 0
        self.window = deque()             # arrival timestamps (1s window)
        self.recent = deque(maxlen=200)   # request log head

    def hit(self, method, path, head=""):
        now = time.time()
        with LOCK:
            self.total += 1
            key = path.split("?")[0]
            self.by_path[key] = self.by_path.get(key, 0) + 1
            self.window.append(now)
            cut = now - 1.0
            while self.window and self.window[0] < cut:
                self.window.popleft()
            rate = len(self.window)
            self.recent.append({"t": round(now - START_TS, 3),
                                "m": method, "p": path[:160], "b": head[:120]})
            return rate

    def over_threshold(self):
        with LOCK:
            return ARGS.busy_threshold > 0 and len(self.window) > ARGS.busy_threshold

    def note_busy(self):
        with LOCK:
            self.busy += 1

    def reset(self):
        with LOCK:
            self.total = self.busy = self.submit_attempts = self.submit_ok = 0
            self.by_path.clear()
            self.recent.clear()


CNT = Counters()
REAPER_STOP = threading.Event()
EXPIRED_CLAIMS = [0]              # total claims that lapsed and were freed


def reap_expired():
    """Free expired claims; returns number reclaimed this pass."""
    n, now = 0, time.time()
    with LOCK:
        for h in HOUSES.values():
            for s in h.seats:
                if s.owner and s.expires_at <= now:
                    s.owner, s.expires_at = None, 0.0
                    n += 1
    EXPIRED_CLAIMS[0] += n
    return n


def reaper_loop():
    while not REAPER_STOP.wait(0.25):
        try:
            reap_expired()
        except Exception:
            pass


def seed_ghost_claims(house):
    """Scenario C helper: hold seats with staggered short expiries."""
    span = max(ARGS.claim_expiry, 1)
    with LOCK:
        bookable = house.bookable()
        k = min(ARGS.ghost_seats, len(bookable))
        step = (span * 0.75) / max(k - 1, 1)
        for i, s in enumerate(bookable[:k]):
            s.owner = f"ghost-{i}"
            s.expires_at = time.time() + 0.5 + i * step


def stats_payload():
    with LOCK:
        sessions = {f"{h.ci}/{h.si}": h.snapshot() for h in HOUSES.values()}
        booked = sum(v["payments"] for v in sessions.values())
        free = sum(v["free"] for v in sessions.values())
        claimed = sum(v["claimed"] for v in sessions.values())
        attempts = CNT.submit_attempts or 1
        return {
            "uptime_s": round(time.time() - START_TS, 2),
            "total_requests": CNT.total,
            "busy_count": CNT.busy,
            "busy_rate_pct": round(100.0 * CNT.busy / max(CNT.total, 1), 2),
            "requests_last_1s": len(CNT.window),
            "by_path": dict(CNT.by_path),
            "submit_attempts": CNT.submit_attempts,
            "submit_ok_204": CNT.submit_ok,
            "success_rate_pct": round(100.0 * CNT.submit_ok / attempts, 2),
            "booked_seats": booked,
            "seats_free": free,
            "seats_claimed": claimed,
            "expired_claims": EXPIRED_CLAIMS[0],
            "sessions": sessions,
            "recent": list(CNT.recent),
        }


# ------------------------------------------------------------ html views ----

IMG = '<img src="/s.gif" seatnum="{sn}" status="{st}" row="{r}" column="{c}"' \
      ' areacode="{ac}" area="{ar}">'


def movie_set_id(si):
    return 14841 + (int(si) % 90000)


def seat_map_html(house):
    """Seat plan exactly as blaze2.parse_seats expects it."""
    now = time.time()
    rows = []
    rows.append('<div class="legend"><img src="/legend_normal.gif">'
                '<img src="/legend_vibrate.gif"><img src="/legend_wc.gif"></div>')
    rows.append(f'<div id="seatmap" data-ci="{house.ci}" data-si="{house.si}">')
    with LOCK:
        for s in house.seats:
            st = s.status
            if s.owner and s.expires_at > now:
                st = "Sold"                     # held claims are not bookable
            rows.append(IMG.format(sn=s.sn, st=st, r=s.row, c=s.col,
                                   ac=s.areacode, ar=s.area))
    rows.append("</div>")
    return "\n".join(rows)


def prime_html(ci, si):
    """MCLSelectSeat.aspx — blaze2 greps MovieSetId here."""
    mset = movie_set_id(si)
    return f"""<html><head><title>MCL Select Seat</title></head><body>
<script>var visLang=2;var MovieSetId = {mset};var cinemaCode='{ci}';var filmSessionId='{si}';</script>
<img src="/poster/V-{mset}.jpg" alt="poster">
<div id="seatplan-frame"></div>
</body></html>"""


def entry_html(ci, si, tid):
    """MCL.Front.Ticketing entry — first form whose BODY contains 'nonmember'
    wins; inputs must have name= BEFORE value=."""
    ret = quote(f"/MCL.Front.Ticketing/PickSeats?language=en-US&ci={ci}&si={si}", safe="")
    return f"""<html><body>
<form action="/MCL.Front.Ticketing/Login/NonMemberLogin?ci={ci}&amp;si={si}&amp;tid={tid}&amp;returnUrl={ret}" method="post">
<input type="hidden" name="loginType" value="nonmember">
<input type="hidden" name="sessionTid" value="{tid}">
<input type="submit" value="Continue as guest">
</form>
<form action="/MCL.Front.Ticketing/Login/MemberLogin?ci={ci}&amp;si={si}&amp;tid={tid}" method="post">
<input type="hidden" name="loginType" value="member">
</form>
</body></html>"""


def tickettype_html(ci, si, tid, max_qty):
    """Nonmember POST result: submitTicketTypes var + qty <select> + PickSeats
    form carrying __RequestVerificationToken."""
    opts = "\n".join(
        f'<option code="TT001" value="{q}" price="130.0" '
        f'ticketTypeName="Adult">Adult x{q}</option>' for q in range(1, max_qty + 1))
    return f"""<html><body>
<script>var submitTicketTypes = "/MCL.Front.Ticketing/PickSeats/SubmitTicketTypes?ci={ci}&si={si}&tid={tid}";</script>
<select id="ticketTypes" name="qty">
{opts}
</select>
<form action="/MCL.Front.Ticketing/PickSeats?ci={ci}&amp;si={si}&amp;tid={tid}&amp;stage=confirm" method="post">
<input type="hidden" name="__RequestVerificationToken" value="tok-{tid}">
<input type="hidden" name="step" value="pickseats">
<button type="submit">Pick Seats</button>
</form>
</body></html>"""


def pickseats_html(ci, si, tid):
    """PickSeats POST result: contains the payment form whose body holds the
    literal 'pickSeatSubmitButton'."""
    return f"""<html><body><h2>Pick your seats</h2>
<div id="canvas"></div>
<form action="/MCL.Front.Ticketing/PickSeats/Payment?ci={ci}&amp;si={si}&amp;tid={tid}" method="post" id="payform">
<input type="submit" name="pickSeatSubmitButton" value="Confirm &amp; Pay">
<input type="hidden" name="__RequestVerificationToken" value="tok-{tid}">
<input type="hidden" name="tid" value="{tid}">
</form>
</body></html>"""


# ----------------------------------------------------------- http server ----

PENDING: dict[str, tuple[str, str, int]] = {}   # tid -> (ci, si, seats claimed)


def cookie_val(handler, name):
    raw = handler.headers.get("Cookie", "")
    for part in raw.split(";"):
        k, _, v = part.strip().partition("=")
        if k == name:
            return v
    return None


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "MockMCL/1.0"

    def log_message(self, fmt, *args):        # silence default stderr chatter
        pass

    # ---- plumbing ----
    def _send(self, status, ctype, body=b"", extra=None):
        if isinstance(body, str):
            body = body.encode()
        self.send_response(status)
        if status != 204:
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
        for k, v in (extra or {}).items():
            if isinstance(v, (list, tuple)):        # e.g. several Set-Cookie
                for item in v:
                    self.send_header(k, str(item))
            else:
                self.send_header(k, v)
        self.end_headers()
        if status != 204 and body:
            self.wfile.write(body)

    def _cookie(self, ci, si):
        return {"Set-Cookie": f"MCLSI={si}; Path=/, MCLCI={ci}; Path=/"}

    def _gate(self, path):
        """Log arrival; apply busy pressure + artificial latency."""
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length) if length else b""
        head = body[:120].decode("utf-8", "replace") if body else ""
        CNT.hit(self.command, self.path, head)
        if path.startswith("/__") or path == "/healthz":
            return body, False
        if CNT.over_threshold():
            CNT.note_busy()
            if path.startswith("/RealSeatPlan/SeatPlan"):
                self._send(200, "text/html; charset=utf-8",
                           "<html><body>Server busy - please try again later.</body></html>")
            else:
                self._send(503, "text/plain", "Server busy",
                           {"Retry-After": "1"})
            return None, True
        if ARGS.latency_ms > 0:
            time.sleep(ARGS.latency_ms / 1000.0)
        return body, False

    def _q(self):
        return {k: v[0] for k, v in parse_qs(urlparse(self.path).query).items()}

    # ---- verbs ----
    def do_GET(self):
        self._route(body=b"")

    def do_POST(self):
        body, busy = self._gate(urlparse(self.path).path)
        if busy:
            return
        self._route(body=body)

    # GET wraps the same gate (body empty)
    def _route(self, body):
        path = urlparse(self.path).path
        if self.command == "GET":
            body, busy = self._gate(path)
            if busy:
                return
        q = self._q()

        if path == "/__stats":
            self._send(200, "application/json", json.dumps(stats_payload()))
        elif path == "/healthz":
            self._send(200, "text/plain", "ok")
        elif path == "/__reset":
            reap_expired()
            CNT.reset()
            EXPIRED_CLAIMS[0] = 0
            if q.get("reseed") == "1":
                HOUSES.clear()
                PENDING.clear()
                _seed_everything()
            self._send(200, "application/json", json.dumps({"ok": True}))
        elif path == "/RealSeatPlan/SeatPlan":
            h = get_house(q.get("cinemaCode", ARGS.cinema),
                          q.get("filmSessionId", "100001"))
            self._send(200, "text/html; charset=utf-8", seat_map_html(h))
        elif path == "/MCLSelectSeat.aspx":
            ci = q.get("ci", ARGS.cinema)
            si = q.get("si", "100001")
            get_house(ci, si)
            self._send(200, "text/html; charset=utf-8",
                       prime_html(ci, si),
                       extra=self._cookies(ci, si,
                                           uuid.uuid4().hex[:12]))
        elif path == "/GetPurchaseIFrameURL.aspx":
            ci, si = q.get("CinemaCodeID", ARGS.cinema), q.get("FilmSessionId", "100001")
            tid = uuid.uuid4().hex[:12]
            host = self.headers.get("Host", f"127.0.0.1:{ARGS.port}")
            url = f"http://{host}/MCL.Front.Ticketing?ci={ci}&si={si}&tid={tid}"
            get_house(ci, si)
            self._send(200, "text/html; charset=utf-8",
                       f'<html><script>var frameUrl="{url}";</script></html>',
                       extra=self._cookies(ci, si, tid))
        elif path == "/MCLWebAPI2/GetNowShowingGrid.aspx":
            self._send(200, "application/json", json.dumps(
                {"movies": [{"id": str(movie_set_id(100001)), 
                             "mn": "Mock Film: The Reckoning", "t": "S"}]}))
        elif path == "/MCLWebAPI2/GetShowDays.aspx":
            self._send(200, "application/json", json.dumps(_showdays_json()))
        elif path in ("/MCL.Front.Ticketing", "/MCL.Front.Ticketing/"):
            self._ticketing_entry(q)
        elif path == "/MCL.Front.Ticketing/Login/NonMemberLogin":
            self._nonmember_login(q)
        elif path == "/MCL.Front.Ticketing/Login/MemberLogin":
            self._ticketing_entry(q)
        elif path == "/MCL.Front.Ticketing/PickSeats/SubmitTicketTypes":
            self._send(200, "text/plain", "ok")
        elif path == "/MCL.Front.Ticketing/PickSeats":
            self._pickseats_page(q)
        elif path == "/MCL.Front.Ticketing/PickSeats/SubmitSelectedSeat":
            self._submit_selected_seat(body, q)
        elif path == "/MCL.Front.Ticketing/PickSeats/Payment":
            self._payment(q, body)
        elif path.startswith("/Payment"):
            self._send(200, "text/html; charset=utf-8",
                       "<html><body><h1>Payment Success</h1>"
                       "<p>Your seats are confirmed.</p></body></html>")
        else:
            self._send(404, "text/plain", f"mock_mcl: no route for {path}")

    # ---- booking-context resolution ------------------------------------

    def _ctx(self, q):
        """(ci, si, tid): query string wins, then cookies, then defaults."""
        ci = q.get("ci") or cookie_val(self, "MCLCI") or ARGS.cinema
        si = q.get("si") or cookie_val(self, "MCLSI") or "100001"
        tid = (q.get("tid") or cookie_val(self, "MCLETID")
               or uuid.uuid4().hex[:12])
        return str(ci), str(si), str(tid)

    @staticmethod
    def _form(body):
        """urlencoded POST body -> flat dict (blank values kept)."""
        if not body:
            return {}
        return {k: v[0] for k, v in
                parse_qs(body.decode("utf-8", "replace"),
                         keep_blank_values=True).items()}

    def _cookies(self, ci, si, tid):
        """Separate Set-Cookie headers (comma-joined confuses some clients)."""
        return {"Set-Cookie": [f"MCLCI={ci}; Path=/",
                               f"MCLSI={si}; Path=/",
                               f"MCLETID={tid}; Path=/"]}

    # ---- www4 ticketing chain -------------------------------------------

    def _ticketing_entry(self, q):
        ci, si, tid = self._ctx(q)
        get_house(ci, si)
        self._send(200, "text/html; charset=utf-8",
                   entry_html(ci, si, tid), extra=self._cookies(ci, si, tid))

    def _nonmember_login(self, q):
        ci, si, tid = self._ctx(q)
        get_house(ci, si)
        self._send(200, "text/html; charset=utf-8",
                   tickettype_html(ci, si, tid, max(1, int(ARGS.seats))),
                   extra=self._cookies(ci, si, tid))

    def _pickseats_page(self, q):
        ci, si, tid = self._ctx(q)
        get_house(ci, si)
        self._send(200, "text/html; charset=utf-8",
                   pickseats_html(ci, si, tid),
                   extra=self._cookies(ci, si, tid))

    # ---- atomic seat claiming -------------------------------------------

    _SEATFIELD = re.compile(r"^selectedSeats\[(\d+)\]\[([A-Za-z]+)\]$")

    def _submit_selected_seat(self, body, q):
        """All-or-nothing claim. Success -> 204 empty; any conflict -> 200
        JSON list [{'title': 'Seat taken', 'content': ...}] and NOTHING held."""
        form = self._form(body)
        picks: dict[int, dict] = {}
        for key, val in form.items():
            m = self._SEATFIELD.match(key)
            if m:
                picks.setdefault(int(m.group(1)), {})[m.group(2)] = val
        sns = [f["SeatName"] for _, f in sorted(picks.items())
               if f.get("SeatName")]
        if not sns:
            return self._send(200, "application/json", json.dumps(
                [{"title": "No seats", "content": "no selectedSeats posted"}]))
        ci, si, tid = self._ctx(q)
        house = get_house(ci, si)
        now = time.time()
        with LOCK:
            reap_expired()                       # RLock: lapsed claims free first
            wanted, conflicts, seen = [], [], set()
            for sn in sns:
                low = sn.lower()
                if low in seen:
                    continue
                seen.add(low)
                seat = next((x for x in house.seats
                             if x.sn.lower() == low), None)
                if (seat is None or seat.status.lower() not in BOOKABLE
                        or (seat.owner and seat.expires_at > now)):
                    conflicts.append(sn)
                else:
                    wanted.append(seat)
            CNT.submit_attempts += 1
            if conflicts:
                house.conflicts += len(conflicts)
                return self._send(200, "application/json", json.dumps(
                    [{"title": "Seat taken",
                      "content": f"Seat {conflicts[0]} has just been taken by "
                                 f"another customer. Please choose again."}]))
            exp = time.time() + ARGS.claim_expiry
            for seat in wanted:
                seat.owner, seat.expires_at = tid, exp
            house.claims_ok += len(wanted)
            prev = PENDING.get(tid, (ci, si, 0))
            PENDING[tid] = (ci, si, prev[2] + len(wanted))
            CNT.submit_ok += 1
        return self._send(204, "application/json")

    def _payment(self, q, body):
        """Mark this tid's claimed seats permanently sold, then redirect."""
        form = self._form(body)
        tid = str(q.get("tid") or form.get("tid")
                  or cookie_val(self, "MCLETID") or "")
        rec = PENDING.pop(tid, None) if tid else None
        ci = rec[0] if rec else (q.get("ci") or cookie_val(self, "MCLCI")
                                 or ARGS.cinema)
        si = rec[1] if rec else (q.get("si") or cookie_val(self, "MCLSI")
                                 or "100001")
        house = get_house(ci, si)
        paid = 0
        with LOCK:
            for s in house.seats:
                if tid and s.owner == tid:
                    s.status = "Sold"            # paid seats never come back
                    s.owner, s.expires_at = None, 0.0
                    paid += 1
            house.payments += paid
        self._send(302, "text/html; charset=utf-8", "",
                   {"Location": f"/Payment/Success?tid={quote(tid)}&seats={paid}"})


# ------------------------------------------------- discovery + lifecycle ----

SHOWDAY_SN = "Sat, Jan 3, 11:00 PM, House 1 $130"   # parseable by discover._parse_sn
CANON_HOUSES = (("018", "100002", "Mock Cinema Harbour"),
                ("019", "100003", "Mock Cinema Peninsula"))


def _showdays_json():
    """GetShowDays.aspx payload in exactly the shape discover.get_movie_sessions
    walks: versions[] -> sd[] -> c[] -> s[], each session carrying si / sn /
    r where 'r' is the CURRENT free bookable-seat count."""
    showdate = (datetime.now() + timedelta(days=7)).strftime("%Y-%m-%d")
    cinemas = []
    for ci, si, cn in [(ARGS.cinema, "100001", "Mock Cinema Central"),
                       *CANON_HOUSES]:
        snap = get_house(ci, si).snapshot()
        cinemas.append({"ci": ci, "cn": cn,
                        "s": [{"si": int(si), "sn": SHOWDAY_SN,
                               "r": snap["free"], "price": 130}]})
    return [{"v": "English", "vn": "English Version",
             "sd": [{"ShowDate": showdate, "c": cinemas}]}]


def _seed_everything():
    """Pre-create the canonical houses so /__stats shows real inventory from
    t0; optional ghost claims on the primary house (scenario C)."""
    primary = get_house(ARGS.cinema, "100001")
    if ARGS.ghost_seats > 0:
        seed_ghost_claims(primary)
    for ci, si, _cn in CANON_HOUSES:
        get_house(ci, si)


def main():
    global ARGS
    p = argparse.ArgumentParser(
        description="OFFLINE mock of the MCL cinema API — binds 127.0.0.1 ONLY, "
                    "never contacts any mclcinema.com host")
    p.add_argument("--port", type=int, default=8618)
    p.add_argument("--seats", type=int, default=113,
                   help="bookable seats per house")
    p.add_argument("--busy-threshold", type=float, default=25.0,
                   help="requests/sec (sliding 1s window) that trips busy mode; "
                        "0 disables pressure simulation")
    p.add_argument("--latency-ms", type=int, default=10,
                   help="artificial per-request latency")
    p.add_argument("--claim-expiry", type=float, default=8.0,
                   help="unpaid seat-claim TTL in seconds")
    p.add_argument("--ghost-seats", type=int, default=0,
                   help="pre-claim N seats at startup with staggered expiries")
    p.add_argument("--cinema", default="017", help="primary cinemaCode id")
    ARGS = p.parse_args()

    _seed_everything()
    threading.Thread(target=reaper_loop, daemon=True,
                     name="claim-reaper").start()

    srv = ThreadingHTTPServer(("127.0.0.1", ARGS.port), Handler)
    srv.daemon_threads = True
    print(f"mock_mcl listening on http://127.0.0.1:{ARGS.port} "
          f"(seats={ARGS.seats}/house, busy@{ARGS.busy_threshold}rps, "
          f"latency={ARGS.latency_ms}ms, claim-expiry={ARGS.claim_expiry}s) "
          f"— offline only", flush=True)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        REAPER_STOP.set()
        srv.server_close()


if __name__ == "__main__":
    main()



