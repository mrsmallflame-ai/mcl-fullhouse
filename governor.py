#!/usr/bin/env python3
"""governor.py — global AIMD concurrency/rate controller (DESIGN.md §2).

One instance is shared by everything in the process (discovery, seat-plan
scanners, booking chains). Per host-class ("seatplan" | "www" | "tix") it
tracks a rolling pressure window and an effective concurrency cap:

* CLEAN_WINDOW (2.0s) with >= MIN_SAMPLES (8) samples and zero pressure
  -> additive increase  conc = min(max_conc, conc + 1), and the current
  backoff cooldown HALVES (fast recovery: re-saturation in seconds).
* Any pressure signal -> multiplicative decrease conc = max(1, conc * 0.5)
  IMMEDIATELY, plus a global cooldown starting at BACKOFF_BASE (0.5s),
  doubling per successive pressure event, capped at BACKOFF_CAP (20s).
  A larger Retry-After header always wins.
* acquire() grants slots up to the current cap; waiters are served FIFO by
  a per-host asyncio.Condition. While a cooldown is active, each arrival
  first sleeps full-jitter random.uniform(0, cooldown) seconds.
* classify(status, body_head) maps raw responses onto Signal so every
  caller reports pressure identically.

Env ceilings (see default_budgets):
    FULLHOUSE_MAX_CONC_SEATPLAN (default 8, init 2)
    FULLHOUSE_MAX_CONC_WWW      (default 8, init 4)
    FULLHOUSE_MAX_CONC_TIX      (default 24, init 6)

NO polling loops anywhere: waiters block on Conditions and are woken by
release()/report() events. Run `python3 governor.py` for the self-test.
"""
from __future__ import annotations

import asyncio
import enum
import math
import os
import random
import time
from dataclasses import dataclass, field

# --------------------------------------------------------------- tuning ----

CLEAN_WINDOW = 2.0     # s, rolling evaluation window
MIN_SAMPLES = 8        # samples required in a window before it can count clean
BACKOFF_BASE = 0.5     # s, first cooldown
BACKOFF_CAP = 20.0     # s, cooldown doubling cap
DECAY = 0.5            # multiplicative-decrease factor
COOLDOWN_EPS = 1e-3    # cooldowns below this are treated as "no cooldown"

BUSY_MARKER = "server busy"


class Signal(enum.Enum):
    """Outcome of one observed response."""
    OK = 0
    BUSY = 1        # 200 whose body contains "server busy"
    RETRYABLE = 2   # 429 / 5xx / network timeouts (usually carries Retry-After)


def classify(status: int, body_head: str) -> Signal:
    """Map (HTTP status, first bytes of body) onto a pressure Signal."""
    if status == 429 or status == 503 or status >= 500:
        return Signal.RETRYABLE
    if BUSY_MARKER in (body_head or "").lower():
        return Signal.BUSY
    return Signal.OK


def retry_after_seconds(headers) -> float | None:
    """Numeric Retry-After header value in seconds (HTTP-date form -> None).

    Accepts httpx.Headers (case-insensitive) and plain dicts."""
    raw = None
    if headers is not None:
        try:
            raw = headers.get("Retry-After") or headers.get("retry-after")
        except AttributeError:
            return None
    if not raw:
        return None
    try:
        return max(0.0, float(raw.strip()))
    except (ValueError, AttributeError):
        return None


class PressureBusy(Exception):
    """A caller-visible pressure abort (busy body / 429 / 5xx / timeout)."""

    def __init__(self, msg: str = "", retry_after: float | None = None):
        super().__init__(msg)
        self.retry_after = retry_after


# --------------------------------------------------------------- budgets ----

@dataclass
class HostBudget:
    name: str            # "seatplan" | "www" | "tix"
    max_conc: int        # hard ceiling
    init_conc: int       # startup level


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ[name])
    except (KeyError, TypeError, ValueError):
        return default


def default_budgets() -> dict[str, HostBudget]:
    """Per-host-class budgets; ceilings env-overridable, inits fixed."""
    return {
        "seatplan": HostBudget("seatplan",
                               _env_int("FULLHOUSE_MAX_CONC_SEATPLAN", 8), 2),
        "www": HostBudget("www",
                          _env_int("FULLHOUSE_MAX_CONC_WWW", 8), 4),
        "tix": HostBudget("tix",
                          _env_int("FULLHOUSE_MAX_CONC_TIX", 24), 6),
    }


# ------------------------------------------------------------ host state ----

@dataclass
class _HostState:
    budget: HostBudget
    conc: float                       # effective cap (float; limit = floor)
    inflight: int = 0
    win_start: float = field(default_factory=time.monotonic)
    ok_samples: int = 0               # OK reports accumulated this window
    pressure_seen: bool = False       # any pressure report this window
    streak: int = 0                   # successive pressure events (no clean
                                      # window in between) -> doubles cooldown
    cooldown: float = 0.0             # current cooldown magnitude (seconds)
    cooldown_until: float = 0.0       # monotonic deadline of the cooldown
    total_ok: int = 0
    total_pressure: int = 0


# -------------------------------------------------------------- governor ----

class Governor:
    """AIMD controller. acquire()/release() bracket every outbound request;
    report() feeds outcomes back in."""

    def __init__(self, budgets: dict[str, HostBudget]):
        self._states: dict[str, _HostState] = {}
        self._conds: dict[str, asyncio.Condition] = {}
        seen: set[str] = set()
        for name, b in budgets.items():
            if name in seen:
                raise ValueError(f"duplicate host budget: {name}")
            seen.add(name)
            b = HostBudget(b.name, max(1, int(b.max_conc)), max(1, int(b.init_conc)))
            self._states[b.name] = _HostState(
                budget=b, conc=float(min(b.init_conc, b.max_conc)))
            self._conds[b.name] = asyncio.Condition()

    # -- introspection -----------------------------------------------------

    def limit(self, host: str) -> int:
        """Current effective concurrency cap."""
        st = self._states[host]
        return max(1, min(st.budget.max_conc, int(math.floor(st.conc))))

    def snapshot(self) -> dict:
        """Cheap state dump for the unified stats line."""
        now = time.monotonic()
        out = {}
        for name, st in self._states.items():
            out[name] = {
                "limit": self.limit(name),
                "conc": round(st.conc, 2),
                "inflight": st.inflight,
                "cooldown": round(max(0.0, st.cooldown_until - now), 3),
                "ok": st.total_ok,
                "pressure": st.total_pressure,
            }
        return out

    # -- slot management (Condition-based; NO polling) ----------------------

    async def acquire(self, host: str) -> None:
        """Block until a slot under the current cap is granted (FIFO)."""
        st = self._states[host]
        now = time.monotonic()
        if st.cooldown > COOLDOWN_EPS:
            remaining = st.cooldown_until - now
            if remaining > 0:
                # full jitter: spread arrivals uniformly across the cooldown
                await asyncio.sleep(random.uniform(0.0, min(st.cooldown, remaining)))

        cond = self._conds[host]
        async with cond:
            await cond.wait_for(lambda: st.inflight < self.limit(host))
            st.inflight += 1

    def release(self, host: str) -> None:
        """Return a slot. Safe from sync code running inside the event loop."""
        st = self._states[host]
        if st.inflight > 0:
            st.inflight -= 1
        try:
            asyncio.get_running_loop().create_task(self._wake(host))
        except RuntimeError:
            pass                        # no waiters possible without a loop

    async def _wake(self, host: str) -> None:
        cond = self._conds[host]
        async with cond:
            cond.notify_all()

    # -- feedback ------------------------------------------------------------

    def report(self, host: str, signal: Signal,
               retry_after: float | None = None) -> None:
        """Feed one observed outcome back into the controller."""
        st = self._states[host]
        now = time.monotonic()

        if signal is Signal.OK:
            st.total_ok += 1
            st.ok_samples += 1
        elif signal in (Signal.BUSY, Signal.RETRYABLE):
            st.total_pressure += 1
            st.pressure_seen = True
            st.streak += 1
            # multiplicative decrease, floored at one slot
            st.conc = max(1.0, st.conc * DECAY)
            # exponential cooldown, doubling per successive pressure event
            cd = min(BACKOFF_CAP, BACKOFF_BASE * (2.0 ** (st.streak - 1)))
            if retry_after is not None and retry_after > cd:
                cd = retry_after                    # Retry-After wins if larger
            st.cooldown = float(cd)
            st.cooldown_until = now + st.cooldown

        grew = self._advance_window(st, now)
        if signal is Signal.OK or grew:
            try:
                asyncio.get_running_loop().create_task(self._wake(host))
            except RuntimeError:
                pass

    def _advance_window(self, st: _HostState, now: float) -> bool:
        """Evaluate the rolling window at most once per CLEAN_WINDOW.

        Returns True if the cap grew (waiters may proceed)."""
        if now - st.win_start < CLEAN_WINDOW:
            return False
        grew = False
        clean = (not st.pressure_seen) and st.ok_samples >= MIN_SAMPLES
        if clean:
            if st.conc < st.budget.max_conc:
                st.conc = min(float(st.budget.max_conc), st.conc + 1.0)
                grew = True
            # fast recovery: cooldown halves per clean window
            st.cooldown = max(0.0, st.cooldown / 2.0)
            if st.cooldown <= COOLDOWN_EPS:
                st.cooldown = 0.0
                st.cooldown_until = 0.0
            else:
                st.cooldown_until = min(st.cooldown_until, now + st.cooldown)
            st.streak = 0
        st.win_start = now
        st.ok_samples = 0
        st.pressure_seen = False
        return grew


# ---------------------------------------------------------- shared instance ----

_SHARED: Governor | None = None


def shared_governor() -> Governor:
    """Process-wide governor, created on first use with default budgets."""
    global _SHARED
    if _SHARED is None:
        _SHARED = Governor(default_budgets())
    return _SHARED


def set_shared_governor(g: Governor) -> None:
    """Install a custom-built governor (orchestrators with CLI overrides)."""
    global _SHARED
    _SHARED = g


# ------------------------------------------------------------- self-test ----

def _selftest() -> int:
    import sys
    failures: list[str] = []

    def check(label: str, cond: bool, extra: str = "") -> None:
        tag = "PASS" if cond else "FAIL"
        print(f"  [{tag}] {label}{(' — ' + extra) if extra else ''}")
        if not cond:
            failures.append(label)

    print("governor self-test — simulating pressure events")

    # 1. defaults + env ceilings -------------------------------------------------
    for var in ("FULLHOUSE_MAX_CONC_SEATPLAN", "FULLHOUSE_MAX_CONC_WWW",
                "FULLHOUSE_MAX_CONC_TIX"):
        os.environ.pop(var, None)
    b = default_budgets()
    check("default ceilings 8/8/24",
          (b["seatplan"].max_conc, b["www"].max_conc, b["tix"].max_conc) == (8, 8, 24))
    check("default inits 2/4/6",
          (b["seatplan"].init_conc, b["www"].init_conc, b["tix"].init_conc) == (2, 4, 6))
    os.environ["FULLHOUSE_MAX_CONC_SEATPLAN"] = "3"
    b2 = default_budgets()
    check("env ceiling override", b2["seatplan"].max_conc == 3)
    del os.environ["FULLHOUSE_MAX_CONC_SEATPLAN"]

    # 2. classify ------------------------------------------------------------------
    check("classify 200 ok", classify(200, "<html>fine</html>") is Signal.OK)
    check("classify busy body", classify(200, "Server Busy - try later") is Signal.BUSY)
    check("classify 429", classify(429, "") is Signal.RETRYABLE)
    check("classify 503", classify(503, "nope") is Signal.RETRYABLE)
    check("classify 500", classify(500, "") is Signal.RETRYABLE)

    # 3. slot gating (init caps concurrent acquisitions; FIFO drains after release)
    async def slot_test() -> None:
        gov = Governor(default_budgets())
        got: list[int] = []

        async def grab(i: int) -> None:
            await gov.acquire("tix")
            got.append(i)

        tasks = [asyncio.create_task(grab(i)) for i in range(10)]
        await asyncio.sleep(0.15)
        snap = gov.snapshot()["tix"]
        check("init caps inflight at 6", snap["inflight"] == 6,
              f"inflight={snap['inflight']}")
        for _ in range(6):
            gov.release("tix")
        await asyncio.gather(*tasks)
        check("released slots admit the rest", len(got) == 10)
        for _ in range(10):
            gov.release("tix")

    asyncio.run(slot_test())

    # 4. AIMD decrease + doubling cooldown (+cap) ------------------------------------
    gov = Governor(default_budgets())             # tix init 6
    cds = []
    for _ in range(7):
        gov.report("tix", Signal.BUSY)
        cds.append(round(gov.snapshot()["tix"]["cooldown"], 3))
    check("multiplicative decrease floors at 1",
          gov.limit("tix") == 1, f"limit={gov.limit('tix')}")
    check("cooldown doubles .5→1→2→4→8→16→20(cap)",
          cds == [0.5, 1.0, 2.0, 4.0, 8.0, 16.0, 20.0], f"{cds}")

    # 5. Retry-After wins when larger (fresh governor: base cooldown is 0.5s) --------
    gov_ra = Governor(default_budgets())
    gov_ra.report("tix", Signal.RETRYABLE, retry_after=9.0)
    rem = gov_ra.snapshot()["tix"]["cooldown"]
    check("Retry-After wins", abs(rem - 9.0) < 0.5, f"remaining≈{rem}")
    check("retry_after_seconds parses",
          retry_after_seconds({"Retry-After": "7"}) == 7.0
          and retry_after_seconds({"retry-after": "3"}) == 3.0)

    # 6. full-jitter gate delays arrivals, bounded by the cooldown ----------------------
    async def jitter_test() -> None:
        gov2 = Governor(default_budgets())
        gov2.report("tix", Signal.RETRYABLE, retry_after=1.0)
        t0 = time.monotonic()
        await gov2.acquire("tix")
        dt = time.monotonic() - t0
        check("arrival during cooldown is jitter-delayed within bound",
              0.0 <= dt <= 1.2, f"delay={dt:.3f}s")
        gov2.release("tix")

    asyncio.run(jitter_test())

    # 7. clean-window recovery: +1/window, cooldown halves -------------------------------
    async def recovery_test() -> None:
        gov3 = Governor({"tix": HostBudget("tix", 6, 2)})
        gov3.report("tix", Signal.BUSY)               # conc 2->1, cd 0.5
        check("pressured down to 1", gov3.limit("tix") == 1)
        limits, cds = [], []
        for _ in range(3):
            for _ in range(MIN_SAMPLES):
                gov3.report("tix", Signal.OK)
            await asyncio.sleep(CLEAN_WINDOW + 0.05)   # let the window elapse
            gov3.report("tix", Signal.OK)              # force window evaluation
            limits.append(gov3.limit("tix"))
            cds.append(round(gov3.snapshot()["tix"]["cooldown"], 3))
        # window 1 still CONTAINS the pressure event -> correctly not clean;
        # windows 2 and 3 are clean -> +1 each.
        check("+1 per clean (pressure-free) window", limits == [1, 2, 3], f"{limits}")
        check("cooldown halves per clean window",
              all(a >= b for a, b in zip(cds, cds[1:])) and cds[-1] <= 0.13,
              f"{cds}")

    asyncio.run(recovery_test())

    print(f"governor self-test: "
          f"{'ALL PASS' if not failures else str(len(failures)) + ' FAILURE(S)'}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(_selftest())

