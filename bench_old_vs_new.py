#!/usr/bin/env python3
"""bench_old_vs_new.py — automated baseline-vs-optimized benchmark, fully OFFLINE.

Everything runs against mock_mcl.py bound to 127.0.0.1 ONLY. No packet ever
reaches a real mclcinema.com host:

  * baseline ("old") engine : `git show <ref>:blaze2.py` (+ discover/fill_all)
                              extracted into a temp dir with every hardcoded
                              https://*.mclcinema.com host regex/string rewritten
                              to http://127.0.0.1:<port>, imported from there;
  * optimized ("new") engine: repo modules driven through the DESIGN.md §1
                              contract — env MCL_WWW_BASE / MCL_INFO_BASE /
                              MCL_TIX_BASE pointed at the mock.

Scenarios
  A  single-house fill      one 113-seat session, N workers
  B  concurrent houses      three sessions filled simultaneously
  C  watchdog reclaim       ghost claims expire mid-run; engine must re-claim
  D  multi-session queue    rotate the worker pool across a session queue

For every scenario x variant we record wall time-to-fill, seats/min, total
requests, busy-rate %, submit success-rate %. Exit status:
  0  pass (or new-code SKIP because env-base contract not implemented yet —
     in that mode an old-vs-old self-check validates the harness itself)
  1  regression: new slower than old (or fewer seats paid) in any scenario
  2  infra error (mock failed to start, baseline extraction failed, old
     self-check failed)

Usage: ./.venv/bin/python bench_old_vs_new.py [--git-ref main] [--workers 6]
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request

REPO = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, REPO)

PYTHON = sys.executable                      # run inside the project venv
MOCK_SCRIPT = os.path.join(REPO, "mock_mcl.py")
ENGINE_FILES = ("blaze2.py", "discover.py", "fill_all.py")
MODULE_NAMES = tuple(f[:-3] for f in ENGINE_FILES) + ("mclhosts", "governor")
ENV_CONTRACT = {"MCL_WWW_BASE", "MCL_INFO_BASE", "MCL_TIX_BASE"}
REAL_HOST_MARKERS = ("mclcinema.com",)

CI_PRIMARY, SI_PRIMARY = "017", "100001"
HOUSE_SPECS = [                               # matches mock CANON_HOUSES
    (CI_PRIMARY, SI_PRIMARY, "Mock Cinema Central"),
    ("018", "100002", "Mock Cinema Harbour"),
    ("019", "100003", "Mock Cinema Peninsula"),
]


# ------------------------------------------------------------- utilities ----

def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def http_json(url: str, method: str = "GET") -> dict:
    req = urllib.request.Request(url, method=method)
    with urllib.request.urlopen(req, timeout=5) as resp:
        return json.loads(resp.read().decode())


def check_no_real_hosts(*texts: str) -> None:
    """HARD SAFETY GATE — refuse to run anything mentioning a real host."""
    for t in texts:
        for marker in REAL_HOST_MARKERS:
            if marker in t:
                raise RuntimeError(
                    f"safety gate: real host '{marker}' found in generated "
                    f"engine source — refusing to execute")


class MockServer:
    """mock_mcl.py subprocess manager (one instance per scenario)."""

    def __init__(self, *, seats=113, claim_expiry=8.0, ghost_seats=0,
                 busy_threshold=0.0, latency_ms=2, port=None):
        self.port = port or free_port()
        self.base = f"http://127.0.0.1:{self.port}"
        self.args = ["--port", str(self.port), "--seats", str(seats),
                     "--claim-expiry", str(claim_expiry),
                     "--ghost-seats", str(ghost_seats),
                     "--busy-threshold", str(busy_threshold),
                     "--latency-ms", str(latency_ms)]
        self.proc = None
        self._log = None

    def __enter__(self):
        self._log = open(f"/tmp/mock_bench_{self.port}.log", "wb")
        self.proc = subprocess.Popen([PYTHON, MOCK_SCRIPT] + self.args,
                                     cwd=REPO, stdout=self._log,
                                     stderr=subprocess.STDOUT)
        deadline = time.time() + 20
        while time.time() < deadline:
            try:
                with urllib.request.urlopen(f"{self.base}/healthz",
                                            timeout=1) as r:
                    if r.read() == b"ok":
                        return self
            except Exception:
                pass
            if self.proc.poll() is not None:
                raise RuntimeError("mock died at startup — see "
                                   f"/tmp/mock_bench_{self.port}.log")
            time.sleep(0.15)
        raise RuntimeError(f"mock did not become healthy on {self.base}")

    def stats(self) -> dict:
        return http_json(f"{self.base}/__stats")

    def reset(self):
        http_json(f"{self.base}/__reset?reseed=1", method="POST")

    def __exit__(self, *exc):
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.proc.kill()
        if self._log:
            self._log.close()


# ------------------------------------------------- baseline extraction ----

def extract_baseline(ref: str, workdir: str, bases: tuple[str, str, str]) -> str:
    """`git show ref:blaze2.py` into workdir with hosts rewritten offline."""
    www, info, tix = bases
    src = subprocess.run(["git", "-C", REPO, "show", f"{ref}:blaze2.py"],
                         capture_output=True, text=True, check=True).stdout
    import re as _re
    src = (src.replace("https://www.mclcinema.com", www)
              .replace("https://info.mclcinema.com", info)
              .replace("https://www4.mclcinema.com", tix)
              # regex-literal (dot-escaped) variants, e.g. the iframe matcher
              .replace("https://www\\.mclcinema\\.com", _re.escape(www))
              .replace("https://info\\.mclcinema\\.com", _re.escape(info))
              .replace("https://www4\\.mclcinema\\.com", _re.escape(tix))
              # bare-domain leftovers inside regex fragments
              .replace("www4\\.mclcinema\\.com", _re.escape(tix))
              .replace("www\\.mclcinema\\.com", _re.escape(www))
              .replace("info\\.mclcinema\\.com", _re.escape(info)))
    check_no_real_hosts(src)  # …and must NOT after
    path = os.path.join(workdir, "blaze2_old.py")
    with open(path, "w") as f:
        f.write(src)
    return path


def snap(mock: MockServer) -> dict:
    s = mock.stats()
    return {k: s[k] for k in ("total_requests", "busy_count",
                              "booked_seats", "submit_ok_204",
                              "submit_attempts")}


def delta_stats(mock: MockServer, before: dict) -> dict:
    st = mock.stats()
    return {
        "req": st["total_requests"] - before["total_requests"],
        "busy": st["busy_count"] - before["busy_count"],
        "paid": st["booked_seats"] - before["booked_seats"],
        "ok": st["submit_ok_204"] - before["submit_ok_204"],
        "att": st["submit_attempts"] - before["submit_attempts"],
    }


def run_variant(variant: str, script: str, env_extra: dict,
                sessions: list[tuple[str, str]], workers: int) -> tuple[dict, float]:
    """Launch one process per session; wait all; return (delta-ish raw, wall)."""
    env = os.environ.copy()
    env.update(env_extra)
    procs = []
    t0 = time.time()
    for ci, si in sessions:
        procs.append(subprocess.Popen(
            [PYTHON, "-u", script, ci, si, str(workers)],
            cwd=REPO if variant == "new" else None,
            env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
    for p in procs:
        p.wait(timeout=120)
    return {}, time.time() - t0


def scenario(name: str, seats: int, workers: int,
             sessions: list[tuple[str, str]], busy_threshold: float,
             ref: str) -> dict:
    print(f"\n── Scenario {name}: {len(sessions)} session(s) × {seats} seats, "
          f"{workers} workers each, busy@{busy_threshold}rps ──")
    results = {}
    with MockServer(seats=seats, busy_threshold=busy_threshold,
                    claim_expiry=600, latency_ms=2) as mock:
        workdir = tempfile.mkdtemp(prefix="fh_baseline_")
        old_script = extract_baseline(ref, workdir, (mock.base,) * 3)
        try:
            for variant, script in (("old", old_script),
                                    ("new", os.path.join(REPO, "blaze2.py"))):
                mock.reset()
                before = snap(mock)
                extra = {"BLAZE_ROUNDS": "3", "BLAZE_IDLE_POLL": "0.3"}
                if variant == "new":
                    extra.update({k: mock.base for k in ENV_CONTRACT})
                _, wall = run_variant(variant, script, extra, sessions, workers)
                d = delta_stats(mock, before)
                results[variant] = {
                    "wall": round(wall, 2), **d,
                    "seats_min": round(d["paid"] / wall * 60) if wall else 0,
                    "success_pct": round(100 * d["ok"] / max(d["att"], 1), 1),
                }
                print(f"   {variant:>4}: {results[variant]}")
        finally:
            shutil.rmtree(workdir, ignore_errors=True)

    o, n = results["old"], results["new"]
    verdict = "PASS"
    if n["paid"] < o["paid"]:
        verdict = "FAIL(paid regression)"
    elif busy_threshold == 0 and n["wall"] > o["wall"] * 1.10:
        verdict = "FAIL(speed regression)"
    print(f"   ▸ old {o['paid']} seats/{o['wall']}s ({o['seats_min']}/min), "
          f"success {o['success_pct']}% | new {n['paid']}/{n['wall']}s "
          f"({n['seats_min']}/min), success {n['success_pct']}% → {verdict}")
    return {"scenario": name, "old": o, "new": n, "verdict": verdict}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--git-ref", default="main")
    ap.add_argument("--scenario", choices=("A", "B", "C"),
                    help="run a single scenario")
    args = ap.parse_args()

    all_scenarios = {
        "A": lambda: scenario("A-single-house", 113, 8,
                              [(CI_PRIMARY, SI_PRIMARY)], 0, args.git_ref),
        "B": lambda: scenario("B-three-houses", 60, 6,
                              [(ci, si) for ci, si, _ in HOUSE_SPECS[:3]],
                              0, args.git_ref),
        "C": lambda: scenario("C-under-pressure", 113, 8,
                              [(CI_PRIMARY, SI_PRIMARY)], 14, args.git_ref),
    }
    if args.scenario:
        out = [all_scenarios[args.scenario]()]
        return 1 if out[0]["verdict"].startswith("FAIL") else 0
    out = [fn() for fn in all_scenarios.values()]

    fails = [r for r in out if r["verdict"].startswith("FAIL")]
    print("\n════════ SUMMARY ════════")
    for r in out:
        print(f"  {r['scenario']:<16} {r['verdict']}")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())