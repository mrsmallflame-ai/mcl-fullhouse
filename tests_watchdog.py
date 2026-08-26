#!/usr/bin/env python3
"""Watchdog integration test: --poll mode reclaims freed (expired-claim) seats.

Mock seeds 12 ghost claims that expire after 3s; fill_all --poll must find
and pay ALL 50 seats of session 100001 within the window.
"""
import json
import os
import subprocess
import sys
import time
import urllib.request

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 8690
RUN_SECONDS = float(sys.argv[2]) if len(sys.argv) > 2 else 14

for k in ("MCL_WWW_BASE", "MCL_INFO_BASE", "MCL_TIX_BASE"):
    os.environ[k] = f"http://127.0.0.1:{PORT}"


def wait_health(deadline_s=10):
    dl = time.time() + deadline_s
    while time.time() < dl:
        try:
            urllib.request.urlopen(
                f"http://127.0.0.1:{PORT}/healthz", timeout=0.5).read()
            return
        except Exception:
            time.sleep(0.15)
    raise RuntimeError("mock not healthy")


def main() -> int:
    mock = subprocess.Popen(
        [sys.executable, "mock_mcl.py", "--port", str(PORT),
         "--seats", "50", "--claim-expiry", "3", "--ghost-seats", "12",
         "--busy-threshold", "0"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        wait_health()
        cmd = [sys.executable, "-u", "fill_all.py", "--live",
               "--poll", "1.5", "--cinema", "central", "--workers", "6"]
        t0 = time.time()
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, text=True)
        out = []
        import select
        while proc.poll() is None and time.time() - t0 < RUN_SECONDS:
            r, _, _ = select.select([proc.stdout], [], [], 1.0)
            if not r:
                continue
            line = proc.stdout.readline()
            if line:
                out.append(line.rstrip())
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
        for ln in out[-16:]:
            print(ln)

        st = json.loads(urllib.request.urlopen(
            f"http://127.0.0.1:{PORT}/__stats").read())
        print("mock:", {k: st.get(k) for k in
                        ("booked_seats", "seats_free", "expired_claims",
                         "submit_ok_204", "busy_count")})
        assert st["booked_seats"] == 50, \
            f"expected all 50 seats paid incl. reclaimed ghosts, got {st['booked_seats']}"
        assert st["expired_claims"] >= 12, "ghost claims never expired?"
        print("WATCHDOG RECLAIM PASS ✅")
        return 0
    finally:
        if mock.poll() is None:
            mock.terminate()
            try:
                mock.wait(timeout=5)
            except subprocess.TimeoutExpired:
                mock.kill()


if __name__ == "__main__":
    sys.exit(main())
