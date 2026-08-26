#!/usr/bin/env python3
"""Live-queue integration test: fill_all.py (queue mode) against mock_mcl.

Spawns its own mock, runs `fill_all --live` for ~18s, then asserts seats
were actually claimed/paid across all three canonical sessions.
"""
import json
import os
import subprocess
import sys
import time
import urllib.request

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 8660
SEATS = int(sys.argv[2]) if len(sys.argv) > 2 else 40
RUN_SECONDS = float(sys.argv[3]) if len(sys.argv) > 3 else 18

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
         "--seats", str(SEATS), "--busy-threshold", "0",
         "--claim-expiry", "600"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        wait_health()
        cmd = [sys.executable, "-u", "fill_all.py", "--live",
               "--houses", "2", "--workers", "4", "--limit-movies", "1"]
        t0 = time.time()
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, text=True)
        out_lines = []
        import select
        while proc.poll() is None and time.time() - t0 < RUN_SECONDS:
            r, _, _ = select.select([proc.stdout], [], [], 1.0)
            if not r:
                continue
            line = proc.stdout.readline()
            if line:
                out_lines.append(line.rstrip())
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
        for ln in out_lines[-14:]:
            print(ln)

        st = json.loads(urllib.request.urlopen(
            f"http://127.0.0.1:{PORT}/__stats").read())
        keys = ("booked_seats", "seats_free", "seats_claimed",
                "submit_ok_204", "submit_attempts", "busy_count")
        print("mock:", {k: st.get(k) for k in keys})
        expected = SEATS * 3                      # 3 canonical sessions
        assert st["booked_seats"] == expected, \
            f"booked {st['booked_seats']} != {expected}"
        assert st["busy_count"] == 0
        print(f"LIVE QUEUE PASS ✅ ({expected} seats paid across 3 sessions)")
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
