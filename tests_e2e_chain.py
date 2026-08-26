#!/usr/bin/env python3
"""E2E: full booking chain of blaze2 v3 against mock_mcl, offline.

Usage: ./.venv/bin/python tests_e2e_chain.py [port] [seats]
Requires mock_mcl.py started separately (or pass --start to spawn one).
"""
import asyncio
import os
import subprocess
import sys
import time

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 8640
SEATS = int(sys.argv[2]) if len(sys.argv) > 2 else 113

os.environ["MCL_WWW_BASE"] = f"http://127.0.0.1:{PORT}"
os.environ["MCL_INFO_BASE"] = f"http://127.0.0.1:{PORT}"
os.environ["MCL_TIX_BASE"] = f"http://127.0.0.1:{PORT}"

import httpx                                    # noqa: E402
import blaze2                                   # noqa: E402


async def run() -> int:
    gov = blaze2.shared_governor()
    pool = blaze2.WorkerPool(4)
    await pool.start()
    sc = httpx.AsyncClient(follow_redirects=True)
    sc.headers.update({"User-Agent": blaze2.UA})

    t0 = time.time()
    seats = await blaze2.fetch_seat_plan(sc, "017", "100001")
    t_fetch = time.time() - t0
    print(f"fetched {len(seats)} bookable seats in {t_fetch:.2f}s")
    assert len(seats) == SEATS, f"expected {SEATS} bookable seats"

    q: asyncio.Queue = asyncio.Queue()
    inflight: set = set()
    chunks = blaze2.enqueue_chunks(seats, q, inflight)
    assert len(inflight) == SEATS and q.qsize() == chunks

    results: dict = {}
    got = await blaze2.drain_chunks("017", "100001", "t", q, inflight, pool,
                                    asyncio.Semaphore(4), results, None, None,
                                    n_consumers=4)
    ok_chains = sum(1 for v in results.values() if "✅" in v)
    dt = time.time() - t0
    print(f"BOOKED {got}/{SEATS} seats via {ok_chains} chains in {dt:.2f}s "
          f"({got/dt*60:.0f} seats/min)")
    sample = sorted(results.items())[0][1]
    print("sample result:", sample[:70])

    st = (await sc.get(f"http://127.0.0.1:{PORT}/__stats")).json()
    expected_chunks = -(-SEATS // blaze2.SEATS_PER)
    keys = ("submit_ok_204", "submit_attempts", "busy_count",
            "booked_seats", "total_requests")
    print("mock:", {k: st.get(k) for k in keys})
    assert st["submit_ok_204"] == expected_chunks, \
        f"{st['submit_ok_204']} claims != {expected_chunks} chunks"
    assert st["busy_count"] == 0, "unexpected busy in low-concurrency scenario"
    assert st["booked_seats"] == SEATS, f"mock payments {st['booked_seats']} != {SEATS}"

    # seat map must now be empty of bookable seats
    after = await blaze2.fetch_seat_plan(sc, "017", "100001")
    assert after == [], f"house should be full, {len(after)} still free"
    print("inventory drained correctly")

    await pool.aclose()
    await sc.aclose()
    print("E2E CHAIN PASS ✅")
    return 0


def main() -> int:
    proc = None
    try:
        proc = subprocess.Popen(
            [sys.executable, "mock_mcl.py", "--port", str(PORT),
             "--seats", str(SEATS), "--claim-expiry", "600",
             "--busy-threshold", "0"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        deadline = time.time() + 10
        import urllib.request
        while True:
            try:
                urllib.request.urlopen(
                    f"http://127.0.0.1:{PORT}/healthz", timeout=0.5).read()
                break
            except Exception:
                if time.time() > deadline:
                    raise RuntimeError("mock did not become healthy")
                time.sleep(0.15)
        return asyncio.run(run())
    finally:
        if proc and proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()


if __name__ == "__main__":
    sys.exit(main())
