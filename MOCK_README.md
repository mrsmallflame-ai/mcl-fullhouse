# Mock & offline testing guide

Everything in this repo's test rig binds **127.0.0.1 only** — no packet ever
reaches a real mclcinema.com host. Dependencies: stdlib + httpx (already the
repo's only dep).

```bash
./.venv/bin/python -m py_compile *.py          # everything compiles
./.venv/bin/python governor.py                 # AIMD self-test (ALL PASS)
./.venv/bin/python tests_e2e_chain.py          # single-house chain E2E
./.venv/bin/python tests_live_queue.py         # queue mode fills 3 sessions
./.venv/bin/python tests_watchdog.py           # --poll reclaims expired claims
./.venv/bin/python bench_old_vs_new.py         # A/B/C old-vs-new benchmark
./.venv/bin/python bench_old_vs_new.py --scenario C   # pressure scenario alone
```

## How it works

* `mock_mcl.py` mirrors every endpoint blaze2/discover/fill_all touch:
  seat-plan HTML (`seatnum/status/row/column/areacode/area`, Normal+Vibrate
  bookable, Wheelchair not), MCLWebAPI2 discovery JSONs, prime page
  (MovieSetId), iframe URL, non-member form, ticket-type select
  (`code=/value=/price=/ticketTypeName=`), PickSeats token page,
  SubmitSelectedSeat (204 vs conflict JSON list), payment redirect.
  Flags: `--seats --claim-expiry --busy-threshold --latency-ms --ghost-seats`.
  Pressure model: over `--busy-threshold` requests/sec (sliding 1 s window),
  SeatPlan answers HTTP 200 "Server busy" (exactly like the real one) and all
  other endpoints answer 503 + `Retry-After`. `/__stats` exposes counters.
* Engines reach the mock through `MCL_WWW_BASE / MCL_INFO_BASE /
  MCL_TIX_BASE` (see `mclhosts.py`). The BASELINE is extracted with
  `git show main:blaze2.py` and its hardcoded hosts rewritten (including the
  dot-escaped regex literals) before import — see `extract_baseline()` in
  `bench_old_vs_new.py`, which also hard-gates that no real-host string
  survives into executed code.

## Interpreting results

Clean scenarios (A/B): both engines should pay every seat at ~100% success —
localhost hides TLS-reuse wins. Scenario C (throttled) is where they differ:
the old engine burns its rounds inside fixed 10 s busy-sleeps and pays ~nothing;
the new governor rides the limit at ~100% submit success.
