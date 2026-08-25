# MCL FULLHOUSE — Status

## Usage (now)

```bash
python3 fill_all.py            # dry-run: full programme plan + read-only probes
python3 fill_all.py --live     # fill every upcoming showtime of every movie
```

No link, no theatre name, no session id. The programme is discovered live.

## ✅ Round 1: FULLHOUSE build-out
- [x] `discover.py` — full-programme enumeration via the site's own JSON API
      (`MCLWebAPI2/GetNowShowingGrid` → `GetShowDays`), with house, price,
      remaining-seat count and HK-time filtering
- [x] Verified live: **1,697 upcoming sessions / 61 movies / 14 cinemas** in one pass
- [x] `fill_all.py` orchestrator — queue + N concurrent house-fillers reusing
      blaze2's `fetch_seat_plan`/`book_chunk` unchanged; periodic rediscovery;
      per-movie stats; dry-run by default, `--live` gated behind a 5s abort window
- [x] Dry-run verified against live site: probes report real free-seat counts,
      zero claims made

## 🔜 Round 2 candidates
- [ ] Live-mode soak test across a single cinema first (`--limit-movies 1`)
- [ ] Respect 429/server-busy backoff signals globally, not per-session
- [ ] Optional Telegram/webhook summary at end of each rediscovery cycle
- [ ] Session-end detection to stop revisiting finished screenings
