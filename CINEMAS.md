# MCL Cinemas — codes & names

All 14 MCL locations, with the code you can pass to `--cinema`.
This roster changes rarely; refresh it any time (needs network):

```bash
python3 discover.py --cinemas     # this table
python3 discover.py --movies      # current movie ids + titles
```

| code | cinema | `--cinema` value |
|---|---|---|
| `002` | MCL METRO CITY CINEMA (Po Lam Station) | `002` or `metro city` |
| `003` | GRAND WINDSOR CINEMA | `003` or `windsor` |
| `005` | MCL TELFORD CINEMA | `005` or `telford` |
| `009` | STAR CINEMA (Tseung Kwan O Station) | `009` or `star` |
| `012` | FESTIVAL GRAND CINEMA (Festival Walk) | `012` or `festival` |
| `013` | MCL GREEN CODE CINEMA (Fanling) | `013` or `green code` / `fanling` |
| `014` | MOVIE TOWN (New Town Plaza) | `014` or `new town` |
| `015` | MCL CHEUNG SHA WAN CINEMA | `015` or `cheung sha wan` |
| `016` | MCL CYBERPORT CINEMA | `016` or `cyberport` |
| `017` | K11 ART HOUSE (East Tsim Sha Tsui Station) | `017` or `k11` |
| `018` | MCL CITYGATE CINEMA | `018` or `citygate` |
| `019` | MCL AMOY CINEMA | `019` or `amoy` |
| `021` | MCL THE ONE CINEMA | `021` or `the one` |
| `022` | MCL AIRSIDE CINEMA (Kai Tak) | `022` or `airside` |

> Prefer the **code** — it is unambiguous. Substrings match case-insensitively
> against the venue's display name.

## One place, one movie — getting the names right

Both filters accept **either** form:

```text
--cinema  014                exact cinema code            (recommended)
--cinema  "new town"         case-insensitive substring of the venue name
--movie   14836              exact MovieSetId             (recommended)
--movie   "kung fu soccer"   case-insensitive substring of the title
```

### The one gotcha: version variants are *separate* movies

MCL lists each format as its own entry with its own id:

```text
14449  The Odyssey
       The Odyssey IMAX with Laser      <- different id, different sessions
       The Odyssey MX4D                 <- different id, different sessions
```

So:

```bash
# ALL Odyssey versions at Movie Town (substring matches every variant):
python3 fill_all.py --live --cinema 014 --movie "the odyssey" --poll 20

# ONLY the plain version there — pin the id from `discover.py --movies`:
python3 fill_all.py --live --cinema 014 --movie 14449 --poll 20
```

Find exact ids for today's programme with:

```bash
python3 discover.py --movies          # ids + titles
python3 discover.py --upcoming        # full session table (ids in context)
```

### Copy-paste recipes

```bash
# watchdog: one venue, one film, re-grab freed seats every 20s
python3 fill_all.py --live --cinema 014 --movie "kung fu soccer" --poll 20

# whole venue, every movie, until nothing is left, then exit
python3 fill_all.py --live --cinema 014 --poll 20 --drain

# rehearse any of the above without claiming anything: drop --live
python3 fill_all.py --cinema 014 --movie "kung fu soccer" --poll 20
```
