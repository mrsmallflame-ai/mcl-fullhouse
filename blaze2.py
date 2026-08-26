"""
mcl-fullhouse: New Design (100x Faster)
--------------------------------------
Run 9 parallel instances simultaneously, each with a different IP address to bypass rate limits.
Each instance books ~13–14 seats in one go (instead of 6), covering the full 120-seat theatre across all 9 workers.
This way, we book all 120 seats in one massive parallel operation, rather than many small sequential steps.

Author: MrSmallFlame
Date: 2026-08-26
"""
