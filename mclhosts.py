"""Base-URL resolution for all MCL endpoints (DESIGN.md §1).

Defaults point at the real hosts; tests/benchmarks override via env:
    MCL_WWW_BASE   (default https://www.mclcinema.com)   pages + MCLWebAPI2
    MCL_INFO_BASE  (default https://info.mclcinema.com)  RealSeatPlan
    MCL_TIX_BASE   (default https://www4.mclcinema.com)  ticketing chain
"""
import os

_RESOLVED: tuple[str, str, str] | None = None


def hosts() -> tuple[str, str, str]:
    """Return (www, info, tix) base URLs, resolved once per process."""
    global _RESOLVED
    if _RESOLVED is None:
        _RESOLVED = (
            os.environ.get("MCL_WWW_BASE", "https://www.mclcinema.com").rstrip("/"),
            os.environ.get("MCL_INFO_BASE", "https://info.mclcinema.com").rstrip("/"),
            os.environ.get("MCL_TIX_BASE", "https://www4.mclcinema.com").rstrip("/"),
        )
    return _RESOLVED
