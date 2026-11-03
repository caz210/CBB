"""Season helpers. KenPom/NCAA seasons are named by their ending year
(2026-27 season -> 2027). New seasons roll over on July 1."""
from __future__ import annotations

from datetime import date, datetime
from zoneinfo import ZoneInfo

CENTRAL = ZoneInfo("America/Chicago")
ROLLOVER_MONTH = 7  # July: offseason, next season's ratings take over


def season_for_date(d: date | datetime | str | None = None) -> int:
    """Return the KenPom season year (ending year) for a given date.

    Nov 2026 -> 2027, Mar 2027 -> 2027, Aug 2027 -> 2028.
    """
    if d is None:
        d = datetime.now(CENTRAL).date()
    elif isinstance(d, str):
        d = date.fromisoformat(d[:10])
    elif isinstance(d, datetime):
        d = d.date()
    return d.year + 1 if d.month >= ROLLOVER_MONTH else d.year


def season_label(season: int) -> str:
    """2027 -> '2026-27'."""
    return f"{season - 1}-{str(season)[-2:]}"


def season_progress(d: date | str | None = None) -> float:
    """0.0 at Nov 1, 1.0 at Apr 10 of the season (clipped). Used for late-season weighting."""
    if d is None:
        d = datetime.now(CENTRAL).date()
    elif isinstance(d, str):
        d = date.fromisoformat(d[:10])
    season = season_for_date(d)
    start = date(season - 1, 11, 1)
    end = date(season, 4, 10)
    frac = (d - start).days / (end - start).days
    return max(0.0, min(1.0, frac))
