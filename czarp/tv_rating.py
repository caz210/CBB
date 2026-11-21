"""Pre-game TV Rating (0–100) for a slate game.

A CZarp analogue of a pre-game watchability score: quality of the weaker
team, closeness of the CZarp win probability, and a small pace lean. It
does not use Vegas lines and is not a pick or a stake.

    tv_rating = 100 × quality × (0.5 + 0.5 × closeness) × pace_factor

quality      = D-I percentile of the weaker team's rating
               (1 at the best team, 0 at the worst). Ranks come from
               high-precision AdjEM = AdjOE − AdjDE, not the 2-decimal
               team_profiles columns.
closeness    = 1 − 2 × |CZarp home win% − 0.5|
               (the same home_win_prob stored on the czarp-v2 card)
pace_factor  = 0.85 + 0.15 × clip((possessions − 62) / 20, 0, 1)
               or, if possessions are missing, the same shape on the
               CZarp total: 0.85 + 0.15 × clip((total − 130) / 40, 0, 1)

The result is rounded to one decimal and clamped to [0, 100]. Missing
quality or win probability yields None. Venue (home / neutral) does not
enter the formula; it is already in the card's win probability.
"""
from __future__ import annotations

from typing import Mapping

import pandas as pd

# Pace lean: possessions of 62 or below sit at 0.85; 82 or above sit at 1.00.
PACE_POSS_FLOOR = 62.0
PACE_POSS_SPAN = 20.0
# Fallback when a row has no possessions (same 0.85–1.00 band).
PACE_TOTAL_FLOOR = 130.0
PACE_TOTAL_SPAN = 40.0
PACE_MIN = 0.85
PACE_SPAN = 0.15


def _f(v) -> float | None:
    if v is None or v == "":
        return None
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    if x != x:  # NaN
        return None
    return x


def _clip01(x: float) -> float:
    return 0.0 if x < 0 else 1.0 if x > 1 else x


def quality_by_team(ratings: pd.DataFrame) -> dict[str, float]:
    """Map team name → quality in [0, 1] from high-precision AdjOE − AdjDE.

    Rank 1 (highest AdjEM) is 1.0; the last team is 0.0:
        quality = 1 − (rank − 1) / (n − 1)
    Ties are broken by team name so ranks are unique and stable. Teams
    with a missing AdjOE or AdjDE are omitted (callers treat that as
    missing quality).
    """
    if ratings is None or ratings.empty or "TeamName" not in ratings.columns:
        return {}
    if "AdjOE" not in ratings.columns or "AdjDE" not in ratings.columns:
        return {}
    em = pd.to_numeric(ratings["AdjOE"], errors="coerce") - pd.to_numeric(ratings["AdjDE"], errors="coerce")
    df = pd.DataFrame({"name": ratings["TeamName"].astype(str), "em": em}).dropna(subset=["em"])
    if df.empty:
        return {}
    df = df.sort_values(["em", "name"], ascending=[False, True], kind="mergesort")
    n = len(df)
    if n == 1:
        return {df.iloc[0]["name"]: 1.0}
    # rank 1..n after the sort above
    denom = n - 1
    return {name: 1.0 - i / denom for i, name in enumerate(df["name"].tolist())}


def quality_from_data(data: dict | None) -> dict[str, float]:
    """Read quality from a daily_run / model.load_data() payload."""
    if not data:
        return {}
    ratings = data.get("ratings")
    if ratings is None:
        return {}
    return quality_by_team(ratings)


def pace_factor(possessions=None, total=None) -> float | None:
    """Small pace lean in [0.85, 1.00]. Prefers possessions; else the CZarp total."""
    poss = _f(possessions)
    if poss is not None:
        return PACE_MIN + PACE_SPAN * _clip01((poss - PACE_POSS_FLOOR) / PACE_POSS_SPAN)
    tot = _f(total)
    if tot is not None:
        return PACE_MIN + PACE_SPAN * _clip01((tot - PACE_TOTAL_FLOOR) / PACE_TOTAL_SPAN)
    return None


def closeness(win_prob) -> float | None:
    """1 at a pick'em, 0 at 0% or 100%."""
    p = _f(win_prob)
    if p is None:
        return None
    return 1.0 - 2.0 * abs(p - 0.5)


def tv_rating(quality, win_prob, possessions=None, total=None) -> float | None:
    """Return a 0–100 score rounded to one decimal, or None if inputs are missing."""
    q = _f(quality)
    close = closeness(win_prob)
    pace = pace_factor(possessions, total)
    if q is None or close is None or pace is None:
        return None
    score = 100.0 * q * (0.5 + 0.5 * close) * pace
    if score < 0:
        score = 0.0
    elif score > 100:
        score = 100.0
    return round(score, 1)


def game_tv_rating(quality_map: Mapping[str, float], home: str, away: str,
                   win_prob, possessions=None, total=None) -> float | None:
    """TV Rating for a game from a precomputed quality map and the card numbers.

    Uses the weaker team's quality. Missing either team, or missing win
    probability / pace, returns None. Neutral vs home does not matter here.
    """
    q_home = quality_map.get(str(home)) if quality_map is not None else None
    q_away = quality_map.get(str(away)) if quality_map is not None else None
    if q_home is None or q_away is None:
        return None
    return tv_rating(min(q_home, q_away), win_prob, possessions=possessions, total=total)
