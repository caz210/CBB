"""Weekly CZarp team rankings: projected margin vs an average D-I team, neutral floor.

The rating is the same margin the matchup maker would produce if the team
played a league-average opponent at a neutral site (czarp.matchup._v2_margin
/ public.project_matchup). It is not a KenPom rank, though the efficiencies
themselves come from KenPom.

Official snapshots are intended for Monday 7 AM America/Chicago, written by
weekly_rankings.py (also invoked from the daily run once that week's Monday
7 AM has passed, including mid-week catch-up). This module does not write
picks or touch the hash chain.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd

from czarp.matchup import _nan, _v2_margin

CT = ZoneInfo("America/Chicago")


def monday_on_or_before(d: date) -> date:
    return d - timedelta(days=d.weekday())  # Monday = 0


def average_opponent(inputs: list[dict], consts: dict) -> dict:
    """A composite D-I-average team: league avg efficiency and tempo, kp_pct 0.5,
    unit score = mean of teams that have one."""
    units = [float(r["unit_v2"]) for r in inputs if r.get("unit_v2") is not None and not _nan(r.get("unit_v2"))]
    return {
        "oe": float(consts["avg_eff"]),
        "de": float(consts["avg_eff"]),
        "tempo": float(consts["avg_tempo"]),
        "kp_pct": 0.5,
        "unit_v2": (sum(units) / len(units)) if units else None,
        "hca": 0.0,
    }


def _team_dict(row: dict) -> dict | None:
    needed = ("oe", "de", "tempo", "kp_pct")
    if any(row.get(k) is None or _nan(row.get(k)) for k in needed):
        return None
    return {
        "oe": float(row["oe"]), "de": float(row["de"]), "tempo": float(row["tempo"]),
        "kp_pct": float(row["kp_pct"]),
        "unit_v2": None if row.get("unit_v2") is None or _nan(row.get("unit_v2")) else float(row["unit_v2"]),
        "hca": 0.0 if row.get("hca") is None or _nan(row.get("hca")) else float(row["hca"]),
    }


def rating_vs_average(team: dict, avg: dict, consts: dict) -> float:
    """Projected margin (points) for `team` vs the average opponent, neutral floor."""
    return float(_v2_margin(team, avg, None, consts))


def di_records(games: list[dict], through: date) -> dict[str, tuple[int, int]]:
    """W-L from stored finals with date <= through, keyed by team name. D-I vs D-I only."""
    rec = defaultdict(lambda: [0, 0])
    for g in games:
        if g.get("status") != "final":
            continue
        try:
            gd = date.fromisoformat(str(g["date"]))
        except (TypeError, ValueError):
            continue
        if gd > through:
            continue
        hs, aws = g.get("home_score"), g.get("away_score")
        if hs is None or aws is None or hs == aws:
            continue
        hw = int(hs) > int(aws)
        rec[g["home"]][0] += int(hw)
        rec[g["home"]][1] += int(not hw)
        rec[g["away"]][0] += int(not hw)
        rec[g["away"]][1] += int(hw)
    return {n: (w, l) for n, (w, l) in rec.items()}


def kp_records(ratings: pd.DataFrame | None) -> dict[str, tuple[int, int]]:
    if ratings is None or ratings.empty:
        return {}
    out = {}
    for _, r in ratings.iterrows():
        name = str(r["TeamName"])
        try:
            out[name] = (int(r["Wins"]), int(r["Losses"]))
        except (TypeError, ValueError, KeyError):
            continue
    return out


def build_poll(inputs: list[dict], consts: dict, teams: list[dict],
               games: list[dict], as_of: date,
               ratings: pd.DataFrame | None = None,
               prev: list[dict] | None = None) -> list[dict]:
    """Return snapshot rows (no team_id missing, ranked 1..n, ties broken by name).

    `inputs` are matchup_inputs-shaped dicts with team_id, oe, de, tempo, kp_pct, unit_v2.
    `teams` have id, name. `prev` are previous official rows with team_id, rank.
    """
    name_of = {int(t["id"]): t["name"] for t in teams}
    avg = average_opponent(inputs, consts)
    scored = []
    for row in inputs:
        tid = int(row["team_id"])
        name = name_of.get(tid)
        td = _team_dict(row)
        if name is None or td is None:
            continue
        rating = rating_vs_average(td, avg, consts)
        scored.append((tid, name, rating))
    scored.sort(key=lambda x: (-x[2], x[1]))
    prev_rank = {int(r["team_id"]): int(r["rank"]) for r in (prev or []) if r.get("rank") is not None}
    di = di_records(games, as_of)
    kp = kp_records(ratings)
    week_of = monday_on_or_before(as_of)
    # season: ending year (2026-11 → 2027)
    season = as_of.year + 1 if as_of.month >= 11 else as_of.year
    rows = []
    for i, (tid, name, rating) in enumerate(scored, start=1):
        dw, dl = di.get(name, (None, None))
        kw, kl = kp.get(name, (None, None))
        gp = None if dw is None else dw + dl
        rows.append({
            "as_of": as_of.isoformat(),
            "week_of": week_of.isoformat(),
            "season": season,
            "league": "ncaam",
            "team_id": tid,
            "name": name,  # display only; stripped before upsert
            "rank": i,
            "prev_rank": prev_rank.get(tid),
            "rating": round(rating, 2),
            "di_wins": dw, "di_losses": dl,
            "kp_wins": kw, "kp_losses": kl,
            "games_played": gp,
        })
    return rows
