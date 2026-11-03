"""New-coach ('new philosophy') detection and the value-play lock.

A team whose KenPom head coach this season differs from last season's is tagged NEW COACH; its
games are excluded from value plays (sides and totals) until it has played `lock_games` games
this season (KenPom W+L). Sources, in order: KenPom ratings/teams 'Coach' field this season vs
last (data/ratings.csv or data/teams.csv vs data/prior/ratings.csv or data/coaches/teams_<prev>.csv),
then data/coach_changes.csv (season,team[,new_coach,prev_coach][,new_coach=0 to clear]) as
an override / fallback.
"""
from __future__ import annotations

import os
import re

import pandas as pd

OVERRIDES = os.path.join("data", "coach_changes.csv")


def _norm(name) -> str:
    s = re.sub(r"[^a-z ]", "", str(name or "").lower().replace(".", " "))
    s = re.sub(r"\b(jr|sr|ii|iii|iv)\b", "", s)
    return " ".join(s.split())


def coach_changes_between(cur: pd.DataFrame, prev: pd.DataFrame) -> set[str]:
    """Teams (cur TeamName) whose Coach differs between two (TeamName, Coach) tables."""
    if cur is None or prev is None or "Coach" not in cur or "Coach" not in prev:
        return set()
    p = dict(zip(prev.TeamName.astype(str), prev.Coach.map(_norm)))
    out = set()
    for t, c in zip(cur.TeamName.astype(str), cur.Coach.map(_norm)):
        if t in p and c and p[t] and c != p[t]:
            out.add(t)
    return out


def _coach_table(paths):
    for p in paths:
        if p and os.path.exists(p):
            try:
                d = pd.read_csv(p)
            except Exception:
                continue
            if {"TeamName", "Coach"} <= set(d.columns) and d.Coach.notna().any():
                return d[["TeamName", "Coach"]]
    return None


def detect_new_coaches(season: int, data_dir: str = "data") -> dict[str, dict]:
    """{team: {"coach": new, "prev": old, "source": ...}} for this season."""
    cur = _coach_table([os.path.join(data_dir, "ratings.csv"), os.path.join(data_dir, "teams.csv")])
    prev = _coach_table([os.path.join(data_dir, "prior", "ratings.csv"), os.path.join(data_dir, "prior", "teams.csv"),
                         os.path.join(data_dir, "coaches", f"teams_{season - 1}.csv")])
    out: dict[str, dict] = {}
    if cur is not None and prev is not None:
        pc = dict(zip(prev.TeamName.astype(str), prev.Coach))
        cc = dict(zip(cur.TeamName.astype(str), cur.Coach))
        for t in coach_changes_between(cur, prev):
            out[t] = {"coach": cc.get(t), "prev": pc.get(t), "source": "kenpom"}
    ov = os.path.join(data_dir, "coach_changes.csv")
    if os.path.exists(ov):
        try:
            x = pd.read_csv(ov)
            x = x[x.season.astype(int) == int(season)]
            for _, r in x.iterrows():
                t = str(r.team)
                if "new_coach" in x and str(r.get("new_coach")).strip() in ("0", "False", "false", "no"):
                    out.pop(t, None)
                    continue
                out[t] = {"coach": r.get("coach"), "prev": r.get("prev_coach"), "source": "coach_changes.csv"}
        except Exception:
            pass
    return out


def games_played(ratings: pd.DataFrame) -> dict[str, int]:
    if ratings is None or not {"Wins", "Losses"} <= set(ratings.columns):
        return {}
    g = pd.to_numeric(ratings.Wins, errors="coerce").fillna(0) + pd.to_numeric(ratings.Losses, errors="coerce").fillna(0)
    return dict(zip(ratings.TeamName.astype(str), g.astype(int)))


def lock_status(team: str, new_coaches: dict, played: dict, lock_games: int) -> dict | None:
    """None if not a new-coach team; else {"games": g, "remaining": r, "locked": r > 0}."""
    if not team or team not in new_coaches:
        return None
    g = int(played.get(team, 0) or 0)
    rem = max(0, int(lock_games) - g)
    return {"games": g, "remaining": rem, "locked": rem > 0, **new_coaches[team]}
