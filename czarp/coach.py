"""New-coach ('new philosophy') detection and the value-play lock.

A team whose KenPom head coach this season differs from last season's is tagged NEW COACH; its
games are excluded from value plays (sides and totals) until it has played `lock_games` games
this season (KenPom W+L). Sources, in order: KenPom ratings/teams 'Coach' field this season vs
last (data/ratings.csv or data/teams.csv vs data/prior/ratings.csv or data/coaches/teams_<prev>.csv),
then data/coach_changes.csv (legacy, untracked) and finally config/coach_locks.csv - the git-tracked,
hand-edited per-team flag file that overrides everything:

    season,team,new_coach,coach,prev_coach,lock_sides,lock_totals,note,source
    2027,LSU,Y,Will Wade,Matt McMahon,,,McMahon fired,https://...

new_coach   Y = locked as a new-coach team (added even if KenPom did not detect it); N = never locked
lock_sides  blank / Y = default length (daily_run COACH_LOCK_SIDES); N or 0 = no side lock for this team;
lock_totals an integer = custom number of games for this team. Same for totals.
Teams not in the file keep the KenPom auto-detection. Edit with `python3 coach_locks.py on|off|list|audit`.
"""
from __future__ import annotations

import os
import re

import pandas as pd

OVERRIDES = os.path.join("data", "coach_changes.csv")
LOCKS_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config", "coach_locks.csv")
LOCK_COLUMNS = ["season", "team", "new_coach", "coach", "prev_coach", "lock_sides", "lock_totals", "note", "source"]
_YES = {"y", "yes", "1", "true", "t", "on"}
_NO = {"n", "no", "0", "false", "f", "off"}


def _flag(v) -> bool | None:
    s = str(v).strip().lower() if v is not None and not (isinstance(v, float) and pd.isna(v)) else ""
    return True if s in _YES else False if s in _NO else None


def _lock_len(v) -> int | None:
    """Per-team lock length cell: None = use the global default, 0 = off, n = n games."""
    s = str(v).strip().lower() if v is not None and not (isinstance(v, float) and pd.isna(v)) else ""
    if s in ("", "y", "yes", "true", "default", "nan"):
        return None
    if s in _NO:
        return 0
    try:
        return max(0, int(float(s)))
    except ValueError:
        return None


def load_coach_locks(season: int | None = None, path: str | None = None) -> pd.DataFrame:
    """The flag file (all seasons, or one season). Empty frame if missing."""
    path = path or LOCKS_FILE
    if not os.path.exists(path):
        return pd.DataFrame(columns=LOCK_COLUMNS)
    d = pd.read_csv(path, dtype=str, keep_default_na=False)
    for c in LOCK_COLUMNS:
        if c not in d:
            d[c] = ""
    if season is not None:
        d = d[pd.to_numeric(d.season, errors="coerce") == int(season)]
    return d[LOCK_COLUMNS]


def apply_coach_locks(out: dict, season: int, path: str | None = None) -> dict:
    """Apply config/coach_locks.csv rows for `season` on top of the auto-detected map (in place)."""
    for _, r in load_coach_locks(season, path).iterrows():
        t = str(r.team).strip()
        f = _flag(r.new_coach)
        if not t or f is None:
            continue
        if not f:
            out.pop(t, None)
            continue
        e = dict(out.get(t, {}))
        e.update({"coach": r.coach or e.get("coach"), "prev": r.prev_coach or e.get("prev"),
                  "source": "config/coach_locks.csv", "lock_sides": _lock_len(r.lock_sides),
                  "lock_totals": _lock_len(r.lock_totals), "note": r.note, "url": r.source})
        out[t] = e
    return out


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


def detect_new_coaches(season: int, data_dir: str = "data", locks_path: str | None = None) -> dict[str, dict]:
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
    if locks_path is None:
        locks_path = LOCKS_FILE if os.path.normpath(data_dir) == "data" else os.path.join(data_dir, "coach_locks.csv")
    return apply_coach_locks(out, season, locks_path)


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
