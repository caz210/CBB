"""'Points so far' (INFO ONLY — never a pick, verdict, stake, or ledger input).

For a game, each team's average game total (points for + against) in its games so far this
season, averaged across the two teams, compared with the posted total line and with CZarp's
total. Only games against D-I opponents count (a non-D-I opponent is an ESPN team that does not
map to a KenPom team). If either team has no D-I games yet, nothing is shown.

Finals come from the ESPN scoreboard (one request per day of the season so far), cached per day
under data/ledger/finals_cache/ once the day is fully final.
"""
from __future__ import annotations

import json
import math
import os
from datetime import date, timedelta
from typing import Callable, Iterable

from czarp.season import season_for_date

CACHE_DIR = os.path.join("data", "ledger", "finals_cache")
MIN_STALE_DAYS = 2           # days at least this old are cached even if a game never went final


def _f(x):
    try:
        v = float(x)
        return None if math.isnan(v) else v
    except (TypeError, ValueError):
        return None


def season_start(today: date) -> date:
    return date(season_for_date(today) - 1, 11, 1)


def _slim(g: dict) -> dict:
    return {k: g.get(k) for k in ("event_id", "game_date", "home", "away", "home_score",
                                  "away_score", "completed", "status_name")}


def load_finals(today: date | str, fetch: Callable[[date], list[dict]],
                cache_dir: str | None = None) -> list[dict]:
    """Completed games from season start through the day before `today` (CT)."""
    if isinstance(today, str):
        today = date.fromisoformat(today[:10])
    cache_dir = cache_dir or CACHE_DIR
    os.makedirs(cache_dir, exist_ok=True)
    out: list[dict] = []
    d = season_start(today)
    while d < today:
        path = os.path.join(cache_dir, f"{d:%Y%m%d}.json")
        rows = None
        if os.path.exists(path):
            with open(path) as fh:
                rows = json.load(fh)
        else:
            rows = [_slim(g) for g in fetch(d) if g.get("game_date") in (None, d.isoformat())]
            done = all(g.get("completed") or str(g.get("status_name", "")).startswith(("STATUS_POSTPONED", "STATUS_CANCEL"))
                       for g in rows)
            if rows and (done or (today - d).days >= MIN_STALE_DAYS):
                with open(path, "w") as fh:
                    json.dump(rows, fh)
        out.extend(g for g in rows if g.get("completed") and _f(g.get("home_score")) is not None
                   and (_f(g["home_score"]) or 0) + (_f(g.get("away_score")) or 0) > 0)
        d += timedelta(days=1)
    return out


def team_totals(finals: Iterable[dict], match: Callable[[str], str | None]) -> dict:
    """{kp_team: [(game_date, game_total, event_id), ...]} for D-I vs D-I finals only."""
    logs: dict[str, list] = {}
    seen = set()
    for g in finals:
        eid = str(g.get("event_id"))
        if eid in seen:
            continue
        seen.add(eid)
        h, a = match(g.get("home")), match(g.get("away"))
        if not h or not a:
            continue                          # non-D-I opponent: excluded
        tot = (_f(g.get("home_score")) or 0) + (_f(g.get("away_score")) or 0)
        for t in (h, a):
            logs.setdefault(t, []).append((str(g.get("game_date")), tot, eid))
    return logs


def points_so_far(g: dict, logs: dict, czarp_total=None) -> dict | None:
    """Combined season-to-date game total for the two teams vs the posted total line."""
    h, a = g.get("kp_home"), g.get("kp_away")
    day = str(g.get("game_date") or "")
    if not h or not a:
        return None
    th = [x[1] for x in logs.get(h, []) if x[0] < day]
    ta = [x[1] for x in logs.get(a, []) if x[0] < day]
    if not th or not ta:
        return None
    pts = round((sum(th) / len(th) + sum(ta) / len(ta)) / 2, 1)
    line = _f(g.get("current_total"))
    if line is None:
        line = _f(g.get("open_total"))
    cz = _f(czarp_total if czarp_total is not None else g.get("v1_total"))
    out = {"points": pts, "n_home": len(th), "n_away": len(ta), "line": line,
           "vs_line": None if line is None else round(pts - line, 1), "czarp": cz,
           "czarp_side": None, "agrees": None}
    if line is not None and cz is not None and cz != line:
        out["czarp_side"] = "Over" if cz > line else "Under"
        if pts != line:
            out["agrees"] = (pts > line) == (cz > line)
    return out


def fmt_line(p: dict | None) -> str | None:
    """'Points so far: 170.3 (8.8 above line, against CZarp Under) · D-I games, 2 + 4 played'."""
    if not p:
        return None
    if p["vs_line"] is None:
        rel = "no line"
    elif p["vs_line"] == 0:
        rel = "on the line"
    else:
        rel = f"{abs(p['vs_line']):g} {'above' if p['vs_line'] > 0 else 'below'} line"
    if p.get("agrees") is not None:
        rel += f", {'with' if p['agrees'] else 'against'} CZarp {p['czarp_side']}"
    return (f"Points so far: {p['points']:g} ({rel}) · info only, D-I games, "
            f"{p['n_away']} + {p['n_home']} played")


def fmt_cell(p: dict | None) -> str:
    """Compact form for the markdown Top 10 table."""
    if not p:
        return "—"
    s = f"{p['points']:g}"
    if p["vs_line"] is not None:
        s += f" ({p['vs_line']:+g} vs line"
        if p.get("agrees") is not None:
            s += f", {'with' if p['agrees'] else 'against'} CZarp"
        s += ")"
    return s
