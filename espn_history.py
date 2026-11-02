"""
espn_history.py
Free historical D-I men's results + DraftKings opening/closing lines from ESPN's
public JSON endpoints (no key). Used by backtest.py.

  scoreboard: site.api.espn.com/.../mens-college-basketball/scoreboard?dates=YYYYMMDD&groups=50
  odds:       sports.core.api.espn.com/.../events/{id}/competitions/{id}/odds
              -> provider "Draft Kings": open/close spread (+price), total (+o/u price), moneyline

Unofficial API: be polite (cached to data/history/, small sleep between calls).

    python espn_history.py 2026-01-10 2026-01-17      # -> data/history/espn_20260110_20260117.csv
"""
from __future__ import annotations

import json
import os
import sys
import time
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pandas as pd
import requests

CENTRAL = ZoneInfo("America/Chicago")
SB_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard"
ODDS_URL = ("https://sports.core.api.espn.com/v2/sports/basketball/leagues/mens-college-basketball/"
            "events/{id}/competitions/{id}/odds")
CACHE = os.path.join("data", "history", "espn_cache")
_S = requests.Session()


def _cached_get(url: str, params: dict | None, key: str, sleep: float = 0.15):
    os.makedirs(CACHE, exist_ok=True)
    path = os.path.join(CACHE, key + ".json")
    if os.path.exists(path):
        with open(path) as fh:
            return json.load(fh)
    r = _S.get(url, params=params, timeout=20)
    r.raise_for_status()
    j = r.json()
    with open(path, "w") as fh:
        json.dump(j, fh)
    time.sleep(sleep)
    return j


def _num(x):
    try:
        if isinstance(x, str):
            x = x.replace("+", "")
            if x.upper() in ("EVEN", "PK", "PICK"):
                return 100.0 if x.upper() == "EVEN" else 0.0
        return float(x)
    except (TypeError, ValueError):
        return None


def scoreboard(day: date) -> list[dict]:
    ds = day.strftime("%Y%m%d")
    j = _cached_get(SB_URL, {"dates": ds, "groups": 50, "limit": 500}, f"sb_{ds}")
    rows = []
    for e in j.get("events", []):
        c = e["competitions"][0]
        comp = {t["homeAway"]: t for t in c["competitors"]}
        if "home" not in comp or "away" not in comp:
            continue
        status = c.get("status", {}).get("type", {})
        utc = datetime.strptime(e["date"], "%Y-%m-%dT%H:%MZ").replace(tzinfo=timezone.utc)
        rows.append({
            "event_id": e["id"],
            "date": utc.astimezone(CENTRAL).strftime("%Y-%m-%d"),
            "tip_ct": utc.astimezone(CENTRAL).strftime("%Y-%m-%d %H:%M"),
            "home": comp["home"]["team"].get("location"),
            "away": comp["away"]["team"].get("location"),
            "home_full": comp["home"]["team"].get("displayName"),
            "away_full": comp["away"]["team"].get("displayName"),
            "home_score": _num(comp["home"].get("score")),
            "away_score": _num(comp["away"].get("score")),
            "neutral": bool(c.get("neutralSite")),
            "completed": bool(status.get("completed")),
        })
    return rows


def dk_lines(event_id: str) -> dict:
    """Opening + closing DraftKings lines, home perspective (home_spread -3.5 = home favored)."""
    try:
        j = _cached_get(ODDS_URL.format(id=event_id), None, f"odds_{event_id}")
    except requests.HTTPError:
        return {}
    items = j.get("items", [])
    it = next((i for i in items if "draft" in (i.get("provider", {}).get("name", "").lower())), None)
    it = it or (items[0] if items else None)
    if not it:
        return {}
    h = it.get("homeTeamOdds", {}) or {}
    a = it.get("awayTeamOdds", {}) or {}
    out = {"book": it.get("provider", {}).get("name")}
    for when in ("open", "close"):
        ho, ao, top = h.get(when, {}) or {}, a.get(when, {}) or {}, it.get(when, {}) or {}
        out[f"{when}_home_spread"] = _num((ho.get("pointSpread") or {}).get("american"))
        out[f"{when}_home_spread_price"] = _num((ho.get("spread") or {}).get("american"))
        out[f"{when}_away_spread_price"] = _num((ao.get("spread") or {}).get("american"))
        out[f"{when}_home_ml"] = _num((ho.get("moneyLine") or {}).get("american"))
        out[f"{when}_away_ml"] = _num((ao.get("moneyLine") or {}).get("american"))
        out[f"{when}_total"] = _num((top.get("total") or {}).get("american"))
        out[f"{when}_over_price"] = _num((top.get("over") or {}).get("american"))
        out[f"{when}_under_price"] = _num((top.get("under") or {}).get("american"))
    if out.get("close_total") is None:
        out["close_total"] = _num(it.get("overUnder"))
    return out


def build_history(start: date, end: date, with_lines: bool = True, progress: bool = True) -> pd.DataFrame:
    rows, d = [], start
    while d <= end:
        games = [g for g in scoreboard(d) if g["completed"]]
        if with_lines:
            for g in games:
                g.update(dk_lines(g["event_id"]))
        rows.extend(games)
        if progress:
            print(f"  {d}: {len(games)} completed games")
        d += timedelta(days=1)
    df = pd.DataFrame(rows).drop_duplicates("event_id")
    return df


if __name__ == "__main__":
    s = date.fromisoformat(sys.argv[1])
    e = date.fromisoformat(sys.argv[2]) if len(sys.argv) > 2 else s
    df = build_history(s, e)
    os.makedirs(os.path.join("data", "history"), exist_ok=True)
    out = os.path.join("data", "history", f"espn_{s:%Y%m%d}_{e:%Y%m%d}.csv")
    df.to_csv(out, index=False)
    print(f"Saved {len(df)} games -> {out}")
