"""Backfill one historical slate day into Supabase from a posted-time snapshot CSV.

Nov 2, 2026 was the first day of the pipeline: the Supabase writer was added after
that morning's run, so only the one official game (Air Force @ Wright St.) was
mirrored. The full slate as projected at post time survives in
data/ledger/snapshots/<date>_HHMM.csv. This script mirrors that slate.

Writes ONLY slate tables, insert-only:
  - games         (new rows; existing games are left untouched)
  - projections   (czarp-v2 + czarp-v1-totals, created_at = snapshot time; existing
                   (game_id, model_version, created_at) rows are left untouched)
  - market_lines  (DK open + current from the snapshot; skipped for games that
                   already have Vegas lines)
Stored edges (edge_now_pct_* / edge_vs_open_pct_*) use daily_run.attach_market_edges,
the same formulas as the live pipeline. Skip-reason text is NOT written: the rules
in force on that day differ from today's, so we don't stamp today's reasons on it.

Never reads or writes picks, the ledger, team_profiles or teams. Teams that don't
already exist in `teams` get a NULL team id (reported). Final scores come from
`python -m czarp.sync_finals --date <date>` afterwards (games.status/scores only).

    python backfill_slate_day.py --snapshot data/ledger/snapshots/2026-11-02_0742.csv [--dry-run]
"""
from __future__ import annotations

import argparse
import ast
import math
import os

import pandas as pd

_CWD = os.getcwd()   # daily_run chdirs to its own folder on import

try:
    from dotenv import find_dotenv, load_dotenv
    load_dotenv(find_dotenv(usecwd=True))   # .env in the working directory (the repo checkout)
except Exception:
    pass

from czarp.supabase_writer import (DEFAULT_VERSION, LEAGUE, SPORT, V1_VERSION, Client, _edge_fields,
                                   _season, _tip_ct_iso)
import daily_run as dr

MARGIN_SD, TOTAL_SD = 11.4, 16.9   # czarp.config.Betting defaults (same fallbacks as supabase_writer.sync)
LINE_FIELDS = ("home_spread", "home_spread_price", "away_spread_price", "total", "over_price", "under_price")


def _f(v):
    if v is None:
        return None
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(x) else x


def _ncdf(x: float) -> float:
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


def _tags(v) -> dict:
    if isinstance(v, dict):
        return v
    try:
        x = ast.literal_eval(str(v))
        return x if isinstance(x, dict) else {}
    except Exception:
        return {}


def load_snapshot(path: str) -> list[dict]:
    """D-I vs D-I rows (both KenPom names present, not skipped) as daily_run-style game dicts."""
    df = pd.read_csv(path)
    out = []
    for r in df.to_dict("records"):
        if not isinstance(r.get("kp_home"), str) or not isinstance(r.get("kp_away"), str):
            continue
        if isinstance(r.get("skip"), str) and r["skip"].strip():
            continue
        g = {k: (None if isinstance(v, float) and math.isnan(v) else v) for k, v in r.items()}
        g["event_id"] = str(int(g["event_id"]))
        for lt in ("open", "current"):
            for f in LINE_FIELDS:
                g[f"{lt}_{f}"] = _f(g.get(f"{lt}_{f}"))
        for k in ("v2_home_margin", "v2_total", "v2_home_win_prob", "v1_total", "v1_home_margin"):
            g[k] = _f(g.get(k))
        g["archetype_tags"] = _tags(g.get("archetype_tags"))
        g["state"] = "post"            # historical: every game has started
        g["tip_utc"] = pd.Timestamp("2000-01-01", tz="UTC").to_pydatetime()
        out.append(g)
    return out


def build_rows(games: list[dict], *, sport_id: int, team_ids: dict[str, int], existing_games: dict[str, int],
               games_with_lines: set[int], existing_proj: set[tuple], stamp: str):
    """Pure. Returns (new_game_rows, projection_rows_fn, line_rows_fn, missing_teams).

    projection/line builders need final game ids, so they are returned as callables
    taking {external_id: game_id}.
    """
    dr.attach_market_edges(games, dr.now_ct())
    base = {"sport_id": sport_id, "league": LEAGUE}
    missing = sorted({n for g in games for n in (g["kp_home"], g["kp_away"]) if n not in team_ids})
    new_games = []
    for g in games:
        if g["event_id"] in existing_games:
            continue
        new_games.append({**base, "season": _season(g["game_date"]), "date": g["game_date"],
                          "home": g["kp_home"], "away": g["kp_away"],
                          "home_team_id": team_ids.get(g["kp_home"]), "away_team_id": team_ids.get(g["kp_away"]),
                          "tipoff": _tip_ct_iso(g.get("tip_ct")), "neutral": str(g.get("neutral")) == "True",
                          "external_id": g["event_id"], "external_ids": {"espn": g["event_id"]},
                          "status": "scheduled", "home_score": None, "away_score": None, "updated_at": stamp})

    def projections(gid: dict[str, int]) -> list[dict]:
        rows = []
        for g in games:
            k = gid.get(g["event_id"])
            if k is None:
                continue
            L, MT = g.get("current_home_spread"), g.get("current_total")
            edges = _edge_fields(g)
            hm, T = g.get("v2_home_margin"), g.get("v2_total")
            if hm is not None and T is not None and (k, DEFAULT_VERSION, stamp) not in existing_proj:
                wp = g.get("v2_home_win_prob")
                rows.append({**base, "season": _season(g["game_date"]), "game_id": k, "model_version": DEFAULT_VERSION,
                             "proj_home": round((T + hm) / 2, 1), "proj_away": round((T - hm) / 2, 1),
                             "spread": round(-hm, 1), "total": round(T, 1),
                             "home_win_prob": round(wp if wp is not None else _ncdf(hm / MARGIN_SD), 4),
                             "home_cover_prob": round(_ncdf((hm + L) / MARGIN_SD), 4) if L is not None else None,
                             "over_prob": round(1 - _ncdf((MT - T) / TOTAL_SD), 4) if MT is not None else None,
                             "archetype_tags": g.get("archetype_tags") or {},
                             **edges, "created_at": stamp})
            t1 = g.get("v1_total")
            if t1 is not None and (k, V1_VERSION, stamp) not in existing_proj:
                m1 = g.get("v1_home_margin")
                rows.append({**base, "season": _season(g["game_date"]), "game_id": k, "model_version": V1_VERSION,
                             "total": round(t1, 1), "spread": round(-m1, 1) if m1 is not None else None,
                             "over_prob": round(1 - _ncdf((MT - t1) / TOTAL_SD), 4) if MT is not None else None,
                             **edges, "created_at": stamp})
        return rows

    def lines(gid: dict[str, int]) -> list[dict]:
        rows = []
        for g in games:
            k = gid.get(g["event_id"])
            if k is None or k in games_with_lines:
                continue
            for lt in ("open", "current"):
                if g.get(f"{lt}_home_spread") is None and g.get(f"{lt}_total") is None:
                    continue
                rows.append({"game_id": k, "source": "vegas", "line_type": lt, "book": "DraftKings",
                             "spread": g.get(f"{lt}_home_spread"), "total": g.get(f"{lt}_total"),
                             "prices": {x: g.get(f"{lt}_{x}") for x in ("home_spread_price", "away_spread_price",
                                                                        "over_price", "under_price")},
                             "fetched_at": stamp})
        return rows

    return new_games, projections, lines, missing


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--snapshot", required=True)
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args(argv)
    games = load_snapshot(os.path.join(_CWD, a.snapshot))
    if not games:
        print("no D-I games in snapshot")
        return 1
    stamp = pd.Timestamp(games[0]["snapshot_ct"]).isoformat()
    day = games[0]["game_date"]
    c = Client()
    sid = c.get("sports", code=f"eq.{SPORT}", select="id")[0]["id"]
    team_ids = {t["name"]: t["id"] for t in c.get("teams", select="id,name", league=f"eq.{LEAGUE}", limit="5000")}
    ex = c.get("games", select="id,external_id", league=f"eq.{LEAGUE}", date=f"eq.{day}", limit="2000")
    existing_games = {str(r["external_id"]): r["id"] for r in ex}
    ids = list(existing_games.values())
    games_with_lines, existing_proj = set(), set()
    if ids:
        chunk = ",".join(map(str, ids))
        games_with_lines = {r["game_id"] for r in c.get("market_lines", select="game_id", source="eq.vegas",
                                                        game_id=f"in.({chunk})", limit="5000")}
        existing_proj = {(r["game_id"], r["model_version"], pd.Timestamp(r["created_at"]).isoformat())
                         for r in c.get("projections", select="game_id,model_version,created_at",
                                        game_id=f"in.({chunk})", limit="5000")}
        existing_proj = {(g, m, pd.Timestamp(t).tz_convert(pd.Timestamp(stamp).tz).isoformat()) for g, m, t in existing_proj}
    new_games, proj_fn, line_fn, missing = build_rows(
        games, sport_id=sid, team_ids=team_ids, existing_games=existing_games,
        games_with_lines=games_with_lines, existing_proj=existing_proj, stamp=stamp)
    print(f"backfill slate {day} from {a.snapshot} (stamp {stamp}): {len(games)} D-I games, "
          f"{len(existing_games)} already in db, {len(new_games)} to insert")
    if missing:
        print(f"  teams not in db (team id left NULL): {', '.join(missing)}")
    if a.dry_run:
        fake = {**existing_games, **{g['external_id']: -i - 1 for i, g in enumerate(new_games)}}
        pr, ln = proj_fn(fake), line_fn(fake)
        print(f"  dry run: would insert {len(pr)} projections, {len(ln)} market_lines")
        for g in games:
            if g["event_id"] in existing_games:
                print(f"  existing: {g['kp_away']} @ {g['kp_home']} edges now/open total "
                      f"{g.get('edge_now_pct_total')}/{g.get('edge_vs_open_pct_total')}")
        return 0
    if new_games:
        c.upsert("games", new_games, on_conflict="league,external_id", returning=False, ignore_duplicates=True)
    ex = c.get("games", select="id,external_id", league=f"eq.{LEAGUE}", date=f"eq.{day}", limit="2000")
    gid = {str(r["external_id"]): r["id"] for r in ex}
    pr = proj_fn(gid)
    c.upsert("projections", pr, on_conflict="game_id,model_version,created_at", returning=False, ignore_duplicates=True)
    ln = line_fn(gid)
    c.insert("market_lines", ln)
    print(f"  inserted {len(new_games)} games, {len(pr)} projections, {len(ln)} market_lines. "
          f"Next: python -m czarp.sync_finals --date {day}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
