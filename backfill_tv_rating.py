"""Fill projections.tv_rating on today's and upcoming games.

Uses high-precision AdjEM from matchup_inputs (oe − de; same KenPom figures the
daily run ranks, not the 2-decimal team_profiles columns) and the card's CZarp
win probability + possessions on the czarp-v2 row. The same score is written
to the paired czarp-v1-totals row from the same run (same created_at).

Never touches picks, the ledger, official_picks_locked, the hash chain, or
any projection column other than tv_rating. Does not use Vegas lines.
Rows that already match are skipped. Dry run is the default; pass --apply
to write. The column must exist (apply db/migrations/2026-11-21_tv_rating.sql
first) before --apply will succeed.

    python3 backfill_tv_rating.py [--start YYYY-MM-DD (default: today CT)] [--apply]
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime
from zoneinfo import ZoneInfo

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

import pandas as pd

from czarp.supabase_writer import DEFAULT_VERSION, V1_VERSION, Client
from czarp.tv_rating import game_tv_rating, quality_by_team


def _f(v):
    try:
        return None if v is None or v == "" else float(v)
    except (TypeError, ValueError):
        return None


def get_all(c: Client, table: str, page: int = 1000, **params) -> list[dict]:
    out, off = [], 0
    while True:
        rows = c.get(table, limit=str(page), offset=str(off), order="id", **params)
        out.extend(rows)
        if len(rows) < page:
            return out
        off += page


def quality_from_inputs(inputs: list[dict], teams: list[dict]) -> dict[str, float]:
    """Build a quality map from matchup_inputs.oe/de joined to teams.name."""
    name = {int(t["id"]): t["name"] for t in teams}
    rows = []
    for r in inputs:
        n = name.get(int(r["team_id"]))
        oe, de = _f(r.get("oe")), _f(r.get("de"))
        if n is None or oe is None or de is None:
            continue
        rows.append({"TeamName": n, "AdjOE": oe, "AdjDE": de})
    return quality_by_team(pd.DataFrame(rows)) if rows else {}


def plan(games: list[dict], proj: list[dict], qmap: dict[str, float]) -> list[dict]:
    """Updates {id, game, old, new} for v2 and same-run v1 rows whose tv_rating differs."""
    meta = {int(g["id"]): g for g in games}
    by_game = defaultdict(list)
    for p in proj:
        by_game[int(p["game_id"])].append(p)
    out = []
    for gid, rows in by_game.items():
        g = meta.get(gid)
        if not g:
            continue
        label = f"{g['away']} @ {g['home']}" if not g.get("neutral") else f"{g['away']} vs {g['home']}"
        label = f"{label} ({g['date']})"
        v2_rows = [p for p in rows if p["model_version"] == DEFAULT_VERSION]
        v1_by_ts = {p["created_at"]: p for p in rows if p["model_version"] == V1_VERSION}
        for p in v2_rows:
            win_p, poss, total = _f(p.get("home_win_prob")), _f(p.get("proj_possessions")), _f(p.get("total"))
            rating = game_tv_rating(qmap, g["home"], g["away"], win_p, possessions=poss, total=total)
            targets = [p]
            if p["created_at"] in v1_by_ts:
                targets.append(v1_by_ts[p["created_at"]])
            for t in targets:
                old = _f(t.get("tv_rating"))
                if rating is None and old is None:
                    continue
                if rating is not None and old is not None and round(old, 1) == rating:
                    continue
                out.append(dict(id=int(t["id"]), game_id=gid, game=label,
                                model_version=t["model_version"], created_at=t["created_at"],
                                win_prob=win_p, possessions=poss, total=total,
                                old=old, new=rating))
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--start", default=datetime.now(ZoneInfo("America/Chicago")).date().isoformat())
    ap.add_argument("--apply", action="store_true", help="write the changes (default: dry run)")
    a = ap.parse_args()
    c = Client()
    games = get_all(c, "games", select="id,date,home,away,neutral,status", date=f"gte.{a.start}")
    teams = get_all(c, "teams", select="id,name")
    inputs = get_all(c, "matchup_inputs", select="team_id,oe,de")
    qmap = quality_from_inputs(inputs, teams)
    ids = [int(g["id"]) for g in games]
    proj = []
    cols = "id,game_id,model_version,created_at,home_win_prob,proj_possessions,total,tv_rating"
    cols_pre = "id,game_id,model_version,created_at,home_win_prob,proj_possessions,total"
    select = cols
    for i in range(0, len(ids), 150):
        chunk = ",".join(map(str, ids[i:i + 150]))
        try:
            proj += get_all(c, "projections",
                            select=select,
                            game_id=f"in.({chunk})",
                            model_version=f"in.({DEFAULT_VERSION},{V1_VERSION})")
        except Exception:
            if select != cols_pre:
                # Column not applied yet: still allow a dry-run preview.
                select = cols_pre
                proj += get_all(c, "projections",
                                select=select,
                                game_id=f"in.({chunk})",
                                model_version=f"in.({DEFAULT_VERSION},{V1_VERSION})")
            else:
                raise
    ups = plan(games, proj, qmap)
    latest = {}
    for u in ups:
        if u["model_version"] != DEFAULT_VERSION:
            continue
        if u["game_id"] not in latest or u["created_at"] > latest[u["game_id"]]["created_at"]:
            latest[u["game_id"]] = u
    # Also include games whose newest v2 already matches, for a top-10 preview
    preview = {}
    v2_newest = {}
    for p in proj:
        if p["model_version"] != DEFAULT_VERSION:
            continue
        gid = int(p["game_id"])
        if gid not in v2_newest or p["created_at"] > v2_newest[gid]["created_at"]:
            v2_newest[gid] = p
    for gid, p in v2_newest.items():
        g = next((x for x in games if int(x["id"]) == gid), None)
        if not g:
            continue
        win_p, poss, total = _f(p.get("home_win_prob")), _f(p.get("proj_possessions")), _f(p.get("total"))
        rating = game_tv_rating(qmap, g["home"], g["away"], win_p, possessions=poss, total=total)
        label = f"{g['away']} @ {g['home']}" if not g.get("neutral") else f"{g['away']} vs {g['home']}"
        preview[gid] = dict(game=f"{label} ({g['date']})", rating=rating, win_prob=win_p,
                            possessions=poss, total=total, tip=g.get("date"))
    print(f"{'APPLY' if a.apply else 'DRY RUN'}: games dated >= {a.start}: {len(games)}; "
          f"quality map: {len(qmap)} teams; rows to update: {len(ups)} across "
          f"{len({u['game_id'] for u in ups})} games")
    ranked = sorted((v for v in preview.values() if v["rating"] is not None),
                    key=lambda x: -x["rating"])
    print("  top 10 by TV Rating (newest czarp-v2 row):")
    for i, v in enumerate(ranked[:10], 1):
        print(f"  {i:2d}. {v['game']}: {v['rating']:.1f}  "
              f"(win% {v['win_prob']:.4f}, poss {v['possessions']}, total {v['total']})")
    if ups:
        print("  rows that would change:")
        for u in ups[:30]:
            print(f"    {u['game']} {u['model_version']}: {u['old']} -> {u['new']}")
        if len(ups) > 30:
            print(f"    ... and {len(ups) - 30} more")
    if not a.apply:
        print("dry run: nothing written (pass --apply to write tv_rating only)")
        return
    for u in ups:
        c.patch("projections", {"tv_rating": u["new"]}, id=f"eq.{u['id']}")
    print(f"updated {len(ups)} projection rows (tv_rating only)")


if __name__ == "__main__":
    main()
