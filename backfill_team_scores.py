"""One-time: make stored per-team projected scores agree with the CZarp total and spread.

For the default-model (czarp-v2) projection rows of today's and upcoming games, rewrite ONLY
proj_home / proj_away with czarp.supabase_writer.team_scores: home + away == the CZarp total
(the czarp-v1-totals total from the same run, else the row's own total) and home - away == -spread.

Never touches picks, the ledger, official_picks_locked, the hash chain, or the stored total / spread /
probability / edge columns. Rows that already match are skipped. Dry run is the default; pass --apply
to write.

    python3 backfill_team_scores.py [--start YYYY-MM-DD (default: today CT)] [--apply]
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

from czarp.supabase_writer import DEFAULT_VERSION, V1_VERSION, Client, team_scores


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


def plan(games: list[dict], proj: list[dict]) -> list[dict]:
    """Updates {id, game, created_at, old, new} for v2 rows whose scores differ from the reconciled pair."""
    name = {int(g["id"]): f"{g['away']} @ {g['home']} ({g['date']})" for g in games}
    v1 = defaultdict(list)                       # game_id -> [(created_at, total)]
    for p in proj:
        if p["model_version"] == V1_VERSION and _f(p.get("total")) is not None:
            v1[int(p["game_id"])].append((p["created_at"], _f(p["total"])))
    out = []
    for p in proj:
        if p["model_version"] != DEFAULT_VERSION:
            continue
        sp, t2 = _f(p.get("spread")), _f(p.get("total"))
        if sp is None or t2 is None:
            continue
        gid = int(p["game_id"])
        runs = sorted(v1.get(gid, []))
        same = [t for ts, t in runs if ts == p["created_at"]]
        before = [t for ts, t in runs if ts <= p["created_at"]]
        t1 = same[-1] if same else (before[-1] if before else None)
        h, a = team_scores(-sp, t2, t1)
        oh, oa = _f(p.get("proj_home")), _f(p.get("proj_away"))
        if oh is not None and oa is not None and round(oh * 100) == round(h * 100) and round(oa * 100) == round(a * 100):
            continue
        out.append(dict(id=int(p["id"]), game_id=gid, game=name.get(gid, str(gid)), created_at=p["created_at"],
                        czarp_total=t1 if t1 is not None else round(t2, 1), spread=sp,
                        old=(oh, oa), new=(h, a)))
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--start", default=datetime.now(ZoneInfo("America/Chicago")).date().isoformat())
    ap.add_argument("--apply", action="store_true", help="write the changes (default: dry run)")
    a = ap.parse_args()
    c = Client()
    games = get_all(c, "games", select="id,date,home,away,neutral,status", date=f"gte.{a.start}")
    ids = [int(g["id"]) for g in games]
    proj = []
    for i in range(0, len(ids), 150):
        chunk = ",".join(map(str, ids[i:i + 150]))
        proj += get_all(c, "projections", select="id,game_id,model_version,created_at,proj_home,proj_away,spread,total",
                        game_id=f"in.({chunk})", model_version=f"in.({DEFAULT_VERSION},{V1_VERSION})")
    ups = plan(games, proj)
    n_v2 = sum(p["model_version"] == DEFAULT_VERSION for p in proj)
    latest = {}
    for u in ups:
        if u["game_id"] not in latest or u["created_at"] > latest[u["game_id"]]["created_at"]:
            latest[u["game_id"]] = u
    print(f"{'APPLY' if a.apply else 'DRY RUN'}: games dated >= {a.start}: {len(games)}; "
          f"{DEFAULT_VERSION} rows: {n_v2}; rows to update: {len(ups)} across {len(latest)} games")
    if ups:
        gaps = sorted((abs((u['old'][0] or 0) + (u['old'][1] or 0) - u['czarp_total']) for u in latest.values()), reverse=True)
        print(f"  newest-row sum gap vs CZarp total: max {gaps[0]:.1f}, >=1 pt {sum(g >= 1 for g in gaps)}, "
              f">=3 pts {sum(g >= 3 for g in gaps)}")
    for u in sorted(latest.values(), key=lambda u: -abs((u['old'][0] or 0) + (u['old'][1] or 0) - u['czarp_total'])):
        (oh, oa), (h, a2) = u["old"], u["new"]
        print(f"  {u['game']}: away/home {oa}/{oh} (sum {(oa or 0) + (oh or 0):.1f}) -> {a2}/{h} "
              f"(sum {a2 + h:.2f}, CZarp total {u['czarp_total']}, spread {u['spread']:+.1f})")
    if not a.apply:
        print("dry run: nothing written (pass --apply to write proj_home/proj_away only)")
        return
    for u in ups:
        c.patch("projections", {"proj_home": u["new"][0], "proj_away": u["new"][1]}, id=f"eq.{u['id']}")
    print(f"updated {len(ups)} projection rows (proj_home/proj_away only)")


if __name__ == "__main__":
    main()
