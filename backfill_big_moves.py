"""Backfill the info-only BIG MOVE tracker (Supabase big_moves) from stored lines.

For every stored game from --start (default 2026-11-02) through --end (default today, CT):
  open    = earliest DK 'open' line in market_lines
  current = the morning 'current' line (first DK 'current' snapshot fetched on the game date, CT)
  CZarp   = the projection stored at/just before that snapshot (v2 spread, v1 total; v2 total fallback)
Rows are written with source='backfill' and close_available=false (no close for these), then graded
from games.home_score / away_score. Existing rows (e.g. live rows) are kept. Never touches picks.

    python backfill_big_moves.py [--start 2026-11-02] [--end YYYY-MM-DD] [--dry-run]
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime, timedelta

import pandas as pd

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

from czarp import big_moves as BM
from czarp.supabase_writer import Client, V1_VERSION

import daily_run as dr


def get_all(c: Client, table: str, page: int = 1000, **params) -> list[dict]:
    out, off = [], 0
    while True:
        rows = c.get(table, limit=str(page), offset=str(off), order="id", **params)
        out.extend(rows)
        if len(rows) < page:
            return out
        off += page


def _ts(x):
    return pd.Timestamp(x) if x else None


def build(games, lines, projs, picks) -> list[tuple[int, dict]]:
    """[(game_id, big-move row)] from raw table rows (pure; unit-tested)."""
    ml = defaultdict(list)
    for r in lines:
        ml[r["game_id"]].append(r)
    pj = defaultdict(list)
    for r in projs:
        pj[(r["game_id"], r["model_version"])].append(r)
    official = {(p["game_id"], p["market"]) for p in picks if p.get("tier", "official") == "official"
                and p.get("result") != "void"}
    out = []
    for g in games:
        rows = sorted(ml.get(g["id"], []), key=lambda r: _ts(r["fetched_at"]))
        opens = [r for r in rows if r["line_type"] == "open"]
        curs = [r for r in rows if r["line_type"] == "current"]
        if not opens or not curs:
            continue
        day = pd.Timestamp(g["date"]).date()
        morning = [r for r in curs if _ts(r["fetched_at"]).tz_convert(dr.CENTRAL).date() == day]
        cur = (morning or curs)[0]
        asof = _ts(cur["fetched_at"]) + timedelta(minutes=5)

        def proj(ver):
            xs = sorted(pj.get((g["id"], ver), []), key=lambda r: _ts(r["created_at"]))
            before = [r for r in xs if _ts(r["created_at"]) <= asof]
            return (before[-1] if before else (xs[0] if xs else None))

        v2, v1 = proj("czarp-v2"), proj(V1_VERSION)
        o_sp = next((r["spread"] for r in opens if r.get("spread") is not None), None)
        o_t = next((r["total"] for r in opens if r.get("total") is not None), None)
        gd = dict(event_id=str(g["external_id"]), game_date=g["date"], kp_home=g["home"], kp_away=g["away"],
                  home=g["home"], away=g["away"], neutral=g.get("neutral"),
                  tip_ct=_ts(g["tipoff"]).tz_convert(dr.CENTRAL).strftime("%Y-%m-%d %H:%M") if g.get("tipoff") else None,
                  open_home_spread=o_sp, current_home_spread=cur.get("spread"),
                  open_total=o_t, current_total=cur.get("total"))
        if v2 is not None and v2.get("spread") is not None:
            gd["v2_home_margin"] = -float(v2["spread"])
        if v2 is not None and v2.get("total") is not None:
            gd["v2_total"] = float(v2["total"])
        if v1 is not None and v1.get("total") is not None:
            gd["v1_total"] = float(v1["total"])
        for m in ("total", "spread"):
            off = (g["id"], m) in official
            r = BM.build_row(gd, m, None, official=off)
            if r is None:
                continue
            stored = (v2 or {}).get(f"skip_reason_{m}") if m == "spread" else \
                ((v2 or {}).get("skip_reason_total") or (v1 or {}).get("skip_reason_total"))
            if not off:
                r["skip_reason"] = stored or "skip reason not recorded (backfill)"
            r["note"] = r["note"] + "; backfill: morning line, close unavailable"
            out.append((g["id"], r))
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--start", default="2026-11-02")
    ap.add_argument("--end", default=None)
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args(argv)
    now = dr.now_ct()
    end = a.end or now.date().isoformat()
    c = Client()
    games = get_all(c, "games", select="id,external_id,date,home,away,tipoff,neutral",
                    league="eq.ncaam", date=f"gte.{a.start}")
    games = [g for g in games if g["date"] <= end]
    ids = [g["id"] for g in games]
    lines, projs = [], []
    for i in range(0, len(ids), 150):
        chunk = ",".join(map(str, ids[i:i + 150]))
        lines += get_all(c, "market_lines", select="id,game_id,line_type,spread,total,fetched_at",
                         source="eq.vegas", game_id=f"in.({chunk})")
        projs += get_all(c, "projections", select="id,game_id,model_version,spread,total,created_at,"
                         "skip_reason_spread,skip_reason_total", game_id=f"in.({chunk})")
    picks = c.get("picks", select="game_id,market,tier,result", league="eq.ncaam")
    rows = build(games, lines, projs, picks)
    print(f"backfill {a.start}..{end}: {len(games)} games, {len(lines)} lines, {len(projs)} projections -> "
          f"{len(rows)} big moves ({sum(r['market'] == 'total' for _, r in rows)} totals, "
          f"{sum(r['market'] == 'spread' for _, r in rows)} spreads)")
    if a.dry_run:
        for gid, r in rows:
            print(f"  {r['game_date']} {r['away']} @ {r['home']} {r['market']} {r['open_line']} -> {r['current_line']} "
                  f"({r['direction']}) czarp {r['czarp_line']} {r['czarp_side']} against={r['against_czarp']}")
        return 0
    out = [BM.db_row(r, gid, now, source="backfill") for gid, r in rows]
    c.upsert("big_moves", out, on_conflict="game_id,market,game_date", returning=False, ignore_duplicates=True)
    n = BM.grade_pending(c, now, lines_for=None, log=print)
    print(f"stored {len(out)} (existing rows kept), graded {n}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
