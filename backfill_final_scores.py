"""Dry-run (default): re-align stored final scores with ESPN by team name.

Root cause: sync_finals used to copy ESPN home_score / away_score into
games.home_score / games.away_score by slot. Neutral events often flip
ESPN's homeAway after the morning slate is persisted, so the points landed
on the wrong team (Houston's 86 credited to California, etc.).

This script does not change games.home / games.away, picks, the ledger, or
the hash chain. Pass --apply to patch games.home_score / games.away_score
only. Re-run after Scantron approves; do not apply from this report alone.

    python3 backfill_final_scores.py [--start 2026-11-02] [--end YYYY-MM-DD] [--apply]
"""
from __future__ import annotations

import argparse
import json
import urllib.request
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

from backtest import NameMatcher
from czarp.supabase_writer import Client
from czarp.sync_finals import map_espn_scores

SB = ("https://site.api.espn.com/apis/site/v2/sports/basketball/"
      "mens-college-basketball/scoreboard")


def get_all(c: Client, table: str, page: int = 1000, **params) -> list[dict]:
    out, off = [], 0
    while True:
        rows = c.get(table, limit=str(page), offset=str(off), order="id", **params)
        out.extend(rows)
        if len(rows) < page:
            return out
        off += page


def scoreboard(day: date) -> dict:
    url = f"{SB}?dates={day.strftime('%Y%m%d')}&groups=50&limit=500"
    with urllib.request.urlopen(url, timeout=20) as r:
        data = json.load(r)
    out = {}
    for e in data.get("events", []):
        c = (e.get("competitions") or [{}])[0]
        comp = {t["homeAway"]: t for t in c.get("competitors") or []}
        if "home" not in comp or "away" not in comp:
            continue
        st = (c.get("status") or {}).get("type") or {}
        if not st.get("completed"):
            continue
        def side(tag):
            t = comp[tag].get("team") or {}
            return t.get("location"), t.get("displayName"), comp[tag].get("score")
        hl, hd, hs = side("home")
        al, ad, aws = side("away")
        try:
            hs, aws = int(hs), int(aws)
        except (TypeError, ValueError):
            continue
        out[str(e["id"])] = dict(home=hl, away=al, home_display=hd, away_display=ad,
                                 home_score=hs, away_score=aws, neutral=bool(c.get("neutralSite")))
    return out


def main():
    today = datetime.now(ZoneInfo("America/Chicago")).date()
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--start", default="2026-11-02")
    ap.add_argument("--end", default=today.isoformat())
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    start, end = date.fromisoformat(a.start), date.fromisoformat(a.end)
    c = Client()
    games = get_all(c, "games",
                    select="id,date,home,away,home_score,away_score,status,neutral,external_id",
                    status="eq.final", date=f"gte.{start.isoformat()}")
    games = [g for g in games if g["date"] <= end.isoformat()]
    names = {g["home"] for g in games} | {g["away"] for g in games}
    match = NameMatcher(names)
    espn = {}
    d = start
    while d <= end:
        espn.update(scoreboard(d))
        d += timedelta(days=1)
    ups = []
    skipped = []
    for g in games:
        ev = espn.get(str(g.get("external_id") or ""))
        if not ev:
            skipped.append((g, "not on ESPN scoreboard"))
            continue
        if g.get("home_score") is None or g.get("away_score") is None:
            skipped.append((g, "stored scores are missing"))
            continue
        aligned = map_espn_scores(g["home"], g["away"], ev["home"], ev["away"],
                                  ev["home_score"], ev["away_score"], match=match)
        if aligned is None:
            skipped.append((g, f"names not matched: ESPN {ev['away']} / {ev['home']} "
                            f"vs stored {g['away']} / {g['home']}"))
            continue
        nhs, nas = aligned
        if int(g["home_score"]) == nhs and int(g["away_score"]) == nas:
            continue
        ups.append(dict(id=int(g["id"]), date=g["date"], neutral=g.get("neutral"),
                        external_id=g["external_id"],
                        game=f"{g['away']} @ {g['home']}",
                        old=(int(g["away_score"]), int(g["home_score"])),
                        new=(nas, nhs),
                        espn=f"{ev['away_display']} {ev['away_score']} at {ev['home_display']} {ev['home_score']}"))
    print(f"{'APPLY' if a.apply else 'DRY RUN'}: finals {start}..{end}: {len(games)} games; "
          f"score patches: {len(ups)}; skipped: {len(skipped)}")
    for g, why in skipped:
        print(f"  SKIP {g.get('date')} {g.get('away')} @ {g.get('home')} "
              f"espn={g.get('external_id')} id={g.get('id')}: {why}")
    for u in ups:
        oa, oh = u["old"]; na, nh = u["new"]
        old_w = u["game"].split(" @ ")[0] if oa > oh else u["game"].split(" @ ")[1]
        new_w = u["game"].split(" @ ")[0] if na > nh else u["game"].split(" @ ")[1]
        print(f"  {u['date']} n={u['neutral']} {u['game']}: {oa}-{oh} ({old_w} won) -> "
              f"{na}-{nh} ({new_w} won)  ESPN: {u['espn']}")
    if not a.apply:
        print("dry run: nothing written (pass --apply to patch games.home_score/away_score only)")
        return
    for u in ups:
        na, nh = u["new"]
        c.patch("games", {"home_score": nh, "away_score": na}, id=f"eq.{u['id']}")
    print(f"updated {len(ups)} games (scores only; picks untouched)")


if __name__ == "__main__":
    main()
