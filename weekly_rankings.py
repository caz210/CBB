"""Publish (or dry-run) the official Monday CZarp rankings snapshot.

Rating = projected margin vs a league-average D-I team on a neutral floor,
using current matchup_inputs and matchup_constants (same formula as the
matchup maker). Writes ranking_snapshots only; never touches picks.

    python3 weekly_rankings.py [--as-of YYYY-MM-DD] [--apply]

Default is a dry run for today in America/Chicago. The daily run also
invokes this on Mondays after a successful sync, still as a no-op unless
the migration has been applied. Do not apply until Scantron approves.
"""
from __future__ import annotations

import argparse
from datetime import datetime
from zoneinfo import ZoneInfo

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

import pandas as pd

from czarp.rankings import CT, build_poll, monday_on_or_before
from czarp.supabase_writer import Client


def get_all(c: Client, table: str, page: int = 1000, order: str = "id", **params) -> list[dict]:
    out, off = [], 0
    while True:
        rows = c.get(table, limit=str(page), offset=str(off), order=order, **params)
        out.extend(rows)
        if len(rows) < page:
            return out
        off += page


def load_inputs(c: Client) -> tuple[list[dict], dict, list[dict], list[dict]]:
    teams = get_all(c, "teams", select="id,name,league", league="eq.ncaam")
    inputs = get_all(c, "matchup_inputs",
                     select="team_id,oe,de,tempo,kp_pct,unit_v2,hca", order="team_id")
    consts_rows = c.get("matchup_constants", league="eq.ncaam", limit="1")
    if not consts_rows:
        raise RuntimeError("matchup_constants is empty — cannot rate teams")
    games = get_all(c, "games",
                    select="id,date,home,away,home_score,away_score,status",
                    league="eq.ncaam")
    return inputs, consts_rows[0], teams, games


def previous_snapshot(c: Client, as_of) -> list[dict]:
    rows = c.get("ranking_snapshots", select="as_of", league="eq.ncaam",
                 as_of=f"lt.{as_of.isoformat()}", order="as_of.desc", limit="1")
    if not rows:
        return []
    prev = rows[0]["as_of"]
    return get_all(c, "ranking_snapshots",
                   select="team_id,rank", as_of=f"eq.{prev}", order="rank")


def maybe_ratings() -> pd.DataFrame | None:
    try:
        return pd.read_csv("data/ratings.csv")
    except Exception:
        try:
            return pd.read_csv("/workspace/CBB/data/ratings.csv")
        except Exception:
            return None


def run(as_of=None, apply: bool = False, log=print, client: Client | None = None) -> list[dict]:
    as_of = as_of or datetime.now(CT).date()
    c = client or Client()
    inputs, consts, teams, games = load_inputs(c)
    try:
        prev = previous_snapshot(c, as_of)
    except Exception as e:
        log(f"  previous snapshot unavailable ({type(e).__name__}: {e}) — movement will be blank")
        prev = []
    poll = build_poll(inputs, consts, teams, games, as_of, ratings=maybe_ratings(), prev=prev)
    log(f"{'APPLY' if apply else 'DRY RUN'}: CZarp rankings for {as_of} "
        f"(week of {monday_on_or_before(as_of)}): {len(poll)} teams")
    log("  Top 25 (rating = projected margin vs average D-I team, neutral floor):")
    for r in poll[:25]:
        rec = ""
        if r["kp_wins"] is not None:
            rec = f"  KenPom {r['kp_wins']}-{r['kp_losses']}"
        if r["di_wins"] is not None:
            rec += f"  D-I {r['di_wins']}-{r['di_losses']}"
        move = "—" if r["prev_rank"] is None else f"{r['prev_rank']-r['rank']:+d} (was {r['prev_rank']})"
        log(f"  {r['rank']:2d}. {r.get('name') or ('team_id='+str(r['team_id']))}  rating {r['rating']:+.2f}  move {move}{rec}")
    if not apply:
        log("dry run: nothing written (pass --apply to upsert ranking_snapshots)")
        return poll
    # upsert in chunks
    cols = [k for k in poll[0] if k != "name"] if poll else []
    payload = [{k: r[k] for k in cols} for r in poll]
    for i in range(0, len(payload), 200):
        c.upsert("ranking_snapshots", payload[i:i + 200], on_conflict="as_of,team_id", returning=False)
    log(f"upserted {len(poll)} ranking_snapshots rows for {as_of}")
    return poll



def maybe_publish(now, log=print, apply: bool = True, client: Client | None = None):
    """Called from the daily run. Publishes once on Monday at/after 7 AM America/Chicago."""
    if now.weekday() != 0 or now.hour < 7:
        return None
    as_of = now.date()
    c = client or Client()
    try:
        existing = c.get("ranking_snapshots", select="id", as_of=f"eq.{as_of.isoformat()}", limit="1")
    except Exception as e:
        log(f"  rankings: table not ready ({type(e).__name__}: {e})")
        return None
    if existing:
        log(f"  rankings: already published for {as_of}")
        return []
    return run(as_of=as_of, apply=apply, log=log, client=c)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--as-of", default=None, help="YYYY-MM-DD (default: today CT)")
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args(argv)
    from datetime import date as _date
    as_of = _date.fromisoformat(a.as_of) if a.as_of else None
    run(as_of=as_of, apply=a.apply)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
