"""Publish (or dry-run) the official weekly CZarp rankings snapshot.

Rating = projected margin vs a league-average D-I team on a neutral floor,
using current matchup_inputs and matchup_constants (same formula as the
matchup maker). Writes ranking_snapshots only; never touches picks.

    python3 weekly_rankings.py [--as-of YYYY-MM-DD] [--apply] [--force]

Default is a dry run for today in America/Chicago. The daily run also
invokes this once the current week's Monday 7 AM CT has passed, if that
week has no snapshot yet. A missed Monday is caught up later in the same
week; a week is never rewritten unless --force is passed. Do not apply
until Scantron approves.
"""
from __future__ import annotations

import argparse
import os
from datetime import date, datetime, time
from zoneinfo import ZoneInfo

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

import pandas as pd

from czarp.rankings import CT, build_poll, monday_on_or_before
from czarp.supabase_writer import Client

# weekly_rankings.py lives at the repo root.
ROOT = os.path.dirname(os.path.abspath(__file__))
RATINGS_CSV = os.path.join(ROOT, "data", "ratings.csv")


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


def week_rows(c: Client, week_of: date, league: str = "ncaam") -> list[dict]:
    return get_all(c, "ranking_snapshots",
                   select="id,team_id,rank,week_of,as_of",
                   league=f"eq.{league}", week_of=f"eq.{week_of.isoformat()}",
                   order="rank")


def previous_snapshot(c: Client, week_of: date, league: str = "ncaam") -> list[dict]:
    """Ranks from the most recent official week strictly before `week_of`."""
    rows = c.get("ranking_snapshots", select="week_of", league=f"eq.{league}",
                 week_of=f"lt.{week_of.isoformat()}", order="week_of.desc", limit="1")
    if not rows:
        return []
    prev = rows[0]["week_of"]
    return get_all(c, "ranking_snapshots",
                   select="team_id,rank", week_of=f"eq.{prev}",
                   league=f"eq.{league}", order="rank")


def maybe_ratings() -> pd.DataFrame | None:
    try:
        return pd.read_csv(RATINGS_CSV)
    except Exception:
        return None


def _payload(poll: list[dict]) -> list[dict]:
    cols = [k for k in poll[0] if k != "name"] if poll else []
    return [{k: r[k] for k in cols} for r in poll]


def write_week(c: Client, poll: list[dict], week_of: date, league: str = "ncaam") -> None:
    """Upsert the whole week in one request. On failure, delete the week's rows so
    a later run can retry and the week is never marked published from a partial write."""
    payload = _payload(poll)
    if not payload:
        return
    try:
        # Client upserts in groups of 500; a full D-I poll (~365) is one request.
        c.upsert("ranking_snapshots", payload, on_conflict="league,week_of,team_id",
                 returning=False)
    except Exception:
        try:
            c.delete("ranking_snapshots", league=f"eq.{league}",
                     week_of=f"eq.{week_of.isoformat()}")
        except Exception:
            pass
        raise


def run(as_of=None, apply: bool = False, force: bool = False, log=print,
        client: Client | None = None) -> list[dict]:
    as_of = as_of or datetime.now(CT).date()
    week_of = monday_on_or_before(as_of)
    c = client or Client()
    inputs, consts, teams, games = load_inputs(c)
    try:
        prev = previous_snapshot(c, week_of)
    except Exception as e:
        log(f"  previous snapshot unavailable ({type(e).__name__}: {e}) — movement will be blank")
        prev = []
    poll = build_poll(inputs, consts, teams, games, as_of, ratings=maybe_ratings(), prev=prev)
    log(f"{'APPLY' if apply else 'DRY RUN'}: CZarp rankings for {as_of} "
        f"(week of {week_of}): {len(poll)} teams")
    log("  Top 25 (rating = projected margin vs average D-I team, neutral floor):")
    for r in poll[:25]:
        rec = ""
        if r["di_wins"] is not None:
            rec = f"  D-I {r['di_wins']}-{r['di_losses']}"
        if r["kp_wins"] is not None:
            rec += f"  src {r['kp_wins']}-{r['kp_losses']}"
        move = "—" if r["prev_rank"] is None else f"{r['prev_rank']-r['rank']:+d} (was {r['prev_rank']})"
        log(f"  {r['rank']:2d}. {r.get('name') or ('team_id='+str(r['team_id']))}  "
            f"rating {r['rating']:+.2f}  move {move}{rec}")
    if not apply:
        log("dry run: nothing written (pass --apply to upsert ranking_snapshots)")
        return poll
    existing = week_rows(c, week_of)
    if existing and not force:
        log(f"  rankings: week of {week_of} already published "
            f"({len(existing)} rows, as_of {existing[0].get('as_of')}); refusing to overwrite (pass --force)")
        return poll
    write_week(c, poll, week_of)
    log(f"upserted {len(poll)} ranking_snapshots rows for week of {week_of} (as_of {as_of})")
    return poll


def maybe_publish(now, log=print, apply: bool = True, client: Client | None = None):
    """Publish the current week's poll once now >= that Monday 7:00 America/Chicago
    and no snapshot exists yet for (league='ncaam', week_of=that Monday).

    A missed Monday is caught up on any later run in the same week. After the
    calendar rolls to the next Monday, last week is no longer made up.
    """
    if now.tzinfo is None:
        now = now.replace(tzinfo=CT)
    else:
        now = now.astimezone(CT)
    week = monday_on_or_before(now.date())
    gate = datetime.combine(week, time(7, 0), tzinfo=CT)
    if now < gate:
        return None
    c = client or Client()
    try:
        existing = week_rows(c, week)
    except Exception as e:
        log(f"  rankings: table not ready ({type(e).__name__}: {e})")
        raise
    if existing:
        log(f"  rankings: already published for week of {week}")
        return []
    return run(as_of=now.date(), apply=apply, log=log, client=c)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--as-of", default=None, help="YYYY-MM-DD (default: today CT)")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--force", action="store_true",
                    help="overwrite an existing snapshot for the same week (default: refuse)")
    a = ap.parse_args(argv)
    as_of = date.fromisoformat(a.as_of) if a.as_of else None
    run(as_of=as_of, apply=a.apply, force=a.force)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
