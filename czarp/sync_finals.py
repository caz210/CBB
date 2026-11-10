"""Pull ESPN final scores for every stored game on a date and update games.status / scores.

The daily writer only stamps scores for today's slate and for ledger games graded the
next morning, so non-pick games stayed 'scheduled'. This module backfills and keeps
those rows current. It never writes picks.

CLI (evening / late-night / ad-hoc)::

    python -m czarp.sync_finals --lookback 7
    python -m czarp.sync_finals --date YYYY-MM-DD
    python -m czarp.sync_finals --date YYYY-MM-DD --lookback 7

Requires SUPABASE_URL and SUPABASE_SERVICE_KEY. The key is never logged.
"""
from __future__ import annotations

import argparse
import sys
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from czarp.supabase_writer import Client

CENTRAL = ZoneInfo("America/Chicago")
LEAGUE = "ncaam"
DEFAULT_LOOKBACK = 7

# ESPN status.type.name values we care about
_POSTPONED = {"STATUS_POSTPONED", "STATUS_POSTPONE"}
_CANCELED = {"STATUS_CANCELED", "STATUS_CANCELLED"}


def today_ct(now: datetime | None = None) -> date:
    return (now or datetime.now(CENTRAL)).astimezone(CENTRAL).date()


def date_window(end: date, lookback: int) -> list[date]:
    lookback = max(0, int(lookback))
    return [end - timedelta(days=i) for i in range(lookback, -1, -1)]


def game_status(g: dict) -> str:
    """Map an ESPN scoreboard row to games.status."""
    name = (g.get("status_name") or "").upper()
    if g.get("completed"):
        return "final"
    if name in _POSTPONED:
        return "postponed"
    if name in _CANCELED:
        return "canceled"
    if g.get("state") == "post":
        return "postponed"
    if g.get("state") == "in":
        return "in_progress"
    return "scheduled"


def should_apply(current: dict, new_status: str, home_score, away_score) -> bool:
    """Never downgrade a final. Skip no-op writes."""
    cur = current.get("status") or "scheduled"
    if cur == "final" and new_status != "final":
        return False
    if cur == new_status and current.get("home_score") == home_score and current.get("away_score") == away_score:
        return False
    return True


def _scoreboard(day: date):
    import daily_run as dr
    return dr.espn_scoreboard(day)


def sync_finals(lookback: int = DEFAULT_LOOKBACK, day: date | None = None, *,
                now: datetime | None = None, client: Client | None = None,
                log=print, scoreboard=None) -> dict:
    """Fetch ESPN scores for `day` and the previous `lookback` days; update matching games.

    `day` defaults to today in America/Chicago. Returns a counts dict (no secrets).
    """
    end = day or today_ct(now)
    days = date_window(end, lookback)
    c = client or Client()
    fetch = scoreboard or _scoreboard
    ts = (now or datetime.now(CENTRAL)).isoformat(timespec="seconds")
    summary = {"start": days[0].isoformat(), "end": end.isoformat(), "dates": len(days),
               "espn_games": 0, "matched": 0, "updated": 0, "unchanged": 0,
               "unmatched_espn": 0, "errors": 0}

    for d in days:
        try:
            events = fetch(d)
        except Exception as e:
            log(f"  finals {d.isoformat()}: ESPN scoreboard failed ({type(e).__name__}: {e})")
            summary["errors"] += 1
            continue
        # only this calendar date (scoreboard can include adjacent-date tips)
        events = [g for g in events if g.get("game_date") == d.isoformat()]
        summary["espn_games"] += len(events)
        existing = {str(r["external_id"]): r for r in c.get(
            "games", select="id,external_id,date,status,home_score,away_score,league",
            league=f"eq.{LEAGUE}", date=f"eq.{d.isoformat()}", limit="2000")}
        rows = []
        for g in events:
            eid = str(g["event_id"])
            cur = existing.get(eid)
            if cur is None:
                summary["unmatched_espn"] += 1
                continue
            summary["matched"] += 1
            st = game_status(g)
            scored = st in ("final", "in_progress") and g.get("home_score") is not None and g.get("away_score") is not None
            hs = int(g["home_score"]) if scored else None
            as_ = int(g["away_score"]) if scored else None
            if st == "final" and (hs is None or as_ is None):
                # completed but no score yet — leave the row alone and retry later
                summary["unchanged"] += 1
                continue
            if not should_apply(cur, st, hs, as_):
                summary["unchanged"] += 1
                continue
            rows.append({"league": cur.get("league") or LEAGUE, "external_id": eid,
                         "status": st, "home_score": hs, "away_score": as_, "updated_at": ts})
        for row in rows:
            # PATCH existing rows only — a partial upsert would fail NOT NULL on INSERT
            c.patch("games",
                    {k: row[k] for k in ("status", "home_score", "away_score", "updated_at")},
                    league=f"eq.{row['league']}", external_id=f"eq.{row['external_id']}")
        summary["updated"] += len(rows)

    log(f"  finals {summary['start']}..{summary['end']}: updated {summary['updated']} of "
        f"{summary['matched']} matched games ({summary['espn_games']} ESPN, "
        f"{summary['unmatched_espn']} not in db, {summary['unchanged']} unchanged"
        + (f", {summary['errors']} date errors" if summary["errors"] else "") + ")")
    return summary


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Sync ESPN final scores into Supabase games.status / scores")
    ap.add_argument("--date", metavar="YYYY-MM-DD",
                    help="End date in America/Chicago (default: today). Alone, syncs only this date.")
    ap.add_argument("--lookback", type=int, default=None, metavar="N",
                    help=f"Also sync the previous N days (default {DEFAULT_LOOKBACK} when --date is omitted, "
                         "0 when --date is set)")
    a = ap.parse_args(argv)
    if a.date:
        try:
            end = date.fromisoformat(a.date)
        except ValueError:
            print(f"invalid --date {a.date!r}; expected YYYY-MM-DD", file=sys.stderr)
            return 2
    else:
        end = today_ct()
    lookback = a.lookback if a.lookback is not None else (0 if a.date else DEFAULT_LOOKBACK)
    try:
        sync_finals(lookback=lookback, day=end)
    except Exception as e:
        print(f"finals sync failed ({type(e).__name__}: {e})", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
