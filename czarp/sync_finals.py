"""Pull ESPN final scores for every stored game on a date and update games.status / scores.

Scores are matched to games.home / games.away by team name, not ESPN's homeAway slot
(neutral events often flip that label after we persist the morning slate).
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
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from czarp.supabase_writer import Client

CENTRAL = ZoneInfo("America/Chicago")
LEAGUE = "ncaam"
DEFAULT_LOOKBACK = 7
FINAL_AT_ESTIMATE = timedelta(hours=2)  # when ESPN has no end time
SUMMARY_URL = ("https://site.api.espn.com/apis/site/v2/sports/basketball/"
               "mens-college-basketball/summary")

# ESPN status.type.name values we care about
_POSTPONED = {"STATUS_POSTPONED", "STATUS_POSTPONE"}
_CANCELED = {"STATUS_CANCELED", "STATUS_CANCELLED"}


class EspnScoreboardError(RuntimeError):
    """Raised when every date in the window fails to fetch from ESPN."""


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


def parse_espn_time(value) -> datetime | None:
    """Parse an ESPN timestamp (…Z or ISO) into an aware datetime."""
    if value is None or (isinstance(value, float) and value != value):
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else None
    s = str(value).strip()
    if not s:
        return None
    s = s.replace("Z", "+00:00")
    if "T" not in s and " " in s:
        s = s.replace(" ", "T", 1)
    # Postgres sometimes emits +00 rather than +00:00
    if len(s) >= 3 and (s[-3] in "+-") and s[-2:].isdigit():
        s = s + ":00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        try:
            dt = datetime.strptime(s[:16], "%Y-%m-%dT%H:%M").replace(tzinfo=timezone.utc)
        except ValueError:
            return None
    return dt if dt.tzinfo else None


def iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def espn_end_utc(event_id: str, session=None) -> datetime | None:
    """End of game from the ESPN summary play-by-play wallclock, if present."""
    import requests
    try:
        s = session or requests
        r = s.get(SUMMARY_URL, params={"event": str(event_id)}, timeout=15,
                  headers={"User-Agent": "czarp-finals/1.0"})
        if r.status_code != 200:
            return None
        plays = r.json().get("plays") or []
    except Exception:
        return None
    if not isinstance(plays, list):
        plays = plays.get("items") or plays.get("plays") or []
    for play in reversed(plays):
        if not isinstance(play, dict):
            continue
        typ = play.get("type") or {}
        end = typ.get("id") == "402" or typ.get("text") in ("End Game", "End of Game")             or play.get("text") in ("End of Game", "End Game")
        if end:
            return parse_espn_time(play.get("wallclock"))
    # last play with a wallclock as a weaker actual
    for play in reversed(plays):
        if isinstance(play, dict) and play.get("wallclock"):
            return parse_espn_time(play.get("wallclock"))
    return None


def resolve_final_at(g: dict, *, tip=None, end_utc=None) -> tuple[str | None, bool | None]:
    """(final_at ISO, estimated). Prefer ESPN end time; else tip + 2 hours."""
    end = end_utc if end_utc is not None else g.get("end_utc")
    end = parse_espn_time(end) if end is not None and not isinstance(end, datetime) else end
    if isinstance(end, datetime) and end.tzinfo:
        return iso_utc(end), False
    tip = tip if tip is not None else g.get("tip_utc") or g.get("tipoff")
    tip = parse_espn_time(tip) if not isinstance(tip, datetime) else tip
    if isinstance(tip, datetime) and tip.tzinfo:
        return iso_utc(tip + FINAL_AT_ESTIMATE), True
    return None, None


def should_apply(current: dict, new_status: str, home_score, away_score, *,
                 filling_final_at: bool = False) -> bool:
    """Never downgrade a final. Skip no-op writes. Allow a final_at backfill on an
    already-final row (filling_final_at)."""
    cur = current.get("status") or "scheduled"
    if cur == "final" and new_status != "final":
        return False
    if cur == new_status and current.get("home_score") == home_score and current.get("away_score") == away_score:
        return filling_final_at
    return True


def _scoreboard(day: date):
    import daily_run as dr
    return dr.espn_scoreboard(day)



def _name_resolver(stored_home, stored_away, match=None):
    """Map an ESPN / stored team name onto the stored label pair.

    If `match` is omitted, a NameMatcher is built from the stored names so
    'San Diego State' lines up with stored 'San Diego St.'.
    """
    if match is None:
        from backtest import NameMatcher
        names = [n for n in (stored_home, stored_away) if n]
        match = NameMatcher(names) if names else None

    def _n(name):
        if not name:
            return None
        if match is not None:
            got = match(str(name))
            if got:
                return got
        return str(name)

    return _n


def espn_sides_flipped(stored_home, stored_away, espn_home, espn_away, match=None,
                       espn_home_alt=None, espn_away_alt=None):
    """True if ESPN's home is our stored away, False if same orientation, None if unknown.

    Neutral events often flip ESPN's homeAway after we persist games.home /
    games.away (or the morning snapshot). Prefer ESPN's location (`home`) over
    displayName (`home_full`); pass the displayName as the *_alt fallback.
    Never falls back to slot order.
    """
    if not stored_home or not stored_away:
        return None
    _n = _name_resolver(stored_home, stored_away, match)
    sh, sa = _n(stored_home), _n(stored_away)
    if not sh or not sa:
        return None

    def _try(eh_name, ea_name):
        if not eh_name or not ea_name:
            return None
        eh, ea = _n(eh_name), _n(ea_name)
        if sh == eh and sa == ea:
            return False
        if sh == ea and sa == eh:
            return True
        return None

    got = _try(espn_home, espn_away)
    if got is not None:
        return got
    return _try(espn_home_alt, espn_away_alt)


def negate_home_spread(value):
    """Negate a home-perspective spread, mapping -0.0 to 0.0. None stays None."""
    if value is None or value == "":
        return None
    try:
        v = -float(value)
    except (TypeError, ValueError):
        return value
    if v != v:          # NaN
        return value
    return 0.0 if v == 0.0 else v


# Home-perspective spread fields on ESPN/DK line dicts and closing-line snapshots.
_HOME_SPREAD_KEYS = (
    "open_home_spread", "current_home_spread", "close_home_spread",
    "home_spread",
)
# Home/away price (and ML, if present) pairs to swap when ESPN's sides are flipped.
_PRICE_SWAPS = (
    ("open_home_spread_price", "open_away_spread_price"),
    ("current_home_spread_price", "current_away_spread_price"),
    ("close_home_spread_price", "close_away_spread_price"),
    ("home_spread_price", "away_spread_price"),
    ("open_home_ml", "open_away_ml"),
    ("current_home_ml", "current_away_ml"),
    ("close_home_ml", "close_away_ml"),
    ("home_ml", "away_ml"),
    ("ml_home", "ml_away"),
    ("vegas_home_ml", "vegas_away_ml"),
)


def reorient_home_lines(lines: dict, flipped: bool) -> dict:
    """If `flipped`, negate home spreads and swap home/away spread prices and MLs.

    Mutates and returns `lines`. Totals are unchanged. A pick'em stays 0.0
    (never -0.0). Unknown keys are left alone. Do not pass a projections row
    (its model `spread` is a fair value, not a Vegas home line).
    """
    if not flipped or not lines:
        return lines
    for k in _HOME_SPREAD_KEYS:
        if k in lines and lines[k] is not None:
            lines[k] = negate_home_spread(lines[k])
    for a, b in _PRICE_SWAPS:
        if a in lines or b in lines:
            lines[a], lines[b] = lines.get(b), lines.get(a)
    prices = lines.get("prices")
    if isinstance(prices, dict):
        prices = dict(prices)
        swapped = False
        for a, b in (("home_spread_price", "away_spread_price"),
                     ("ml_home", "ml_away"), ("home_ml", "away_ml")):
            if a in prices or b in prices:
                prices[a], prices[b] = prices.get(b), prices.get(a)
                swapped = True
        if swapped:
            lines["prices"] = prices
    ml = lines.get("moneyline")
    if isinstance(ml, dict) and ("home" in ml or "away" in ml):
        lines["moneyline"] = {**ml, "home": ml.get("away"), "away": ml.get("home")}
    return lines


def align_fetched_lines(lines, stored_home, stored_away, espn_home, espn_away,
                        match=None, espn_home_alt=None, espn_away_alt=None):
    """Reorient a home-frame ESPN/DK line dict onto stored home/away.

    Returns (lines, flipped). `lines` is mutated only when flipped is True.
    When ESPN names are missing or cannot be matched, lines are left unchanged
    and flipped is None (do not guess).
    """
    flipped = espn_sides_flipped(stored_home, stored_away, espn_home, espn_away,
                                 match=match, espn_home_alt=espn_home_alt,
                                 espn_away_alt=espn_away_alt)
    if flipped is True:
        reorient_home_lines(lines, True)
    return lines, flipped


def align_espn_box(stored_home, stored_away, espn_home, espn_away, home_score, away_score,
                   match=None):
    """Assign ESPN scores (and close-line orientation) to our stored home/away labels.

    Neutral events often flip ESPN's homeAway between the morning slate (when we
    first persist games.home / games.away) and the final box. Copying ESPN's
    home_score into games.home_score then credits the points to the wrong team.

    Returns (home_score, away_score, flipped) in *our* stored labels, or None
    if any name is missing or the two ESPN teams cannot be matched to the
    stored pair. Never falls back to slot order. `flipped` is True when ESPN's
    home is our stored away (the ESPN home-frame close must be negated).
    `match`, if given, maps an ESPN name to a KenPom / stored name.
    """
    if home_score is None or away_score is None:
        return None
    flipped = espn_sides_flipped(stored_home, stored_away, espn_home, espn_away, match=match)
    if flipped is None:
        return None
    hs, aws = int(home_score), int(away_score)
    if flipped:
        return aws, hs, True
    return hs, aws, False


def map_espn_scores(stored_home, stored_away, espn_home, espn_away, home_score, away_score,
                    match=None):
    """(home_score, away_score) in stored labels, or None. See align_espn_box."""
    got = align_espn_box(stored_home, stored_away, espn_home, espn_away,
                         home_score, away_score, match=match)
    return None if got is None else (got[0], got[1])


def sync_finals(lookback: int = DEFAULT_LOOKBACK, day: date | None = None, *,
                now: datetime | None = None, client: Client | None = None,
                log=print, scoreboard=None, end_time=None) -> dict:
    """Fetch ESPN scores for `day` and the previous `lookback` days; update matching games.

    `day` defaults to today in America/Chicago. Returns a counts dict (no secrets).
    Sets games.final_at once (ESPN end time, else tip + 2h) and does not bump
    games.updated_at — that field is not a finish clock.
    """
    end = day or today_ct(now)
    days = date_window(end, lookback)
    c = client or Client()
    fetch = scoreboard or _scoreboard
    if end_time is None:
        import requests
        _sess = requests.Session()
        _sess.headers.update({"User-Agent": "czarp-finals/1.0"})
        get_end = lambda eid, _s=_sess: espn_end_utc(eid, session=_s)
    else:
        get_end = end_time
    summary = {"start": days[0].isoformat(), "end": end.isoformat(), "dates": len(days),
               "espn_games": 0, "matched": 0, "updated": 0, "unchanged": 0,
               "unmatched_espn": 0, "name_mismatch": 0, "errors": 0,
               "final_at_set": 0, "final_at_actual": 0, "final_at_estimated": 0}

    for d in days:
        try:
            events = fetch(d)
        except Exception as e:
            log(f"  finals {d.isoformat()}: ESPN scoreboard failed ({type(e).__name__}: {e})")
            summary["errors"] += 1
            continue
        events = [g for g in events if g.get("game_date") == d.isoformat()]
        summary["espn_games"] += len(events)
        existing = {str(r["external_id"]): r for r in c.get(
            "games",
            select="id,external_id,date,status,home,away,home_score,away_score,league,tipoff,final_at,final_at_estimated",
            league=f"eq.{LEAGUE}", date=f"eq.{d.isoformat()}", limit="2000")}
        from backtest import NameMatcher
        names = {r.get("home") for r in existing.values()} | {r.get("away") for r in existing.values()}
        names.discard(None)
        names.discard("")
        matcher = NameMatcher(names) if names else None
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
            if scored:
                aligned = map_espn_scores(cur.get("home"), cur.get("away"),
                                          g.get("home"), g.get("away"),
                                          g.get("home_score"), g.get("away_score"),
                                          match=matcher)
                if aligned is None:
                    log(f"  finals {d.isoformat()} espn={eid}: ESPN teams "
                        f"{g.get('away')} / {g.get('home')} do not match stored "
                        f"{cur.get('away')} / {cur.get('home')} — scores not applied")
                    summary["name_mismatch"] += 1
                    continue
                hs, as_ = aligned
            else:
                hs, as_ = None, None
            if st == "final" and (hs is None or as_ is None):
                summary["unchanged"] += 1
                continue
            need_final_at = st == "final" and not cur.get("final_at")
            maybe_upgrade = (st == "final" and bool(cur.get("final_at"))
                             and cur.get("final_at_estimated") is True)
            if not should_apply(cur, st, hs, as_, filling_final_at=need_final_at or maybe_upgrade):
                summary["unchanged"] += 1
                continue
            score_noop = (cur.get("status") == st
                          and cur.get("home_score") == hs
                          and cur.get("away_score") == as_)
            patch = {"league": cur.get("league") or LEAGUE, "external_id": eid}
            if not score_noop:
                patch.update(status=st, home_score=hs, away_score=as_)
            if need_final_at:
                actual = None
                try:
                    actual = get_end(eid) if get_end else None
                except Exception:
                    actual = None
                if isinstance(actual, str):
                    actual = parse_espn_time(actual)
                tip = g.get("tip_utc") or cur.get("tipoff")
                fa, est = resolve_final_at(g, tip=tip, end_utc=actual)
                if fa:
                    patch["final_at"] = fa
                    patch["final_at_estimated"] = bool(est)
                    summary["final_at_set"] += 1
                    summary["final_at_estimated" if est else "final_at_actual"] += 1
                elif score_noop:
                    summary["unchanged"] += 1
                    continue
            elif maybe_upgrade:
                try:
                    actual = get_end(eid) if get_end else None
                except Exception:
                    actual = None
                fa, est = resolve_final_at(g, end_utc=actual)
                if fa and est is False:
                    patch = {"league": cur.get("league") or LEAGUE, "external_id": eid,
                             "final_at": fa, "final_at_estimated": False}
                    summary["final_at_set"] += 1
                    summary["final_at_actual"] += 1
                elif score_noop:
                    summary["unchanged"] += 1
                    continue
            if set(patch) <= {"league", "external_id"}:
                summary["unchanged"] += 1
                continue
            rows.append(patch)
        for row in rows:
            body = {k: v for k, v in row.items() if k not in ("league", "external_id")}
            # never bump updated_at — ingest time is not when the game ended
            filters = {
                "league": f"eq.{row['league']}",
                "external_id": f"eq.{row['external_id']}",
            }
            # Defense in depth: a stale scheduled/in_progress/postponed write
            # must not downgrade a row that became final after we read it.
            # Score corrections and final_at backfills keep or omit
            # status="final", so they still match an already-final row.
            if body.get("status") not in (None, "final"):
                filters["status"] = "neq.final"
            c.patch("games", body, **filters)
        summary["updated"] += len(rows)

    log(f"  finals {summary['start']}..{summary['end']}: updated {summary['updated']} of "
        f"{summary['matched']} matched games ({summary['espn_games']} ESPN, "
        f"{summary['unmatched_espn']} not in db, {summary['unchanged']} unchanged"
        + (f", final_at {summary['final_at_actual']} actual / {summary['final_at_estimated']} estimated"
           if summary["final_at_set"] else "")
        + (f", {summary['errors']} date errors" if summary["errors"] else "") + ")")
    if summary["dates"] and summary["errors"] == summary["dates"]:
        raise EspnScoreboardError(
            f"ESPN scoreboard failed for every date in {summary['start']}..{summary['end']}"
        )
    return summary


def main(argv: list[str] | None = None) -> int:
    try:                       # same .env daily_run uses (the CLI failed with KeyError: SUPABASE_URL without it)
        from dotenv import load_dotenv
        load_dotenv()
    except Exception:
        pass
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
    # Non-D-I results + missing D-I finals + null team_id repair. Failures
    # are logged but do not fail the finals job (scores already landed).
    try:
        from czarp.sync_game_log import sync_game_log
        sync_game_log(lookback=lookback, day=end)
    except Exception as e:
        print(f"game log sync failed ({type(e).__name__}: {e})", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
