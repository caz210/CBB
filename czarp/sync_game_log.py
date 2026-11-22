"""Keep the team game log complete: non-D-I results plus any missing D-I finals.

The D-I slate writer only upserts games where both sides map to a ratings
name (`kp_home` and `kp_away`). team_profiles.games_played is the ratings
W+L, which includes games against non-D-I opponents, so the team log
(model_grades, which is games ⨝ projections) ran short of games_played.

This module:

- upserts completed D-I vs non-D-I games into non_di_games (no projections,
  no market lines, never on the slate)
- inserts completed D-I vs D-I finals that are not yet in games (e.g. the
  Nov 1 openers, before slate tracking started) — still no projections
- fills games.home_team_id / away_team_id when the names match a team but
  the ids were left null (ledger-first inserts)

It never writes picks, the ledger, or the hash chain.

CLI::

    python -m czarp.sync_game_log --lookback 7
    python -m czarp.sync_game_log --date YYYY-MM-DD --lookback 7

Requires SUPABASE_URL and SUPABASE_SERVICE_KEY. The key is never logged.
"""
from __future__ import annotations

import argparse
import json
import sys
import urllib.request
from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo

from backtest import NameMatcher
from czarp.supabase_writer import LEAGUE, SPORT, Client, _season
from czarp.sync_finals import (
    DEFAULT_LOOKBACK, date_window, espn_end_utc, resolve_final_at, today_ct,
)

CENTRAL = ZoneInfo("America/Chicago")


def get_all(c: Client, table: str, page: int = 1000, **params) -> list[dict]:
    out, off = [], 0
    while True:
        rows = c.get(table, limit=str(page), offset=str(off), order="id", **params)
        out.extend(rows)
        if len(rows) < page:
            return out
        off += page

SB_URL = ("https://site.api.espn.com/apis/site/v2/sports/basketball/"
          "mens-college-basketball/scoreboard")
TEAMS_URL = ("https://site.api.espn.com/apis/site/v2/sports/basketball/"
             "mens-college-basketball/teams?limit=400")


def _int(v):
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def parse_scoreboard(payload: dict, day: date | None = None) -> list[dict]:
    """Normalize an ESPN scoreboard payload into row dicts (includes team ids)."""
    rows = []
    for e in (payload or {}).get("events", []):
        c = (e.get("competitions") or [{}])[0]
        comp = {t["homeAway"]: t for t in c.get("competitors") or []}
        if "home" not in comp or "away" not in comp:
            continue
        st = (c.get("status") or {}).get("type") or {}
        raw = e.get("date") or ""
        try:
            utc = datetime.strptime(raw, "%Y-%m-%dT%H:%MZ").replace(tzinfo=timezone.utc)
        except ValueError:
            continue
        tip_ct = utc.astimezone(CENTRAL)
        game_date = tip_ct.date().isoformat()
        if day is not None and game_date != day.isoformat():
            # ESPN's dates= param can include a tip that lands on a neighbor in CT
            continue
        ht, at = comp["home"].get("team") or {}, comp["away"].get("team") or {}
        rows.append({
            "event_id": str(e["id"]),
            "game_date": game_date,
            "tip_ct": tip_ct,
            "home": ht.get("location") or ht.get("displayName"),
            "away": at.get("location") or at.get("displayName"),
            "home_full": ht.get("displayName"),
            "away_full": at.get("displayName"),
            "home_id": str(ht.get("id") or "") or None,
            "away_id": str(at.get("id") or "") or None,
            "home_score": _int(comp["home"].get("score")),
            "away_score": _int(comp["away"].get("score")),
            "neutral": bool(c.get("neutralSite")),
            "completed": bool(st.get("completed")),
            "state": st.get("state"),
            "status_name": st.get("name"),
        })
    return rows


def fetch_scoreboard(day: date, opener=None) -> list[dict]:
    opener = opener or urllib.request.urlopen
    url = f"{SB_URL}?dates={day.strftime('%Y%m%d')}&groups=50&limit=500"
    with opener(url, timeout=20) as r:
        payload = json.load(r)
    return parse_scoreboard(payload, day)


def fetch_di_espn_ids(opener=None) -> set[str]:
    """ESPN ids for the 365 D-I teams. Empty set means the call failed; do not guess."""
    opener = opener or urllib.request.urlopen
    try:
        with opener(TEAMS_URL, timeout=20) as r:
            payload = json.load(r)
    except Exception:
        return set()
    out = set()
    for s in payload.get("sports") or []:
        for lg in s.get("leagues") or []:
            for t in lg.get("teams") or []:
                tid = (t.get("team") or {}).get("id")
                if tid:
                    out.add(str(tid))
    return out


def classify(row: dict, match: NameMatcher, di_ids: set[str] | None = None) -> dict:
    """Return {home_name, away_name, kind} where kind is di, non_di, or skip.

    When `di_ids` is non-empty, a side is D-I only if its ESPN id is in that
    set *and* its name maps to a stored team. Both-name matches without
    both ESPN ids are not treated as D-I (avoids a non-D-I namesake).
    If ESPN ids say both sides are D-I but a name failed to map, skip
    (never store that event as a non-D-I result).
    """
    home = match(row.get("home") or "") or match(row.get("home_full") or "")
    away = match(row.get("away") or "") or match(row.get("away_full") or "")
    di_ids = di_ids or set()
    h_espn_di = bool(row.get("home_id") and row["home_id"] in di_ids)
    a_espn_di = bool(row.get("away_id") and row["away_id"] in di_ids)
    if di_ids:
        home_di = bool(home) and h_espn_di
        away_di = bool(away) and a_espn_di
        if home_di and away_di:
            kind = "di"
        elif h_espn_di and a_espn_di:
            kind = "skip"  # both ESPN D-I, but a name failed to map
        elif home_di or away_di:
            kind = "non_di"
        else:
            kind = "skip"
    else:
        # Caller must skip non-D-I writes when ids were unavailable.
        home_di, away_di = bool(home), bool(away)
        if home and away:
            kind = "di"
        elif home or away:
            kind = "non_di"
        else:
            kind = "skip"
    return {
        "home_name": home, "away_name": away, "kind": kind,
        "home_di": home_di, "away_di": away_di,
    }


def _tip_iso(row: dict) -> str | None:
    tip = row.get("tip_ct")
    if isinstance(tip, datetime):
        return tip.isoformat(timespec="seconds")
    return None


def _final_at(row: dict, event_id: str, end_time) -> tuple[str | None, bool | None]:
    """ESPN end clock when available, otherwise tip + 2 hours (estimated)."""
    end = None
    if end_time is not None:
        try:
            end = end_time(event_id)
        except Exception:
            end = None
    return resolve_final_at({}, tip=row.get("tip_ct"), end_utc=end)


_EMPTY = {
    "non_di_new": 0, "non_di_unchanged": 0, "non_di_updated": 0,
    "di_inserted": 0, "ids_filled": 0, "days": 0, "ids_unavailable": False,
}


def sync_game_log(lookback: int = DEFAULT_LOOKBACK, day: date | None = None, *,
                  now: datetime | None = None, client: Client | None = None,
                  scoreboard=None, di_ids: set[str] | None = None,
                  end_time=None, log=print) -> dict:
    """Sync non-D-I results and missing D-I finals for `day` and `lookback` days."""
    c = client or Client()
    end = day or today_ct(now)
    days = date_window(end, lookback)
    teams = get_all(c, "teams", select="id,name", league=f"eq.{LEAGUE}")
    if not teams:
        log("  game log: no teams in db — skip")
        return {**_EMPTY, "days": len(days)}
    tid = {t["name"]: t["id"] for t in teams}
    match = NameMatcher(tid.keys())
    ids_unavailable = False
    if di_ids is None:
        di_ids = fetch_di_espn_ids()
        if not di_ids:
            ids_unavailable = True
            log("  game log: ESPN teams list failed — skipping non-D-I writes")
    elif not di_ids:
        ids_unavailable = True
        log("  game log: ESPN D-I ids unavailable — skipping non-D-I writes")
    if end_time is None:
        end_time = espn_end_utc

    sport = c.get("sports", code=f"eq.{SPORT}", select="id")
    if not sport:
        raise RuntimeError("sports row 'cbb' missing — apply db/schema.sql first")
    sid = sport[0]["id"]
    base = {"sport_id": sid, "league": LEAGUE}

    existing = get_all(c, "games",
                       select="id,external_id,home,away,home_team_id,away_team_id,status",
                       league=f"eq.{LEAGUE}")
    by_eid = {str(g["external_id"]): g for g in existing if g.get("external_id")}

    fetch = scoreboard or fetch_scoreboard
    non_di_rows, di_rows, patches = [], [], []
    seen_nd, seen_di = set(), set()

    for d in days:
        try:
            events = fetch(d)
        except Exception as e:
            log(f"  game log {d}: scoreboard failed ({type(e).__name__}: {e})")
            continue
        for ev in events:
            if not ev.get("completed") or ev.get("home_score") is None or ev.get("away_score") is None:
                continue
            info = classify(ev, match, di_ids)
            eid = ev["event_id"]
            if info["kind"] == "skip":
                continue
            if info["kind"] == "di":
                if not info["home_name"] or not info["away_name"]:
                    continue
                cur = by_eid.get(eid)
                if cur is None:
                    if eid in seen_di:
                        continue
                    seen_di.add(eid)
                    fa, est = _final_at(ev, eid, end_time)
                    di_rows.append({
                        **base,
                        "season": _season(ev["game_date"]),
                        "date": ev["game_date"],
                        "home": info["home_name"],
                        "away": info["away_name"],
                        "home_team_id": tid.get(info["home_name"]),
                        "away_team_id": tid.get(info["away_name"]),
                        "tipoff": _tip_iso(ev),
                        "neutral": bool(ev.get("neutral")),
                        "external_id": eid,
                        "external_ids": {"espn": eid},
                        "status": "final",
                        "home_score": ev["home_score"],
                        "away_score": ev["away_score"],
                        "final_at": fa,
                        "final_at_estimated": est,
                    })
                else:
                    patch = {}
                    if cur.get("home_team_id") is None and tid.get(cur.get("home") or info["home_name"]):
                        patch["home_team_id"] = tid.get(cur.get("home") or info["home_name"])
                    if cur.get("away_team_id") is None and tid.get(cur.get("away") or info["away_name"]):
                        patch["away_team_id"] = tid.get(cur.get("away") or info["away_name"])
                    if patch:
                        patch["external_id"] = eid
                        patches.append(patch)
                continue
            # non-D-I: exactly one side mapped
            if ids_unavailable:
                continue
            if eid in seen_nd or eid in by_eid:
                # already a D-I slate game (shouldn't be) — leave it alone
                continue
            seen_nd.add(eid)
            home_name = info["home_name"] or ev.get("home") or ev.get("home_full")
            away_name = info["away_name"] or ev.get("away") or ev.get("away_full")
            non_di_rows.append({
                **base,
                "season": _season(ev["game_date"]),
                "date": ev["game_date"],
                "home": home_name,
                "away": away_name,
                "home_team_id": tid.get(info["home_name"]) if info["home_di"] else None,
                "away_team_id": tid.get(info["away_name"]) if info["away_di"] else None,
                "tipoff": _tip_iso(ev),
                "neutral": bool(ev.get("neutral")),
                "external_id": eid,
                "external_ids": {"espn": eid},
                "status": "final",
                "home_score": ev["home_score"],
                "away_score": ev["away_score"],
                "final_at": _final_at(ev, eid, end_time)[0],
            })

    # Also repair null team_ids on stored D-I games even if they were not in this window's scoreboard
    for g in existing:
        if g.get("home_team_id") is not None and g.get("away_team_id") is not None:
            continue
        if g.get("external_id") and any(p.get("external_id") == str(g["external_id"]) for p in patches):
            continue
        patch = {"external_id": str(g["external_id"])} if g.get("external_id") else None
        if not patch:
            continue
        changed = False
        if g.get("home_team_id") is None and tid.get(g.get("home")):
            patch["home_team_id"] = tid[g["home"]]
            changed = True
        if g.get("away_team_id") is None and tid.get(g.get("away")):
            patch["away_team_id"] = tid[g["away"]]
            changed = True
        if changed:
            patches.append(patch)

    existing_nd = get_all(
        c, "non_di_games",
        select="external_id,home,away,home_team_id,away_team_id,home_score,away_score,neutral,status,date",
        league=f"eq.{LEAGUE}",
    )
    by_nd = {str(r["external_id"]): r for r in existing_nd if r.get("external_id")}

    def _nd_same(row, cur):
        keys = ("home", "away", "home_team_id", "away_team_id", "home_score", "away_score",
                "neutral", "status", "date")
        return all(row.get(k) == cur.get(k) for k in keys)

    n_new = n_unchanged = n_updated = 0
    for row in non_di_rows:
        cur = by_nd.get(str(row["external_id"]))
        if cur is None:
            n_new += 1
        elif _nd_same(row, cur):
            n_unchanged += 1
        else:
            n_updated += 1

    # Fill team_ids first so a later upsert failure cannot skip the repair.
    n_id = 0
    for p in patches:
        eid = p.pop("external_id")
        c.patch("games", p, league="eq." + LEAGUE, external_id=f"eq.{eid}")
        n_id += 1
    try:
        if non_di_rows:
            c.upsert("non_di_games", non_di_rows, on_conflict="league,external_id", returning=False)
        if di_rows:
            c.upsert("games", di_rows, on_conflict="league,external_id", returning=False)
    except Exception:
        log(f"  game log: team_id repair applied ({n_id}); upsert failed")
        raise

    skip = "non-D-I skipped (ESPN teams list unavailable); " if ids_unavailable else ""
    log(f"  game log: {skip}{n_new} new, {n_unchanged} unchanged, {n_updated} updated non-D-I, "
        f"{len(di_rows)} missing D-I finals inserted, {n_id} team_id(s) filled, {len(days)} day(s)")
    return {
        "non_di_new": n_new, "non_di_unchanged": n_unchanged, "non_di_updated": n_updated,
        "di_inserted": len(di_rows), "ids_filled": n_id, "days": len(days),
        "ids_unavailable": ids_unavailable,
    }


def main(argv: list[str] | None = None) -> int:
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except Exception:
        pass
    ap = argparse.ArgumentParser(description="Sync non-D-I results and missing D-I finals into the team game log")
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
        sync_game_log(lookback=lookback, day=end)
    except Exception as e:
        print(f"game log sync failed ({type(e).__name__}: {e})", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
