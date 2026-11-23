"""Build db/migrations/2026-11-23_teams_espn_ids.sql (teams.external_ids.espn backfill).

Every games.external_id is an ESPN event id. Each ESPN event lists exactly two competitor team ids,
so for each of our teams the ESPN id must appear in *every* event that team played. We intersect the
competitor-id pairs across a team's games; different opponents collapse the set to one id.
Leftovers (e.g. a team with one game) are resolved by elimination once the opponent's id is known,
then by ESPN home/away on non-neutral games. A team whose candidate set is still >1, or an id
claimed by two teams, is reported as ambiguous and left unset. Teams with no games are unmatched.

The live Live Line matcher uses these ids as an exact match before falling back to names.

  python scripts/build_espn_team_ids.py                       # read teams/games via SUPABASE_URL + key
  python scripts/build_espn_team_ids.py --teams t.json --games g.json   # offline snapshot
Snapshot JSON: teams = [{"id", "name"}], games = [{"id","date","external_id","home_team_id","away_team_id","neutral"}].
Never writes to the database; it only emits the migration (apply it after review).
"""
from __future__ import annotations

import argparse
import collections
import json
import os
import sys
import time
from datetime import date, timedelta

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

OUT = os.path.join(ROOT, "db", "migrations", "2026-11-23_teams_espn_ids.sql")
SB_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard"
HEADERS = {"User-Agent": "Mozilla/5.0"}  # ESPN 403s some custom agents


def events_from_scoreboard(payload: dict) -> dict[str, dict]:
    """{event_id: {"neutral": bool, "comp": [{"id", "home_away", "name"}]}} for two-team events."""
    out: dict[str, dict] = {}
    for ev in payload.get("events") or []:
        comps = ev.get("competitions") or []
        if not comps:
            continue
        c = comps[0]
        teams = []
        for x in c.get("competitors") or []:
            t = x.get("team") or {}
            if t.get("id") is None:
                continue
            teams.append({"id": str(t["id"]), "home_away": x.get("homeAway"),
                          "name": t.get("displayName") or t.get("location") or ""})
        if len(teams) == 2:
            out[str(ev["id"])] = {"neutral": bool(c.get("neutralSite")), "comp": teams}
    return out


def fetch_events(days: list[str]) -> dict[str, dict]:
    import requests
    out: dict[str, dict] = {}
    for d in days:
        for attempt in range(3):
            try:
                r = requests.get(SB_URL, params={"dates": d, "groups": "50", "limit": "500"},
                                 headers=HEADERS, timeout=30)
                r.raise_for_status()
                out.update(events_from_scoreboard(r.json()))
                break
            except Exception:
                if attempt == 2:
                    raise
                time.sleep(2)
    return out


def derive_espn_ids(teams: list[dict], games: list[dict], events: dict[str, dict]) -> dict:
    """Return {"unique": {team_id: espn_id}, "ambiguous": {team_id: [candidates]}, "unmatched": [team_id]}."""
    team_ids = [int(t["id"]) for t in teams]
    cand: dict[int, set[str]] = {}
    by_team: dict[int, list[dict]] = collections.defaultdict(list)
    usable = []
    for g in games:
        ev = events.get(str(g.get("external_id") or ""))
        if not ev:
            continue
        ids = {c["id"] for c in ev["comp"]}
        usable.append((g, ev, ids))
        for tid in (g.get("home_team_id"), g.get("away_team_id")):
            if tid is None:
                continue
            tid = int(tid)
            by_team[tid].append((g, ev))
            cand[tid] = cand[tid] & ids if tid in cand else set(ids)

    changed = True
    while changed:  # elimination: my id is the pair minus my opponent's known id
        changed = False
        for g, _ev, ids in usable:
            h, a = g.get("home_team_id"), g.get("away_team_id")
            if h is None or a is None:
                continue
            for x, y in ((int(h), int(a)), (int(a), int(h))):
                if len(cand.get(x, ())) > 1 and len(cand.get(y, ())) == 1:
                    rest = cand[x] - cand[y]
                    if len(rest) == 1:
                        cand[x] = rest
                        changed = True

    for tid, c in cand.items():  # last resort: ESPN home/away agrees with ours on true home games
        if len(c) <= 1:
            continue
        votes = collections.Counter()
        for g, ev in by_team[tid]:
            if g.get("neutral") or ev["neutral"]:
                continue
            side = "home" if int(g["home_team_id"]) == tid else "away"
            for comp in ev["comp"]:
                if comp["home_away"] == side and comp["id"] in c:
                    votes[comp["id"]] += 1
        if len(votes) == 1:
            cand[tid] = {next(iter(votes))}

    owners: dict[str, list[int]] = collections.defaultdict(list)
    for tid, c in cand.items():
        if len(c) == 1:
            owners[next(iter(c))].append(tid)
    unique, ambiguous = {}, {}
    for tid in team_ids:
        c = cand.get(tid)
        if not c:
            continue
        if len(c) == 1 and len(owners[next(iter(c))]) == 1:
            unique[tid] = next(iter(c))
        else:
            ambiguous[tid] = sorted(c)
    unmatched = [tid for tid in team_ids if tid not in cand]
    return {"unique": unique, "ambiguous": ambiguous, "unmatched": unmatched}


def check_consistent(result: dict, games: list[dict], events: dict[str, dict]) -> list[int]:
    """Game ids whose two mapped ESPN ids don't equal the event's competitor pair (should be empty)."""
    m = result["unique"]
    bad = []
    for g in games:
        ev = events.get(str(g.get("external_id") or ""))
        h, a = g.get("home_team_id"), g.get("away_team_id")
        if not ev or h is None or a is None or int(h) not in m or int(a) not in m:
            continue
        if {m[int(h)], m[int(a)]} != {c["id"] for c in ev["comp"]}:
            bad.append(int(g["id"]))
    return bad


def _q(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def render_sql(result: dict, names: dict[int, str]) -> str:
    rows = sorted(result["unique"].items(), key=lambda kv: names[kv[0]].lower())
    amb = result["ambiguous"]
    lines = [
        "-- GENERATED by scripts/build_espn_team_ids.py — do not edit by hand; re-run the script.",
        "-- Backfills public.teams.external_ids.espn (ESPN team id, as text) for the Live Line exact match.",
        "-- Derived from games.external_id (ESPN event ids): each team's id is the one ESPN competitor id",
        "-- present in every event it played. Rows are matched on id AND name, so a renumbered table is a no-op.",
        "-- Idempotent: only fills a missing espn key; never overwrites an existing different value.",
        f"-- {len(rows)} unique, {len(amb)} ambiguous (left unset), {len(result['unmatched'])} unmatched.",
    ]
    for tid, c in sorted(amb.items()):
        lines.append(f"--   ambiguous: {names.get(tid, tid)} ({tid}) candidates {', '.join(c)}")
    lines += ["", "begin;", "",
              "update public.teams t",
              "set external_ids = coalesce(t.external_ids, '{}'::jsonb) || jsonb_build_object('espn', v.espn)",
              "from (values"]
    vals = [f"  ({tid}, {_q(names[tid])}, {_q(eid)})" for tid, eid in rows]
    lines.append(",\n".join(vals))
    lines += [") as v(id, name, espn)",
              "where t.id = v.id and t.name = v.name and t.league = 'ncaam'",
              "  and coalesce(t.external_ids->>'espn', '') = '';", "",
              "commit;", ""]
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--teams")
    ap.add_argument("--games")
    ap.add_argument("--events", help="cached {event_id: event} JSON (skips ESPN fetch)")
    ap.add_argument("--out", default=OUT)
    args = ap.parse_args()
    if args.teams and args.games:
        teams = json.load(open(args.teams))
        games = json.load(open(args.games))
    else:
        from czarp.supabase_writer import Client
        c = Client()
        teams = c.get_all("teams", order="id", select="id,name", league="eq.ncaam")
        games = c.get_all("games", order="id", select="id,date,external_id,home_team_id,away_team_id,neutral")
    if args.events:
        events = json.load(open(args.events))
    else:
        ds = sorted({str(g["date"]) for g in games if g.get("date")})
        d0, d1 = date.fromisoformat(ds[0]), date.fromisoformat(ds[-1])
        days = [(d0 + timedelta(i)).strftime("%Y%m%d") for i in range((d1 - d0).days + 1)]
        events = fetch_events(days)
    result = derive_espn_ids(teams, games, events)
    bad = check_consistent(result, games, events)
    if bad:
        print(f"inconsistent games {bad[:20]} — refusing to write", file=sys.stderr)
        return 1
    names = {int(t["id"]): t["name"] for t in teams}
    with open(args.out, "w") as f:
        f.write(render_sql(result, names))
    print(f"unique {len(result['unique'])}, ambiguous {len(result['ambiguous'])}, "
          f"unmatched {len(result['unmatched'])}; wrote {os.path.relpath(args.out, ROOT)}")
    for tid, c in sorted(result["ambiguous"].items()):
        print(f"  ambiguous: {names.get(tid, tid)} ({tid}) -> {c}")
    for tid in result["unmatched"]:
        print(f"  unmatched: {names.get(tid, tid)} ({tid})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
