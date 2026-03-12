# -*- coding: utf-8 -*-
"""
results_tracker.py
──────────────────
Handles two jobs:

1. SNAPSHOT  — locks today's CZarp projections into Supabase once per day
               (idempotent — safe to call multiple times, only inserts once)

2. RESULTS   — fetches final scores from Odds API for completed games,
               computes whether CZarp covered, writes to bet_results table

Call from app.py at startup:
    from results_tracker import run_snapshot, run_results
    run_snapshot(results)      # pass today's projections list
    run_results()              # fetch completed scores and grade bets
"""

import os
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

import requests

CENTRAL       = ZoneInfo("America/Chicago")
SCORES_URL    = "https://api.the-odds-api.com/v4/sports/basketball_ncaab/scores"
HIST_ODDS_URL = "https://api.the-odds-api.com/v4/historical/sports/basketball_ncaab/odds"


# ── Supabase client ───────────────────────────────────────────────────────────

def _get_supabase():
    try:
        import streamlit as st
        url = st.secrets["supabase"]["url"]
        key = st.secrets["supabase"]["key"]
    except Exception:
        url = os.environ.get("SUPABASE_URL", "")
        key = os.environ.get("SUPABASE_KEY", "")
    if not url or not key:
        raise RuntimeError("Supabase credentials not found in secrets")
    from supabase import create_client
    return create_client(url, key)


def _get_odds_key():
    try:
        import streamlit as st
        return st.secrets.get("ODDS_API_KEY") or os.environ.get("ODDS_API_KEY", "")
    except Exception:
        return os.environ.get("ODDS_API_KEY", "")


# ── 1. Snapshot today's projections ──────────────────────────────────────────

def run_snapshot(results: list[dict], force: bool = False) -> dict:
    """
    Lock today's CZarp projections into daily_snapshots.
    Only runs between 11am–3pm CT (noon window) unless force=True.
    Idempotent — skips games already saved for today.

    Returns {"inserted": N, "skipped": N, "errors": [...]}
    """
    now_ct = datetime.now(CENTRAL)
    today  = now_ct.date().isoformat()

    # Only snapshot during the noon window (11am–3pm CT)
    if not force and not (11 <= now_ct.hour < 15):
        return {"inserted": 0, "skipped": 0, "errors": [],
                "message": f"Outside snapshot window (current hour CT: {now_ct.hour})"}

    if not results:
        return {"inserted": 0, "skipped": 0, "errors": [], "message": "No results to snapshot"}

    try:
        db = _get_supabase()
    except Exception as e:
        return {"inserted": 0, "skipped": 0, "errors": [str(e)]}

    inserted = 0
    skipped  = 0
    errors   = []

    for r in results:
        team1 = r.get("team1", "")
        team2 = r.get("team2", "")
        if not team1 or not team2:
            continue

        row = {
            "snapshot_date":  today,
            "snapshot_time":  now_ct.isoformat(),
            "team1":          team1,
            "team2":          team2,
            "is_neutral":     bool(r.get("is_neutral", False)),
            "game_time":      r.get("game_time"),
            "czarp_t1_score": r.get("team1_score"),
            "czarp_t2_score": r.get("team2_score"),
            "czarp_spread":   r.get("spread"),
            "czarp_total":    r.get("total"),
            "czarp_side":     r.get("bet_side"),
            "bet_type":       r.get("bet_type"),
            "is_upset_pick":  bool(r.get("is_upset_pick", False)),
            "edge_score":     r.get("edge_score"),
            "vegas_spread":   r.get("vegas_spread"),
            "vegas_fav":      r.get("vegas_fav"),
            "vegas_total":    r.get("vegas_total"),
            "spread_edge":    r.get("spread_edge"),
            "kp_t1_score":    r.get("kp_home_score"),
            "kp_t2_score":    r.get("kp_away_score"),
        }

        try:
            # Check if already saved today WITH a Vegas line
            existing = db.table("daily_snapshots").select("id,vegas_spread").eq(
                "snapshot_date", today
            ).eq("team1", team1).eq("team2", team2).execute()

            existing_row = (existing.data or [None])[0] if existing.data else None

            if existing_row:
                # Upgrade if: new line available when old had none, OR bet_type was missing
                has_new_line  = row.get("vegas_spread") is not None
                had_old_line  = existing_row.get("vegas_spread") is not None
                missing_bet   = existing_row.get("bet_type") is None and row.get("bet_type") is not None
                if (has_new_line and not had_old_line) or missing_bet:
                    db.table("daily_snapshots").update(row).eq("id", existing_row["id"]).execute()
                    inserted += 1
                    reason = "added line" if not had_old_line else "repaired bet_type"
                    print(f"    ↑ upgraded ({reason}): {team1} vs {team2}")
                else:
                    skipped += 1
            else:
                resp = db.table("daily_snapshots").insert(row).execute()
                if resp.data:
                    inserted += 1
                else:
                    errors.append(f"{team1} vs {team2}: no data returned")
        except Exception as e:
            errors.append(f"{team1} vs {team2}: {e}")

    print(f"  [snapshot] {inserted} inserted, {skipped} already existed, {len(errors)} errors")
    return {"inserted": inserted, "skipped": skipped, "errors": errors}


# ── 2. Fetch final scores from Odds API ──────────────────────────────────────

def _build_odds_to_kenpom() -> dict:
    """
    Invert the KENPOM_TO_ODDS map so we can normalize Odds API team names
    back to KenPom names for matching against daily_snapshots.
    Also includes hard-coded overrides for known problem cases.
    """
    # Hard-coded overrides: exact Odds API nickname-stripped name → KenPom snapshot name
    # Add entries here whenever Debug Grade shows a ❌ NO MATCH.
    # IMPORTANT: include BOTH the full Odds API name (with nickname) AND the stripped version
    # because some entries in KENPOM_TO_ODDS map abbreviations → full Odds names,
    # causing the inverted map to return abbreviations (e.g. ETSU) that don't match snapshots.
    OVERRIDES = {
        # ── Full Odds API name (with nickname) → KenPom snapshot name ──────────
        # These are needed when KENPOM_TO_ODDS has an abbreviation as the key
        # and the inverted map returns that abbreviation instead of the full name.
        "east tennessee st buccaneers":         "East Tennessee St.",
        "east tennessee state buccaneers":      "East Tennessee St.",
        "etsu buccaneers":                      "East Tennessee St.",
        "texas a&m-cc islanders":               "Texas A&M Corpus Chris",
        "texas a&m corpus christi islanders":   "Texas A&M Corpus Chris",
        "siu edwardsville cougars":             "SIUE",
        "southern illinois edwardsville cougars": "SIUE",
        "umkc kangaroos":                       "UMKC",
        "iu indianapolis jaguars":              "IUPUI",
        "uconn huskies":                        "UConn",
        "ole miss rebels":                      "Ole Miss",
        "nc state wolfpack":                    "NC State",
        "usc trojans":                          "USC",
        "pitt panthers":                        "Pitt",
        "vcu rams":                             "VCU",
        "smu mustangs":                         "SMU",
        "tcu horned frogs":                     "TCU",
        "byu cougars":                          "BYU",
        "ucf knights":                          "UCF",
        "unlv rebels":                          "UNLV",
        "lsu tigers":                           "LSU",
        "miami fl hurricanes":                  "Miami FL",
        "miami oh redhawks":                    "Miami OH",
        # ── Stripped name (no nickname) → KenPom snapshot name ─────────────────
        "east tennessee st":                    "East Tennessee St.",
        "etsu":                                 "East Tennessee St.",
        "texas a&m-cc":                         "Texas A&M Corpus Chris",
        "texas a&m corpus christi":             "Texas A&M Corpus Chris",
        "wright st":                            "Wright St.",
        "grambling st":                         "Grambling St.",
        "alabama st":                           "Alabama St.",
        "alcorn st":                            "Alcorn St.",
        "weber state":                          "Weber St.",
        "ut rio grande valley":                 "UT Rio Grande Valley",
        "nicholls st":                          "Nicholls",
        "saint mary's":                         "Saint Mary's",
        "ole miss":                             "Ole Miss",
        "uconn":                                "UConn",
        "connecticut":                          "UConn",
        "loyola chicago":                       "Loyola Chicago",
        "miami fl":                             "Miami FL",
        "miami oh":                             "Miami OH",
        "nc state":                             "NC State",
        "north carolina state":                 "NC State",
        "siu edwardsville":                     "SIUE",
        "southern illinois edwardsville":       "SIUE",
    }

    try:
        from odds_fetcher import KENPOM_TO_ODDS
        # Invert: "Duke Blue Devils" (lowercase) → "Duke"
        inverted = {v.lower(): k for k, v in KENPOM_TO_ODDS.items()}
    except Exception:
        inverted = {}

    # Overrides take priority over the inverted map
    inverted.update({k.lower(): v for k, v in OVERRIDES.items()})
    return inverted


def _normalize_odds_name(odds_name: str, odds_to_kenpom: dict) -> str:
    """
    Convert an Odds API team name to a KenPom-style name.
    Priority: exact inverted map → strip nickname → original
    """
    # 1. Exact inverted lookup (full Odds API name → KenPom name)
    kp = odds_to_kenpom.get(odds_name.lower())
    if kp:
        return kp

    # 2. Strip team nicknames — Odds API always appends them
    _SUFFIXES = [
        # Power conferences
        " Blue Devils", " Tar Heels", " Wildcats", " Bulldogs", " Tigers",
        " Huskies", " Volunteers", " Jayhawks", " Longhorns", " Buckeyes",
        " Wolverines", " Hoosiers", " Boilermakers", " Hawkeyes", " Cornhuskers",
        " Huskers", " Nittany Lions", " Spartans", " Trojans", " Bruins",
        " Cardinal", " Buffaloes", " Utes", " Cowboys", " Pokes",
        " Cyclones", " Sooners", " Horned Frogs", " Mustangs", " Cougars",
        " Aggies", " Rebels", " Crimson Tide", " Gators", " Seminoles",
        " Hurricanes", " Panthers", " Rams", " Eagles", " Owls",
        " Mountaineers", " Pirates", " Demon Deacons", " Chanticleers",
        " Monarchs", " Flames", " Blazers", " Miners", " Roadrunners",
        " Road Runners", " Knights", " Retrievers", " Terrapins", " Terps",
        " Cavaliers", " Hokies", " Deacons", " Orange", " Green Wave",
        " Red Storm", " Friars", " Billikens", " Bluejays", " Blue Jays",
        " Flyers", " Musketeers", " Ramblers", " Wolfpack", " Wolf Pack",
        " Anteaters", " Ducks", " Beavers", " Sun Devils",
        " Golden Bears", " Golden Gophers", " Red Foxes",
        " Spiders", " Dukes", " Patriots", " Colonials", " Keydets",
        " Highlanders", " Bearcats", " Redhawks", " RedHawks",
        " Thunderbirds", " Skyhawks", " Explorers", " Quakers",
        " Fighting Illini", " Illini", " Hoyas",
        # Mid-major / smaller schools (the ones actually failing)
        " Hornets", " Braves", " Islanders", " Privateers", " Raiders",
        " Norse", " Buccaneers", " Paladins", " Grizzlies", " Bears",
        " Delta Devils", " Pride", " Vaqueros", " Colonels",
        " Titans", " Gaels", " Broncos", " Fighting Camels", " Camels",
        " Red Raiders", " Racers", " Bisons",
        " Warhawks", " Bobcats",
        " Penmen", " Tommies", " Jaspers", " Peacocks", " Stags",
        " Waves", " Pilots", " Trailblazers", " Mastodons",
        " Blue Raiders", " Mocs",
        " Leathernecks", " Lancers", " Sycamores", " Purple Aces",
        " Vikings", " Redbirds", " Salukis",
        " Seawolves", " Matadors",
        " Lumberjacks", " Cardinals", " Falcons",
        " Penguins", " Flashes", " Zips", " Golden Flashes",
        " Ospreys", " Hatters", " Great Danes", " Leopards", " Statesmen",
        " River Hawks", " RiverHawks",
    ]
    name = odds_name.strip()
    for sfx in sorted(_SUFFIXES, key=len, reverse=True):
        if name.lower().endswith(sfx.lower()):
            return name[:-len(sfx)].strip()

    return odds_name  # fallback: unchanged


def _snap_sim(a: str, b: str) -> float:
    """
    Fuzzy similarity between two team name strings.
    Normalizes periods, ampersands, and hyphens before comparing.
    """
    from difflib import SequenceMatcher

    def _clean(s: str) -> str:
        return (s.lower()
                .replace(".", "")
                .replace("&", "and")
                .replace("-", " ")
                .replace("  ", " ")
                .strip())

    return SequenceMatcher(None, _clean(a), _clean(b)).ratio()


def fetch_final_scores(date_str: str | None = None) -> list[dict]:
    """
    Fetch completed NCAAB game scores from Odds API.
    date_str: YYYY-MM-DD (defaults to yesterday CT — games finish overnight)
    Returns list of {team1, team2, kp_team1, kp_team2, t1_final, t2_final, game_date}
    where kp_team1/kp_team2 are normalized KenPom-style names for snapshot matching.
    """
    api_key = _get_odds_key()
    if not api_key:
        raise RuntimeError("ODDS_API_KEY not configured")

    if date_str is None:
        date_str = (datetime.now(CENTRAL).date() - timedelta(days=1)).isoformat()

    params = {
        "apiKey":     api_key,
        "daysFrom":   3,        # look back 3 days — catches missed grade days
        "dateFormat": "iso",
    }

    resp = requests.get(SCORES_URL, params=params, timeout=15)
    resp.raise_for_status()
    games = resp.json()

    odds_to_kenpom = _build_odds_to_kenpom()

    scores = []
    for g in games:
        if g.get("completed") is not True:
            continue

        # Game date in CT
        commence = g.get("commence_time", "")
        try:
            game_dt   = datetime.fromisoformat(commence.replace("Z", "+00:00"))
            game_date = game_dt.astimezone(CENTRAL).date().isoformat()
        except Exception:
            continue

        # Only keep games from the target date
        if game_date != date_str:
            continue

        home = g.get("home_team", "")
        away = g.get("away_team", "")

        t_scores   = {s["name"]: s.get("score") for s in g.get("scores") or []}
        home_score = t_scores.get(home)
        away_score = t_scores.get(away)

        if home_score is None or away_score is None:
            continue

        scores.append({
            "game_date": game_date,
            "team1":     home,
            "team2":     away,
            # KenPom-normalized names used for snapshot matching
            "kp_team1":  _normalize_odds_name(home, odds_to_kenpom),
            "kp_team2":  _normalize_odds_name(away, odds_to_kenpom),
            "t1_final":  int(home_score),
            "t2_final":  int(away_score),
        })

    print(f"  [scores] {len(scores)} completed games found for {date_str}")
    return scores


# ── 3. Grade bets ────────────────────────────────────────────────────────────

def _grade_bet(snap: dict, result: dict) -> dict | None:
    """
    Given a snapshot and a final score result, compute whether CZarp covered.
    Returns a bet_results row dict, or None if ungradeable.
    """
    t1_final = result.get("t1_final")
    t2_final = result.get("t2_final")
    if t1_final is None or t2_final is None:
        return None

    vegas_spread = snap.get("vegas_spread")   # absolute value
    vegas_fav    = snap.get("vegas_fav")
    czarp_side   = snap.get("czarp_side")
    team1        = snap.get("team1")
    team2        = snap.get("team2")

    actual_spread = t1_final - t2_final       # positive = team1 won

    # ML correctness
    czarp_spread_val = snap.get("czarp_spread") or 0
    czarp_winner = team1 if czarp_spread_val >= 0 else team2
    actual_winner = team1 if actual_spread > 0 else (team2 if actual_spread < 0 else None)
    czarp_ml_correct = (czarp_winner == actual_winner) if actual_winner else None

    # ATS coverage
    czarp_covers = None
    push         = False

    if vegas_spread is not None and vegas_fav and czarp_side:
        # Determine line from czarp_side's perspective
        if czarp_side == team1:
            # CZarp backing team1. Team1 covers if actual_spread > -line_for_t1
            if vegas_fav == team1:
                line = -abs(vegas_spread)      # team1 is fav, giving points
            else:
                line = abs(vegas_spread)       # team1 is dog, getting points
            margin = actual_spread - line
        else:
            # CZarp backing team2
            if vegas_fav == team2:
                line = abs(vegas_spread)       # team2 is fav (as away), margin reversed
            else:
                line = -abs(vegas_spread)
            margin = -actual_spread - abs(vegas_spread) if vegas_fav == team2 else -actual_spread + abs(vegas_spread)
            # Simpler: from team2's perspective
            t2_actual = t2_final - t1_final
            if vegas_fav == team2:
                margin = t2_actual - abs(vegas_spread)
            else:
                margin = t2_actual + abs(vegas_spread)

        if margin > 0:
            czarp_covers = True
        elif margin < 0:
            czarp_covers = False
        else:
            push = True
            czarp_covers = None

    sides_agree = (czarp_side == vegas_fav) if (czarp_side and vegas_fav) else None

    return {
        "game_date":        snap.get("snapshot_date"),
        "team1":            team1,
        "team2":            team2,
        "czarp_side":       czarp_side,
        "bet_type":         snap.get("bet_type"),
        "is_upset_pick":    snap.get("is_upset_pick"),
        "is_neutral":       snap.get("is_neutral"),
        "czarp_spread":     snap.get("czarp_spread"),
        "czarp_total":      snap.get("czarp_total"),
        "vegas_spread":     vegas_spread,
        "vegas_fav":        vegas_fav,
        "vegas_total":      snap.get("vegas_total"),
        "edge_score":       snap.get("edge_score"),
        "spread_edge":      snap.get("spread_edge"),
        "sides_agree":      sides_agree,
        "t1_final":         t1_final,
        "t2_final":         t2_final,
        "actual_spread":    float(actual_spread),
        "czarp_covers":     czarp_covers,
        "czarp_ml_correct": czarp_ml_correct,
        "push":             push,
    }


def _save_scores_to_db(db, scores: list[dict]) -> int:
    """
    Persist raw final scores to the game_results table.
    Idempotent — upserts on (game_date, team1, team2).
    Returns count of rows written.
    """
    saved = 0
    for sc in scores:
        try:
            db.table("game_results").upsert({
                "game_date": sc["game_date"],
                "team1":     sc["team1"],       # Odds API name (home)
                "team2":     sc["team2"],       # Odds API name (away)
                "t1_final":  sc["t1_final"],
                "t2_final":  sc["t2_final"],
            }, on_conflict="game_date,team1,team2").execute()
            saved += 1
        except Exception as e:
            print(f"  [game_results] write failed for {sc['team1']} vs {sc['team2']}: {e}")
    return saved




def run_results(date_str: str | None = None, debug: bool = False) -> dict:
    """
    Fetch final scores → save to game_results → match snapshots → grade → save to bet_results.
    Falls back to game_results DB table when Odds API scores have expired (>3 days old).
    """
    if date_str is None:
        date_str = (datetime.now(CENTRAL).date() - timedelta(days=1)).isoformat()

    debug_lines = []

    try:
        db = _get_supabase()
    except Exception as e:
        return {"graded": 0, "skipped": 0, "errors": [str(e)]}

    # ── Fetch snapshots ───────────────────────────────────────────────────────
    try:
        snaps_resp = db.table("daily_snapshots").select("*").eq(
            "snapshot_date", date_str
        ).execute()
        snap_list = snaps_resp.data or []
        snaps = {(r["team1"].lower(), r["team2"].lower()): r for r in snap_list}
    except Exception as e:
        return {"graded": 0, "skipped": 0, "errors": [f"Snapshot fetch: {e}"]}

    if not snaps:
        return {"graded": 0, "skipped": 0, "errors": [],
                "message": f"No snapshots found for {date_str}"}

    debug_lines.append(f"📋 {len(snaps)} snapshots for {date_str}:")
    for (s1, s2) in snaps.keys():
        debug_lines.append(f"   snap: '{s1}' vs '{s2}'")

    # ── Fetch scores: live API → save permanently → fallback to DB ────────────
    scores = []
    source = "api"
    try:
        scores = fetch_final_scores(date_str)
        if scores:
            n_saved = _save_scores_to_db(db, scores)
            debug_lines.append(f"\n💾 Saved {n_saved} scores to game_results table")
    except Exception as e:
        debug_lines.append(f"⚠ Scores API error: {e}")

    if not scores:
        # Fallback: pull from permanent game_results table
        try:
            gr_resp = db.table("game_results").select("*").eq(
                "game_date", date_str
            ).execute()
            raw = gr_resp.data or []
            if raw:
                odds_to_kenpom = _build_odds_to_kenpom()
                scores = [{
                    "game_date": r["game_date"],
                    "team1":     r["team1"],
                    "team2":     r["team2"],
                    "kp_team1":  _normalize_odds_name(r["team1"], odds_to_kenpom),
                    "kp_team2":  _normalize_odds_name(r["team2"], odds_to_kenpom),
                    "t1_final":  r["t1_final"],
                    "t2_final":  r["t2_final"],
                } for r in raw]
                source = "db"
                debug_lines.append(f"📦 Using {len(scores)} scores from game_results (API expired)")
        except Exception as e:
            debug_lines.append(f"⚠ game_results fallback error: {e}")

    if not scores:
        return {"graded": 0, "skipped": 0, "errors": [],
                "message": f"No scores found for {date_str} (API or DB)"}

    debug_lines.append(f"\n🏀 {len(scores)} scores [{source}] for {date_str}:")
    for sc in scores:
        debug_lines.append(
            f"   odds: '{sc['team1']}' vs '{sc['team2']}'"
            f"  →  kp: '{sc['kp_team1']}' vs '{sc['kp_team2']}'"
            f"  ({sc['t1_final']}-{sc['t2_final']})"
        )

    graded  = 0
    skipped = 0
    errors  = []

    for score in scores:
        # Use normalized KenPom names for matching
        t1 = score["kp_team1"].lower()
        t2 = score["kp_team2"].lower()

        snap = snaps.get((t1, t2)) or snaps.get((t2, t1))

        if snap is None:
            # Fuzzy fallback using SequenceMatcher — handles periods, abbreviations, etc.
            FUZZY_THRESHOLD = 0.82
            best_score  = 0.0
            best_snap   = None
            best_flipped = False

            for (s1, s2), s in snaps.items():
                fwd = (_snap_sim(t1, s1) + _snap_sim(t2, s2)) / 2
                rev = (_snap_sim(t2, s1) + _snap_sim(t1, s2)) / 2

                if fwd >= FUZZY_THRESHOLD and fwd > best_score:
                    best_score, best_snap, best_flipped = fwd, s, False
                if rev >= FUZZY_THRESHOLD and rev > best_score:
                    best_score, best_snap, best_flipped = rev, s, True

            if best_snap is not None:
                snap = best_snap
                if best_flipped:
                    score = dict(score)
                    score["t1_final"], score["t2_final"] = score["t2_final"], score["t1_final"]
                    debug_lines.append(
                        f"   ✅ fuzzy-rev ({best_score:.2f}): '{t2}'→'{list(snaps.keys())[[v is snap for v in snaps.values()].index(True)][0]}'"
                    )
                else:
                    snap_key = next(k for k, v in snaps.items() if v is snap)
                    debug_lines.append(
                        f"   ✅ fuzzy-fwd ({best_score:.2f}): '{t1}'→'{snap_key[0]}', '{t2}'→'{snap_key[1]}'"
                    )

        if snap is None:
            debug_lines.append(f"   ❌ NO MATCH: '{t1}' vs '{t2}'  (odds: '{score['team1']}' vs '{score['team2']}')")
            continue

        if snap.get("bet_type") is None:
            debug_lines.append(f"   ⏭ SKIP (no Vegas line): '{snap['team1']}' vs '{snap['team2']}'")
            skipped += 1
            continue

        graded_row = _grade_bet(snap, score)
        if graded_row is None:
            errors.append(f"Grade failed: {snap['team1']} vs {snap['team2']}")
            continue

        try:
            db.table("bet_results").upsert(
                graded_row,
                on_conflict="game_date,team1,team2",
                ignore_duplicates=False
            ).execute()
            cover = "✅ COVER" if graded_row.get("czarp_covers") else ("PUSH" if graded_row.get("push") else "❌ NO COVER")
            debug_lines.append(
                f"   💾 GRADED: '{snap['team1']}' vs '{snap['team2']}'"
                f"  actual={graded_row['actual_spread']:+.0f}  {cover}"
            )
            graded += 1
        except Exception as e:
            errors.append(f"{snap['team1']} vs {snap['team2']}: {e}")

    print(f"  [results] {graded} graded, {skipped} skipped (no line), {len(errors)} errors")
    return {"graded": graded, "skipped": skipped, "errors": errors, "debug_lines": debug_lines}


# ── 5. Fetch performance data for analytics tab ───────────────────────────────

def get_performance_data() -> dict:
    """
    Pull all bet_results from Supabase for the analytics tab.
    Returns a dict with DataFrames ready for display.
    """
    try:
        import pandas as pd
        db = _get_supabase()
        resp = db.table("bet_results").select("*").order("game_date", desc=True).execute()
        rows = resp.data or []
        if not rows:
            return {"df": pd.DataFrame(), "total": 0}

        df = pd.DataFrame(rows)
        return {"df": df, "total": len(df)}
    except Exception as e:
        return {"df": __import__("pandas").DataFrame(), "total": 0, "error": str(e)}


# ── 6. Backfill closing lines from historical Odds API ────────────────────────

def backfill_closing_lines(date_str: str | None = None) -> dict:
    """
    For snapshots on date_str that are missing vegas_spread, fetch the closing
    line from the Odds API historical endpoint using an 11am CT timestamp.
    ONE call = all NCAAB games = ~20 credits total.
    """
    api_key = _get_odds_key()
    if not api_key:
        return {"updated": 0, "no_match": 0, "errors": ["ODDS_API_KEY not set"], "credits_used": 0}

    if date_str is None:
        date_str = datetime.now(CENTRAL).date().isoformat()

    try:
        db = _get_supabase()
    except Exception as e:
        return {"updated": 0, "no_match": 0, "errors": [str(e)], "credits_used": 0}

    # Find snapshots missing Vegas lines
    try:
        resp = db.table("daily_snapshots").select("*") \
            .eq("snapshot_date", date_str) \
            .is_("vegas_spread", "null") \
            .execute()
        missing = resp.data or []
    except Exception as e:
        return {"updated": 0, "no_match": 0, "errors": [f"DB fetch: {e}"], "credits_used": 0}

    if not missing:
        return {"updated": 0, "no_match": 0, "errors": [],
                "credits_used": 0, "message": "No snapshots missing lines"}

    # 11am CT = 17:00 UTC — lines are stable by then, before most games tip
    try:
        from datetime import date as date_type
        d = date_type.fromisoformat(date_str)
        date_iso = f"{d.isoformat()}T17:00:00Z"
    except Exception as e:
        return {"updated": 0, "no_match": 0, "errors": [f"Date parse: {e}"], "credits_used": 0}

    BOOK_PRIORITY = ["pinnacle", "draftkings", "fanduel", "betmgm",
                     "caesars", "williamhill_us", "pointsbetus"]

    params = {
        "apiKey":     api_key,
        "regions":    "us",
        "markets":    "spreads,totals",
        "oddsFormat": "american",
        "bookmakers": ",".join(BOOK_PRIORITY),
        "date":       date_iso,
        "dateFormat": "iso",
    }

    HIST_ODDS_URL = "https://api.the-odds-api.com/v4/historical/sports/basketball_ncaab/odds"

    try:
        resp_api = requests.get(HIST_ODDS_URL, params=params, timeout=15)
        resp_api.raise_for_status()
        payload  = resp_api.json()
        games    = payload.get("data", [])
        remaining = resp_api.headers.get("x-requests-remaining", "?")
        print(f"  [backfill] {len(games)} games in snapshot | Credits remaining: {remaining}")
    except Exception as e:
        return {"updated": 0, "no_match": 0, "errors": [f"Historical API: {e}"], "credits_used": 20}

    # Parse each game's best line
    def _parse_line(g):
        home = g.get("home_team", "")
        away = g.get("away_team", "")
        spread_val, spread_fav, total_val = None, None, None
        for bk in BOOK_PRIORITY:
            bm = next((b for b in g.get("bookmakers", []) if b["key"] == bk), None)
            if not bm:
                continue
            for mkt in bm.get("markets", []):
                if mkt["key"] == "spreads" and spread_val is None:
                    for o in mkt.get("outcomes", []):
                        if o.get("point") is not None and o["point"] < 0:
                            spread_fav = o["name"]
                            spread_val = abs(o["point"])
                            break
                if mkt["key"] == "totals" and total_val is None:
                    for o in mkt.get("outcomes", []):
                        if o.get("name") == "Over":
                            total_val = o.get("point")
                            break
            if spread_val and total_val:
                break
        if spread_val is None:
            return None
        return {"home": home, "away": away,
                "vegas_spread": spread_val, "vegas_fav": spread_fav, "vegas_total": total_val}

    hist_lookup = {}
    for g in games:
        p = _parse_line(g)
        if p:
            hist_lookup[frozenset([p["home"].lower(), p["away"].lower()])] = p

    updated, no_match, errors = 0, 0, []

    for snap in missing:
        t1  = snap["team1"].lower()
        t2  = snap["team2"].lower()
        key = frozenset([t1, t2])

        line = hist_lookup.get(key)
        if line is None:
            for fkey, fl in hist_lookup.items():
                flist = list(fkey)
                if len(flist) == 2:
                    a, b = flist[0], flist[1]
                    if (t1 in a or a in t1) and (t2 in b or b in t2):
                        line = fl; break
                    if (t2 in a or a in t2) and (t1 in b or b in t1):
                        line = fl; break

        if line is None:
            no_match += 1
            continue

        czarp_spread = snap.get("czarp_spread") or 0
        vs = line["vegas_spread"]
        vf = line["vegas_fav"]
        spread_edge = round(czarp_spread - (-vs if vf == snap["team1"] else vs), 2) if vs else None

        try:
            db.table("daily_snapshots").update({
                "vegas_spread": line["vegas_spread"],
                "vegas_fav":    line["vegas_fav"],
                "vegas_total":  line["vegas_total"],
                "spread_edge":  spread_edge,
            }).eq("id", snap["id"]).execute()
            updated += 1
        except Exception as e:
            errors.append(f"{snap['team1']} vs {snap['team2']}: {e}")

    print(f"  [backfill] {updated} updated, {no_match} no match, {len(errors)} errors")
    return {"updated": updated, "no_match": no_match, "errors": errors, "credits_used": 20}
