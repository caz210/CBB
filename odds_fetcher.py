# -*- coding: utf-8 -*-
"""
odds_fetcher.py
Fetches pre-game Vegas lines from The Odds API.

LINE CAPTURE STRATEGY
─────────────────────
Goal: always show the CLOSING pre-game line, never a live/in-game line.

1. Daily cache  (data/lines_YYYY-MM-DD.csv)
   - Written on first fetch of the day
   - Persists all pre-game lines so they survive after tip-off
   - Refreshed by adding any newly-available pre-game lines each call

2. Live endpoint  (/v4/sports/basketball_ncaab/odds)
   - Called every refresh to pick up upcoming games
   - Only pre-game lines are extracted (commence_time > now)

3. Historical endpoint fallback  (/v4/historical/…)
   - Used ONLY when a game has already started but has no cached line
     (e.g. app was first loaded mid-afternoon after morning games tipped)
   - Fetches the snapshot from 5 min before commence_time = closing line
   - Results written back to cache so the API is only called once per game

Credit cost: 1 live call/refresh + 1 historical call per uncached started game.
"""

import os
import requests
import pandas as pd
from difflib import SequenceMatcher
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

load_dotenv()

CENTRAL = ZoneInfo("America/Chicago")


def _get_secret(key: str) -> str:
    try:
        import streamlit as st
        return st.secrets.get(key) or os.getenv(key)
    except Exception:
        return os.getenv(key)


ODDS_API_KEY    = _get_secret("ODDS_API_KEY")
BASE_URL        = "https://api.the-odds-api.com/v4/sports/basketball_ncaab/odds"
HIST_BASE_URL   = "https://api.the-odds-api.com/v4/historical/sports/basketball_ncaab/odds"
BOOK_PRIORITY   = ["draftkings", "fanduel", "betmgm", "caesars", "bovada", "mybookieag"]
MATCH_THRESHOLD = 0.78
TEAM_MAP_PATH   = "data/team_map.csv"

# Cache lives in data/lines_YYYY-MM-DD.csv
_CACHE_DIR      = "data"
_CACHE_COLS     = ["game_id", "vegas_home", "vegas_away", "commence_time_utc",
                   "vegas_spread", "vegas_total", "vegas_home_ml",
                   "source_book", "odds_game_time", "line_source"]

_odds_last_fetched: str = ""

# ─────────────────────────────────────────────────────────────────────────────
# SEED TABLE  —  KenPom name  →  exact Odds API full_name
# ─────────────────────────────────────────────────────────────────────────────
_SEED: dict = {
    "BYU":                        "BYU Cougars",
    "TCU":                        "TCU Horned Frogs",
    "SMU":                        "SMU Mustangs",
    "UCF":                        "UCF Knights",
    "UNLV":                       "UNLV Rebels",
    "VCU":                        "VCU Rams",
    "VMI":                        "VMI Keydets",
    "UMBC":                       "UMBC Retrievers",
    "UMKC":                       "UMKC Kangaroos",
    "UAB":                        "UAB Blazers",
    "UTEP":                       "UTEP Miners",
    "UTSA":                       "UTSA Roadrunners",
    "NJIT":                       "NJIT Highlanders",
    "LIU":                        "LIU Sharks",
    "UIC":                        "UIC Flames",
    "FDU":                        "Fairleigh Dickinson Knights",
    "SFA":                        "Stephen F. Austin Lumberjacks",
    "UIW":                        "Incarnate Word Cardinals",
    # Add more seed entries here as needed — these are checked first
}


def _load_team_map() -> dict:
    mapping = dict(_SEED)
    paths_to_try = [
        TEAM_MAP_PATH,
        os.path.join(os.path.dirname(os.path.abspath(__file__)), TEAM_MAP_PATH),
        os.path.join(os.getcwd(), TEAM_MAP_PATH),
    ]
    for path in paths_to_try:
        if os.path.exists(path):
            try:
                df = pd.read_csv(path)
                df = df[df["odds_name"].notna() & (df["odds_name"].str.strip() != "")]
                csv_map = dict(zip(df["kenpom_name"], df["odds_name"]))
                mapping.update(csv_map)
                print(f"    team_map.csv loaded: {len(csv_map)} CSV + {len(_SEED)} seed = {len(mapping)} total")
                return mapping
            except Exception as e:
                print(f"    team_map.csv error ({path}): {e}")
    print(f"    team_map.csv not found — using hardcoded seed ({len(mapping)} entries)")
    return mapping


KENPOM_TO_ODDS = _load_team_map()
_ODDS_NAME_INDEX: dict = {v.lower(): v for v in KENPOM_TO_ODDS.values()}


# ─────────────────────────────────────────────────────────────────────────────
# CACHE HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _cache_path(date_str: str) -> str:
    """Returns path like data/lines_2026-02-27.csv"""
    os.makedirs(_CACHE_DIR, exist_ok=True)
    return os.path.join(_CACHE_DIR, f"lines_{date_str}.csv")


def _load_cache(date_str: str) -> pd.DataFrame:
    path = _cache_path(date_str)
    if os.path.exists(path):
        try:
            df = pd.read_csv(path)
            # Ensure all expected columns exist
            for col in _CACHE_COLS:
                if col not in df.columns:
                    df[col] = None
            print(f"    Lines cache loaded: {len(df)} games from {path}")
            return df
        except Exception as e:
            print(f"    Cache load error: {e}")
    return pd.DataFrame(columns=_CACHE_COLS)


def _save_cache(df: pd.DataFrame, date_str: str) -> None:
    path = _cache_path(date_str)
    try:
        df.to_csv(path, index=False)
        print(f"    Lines cache saved: {len(df)} games → {path}")
    except Exception as e:
        print(f"    Cache save error: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# ROW EXTRACTION — shared between live + historical responses
# ─────────────────────────────────────────────────────────────────────────────

def _extract_rows(games: list, line_source: str = "live") -> list[dict]:
    """
    Parse a list of game dicts (from /odds or historical data[]) into row dicts.
    Does NOT filter by time — caller decides what to include.
    """
    rows = []
    for game in games:
        raw_time = game.get("commence_time", "")
        game_time_ct = ""
        commence_utc = None
        if raw_time:
            try:
                utc_dt = datetime.strptime(raw_time, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
                commence_utc = raw_time
                ct_dt = utc_dt.astimezone(CENTRAL)
                game_time_ct = ct_dt.strftime("%-I:%M %p CT")
            except Exception:
                pass

        home = game["home_team"]
        away = game["away_team"]
        vegas_spread  = None
        vegas_total   = None
        vegas_home_ml = None
        source_book   = None

        for book_key in BOOK_PRIORITY:
            book = next((b for b in game.get("bookmakers", []) if b["key"] == book_key), None)
            if not book:
                continue
            markets = {m["key"]: m["outcomes"] for m in book["markets"]}

            if vegas_spread is None and "spreads" in markets:
                for outcome in markets["spreads"]:
                    if outcome["name"] == home:
                        vegas_spread = outcome["point"]
                        source_book  = book["title"]
                        break

            if vegas_total is None and "totals" in markets:
                for outcome in markets["totals"]:
                    if outcome["name"] == "Over":
                        vegas_total = outcome["point"]
                        break

            if vegas_home_ml is None and "h2h" in markets:
                for outcome in markets["h2h"]:
                    if outcome["name"] == home:
                        vegas_home_ml = outcome["price"]
                        break

            if vegas_spread is not None and vegas_total is not None:
                break

        rows.append({
            "game_id":           game.get("id", ""),
            "vegas_home":        home,
            "vegas_away":        away,
            "commence_time_utc": commence_utc,
            "vegas_spread":      vegas_spread,
            "vegas_total":       vegas_total,
            "vegas_home_ml":     vegas_home_ml,
            "source_book":       source_book,
            "odds_game_time":    game_time_ct,
            "line_source":       line_source,
        })
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# HISTORICAL ENDPOINT — closing line for a single already-started game
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_closing_line(game_id: str, commence_time_utc: str) -> dict | None:
    """
    Fetch the pre-game closing line for a game that has already started.
    Requests a historical snapshot 5 minutes before commence_time.
    Returns a single row dict or None on failure.
    """
    if not ODDS_API_KEY or not commence_time_utc:
        return None
    try:
        utc_dt = datetime.strptime(commence_time_utc, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except Exception:
        return None

    # Snapshot 5 min before tip = final pre-game line
    snapshot_dt  = utc_dt - timedelta(minutes=5)
    snapshot_iso = snapshot_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    params = {
        "apiKey":     ODDS_API_KEY,
        "regions":    "us",
        "markets":    "spreads,totals,h2h",
        "oddsFormat": "american",
        "bookmakers": ",".join(BOOK_PRIORITY),
        "dateFormat": "iso",
        "date":       snapshot_iso,
    }
    if game_id:
        params["eventIds"] = game_id

    try:
        resp = requests.get(HIST_BASE_URL, params=params, timeout=10)
        remaining = resp.headers.get("x-requests-remaining", "?")
        if resp.status_code != 200:
            print(f"    Historical odds error {resp.status_code} for game {game_id}")
            return None
        payload = resp.json()
        games   = payload.get("data", [])
        print(f"    Historical closing line fetched for {game_id} | Credits remaining: {remaining}")
        rows = _extract_rows(games, line_source="historical_closing")
        # Find matching game
        for row in rows:
            if game_id and row["game_id"] == game_id:
                return row
        # If eventIds filter not applied or id mismatch, return first result
        return rows[0] if rows else None
    except Exception as e:
        print(f"    Historical odds request failed: {e}")
        return None


# ─────────────────────────────────────────────────────────────────────────────
# MAIN PUBLIC FUNCTION
# ─────────────────────────────────────────────────────────────────────────────

def fetch_vegas_lines() -> pd.DataFrame:
    """
    Returns a DataFrame of today's Vegas lines, always showing the
    pre-game closing line regardless of whether the game has started.

    Logic:
      1. Load today's cache (persisted pre-game lines from earlier fetches)
      2. Call live /odds endpoint → add any new pre-game lines to cache
      3. For games that started but aren't in cache → call historical endpoint
      4. Return merged cache (all today's lines, pre-game only)
    """
    if not ODDS_API_KEY:
        print("     ODDS_API_KEY not set — skipping Vegas lines.")
        return pd.DataFrame()

    now_utc  = datetime.now(timezone.utc)
    now_ct   = datetime.now(CENTRAL)
    date_str = now_ct.strftime("%Y-%m-%d")

    # ── 1. Load existing cache ────────────────────────────────────────────────
    cache_df = _load_cache(date_str)
    cached_ids = set(cache_df["game_id"].dropna().tolist()) if not cache_df.empty else set()

    # ── 2. Fetch live pre-game lines ──────────────────────────────────────────
    day_start_ct = now_ct.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end_ct   = day_start_ct + timedelta(hours=32)
    to_iso = lambda dt: dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    params = {
        "apiKey":           ODDS_API_KEY,
        "regions":          "us",
        "markets":          "spreads,totals,h2h",
        "oddsFormat":       "american",
        "bookmakers":       ",".join(BOOK_PRIORITY),
        "commenceTimeFrom": to_iso(day_start_ct),
        "commenceTimeTo":   to_iso(day_end_ct),
        "dateFormat":       "iso",
    }

    live_rows = []
    try:
        resp = requests.get(BASE_URL, params=params, timeout=10)
        if resp.status_code == 401:
            print("    Odds API: 401 Unauthorized — check ODDS_API_KEY.")
        elif resp.status_code == 429:
            print("    Odds API: 429 Too Many Requests — rate limited.")
        else:
            resp.raise_for_status()
            all_games = resp.json()
            remaining = resp.headers.get("x-requests-remaining", "?")
            print(f"    Live odds fetched ({len(all_games)} games) | Credits remaining: {remaining}")

            # Only keep pre-game lines from the live endpoint
            pregame_games = []
            started_games = []
            for g in all_games:
                raw = g.get("commence_time", "")
                try:
                    utc_dt = datetime.strptime(raw, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
                    if utc_dt > now_utc:
                        pregame_games.append(g)
                    else:
                        started_games.append(g)  # track for historical fallback below
                except Exception:
                    pregame_games.append(g)

            live_rows = _extract_rows(pregame_games, line_source="live")
            if started_games:
                print(f"    {len(started_games)} game(s) already started — will use cache or historical line")

    except Exception as e:
        print(f"    Odds API request failed: {e}")

    # ── 3. Merge live rows into cache (new pre-game games only) ───────────────
    new_rows = [r for r in live_rows if r["game_id"] not in cached_ids]
    if new_rows:
        new_df   = pd.DataFrame(new_rows)
        cache_df = pd.concat([cache_df, new_df], ignore_index=True)
        cached_ids.update(r["game_id"] for r in new_rows)
        print(f"    Added {len(new_rows)} new pre-game line(s) to cache")

    # ── 4. Historical fallback for started games not in cache ─────────────────
    # We need to know which games tipped today but were never cached.
    # Source: live endpoint returned them as "started" AND they're not in cache.
    # Use their game_id + commence_time to pull the closing line.
    if 'started_games' in dir() and started_games:
        hist_added = 0
        for g in started_games:
            gid = g.get("id", "")
            if gid in cached_ids:
                continue   # already have this one — no API call needed
            print(f"    Fetching historical closing line: {g['home_team']} vs {g['away_team']}")
            row = _fetch_closing_line(gid, g.get("commence_time", ""))
            if row:
                new_df   = pd.DataFrame([row])
                cache_df = pd.concat([cache_df, new_df], ignore_index=True)
                cached_ids.add(gid)
                hist_added += 1
        if hist_added:
            print(f"    Retrieved {hist_added} historical closing line(s)")

    # ── 5. Save updated cache ─────────────────────────────────────────────────
    if not cache_df.empty:
        _save_cache(cache_df, date_str)

    # ── 6. Update last-fetched timestamp ──────────────────────────────────────
    global _odds_last_fetched
    _odds_last_fetched = now_ct.strftime("%-I:%M %p CT")

    return cache_df if not cache_df.empty else pd.DataFrame()


def get_odds_last_fetched() -> str:
    return _odds_last_fetched


# ─────────────────────────────────────────────────────────────────────────────
# MATCHING — unchanged from original
# ─────────────────────────────────────────────────────────────────────────────

def _sim(a: str, b: str) -> float:
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()


def _strip_nickname(name: str) -> str:
    """Strip team nickname so fuzzy matching works on school/city name only."""
    suffixes = [
        " Wildcats", " Bulldogs", " Tigers", " Bears", " Eagles", " Hawks",
        " Owls", " Wolves", " Huskies", " Trojans", " Spartans", " Bruins",
        " Tar Heels", " Blue Devils", " Cavaliers", " Hokies", " Demon Deacons",
        " Hurricanes", " Seminoles", " Gators", " Volunteers", " Razorbacks",
        " Longhorns", " Aggies", " Cowboys", " Sooners", " Jayhawks",
        " Cyclones", " Cornhuskers", " Hawkeyes", " Badgers", " Gophers",
        " Hoosiers", " Boilermakers", " Buckeyes", " Wolverines", " Nittany Lions",
        " Terrapins", " Terps", " Mountaineers", " Orange", " Fighting Irish",
        " Blazers", " Flames", " Phoenix", " Lions", " Panthers", " Rams",
        " Cougars", " Lobos", " Rebels", " Miners", " Roadrunners", " Mustangs",
        " Horned Frogs", " Knights", " Mean Green", " Mavericks",
        " Thunderbirds", " Sun Devils", " Buffaloes", " Falcons", " Jets",
        " Cardinals", " Redbirds", " Leathernecks", " Fighting Illini", " Illini",
        " Ramblers", " Explorers", " Blue Demons", " Flyers", " Pilots",
        " Gaels", " Dons", " Anteaters", " Highlanders", " Matadors",
        " Seawolves", " Ospreys", " Stetson Hatters", " Hatters",
        " Retrievers", " Sharks", " Kangaroos", " Mocs", " Paladins",
        " Warhawks", " Skyhawks", " Buccaneers", " Privateers",
        " Delta Devils", " Camels", " Quakers", " Big Red", " Crimson",
        " Big Green", " Ephs", " Judges", " Bald Eagles", " Comets",
        " Norse", " Hilltoppers", " Racers", " Commodores", " Salukis",
        " Sycamores", " Purple Aces", " Vikings", " Grizzlies", " Bobcats",
        " RedHawks", " Vaqueros", " Dukes", " Peacocks", " Stags",
        " Waves", " Trailblazers", " Mastodons", " Blue Raiders",
        " Penmen", " Tommies", " Jaspers", " Red Foxes",
        " Revolutionaries", " Shockers", " Seahawks",
        " Lumberjacks", " Bearkats", " Texans", " Roadrunners",
        " Keydets", " Lancers", " Flames", " Highlanders",
    ]
    for s in sorted(suffixes, key=len, reverse=True):
        if name.endswith(s):
            return name[: -len(s)].strip()
    return name.strip()


def match_vegas_to_game(result: dict, vegas_df: pd.DataFrame) -> dict:
    """
    Match KenPom game to Vegas line.
    Strategy:
      1. If both KenPom teams are in the seed, do direct exact-name lookup (fast, accurate)
      2. Otherwise fall back to normalize+fuzzy (covers teams not in seed)
    """
    _null = lambda r: {**r,
        "vegas_spread": None, "vegas_total": None, "vegas_home_ml": None,
        "spread_edge": None, "total_edge": None, "edge_score": None,
        "vegas_fav": None, "my_fav": None, "sides_agree": None,
        "source_book": None, "line_source": None}

    if vegas_df is None or vegas_df.empty:
        return _null(result)

    kp_t1 = result["team1"]
    kp_t2 = result["team2"]

    odds_t1 = KENPOM_TO_ODDS.get(kp_t1)
    odds_t2 = KENPOM_TO_ODDS.get(kp_t2)

    best_match   = None
    best_flipped = False

    # ── Strategy 1: direct exact lookup ──────────────────────────────────────
    if odds_t1 and odds_t2:
        for _, row in vegas_df.iterrows():
            vh = row["vegas_home"]
            va = row["vegas_away"]
            if vh == odds_t1 and va == odds_t2:
                best_match, best_flipped = row, False
                break
            if vh == odds_t2 and va == odds_t1:
                best_match, best_flipped = row, True
                break

    # ── Strategy 2: fuzzy fallback ────────────────────────────────────────────
    if best_match is None:
        def norm(name: str) -> str:
            mapped = KENPOM_TO_ODDS.get(name, name)
            return _strip_nickname(mapped).lower()

        t1 = norm(kp_t1)
        t2 = norm(kp_t2)

        best_score = 0.0
        for _, row in vegas_df.iterrows():
            vh = _strip_nickname(row["vegas_home"]).lower()
            va = _strip_nickname(row["vegas_away"]).lower()

            s1n = _sim(t1, vh);  s2n = _sim(t2, va)
            s1f = _sim(t1, va);  s2f = _sim(t2, vh)

            normal_ok  = s1n >= MATCH_THRESHOLD and s2n >= MATCH_THRESHOLD
            flipped_ok = s1f >= MATCH_THRESHOLD and s2f >= MATCH_THRESHOLD

            if not normal_ok and not flipped_ok:
                continue

            normal_total  = (s1n + s2n) if normal_ok  else 0.0
            flipped_total = (s1f + s2f) if flipped_ok else 0.0

            if normal_total >= flipped_total:
                top_score, is_flipped = normal_total, False
            else:
                top_score, is_flipped = flipped_total, True

            if top_score > best_score:
                best_score   = top_score
                best_match   = row
                best_flipped = is_flipped

        if best_match is None:
            print(f"    NO MATCH: {kp_t1} vs {kp_t2}  (normalized: '{t1}' vs '{t2}')")
            return _null(result)

    # ── Apply matched Vegas line ──────────────────────────────────────────────
    v_total     = best_match["vegas_total"]
    raw_vspread = best_match["vegas_spread"]
    v_spread    = (-raw_vspread if raw_vspread is not None else None) if best_flipped else raw_vspread

    result["vegas_spread"]   = v_spread
    result["vegas_total"]    = v_total
    result["vegas_home_ml"]  = best_match["vegas_home_ml"]
    result["source_book"]    = best_match["source_book"]
    result["odds_game_time"] = best_match.get("odds_game_time", "")
    result["line_source"]    = best_match.get("line_source", "")   # "live" | "historical_closing"

    if v_spread is not None and v_total and v_total > 0:
        my_spread = result["spread"]
        my_vs     = -my_spread

        spread_diff = abs(v_spread - my_vs)
        vegas_fav   = result["team1"] if v_spread < 0 else (result["team2"] if v_spread > 0 else "Pick")
        my_fav      = result["team1"] if my_spread > 0 else (result["team2"] if my_spread < 0 else "Pick")

        result["spread_edge"] = round(spread_diff, 2)
        result["total_edge"]  = round(abs(result["total"] - v_total), 2)
        result["edge_score"]  = round(spread_diff / v_total, 4)
        result["vegas_fav"]   = vegas_fav
        result["my_fav"]      = my_fav
        result["sides_agree"] = (vegas_fav == my_fav)
    else:
        result["spread_edge"] = None
        result["total_edge"]  = None
        result["edge_score"]  = None
        result["vegas_fav"]   = None
        result["my_fav"]      = None
        result["sides_agree"] = None

    return result
