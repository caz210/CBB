# -*- coding: utf-8 -*-
"""
odds_fetcher.py
Fetches live Vegas lines from The Odds API.
"""

import os
import requests
import pandas as pd
from difflib import SequenceMatcher
from dotenv import load_dotenv

load_dotenv()

def _get_secret(key: str) -> str:
    try:
        import streamlit as st
        return st.secrets.get(key) or os.getenv(key)
    except Exception:
        return os.getenv(key)

ODDS_API_KEY    = _get_secret("ODDS_API_KEY")
BASE_URL        = "https://api.the-odds-api.com/v4/sports/basketball_ncaab/odds"
BOOK_PRIORITY   = ["draftkings", "fanduel", "betmgm", "caesars", "bovada", "mybookieag"]
MATCH_THRESHOLD = 0.75
TEAM_MAP_PATH   = "data/team_map.csv"

# Tracks when odds were last successfully fetched — displayed in the UI
_odds_last_fetched: str = ""


def _load_team_map() -> dict:
    """
    Load kenpom_name → odds_name from data/team_map.csv.
    Run team_mapper.py once to generate this file.
    Returns empty dict if file not found yet.
    """
    # Try relative path first, then path relative to this file
    paths_to_try = [
        TEAM_MAP_PATH,
        os.path.join(os.path.dirname(os.path.abspath(__file__)), TEAM_MAP_PATH),
    ]
    for path in paths_to_try:
        if os.path.exists(path):
            try:
                df = pd.read_csv(path)
                df = df[df["odds_name"].notna() & (df["odds_name"].str.strip() != "")]
                mapping = dict(zip(df["kenpom_name"], df["odds_name"]))
                print(f"    team_map loaded: {len(mapping)} entries from {path}")
                return mapping
            except Exception as e:
                print(f"    team_map load error: {e}")
    print(f"    WARNING: team_map.csv not found — run team_mapper.py locally and commit data/team_map.csv")
    return {}


# Loaded once at import; re-import or call _load_team_map() after editing the CSV
KENPOM_TO_ODDS = _load_team_map()


def fetch_vegas_lines() -> pd.DataFrame:
    if not ODDS_API_KEY:
        print("     ODDS_API_KEY not set -- skipping Vegas lines.")
        return pd.DataFrame()

    from datetime import datetime, timezone, timedelta
    # Only pull lines for TODAY's games (CT) — avoids completed games with no odds
    # and prevents pulling tomorrow's lines
    now_ct   = datetime.now(timezone(timedelta(hours=-6)))  # Central time
    day_start = now_ct.replace(hour=0,  minute=0,  second=0,  microsecond=0)
    day_end   = now_ct.replace(hour=23, minute=59, second=59, microsecond=0)
    to_iso    = lambda dt: dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    params = {
        "apiKey":             ODDS_API_KEY,
        "regions":            "us",
        "markets":            "spreads,totals,h2h",
        "oddsFormat":         "american",
        "bookmakers":         ",".join(BOOK_PRIORITY),
        "commenceTimeFrom":   to_iso(day_start),
        "commenceTimeTo":     to_iso(day_end),
        "dateFormat":         "iso",
    }

    resp = requests.get(BASE_URL, params=params)

    if resp.status_code == 401:
        print("    Odds API: 401 Unauthorized — API key invalid or credits exhausted. Skipping Vegas lines.")
        return pd.DataFrame()
    if resp.status_code == 429:
        print("    Odds API: 429 Too Many Requests — rate limited. Skipping Vegas lines.")
        return pd.DataFrame()
    resp.raise_for_status()

    games = resp.json()
    remaining = resp.headers.get("x-requests-remaining", "?")
    print(f"    Vegas lines fetched ({len(games)} games) | Credits remaining: {remaining}")

    rows = []
    for game in games:
        home = game["home_team"]
        away = game["away_team"]
        vegas_spread = None
        vegas_total  = None
        vegas_home_ml = None
        source_book  = None

        for book_key in BOOK_PRIORITY:
            book = next((b for b in game["bookmakers"] if b["key"] == book_key), None)
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

        # Parse game time from ISO to Central time display
        raw_time = game.get("commence_time", "")
        game_time_ct = ""
        if raw_time:
            try:
                from datetime import timezone, timedelta
                utc_dt = datetime.strptime(raw_time, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
                ct_dt  = utc_dt.astimezone(timezone(timedelta(hours=-6)))
                game_time_ct = ct_dt.strftime("%-I:%M %p CT").lstrip("0")
            except Exception:
                game_time_ct = ""

        rows.append({
            "vegas_home":    home,
            "vegas_away":    away,
            "vegas_spread":  vegas_spread,
            "vegas_total":   vegas_total,
            "vegas_home_ml": vegas_home_ml,
            "source_book":   source_book,
            "odds_game_time": game_time_ct,
        })

    global _odds_last_fetched
    from datetime import timezone, timedelta
    _odds_last_fetched = datetime.now(timezone(timedelta(hours=-6))).strftime("%-I:%M %p CT")

    return pd.DataFrame(rows)


def get_odds_last_fetched() -> str:
    """Returns the time odds were last successfully fetched, or empty string."""
    return _odds_last_fetched


def normalize_name(name: str) -> str:
    """
    Translate KenPom name → Odds API name via lookup table, then strip
    team nicknames so fuzzy matching works on the school/city name only.
    """
    name = KENPOM_TO_ODDS.get(name, name)
    suffixes = [
        # Power conferences
        " Blue Devils", " Tar Heels", " Wolfpack", " Wolf Pack", " Cavaliers",
        " Hokies", " Fighting Irish", " Wildcats", " Boilermakers", " Hoosiers",
        " Buckeyes", " Wolverines", " Spartans", " Badgers", " Hawkeyes",
        " Cornhuskers", " Huskers", " Gophers", " Golden Gophers", " Illini",
        " Fighting Illini", " Nittany Lions", " Scarlet Knights", " Terrapins",
        " Terps", " Crimson Tide", " Tigers", " Bulldogs", " Gators",
        " Seminoles", " Hurricanes", " Volunteers", " Razorbacks", " Gamecocks",
        " Aggies", " Longhorns", " Red Raiders", " Horned Frogs", " Sooners",
        " Cowboys", " Bears", " Mountaineers", " Cougars", " Utes",
        " Sun Devils", " Ducks", " Beavers", " Huskies", " Bruins",
        " Trojans", " Cardinal", " Golden Bears",
        # Mid-major / misc
        " Eagles", " Panthers", " Cardinals", " Hawks", " Owls", " Rams",
        " Flames", " Falcons", " Ravens", " Blue Hens", " Retrievers",
        " Musketeers", " Ramblers", " Bluejays", " Blue Jays", " Flyers",
        " Pilots", " Zags", " Bulldogs", " Friars", " Hoyas", " Dons",
        " Gaels", " Toreros", " Dons", " Matadors", " Anteaters",
        " Tritons", " Gauchos", " Highlanders", " Roadrunners",
        " Broncos", " Mustangs", " Hornets", " 49ers", " Spiders",
        " Penguins", " Zips", " Rockets", " Chippewas", " Red Hawks",
        " Golden Flashes", " Thunderbirds", " Lopes", " Skyhawks",
        " Lions", " Monarchs", " Colonials", " Colonels", " Patriots",
        " Golden Knights", " Rebels", " Wolf Pack", " Wolfpack",
        " Jackrabbits", " Coyotes", " Bison", " Fighting Hawks",
        " Lumberjacks", " Governors", " Red Wolves", " Warhawks",
        " Red Storm", " Great Danes", " Seawolves", " Terriers",
        " Crusaders", " Paladins", " Catamounts", " Minutemen",
        " River Hawks", " Green Terror", " Jaguars", " Royals",
        " Mavericks", " Ospreys", " Hatters", " Dolphins", " Corsairs",
        " Blazers", " Billikens", " Braves", " Redhawks", " Mean Green",
        " Chanticleers", " Thundering Herd", " Screaming Eagles",
        " Fighting Eagles", " Prairie View", " Rainbow Warriors",
        " Warriors", " Lobos", " Miners", " Bearkats", " Lumberjacks",
        " Beacons", " Phoenix", " Penmen", " Mids", " Black Knights",
        " Cadets", " Midshipmen", " Keydets", " Bonnies", " Green Wave",
        " Golden Hurricane", " Shockers", " Runnin Rebels", " Seahawks",
        " Demon Deacons", " Norse", " Flames", " Hilltoppers",
        " Toppers", " Lady Toppers", " Running Eagles",
        " Golden Eagles", " Blue Demons", " Explorers",
        " Princes", " Leopards", " Mountain Hawks", " Raiders",
        " Pride", " Big Green", " Scots", " Crimson",
        " Ephs", " Mammoths", " Lord Jeffs", " Jeffs", " Little Giants",
        " Judges", " Deacons", " Bald Eagles", " Comets",
        " Privateers", " Warhawks", " Sand Sharks",
    ]
    for s in sorted(suffixes, key=len, reverse=True):  # longest first to avoid partial strip
        if name.endswith(s):
            return name[:-len(s)].strip()
    return name.strip()


def _sim(a: str, b: str) -> float:
    """Similarity ratio 0-1 between two team name strings."""
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()


def match_vegas_to_game(result: dict, vegas_df: pd.DataFrame) -> dict:
    """
    Match model result to Vegas line using similarity scoring.
    BOTH teams must score >= MATCH_THRESHOLD to avoid false positives
    (e.g. 'Kansas' wrongly matching 'Kansas St.' game).
    Handles flipped team orientation between KenPom and Odds API.
    """
    _null = lambda r: {**r,
        "vegas_spread": None, "vegas_total": None, "vegas_home_ml": None,
        "spread_edge": None, "total_edge": None, "edge_score": None,
        "vegas_fav": None, "my_fav": None, "sides_agree": None, "source_book": None}

    if vegas_df.empty:
        return _null(result)

    t1 = normalize_name(result["team1"])
    t2 = normalize_name(result["team2"])

    best_score   = 0.0
    best_match   = None
    best_flipped = False

    for _, row in vegas_df.iterrows():
        vh = normalize_name(row["vegas_home"])
        va = normalize_name(row["vegas_away"])

        # Normal: t1=home matches vh, t2=away matches va
        s1n = _sim(t1, vh)
        s2n = _sim(t2, va)
        # Flipped: t1=home matches va, t2=away matches vh
        s1f = _sim(t1, va)
        s2f = _sim(t2, vh)

        # Both teams must individually clear the threshold
        normal_ok  = s1n >= MATCH_THRESHOLD and s2n >= MATCH_THRESHOLD
        flipped_ok = s1f >= MATCH_THRESHOLD and s2f >= MATCH_THRESHOLD

        if not normal_ok and not flipped_ok:
            continue

        # Use combined score to pick best among valid matches
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
        print(f"    NO MATCH: {result['team1']} vs {result['team2']}  (normalized: '{t1}' vs '{t2}')")
        return _null(result)

    v_total     = best_match["vegas_total"]
    raw_vspread = best_match["vegas_spread"]

    # Negate if flipped so spread is always from model's team1 perspective
    v_spread = (-raw_vspread if raw_vspread is not None else None) if best_flipped else raw_vspread

    result["vegas_spread"]   = v_spread
    result["vegas_total"]    = v_total
    result["vegas_home_ml"]  = best_match["vegas_home_ml"]
    result["source_book"]    = best_match["source_book"]
    result["odds_game_time"] = best_match.get("odds_game_time", "")

    if v_spread is not None and v_total and v_total > 0:
        my_spread = result["spread"]
        my_vs     = -my_spread           # flip to same sign convention as v_spread

        spread_diff = abs(v_spread - my_vs)

        vegas_fav = result["team1"] if v_spread < 0 else (result["team2"] if v_spread > 0 else "Pick")
        my_fav    = result["team1"] if my_spread > 0 else (result["team2"] if my_spread < 0 else "Pick")

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
