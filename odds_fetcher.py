# -*- coding: utf-8 -*-
"""
odds_fetcher.py
Fetches pre-game Vegas lines from The Odds API.
- Skips games that have already started (no live lines)
- Covers 10pm CT games by extending window into next UTC day
- Uses exact Odds API team names as seed (ground truth from API team list)
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
BOOK_PRIORITY   = ["draftkings", "fanduel", "betmgm", "caesars", "bovada", "mybookieag"]
MATCH_THRESHOLD = 0.78   # fallback fuzzy threshold
TEAM_MAP_PATH   = "data/team_map.csv"

_odds_last_fetched: str = ""

# ─────────────────────────────────────────────────────────────────────────────
# SEED TABLE  —  KenPom name  →  exact Odds API full_name
# Built from the official Odds API team list. This is ground truth.
# The matching logic uses this for direct lookup before fuzzy fallback.
# ─────────────────────────────────────────────────────────────────────────────
_SEED: dict = {
    # ── Abbreviations that stay abbreviated in the Odds API ──────────────────
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

    # ── KenPom full name → Odds API full name (differ in spelling/suffix) ────
    "UConn":                      "UConn Huskies",
    "Ole Miss":                   "Ole Miss Rebels",         # NOT "Mississippi"
    "LSU":                        "LSU Tigers",              # NOT "Louisiana State"
    "Pitt":                       "Pittsburgh Panthers",
    "USC":                        "USC Trojans",
    "UNC":                        "North Carolina Tar Heels",
    "UNCW":                       "UNC Wilmington Seahawks",
    "NC State":                   "NC State Wolfpack",
    "Miami FL":                   "Miami Hurricanes",
    "Miami OH":                   "Miami (OH) RedHawks",
    "Detroit":                    "Detroit Mercy Titans",
    "Army":                       "Army Knights",            # NOT "Army West Point"
    "IU Indy":                    "IUPUI Jaguars",
    "Purdue Fort Wayne":          "Fort Wayne Mastodons",
    "ETSU":                       "East Tennessee St Buccaneers",
    "East Tennessee St.":         "East Tennessee St Buccaneers",
    "ULM":                        "UL Monroe Warhawks",
    "Louisiana Monroe":           "UL Monroe Warhawks",
    "SIUE":                       "SIU-Edwardsville Cougars",
    "SIU":                        "Southern Illinois Salukis",
    "Southern Illinois":          "Southern Illinois Salukis",
    "FIU":                        "Florida Int'l Golden Panthers",
    "FAU":                        "Florida Atlantic Owls",
    "UCSB":                       "UC Santa Barbara Gauchos",
    "UALR":                       "Arkansas-Little Rock Trojans",
    "UMass":                      "Massachusetts Minutemen",
    "UMass Lowell":               "UMass Lowell River Hawks",
    "GW":                         "GW Revolutionaries",
    "George Washington":          "GW Revolutionaries",
    "Loyola Chicago":             "Loyola (Chi) Ramblers",
    "Loyola MD":                  "Loyola (MD) Greyhounds",
    "St. Mary's":                 "Saint Mary's Gaels",
    "Saint Mary's":               "Saint Mary's Gaels",
    "St. John's":                 "St. John's Red Storm",
    "St. Bonaventure":            "St. Bonaventure Bonnies",
    "St. Francis PA":             "St. Francis (PA) Red Flash",
    "St. Francis NY":             "St. Francis BKN Terriers",
    "St. Peter's":                "Saint Peter's Peacocks",
    "St. Thomas":                 "St. Thomas (MN) Tommies",
    "Mount St. Mary's":           "Mt. St. Mary's Mountaineers",
    "Texas A&M Corpus Chris":     "Texas A&M-CC Islanders",
    "Omaha":                      "Omaha Mavericks",
    "Kansas City":                "UMKC Kangaroos",
    "American":                   "American Eagles",
    "The Citadel":                "The Citadel Bulldogs",
    "Albany":                     "Albany Great Danes",
    "Central Connecticut":        "Central Connecticut St Blue Devils",
    "William & Mary":             "William & Mary Tribe",
    "Cal Poly":                   "Cal Poly Mustangs",
    "Cal Baptist":                "Cal Baptist Lancers",
    "Southern Miss":              "Southern Miss Golden Eagles",
    "Queens":                     "Queens University Royals",
    "Prairie View":               "Prairie View Panthers",
    "Bethune-Cookman":            "Bethune-Cookman Wildcats",
    "North Carolina A&T":         "North Carolina A&T Aggies",
    "North Carolina Central":     "North Carolina Central Eagles",
    "Gardner-Webb":               "Gardner-Webb Bulldogs",
    "UT Martin":                  "Tenn-Martin Skyhawks",
    "UT Arlington":               "UT-Arlington Mavericks",
    "West Georgia":               "West Georgia Wolves",
    "Lipscomb":                   "Lipscomb Bisons",
    "Stephen F. Austin":          "Stephen F. Austin Lumberjacks",
    "Hawaii":                     "Hawai'i Rainbow Warriors",
    "Hawai'i":                    "Hawai'i Rainbow Warriors",
    "Long Beach St.":             "Long Beach St 49ers",
    "Long Beach St":              "Long Beach St 49ers",
    "SE Louisiana":               "SE Louisiana Lions",
    "Southeastern Louisiana":     "SE Louisiana Lions",
    "Middle Tennessee":           "Middle Tennessee Blue Raiders",

    # ── "St." abbreviation → Odds API keeps "St" (no period) ────────────────
    "Kansas St.":                 "Kansas St Wildcats",
    "Iowa St.":                   "Iowa State Cyclones",
    "Ohio St.":                   "Ohio State Buckeyes",
    "Michigan St.":               "Michigan St Spartans",
    "Penn St.":                   "Penn State Nittany Lions",
    "Arizona St.":                "Arizona St Sun Devils",
    "Oregon St.":                 "Oregon St Beavers",
    "Colorado St.":               "Colorado St Rams",
    "Utah St.":                   "Utah State Aggies",
    "Boise St.":                  "Boise State Broncos",
    "Fresno St.":                 "Fresno St Bulldogs",
    "San Diego St.":              "San Diego St Aztecs",
    "Montana St.":                "Montana St Bobcats",
    "Idaho St.":                  "Idaho State Bengals",
    "Weber St.":                  "Weber State Wildcats",
    "Portland St.":               "Portland St Vikings",
    "Sacramento St.":             "Sacramento St Hornets",
    "North Dakota St.":           "North Dakota St Bison",
    "South Dakota St.":           "South Dakota St Jackrabbits",
    "Indiana St.":                "Indiana St Sycamores",
    "Illinois St.":               "Illinois St Redbirds",
    "Missouri St.":               "Missouri St Bears",
    "Murray St.":                 "Murray St Racers",
    "Morehead St.":               "Morehead St Eagles",
    "McNeese St.":                "McNeese Cowboys",
    "McNeese":                    "McNeese Cowboys",
    "Nicholls St.":               "Nicholls St Colonels",
    "Nicholls":                   "Nicholls St Colonels",
    "Tennessee St.":              "Tennessee St Tigers",
    "Jackson St.":                "Jackson St Tigers",
    "Alcorn St.":                 "Alcorn St Braves",
    "Grambling St.":              "Grambling St Tigers",
    "Grambling":                  "Grambling St Tigers",
    "Cleveland St.":              "Cleveland St Vikings",
    "Kennesaw St.":               "Kennesaw St Owls",
    "Sam Houston St.":            "Sam Houston St Bearkats",
    "Sam Houston":                "Sam Houston St Bearkats",
    "Tarleton St.":               "Tarleton State Texans",
    "New Mexico St.":             "New Mexico St Aggies",
    "SE Missouri St.":            "SE Missouri St Redhawks",
    "Appalachian St.":            "Appalachian St Mountaineers",
    "Georgia St.":                "Georgia St Panthers",
    "Wright St.":                 "Wright St Raiders",
    "Youngstown St.":             "Youngstown St Penguins",
    "Oklahoma St.":               "Oklahoma St Cowboys",
    "Washington St.":             "Washington St Cougars",
    "Jacksonville St.":           "Jacksonville St Gamecocks",
    "Mississippi St.":            "Mississippi St Bulldogs",
    "Arkansas St.":               "Arkansas St Red Wolves",
    "Cal St. Fullerton":          "CSU Fullerton Titans",
    "Cal St. Bakersfield":        "CSU Bakersfield Roadrunners",
    "Cal St. Northridge":         "CSU Northridge Matadors",
    "CSU Fullerton":              "CSU Fullerton Titans",
    "CSU Bakersfield":            "CSU Bakersfield Roadrunners",
    "CSU Northridge":             "CSU Northridge Matadors",
    "Florida St.":                "Florida St Seminoles",
    "Coppin St.":                 "Coppin St Eagles",
    "Norfolk St.":                "Norfolk St Spartans",
    "Morgan St.":                 "Morgan St Bears",
    "Chicago St.":                "Chicago St Cougars",
    "Delaware St.":               "Delaware St Hornets",
    "South Carolina St.":         "South Carolina St Bulldogs",
    "N.C. A&T":                   "North Carolina A&T Aggies",
    "Wis.-Milwaukee":             "Milwaukee Panthers",
    "Green Bay":                  "Green Bay Phoenix",
    "N. Colorado":                "N Colorado Bears",
    "Northern Colorado":          "N Colorado Bears",
    "Miss. Valley St.":           "Miss Valley St Delta Devils",
    "Mississippi Valley St.":     "Miss Valley St Delta Devils",
}


def _load_team_map() -> dict:
    """
    Start with hardcoded seed (ground truth), then layer data/team_map.csv on top.
    Tries multiple path resolutions for Streamlit Cloud compatibility.
    """
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

# Inverted index: exact Odds API full_name → key for O(1) lookup
_ODDS_NAME_INDEX: dict = {v.lower(): v for v in KENPOM_TO_ODDS.values()}


def fetch_vegas_lines() -> pd.DataFrame:
    if not ODDS_API_KEY:
        print("     ODDS_API_KEY not set — skipping Vegas lines.")
        return pd.DataFrame()

    now_utc  = datetime.now(timezone.utc)
    now_ct   = datetime.now(CENTRAL)

    # Window: midnight CT today → midnight CT tomorrow + 8hr buffer
    # This captures 10pm CT games (= 4am UTC next day) without relying on DST math
    day_start_ct = now_ct.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end_ct   = day_start_ct + timedelta(hours=32)   # covers up to 8am CT next day
    to_iso = lambda dt: dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    params = {
        "apiKey":             ODDS_API_KEY,
        "regions":            "us",
        "markets":            "spreads,totals,h2h",
        "oddsFormat":         "american",
        "bookmakers":         ",".join(BOOK_PRIORITY),
        "commenceTimeFrom":   to_iso(day_start_ct),
        "commenceTimeTo":     to_iso(day_end_ct),
        "dateFormat":         "iso",
    }

    try:
        resp = requests.get(BASE_URL, params=params, timeout=10)
    except Exception as e:
        print(f"    Odds API request failed: {e}")
        return pd.DataFrame()

    if resp.status_code == 401:
        print("    Odds API: 401 Unauthorized — check ODDS_API_KEY.")
        return pd.DataFrame()
    if resp.status_code == 429:
        print("    Odds API: 429 Too Many Requests — rate limited.")
        return pd.DataFrame()
    resp.raise_for_status()

    games = resp.json()
    remaining = resp.headers.get("x-requests-remaining", "?")
    print(f"    Vegas lines fetched ({len(games)} games) | Credits remaining: {remaining}")

    rows = []
    skipped_live = 0
    for game in games:
        # ── Skip games that have already started ─────────────────────────────
        raw_time = game.get("commence_time", "")
        game_time_ct = ""
        if raw_time:
            try:
                utc_dt = datetime.strptime(raw_time, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
                if utc_dt <= now_utc:
                    skipped_live += 1
                    continue   # game started — skip live lines
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

        rows.append({
            "vegas_home":     home,
            "vegas_away":     away,
            "vegas_spread":   vegas_spread,
            "vegas_total":    vegas_total,
            "vegas_home_ml":  vegas_home_ml,
            "source_book":    source_book,
            "odds_game_time": game_time_ct,
        })

    if skipped_live:
        print(f"    Skipped {skipped_live} live/started games")

    global _odds_last_fetched
    _odds_last_fetched = now_ct.strftime("%-I:%M %p CT")

    return pd.DataFrame(rows)


def get_odds_last_fetched() -> str:
    return _odds_last_fetched


def _sim(a: str, b: str) -> float:
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()


def _strip_nickname(name: str) -> str:
    """Strip team nickname so fuzzy matching works on school/city name only."""
    suffixes = [
        # Must be sorted longest-first (done at call site)
        " Fighting Illini", " Nittany Lions", " Golden Gophers", " Demon Deacons",
        " Rainbow Warriors", " Thundering Herd", " Screaming Eagles", " Fighting Irish",
        " Scarlet Knights", " Fighting Hawks", " Golden Eagles", " Golden Bears",
        " Golden Flashes", " Golden Hurricane", " Golden Knights", " Blue Devils",
        " Crimson Tide", " Tar Heels", " Wolfpack", " Cavaliers", " Hokies",
        " Wildcats", " Boilermakers", " Hoosiers", " Buckeyes", " Wolverines",
        " Spartans", " Badgers", " Hawkeyes", " Cornhuskers", " Huskers",
        " Gophers", " Illini", " Terrapins", " Terps", " Tigers", " Bulldogs",
        " Gators", " Seminoles", " Hurricanes", " Volunteers", " Razorbacks",
        " Gamecocks", " Aggies", " Longhorns", " Red Raiders", " Horned Frogs",
        " Sooners", " Cowboys", " Bears", " Mountaineers", " Cougars", " Utes",
        " Sun Devils", " Ducks", " Beavers", " Huskies", " Bruins", " Trojans",
        " Cardinal", " Eagles", " Panthers", " Cardinals", " Hawks", " Owls",
        " Rams", " Flames", " Falcons", " Ravens", " Blue Hens", " Retrievers",
        " Musketeers", " Ramblers", " Bluejays", " Blue Jays", " Flyers",
        " Pilots", " Zags", " Friars", " Hoyas", " Dons", " Gaels", " Toreros",
        " Matadors", " Anteaters", " Tritons", " Gauchos", " Highlanders",
        " Roadrunners", " Broncos", " Mustangs", " Hornets", " 49ers", " Spiders",
        " Penguins", " Zips", " Rockets", " Chippewas", " Red Hawks", " Thunderbirds",
        " Lopes", " Skyhawks", " Lions", " Monarchs", " Colonials", " Colonels",
        " Patriots", " Rebels", " Jackrabbits", " Coyotes", " Bison",
        " Lumberjacks", " Governors", " Red Wolves", " Warhawks", " Red Storm",
        " Great Danes", " Seawolves", " Terriers", " Crusaders", " Paladins",
        " Catamounts", " Minutemen", " River Hawks", " Jaguars", " Royals",
        " Mavericks", " Ospreys", " Hatters", " Dolphins", " Corsairs",
        " Blazers", " Billikens", " Braves", " Redhawks", " Mean Green",
        " Chanticleers", " Warriors", " Lobos", " Miners", " Bearkats",
        " Beacons", " Phoenix", " Mids", " Black Knights", " Midshipmen",
        " Keydets", " Bonnies", " Green Wave", " Shockers", " Seahawks",
        " Norse", " Hilltoppers", " Racers", " Commodores", " Salukis",
        " Sycamores", " Purple Aces", " Vikings", " Grizzlies", " Bobcats",
        " RedHawks", " Vaqueros", " Privateers", " Delta Devils", " Dukes",
        " Redbirds", " Leathernecks", " Lancers", " Flames", " Penmen",
        " Tommies", " Jaspers", " Red Foxes", " Peacocks", " Stags",
        " Waves", " Pilots", " Trailblazers", " Mastodons", " Blue Raiders",
        " Mocs", " Paladins", " Warhawks", " Thunderbirds", " Skyhawks",
        " Buccaneers", " Explorers", " Ramblers", " Revolutionaries",
        " Cyclones", " Tar Heels", " Mountaineers", " Hilltoppers",
        " Camels", " Quakers", " Big Red", " Crimson", " Big Green",
        " Ephs", " Judges", " Bald Eagles", " Comets", " Sharks",
        " Kangaroos", " Retrievers", " Ospreys", " Lancers",
    ]
    for s in sorted(suffixes, key=len, reverse=True):
        if name.endswith(s):
            return name[:-len(s)].strip()
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
        "vegas_fav": None, "my_fav": None, "sides_agree": None, "source_book": None}

    if vegas_df.empty:
        return _null(result)

    kp_t1 = result["team1"]
    kp_t2 = result["team2"]

    # ── Strategy 1: direct exact lookup if both teams are seeded ─────────────
    odds_t1 = KENPOM_TO_ODDS.get(kp_t1)
    odds_t2 = KENPOM_TO_ODDS.get(kp_t2)

    best_match   = None
    best_flipped = False

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

    # ── Strategy 2: fuzzy fallback ───────────────────────────────────────────
    if best_match is None:
        # Normalize: seed lookup then strip nickname
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
