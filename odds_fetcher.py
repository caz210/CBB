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

ODDS_API_KEY = _get_secret("ODDS_API_KEY")
BASE_URL     = "https://api.the-odds-api.com/v4/sports/basketball_ncaab/odds"
BOOK_PRIORITY = ["draftkings", "fanduel", "betmgm", "caesars", "bovada", "mybookieag"]
MATCH_THRESHOLD = 0.75   # both teams must score >= this to be a valid match

# KenPom name → Odds API name for known mismatches
KENPOM_TO_ODDS = {
    "Ole Miss":          "Mississippi",
    "Miami FL":          "Miami (FL)",
    "Miami OH":          "Miami (OH)",
    "UConn":             "Connecticut",
    "UNLV":              "Nevada Las Vegas",
    "Pitt":              "Pittsburgh",
    "USC":               "Southern California",
    "Detroit":           "Detroit Mercy",
    "St. Mary's":        "Saint Mary's",
    "UCSB":              "UC Santa Barbara",
    "UNC":               "North Carolina",
    "UNCW":              "UNC Wilmington",
    "Texas A&M Corpus Chris": "Texas A&M Corpus Christi",
}


def fetch_vegas_lines() -> pd.DataFrame:
    if not ODDS_API_KEY:
        print("     ODDS_API_KEY not set -- skipping Vegas lines.")
        return pd.DataFrame()

    params = {
        "apiKey":      ODDS_API_KEY,
        "regions":     "us",
        "markets":     "spreads,totals,h2h",
        "oddsFormat":  "american",
        "bookmakers":  ",".join(BOOK_PRIORITY),
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

        rows.append({
            "vegas_home":    home,
            "vegas_away":    away,
            "vegas_spread":  vegas_spread,
            "vegas_total":   vegas_total,
            "vegas_home_ml": vegas_home_ml,
            "source_book":   source_book,
        })

    return pd.DataFrame(rows)


def normalize_name(name: str) -> str:
    """Apply KenPom→OddsAPI name mapping, then strip common team name suffixes."""
    name = KENPOM_TO_ODDS.get(name, name)
    suffixes = [
        " Blue Devils", " Wildcats", " Bulldogs", " Tigers", " Bears", " Trojans",
        " Cardinals", " Eagles", " Cougars", " Panthers", " Spartans", " Tar Heels",
        " Volunteers", " Longhorns", " Aggies", " Seminoles", " Gators", " Hurricanes",
        " Demon Deacons", " Mountaineers", " Thunderbirds", " Red Hawks", " Zips",
        " Flyers", " Golden Flashes", " Chippewas", " Falcons", " Rockets",
        " Rams", " Owls", " Flames", " Hawks", " Norse",
    ]
    for s in suffixes:
        if name.endswith(s):
            return name[:-len(s)]
    return name


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
        return _null(result)

    v_total     = best_match["vegas_total"]
    raw_vspread = best_match["vegas_spread"]

    # Negate if flipped so spread is always from model's team1 perspective
    v_spread = (-raw_vspread if raw_vspread is not None else None) if best_flipped else raw_vspread

    result["vegas_spread"]  = v_spread
    result["vegas_total"]   = v_total
    result["vegas_home_ml"] = best_match["vegas_home_ml"]
    result["source_book"]   = best_match["source_book"]

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
