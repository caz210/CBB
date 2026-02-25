# -*- coding: utf-8 -*-
"""
odds_fetcher.py
Fetches live Vegas lines from The Odds API.
"""

import os
import requests
import pandas as pd
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
                        vegas_spread = outcome["point"]  # negative = home favored
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
            "vegas_spread":  vegas_spread,  # negative = home favored
            "vegas_total":   vegas_total,
            "vegas_home_ml": vegas_home_ml,
            "source_book":   source_book,
        })

    return pd.DataFrame(rows)


def normalize_name(name: str) -> str:
    """Strip common suffixes so 'Duke Blue Devils' matches 'Duke'."""
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


def _team_match_score(t: str, v: str) -> int:
    """1 if names overlap, 0 otherwise."""
    return 1 if (t.lower() in v.lower() or v.lower() in t.lower()) else 0


def match_vegas_to_game(result: dict, vegas_df: pd.DataFrame) -> dict:
    """
    Match model result to Vegas line. Handles both orientations:
      - Normal:   model t1=home matches vegas_home, t2=away matches vegas_away
      - Flipped:  Vegas lists the game with teams swapped vs KenPom
    When flipped orientation detected, negate the spread so it's always
    expressed from model's team1 (home) perspective.
    """
    _null = lambda r: {**r,
        "vegas_spread": None, "vegas_total": None, "vegas_home_ml": None,
        "spread_edge": None, "total_edge": None, "edge_score": None,
        "vegas_fav": None, "my_fav": None, "sides_agree": None, "source_book": None}

    if vegas_df.empty:
        return _null(result)

    t1 = normalize_name(result["team1"])  # model home
    t2 = normalize_name(result["team2"])  # model away

    best_score    = 0
    best_match    = None
    best_flipped  = False   # True if Vegas has teams in reverse order

    for _, row in vegas_df.iterrows():
        vh = normalize_name(row["vegas_home"])
        va = normalize_name(row["vegas_away"])

        # Normal orientation: t1=home matches vh, t2=away matches va
        normal_score  = _team_match_score(t1, vh) + _team_match_score(t2, va)
        # Flipped orientation: t1=home matches va, t2=away matches vh
        flipped_score = _team_match_score(t1, va) + _team_match_score(t2, vh)

        top_score  = max(normal_score, flipped_score)
        is_flipped = flipped_score > normal_score

        if top_score > best_score:
            best_score   = top_score
            best_match   = row
            best_flipped = is_flipped

    if best_match is None or best_score < 1:
        return _null(result)

    v_total   = best_match["vegas_total"]
    raw_vspread = best_match["vegas_spread"]  # negative = vegas_home favored

    # If flipped: Vegas home is actually our away team, so negate to get
    # spread from our team1 (home) perspective
    if best_flipped and raw_vspread is not None:
        v_spread = -raw_vspread
    else:
        v_spread = raw_vspread

    # v_spread is now from model's team1 (home) perspective: negative = t1 favored
    result["vegas_spread"]  = v_spread
    result["vegas_total"]   = v_total
    result["vegas_home_ml"] = best_match["vegas_home_ml"]
    result["source_book"]   = best_match["source_book"]

    if v_spread is not None and v_total and v_total > 0:
        my_spread = result["spread"]  # positive = t1 (home) favored

        # Both now from t1 perspective. Convert my_spread to same sign convention as v_spread.
        # my_spread: positive = home favored
        # v_spread:  negative = home favored
        # So: my_spread_vs_convention = -my_spread
        my_vs = -my_spread

        spread_diff = abs(v_spread - my_vs)

        # Who each side favors (using t1=home, t2=away)
        # v_spread < 0 means home (t1) favored
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
