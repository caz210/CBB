"""
odds_fetcher.py
Fetches live Vegas lines (spread, total, moneyline) from The Odds API.
Free tier: 500 credits/month — each NCAAB pull costs ~10 credits.
Sign up at https://the-odds-api.com to get your free API key.
"""

import os
import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

ODDS_API_KEY = os.getenv("ODDS_API_KEY")
BASE_URL     = "https://api.the-odds-api.com/v4/sports/basketball_ncaab/odds"

# Preferred books in priority order — first available line wins
BOOK_PRIORITY = ["draftkings", "fanduel", "betmgm", "caesars", "bovada", "mybookieag"]


def fetch_vegas_lines() -> pd.DataFrame:
    """
    Pulls current NCAAB spreads, totals, and moneylines from The Odds API.
    Returns a DataFrame with one row per game, using consensus line from best available book.
    """
    if not ODDS_API_KEY:
        print("   ⚠️  ODDS_API_KEY not set in .env — skipping Vegas lines.")
        return pd.DataFrame()

    params = {
        "apiKey":      ODDS_API_KEY,
        "regions":     "us",
        "markets":     "spreads,totals,h2h",
        "oddsFormat":  "american",
        "bookmakers":  ",".join(BOOK_PRIORITY),
    }

    resp = requests.get(BASE_URL, params=params)
    resp.raise_for_status()

    games = resp.json()
    remaining = resp.headers.get("x-requests-remaining", "?")
    print(f"   ✅ Vegas lines fetched ({len(games)} games) | Credits remaining: {remaining}")

    rows = []
    for game in games:
        home = game["home_team"]
        away = game["away_team"]

        vegas_spread = None
        vegas_total  = None
        vegas_home_ml = None
        source_book  = None

        # Try books in priority order until we get all three markets
        for book_key in BOOK_PRIORITY:
            book = next((b for b in game["bookmakers"] if b["key"] == book_key), None)
            if not book:
                continue

            markets = {m["key"]: m["outcomes"] for m in book["markets"]}

            # Spread (from home team perspective)
            if vegas_spread is None and "spreads" in markets:
                for outcome in markets["spreads"]:
                    if outcome["name"] == home:
                        vegas_spread = outcome["point"]
                        source_book  = book["title"]
                        break

            # Total (O/U)
            if vegas_total is None and "totals" in markets:
                for outcome in markets["totals"]:
                    if outcome["name"] == "Over":
                        vegas_total = outcome["point"]
                        break

            # Moneyline (home team)
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
            "vegas_spread":  vegas_spread,   # negative = home favored
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
        " Rams", " Owls", " Flames", " Flames", " Hawks", " Norse",
    ]
    for s in suffixes:
        if name.endswith(s):
            return name[:-len(s)]
    return name


def match_vegas_to_game(result: dict, vegas_df: pd.DataFrame) -> dict:
    """
    Match a model game result to a Vegas line by fuzzy team name.
    Adds vegas_spread, vegas_total, spread_edge, total_edge to the result dict.
    """
    if vegas_df.empty:
        return result

    t1 = normalize_name(result["team1"])
    t2 = normalize_name(result["team2"])

    best_match = None
    best_score = 0

    for _, row in vegas_df.iterrows():
        vh = normalize_name(row["vegas_home"])
        va = normalize_name(row["vegas_away"])

        # Check both orientations
        score = 0
        if t1.lower() in vh.lower() or vh.lower() in t1.lower():
            score += 1
        if t2.lower() in va.lower() or va.lower() in t2.lower():
            score += 1

        if score > best_score:
            best_score = score
            best_match = row

    if best_match is None or best_score < 1:
        result["vegas_spread"]  = None
        result["vegas_total"]   = None
        result["vegas_home_ml"] = None
        result["spread_edge"]   = None
        result["total_edge"]    = None
        result["source_book"]   = None
        return result

    v_spread = best_match["vegas_spread"]   # e.g. -7.5 means home favored by 7.5
    v_total  = best_match["vegas_total"]
    my_spread = result["spread"]             # + = team1 (home) favored

    result["vegas_spread"]  = v_spread
    result["vegas_total"]   = v_total
    result["vegas_home_ml"] = best_match["vegas_home_ml"]
    result["source_book"]   = best_match["source_book"]

    # Edge score: |my_spread - vegas_spread| / vegas_total
    # Higher = bigger disagreement with Vegas relative to the total
    if v_spread is not None and v_total and v_total > 0:
        spread_diff = abs(my_spread - (-v_spread))  # convert vegas to same sign convention
        result["spread_edge"]  = round(spread_diff, 2)
        result["total_edge"]   = round(abs(result["total"] - v_total), 2)
        result["edge_score"]   = round(spread_diff / v_total, 4)  # YOUR KEY METRIC
    else:
        result["spread_edge"] = None
        result["total_edge"]  = None
        result["edge_score"]  = None

    return result
