"""
run.py
Main entry point.
- Scrapes KenPom FanMatch page for the FULL game list + correct neutral/home detection
- Fetches KenPom API predictions (HomePred, VisitorPred, HomeWP, PredTempo) and joins them
- Fetches fresh ratings, four factors, height, and NET rankings
- Runs your model on every game and saves results to outputs/projections.csv

Neutral detection strategy:
  - kenpom_scraper.py scrapes the HTML page → "at" = home game, "vs" = neutral site
  - This is the source of truth for team1_is_home (None = neutral, True = home)
  - kenpom_fetcher.py API provides HomePred/VisitorPred/HomeWP/PredTempo
  - The two sources are joined by team name so no prediction data is lost
  - Scraper catches ALL games (e.g. 23 today) even when API returns fewer (e.g. 18)
"""

import os
import pandas as pd
from datetime import date, datetime, timedelta
from difflib import SequenceMatcher
from zoneinfo import ZoneInfo
from kenpom_fetcher import fetch_all, fetch_fanmatch, save_data
from kenpom_scraper import scrape_fanmatch_games
from net_fetcher import fetch_net_rankings
from model import load_data, project_game
from debug_logger import write_debug_excel
from odds_fetcher import fetch_vegas_lines, match_vegas_to_game

SEASON = 2026  # Current season (2025-26)
CENTRAL = ZoneInfo("America/Chicago")


# ── Name similarity helper for joining scraper ↔ API team names ──────────────

def _sim(a: str, b: str) -> float:
    return SequenceMatcher(None, a.lower().strip(), b.lower().strip()).ratio()


def _best_api_match(scraper_name: str, api_names: list[str], threshold: float = 0.82) -> str | None:
    """
    Find the closest API team name to a scraped team name.
    Returns None if no match exceeds threshold.
    """
    best_score = 0.0
    best_name  = None
    for api_name in api_names:
        s = _sim(scraper_name, api_name)
        if s > best_score:
            best_score = s
            best_name  = api_name
    return best_name if best_score >= threshold else None


# ── Main game builder ─────────────────────────────────────────────────────────

def games_from_fanmatch(today: str) -> list[dict]:
    """
    Builds today's game list by combining two sources:

    1. kenpom_scraper  → scrapes the full HTML FanMatch page
                         - Gets ALL games (catches tournament games API misses)
                         - "Team A at Team B" → home game (team2 is home)
                         - "Team A vs Team B" → neutral site
                         - This is the authoritative source for team1_is_home

    2. kenpom_fetcher  → API call for HomePred, VisitorPred, HomeWP, PredTempo
                         - Joined onto scraper results by fuzzy team name match
                         - Missing for API-absent games → None (model still runs)

    team1 = listed first on KenPom (visitor or neutral-left)
    team2 = listed second (home or neutral-right)
    team1_is_home = True  → team2 is home (team1 is away)
                  = None  → neutral site
    """
    print(f" Scraping KenPom FanMatch page ({today})...")

    tomorrow = (date.fromisoformat(today) + timedelta(days=1)).isoformat()

    # ── Step 1: Scrape full game list from HTML (today + tomorrow) ────────────
    scraped_games = []
    try:
        scraped_today = scrape_fanmatch_games(today)
        scraped_games.extend(scraped_today)
        print(f"    Scraper ({today}): {len(scraped_today)} games")
    except Exception as e:
        print(f"    Scraper failed for {today}: {e}")

    try:
        scraped_tomorrow = scrape_fanmatch_games(tomorrow)
        if scraped_tomorrow:
            # Deduplicate by team pair before extending
            existing_pairs = {frozenset([g["team1"].lower(), g["team2"].lower()]) for g in scraped_games}
            new_games = [g for g in scraped_tomorrow
                         if frozenset([g["team1"].lower(), g["team2"].lower()]) not in existing_pairs]
            scraped_games.extend(new_games)
            print(f"    Scraper ({tomorrow}): {len(scraped_tomorrow)} games ({len(new_games)} new)")
    except Exception as e:
        print(f"    Scraper failed for {tomorrow}: {e}")

    if scraped_games:
        neutral_count = sum(1 for g in scraped_games if g["neutral"])
        home_count    = len(scraped_games) - neutral_count
        print(f"    Scraper total: {len(scraped_games)} games ({neutral_count} neutral, {home_count} home/away)")

    # ── Step 2: Fetch API predictions ────────────────────────────────────────
    api_lookup: dict[tuple, dict] = {}   # (home_lower, visitor_lower) → row dict

    frames = []
    try:
        fm = fetch_fanmatch(today)
        if not fm.empty:
            frames.append(fm)
            fm.to_csv(f"data/fanmatch_{today}.csv", index=False)
            print(f"    API: {len(fm)} games with predictions")
    except Exception as e:
        print(f"    API fanmatch fetch failed for {today}: {e}")

    # Always check tomorrow too — covers UTC rollover + date-picker-ahead use
    try:
        fm_tomorrow = fetch_fanmatch(tomorrow)
        if not fm_tomorrow.empty:
            frames.append(fm_tomorrow)
            fm_tomorrow.to_csv(f"data/fanmatch_{tomorrow}.csv", index=False)
            print(f"    API ({tomorrow}): {len(fm_tomorrow)} games")
    except Exception as e:
        print(f"    API fanmatch fetch failed for {tomorrow}: {e}")

    if frames:
        combined = pd.concat(frames, ignore_index=True)
        if "GameID" in combined.columns:
            combined = combined.drop_duplicates(subset="GameID")
        # Build lookup: (home_lower, visitor_lower) → prediction dict
        for _, row in combined.iterrows():
            key = (str(row.get("Home", "")).lower(), str(row.get("Visitor", "")).lower())
            api_lookup[key] = row.to_dict()

    api_names = list({k[0] for k in api_lookup} | {k[1] for k in api_lookup})

    # ── Step 3: If scraper got nothing, fall back to API-only ─────────────────
    if not scraped_games:
        if not api_lookup:
            print("     No games found from scraper or API.")
            return []
        print("    Using API-only fallback (neutral detection unreliable)")
        games = []
        for (home_lower, visitor_lower), row in api_lookup.items():
            games.append({
                "team1":         row.get("Home"),
                "team2":         row.get("Visitor"),
                "team1_is_home": True,   # unknown — API fallback, assume home
                "kp_home_score": row.get("HomePred"),
                "kp_away_score": row.get("VisitorPred"),
                "kp_home_wp":    row.get("HomeWP"),
                "kp_tempo":      row.get("PredTempo"),
                "game_id":       row.get("GameID"),
                "game_time":     row.get("GameTime", row.get("Time")),
            })
        print(f"    {len(games)} games (API fallback, no neutral detection)")
        return games

    # ── Step 4: Join scraper games with API predictions ───────────────────────
    games        = []
    api_hit      = 0
    api_miss     = 0

    for g in scraped_games:
        t1 = g["team1"]   # visitor / neutral-left
        t2 = g["team2"]   # home    / neutral-right

        # Neutral flag comes directly from scraper ("vs" = neutral, "at" = home game)
        neutral       = g["neutral"]
        team1_is_home = None if neutral else True

        # Try to find matching API row (exact first, then fuzzy)
        pred_row = None
        # Exact match
        key_normal  = (t2.lower(), t1.lower())   # API stores Home first
        key_flipped = (t1.lower(), t2.lower())
        if key_normal in api_lookup:
            pred_row = api_lookup[key_normal]
        elif key_flipped in api_lookup:
            pred_row = api_lookup[key_flipped]
        else:
            # Fuzzy match — useful when API name differs slightly from scraped name
            t1_api = _best_api_match(t1, api_names)
            t2_api = _best_api_match(t2, api_names)
            if t1_api and t2_api:
                k1 = (t2_api.lower(), t1_api.lower())
                k2 = (t1_api.lower(), t2_api.lower())
                if k1 in api_lookup:
                    pred_row = api_lookup[k1]
                elif k2 in api_lookup:
                    pred_row = api_lookup[k2]

        if pred_row is not None:
            api_hit += 1
            kp_home_score = pred_row.get("HomePred")
            kp_away_score = pred_row.get("VisitorPred")
            kp_home_wp    = pred_row.get("HomeWP")
            kp_tempo      = pred_row.get("PredTempo")
            game_id       = pred_row.get("GameID")
            game_time     = pred_row.get("GameTime", pred_row.get("Time"))
        else:
            api_miss += 1
            print(f"    ⚠  No API prediction for: {t1} vs {t2} (model will still run)")
            kp_home_score = None
            kp_away_score = None
            kp_home_wp    = None
            kp_tempo      = None
            game_id       = None
            game_time     = None

        games.append({
            "team1":         t1,
            "team2":         t2,
            "team1_is_home": team1_is_home,
            "kp_home_score": kp_home_score,
            "kp_away_score": kp_away_score,
            "kp_home_wp":    kp_home_wp,
            "kp_tempo":      kp_tempo,
            "game_id":       game_id,
            "game_time":     game_time,
        })

    neutral_final = sum(1 for g in games if g["team1_is_home"] is None)
    print(f"    Final: {len(games)} games — {neutral_final} neutral, {len(games)-neutral_final} home/away")
    print(f"    Predictions joined: {api_hit} matched, {api_miss} missing from API")
    return games


# ── Main runner ───────────────────────────────────────────────────────────────

def run(refresh_data: bool = True, target_date: str = None):
    today = target_date or str(date.today())

    if refresh_data:
        # 1. Pull fresh KenPom ratings, four factors, height
        kp_data = fetch_all(year=SEASON)
        save_data(kp_data)

        # 2. Pull NCAA NET rankings
        print(" Fetching NCAA NET rankings...")
        net_df = fetch_net_rankings()
        net_df.to_csv("data/net.csv", index=False)
        print(f"    NET rankings  ({len(net_df)} teams)")

    # 3. Load all model data
    data = load_data()

    # 4. Get today's games (scraper for full list + neutral detection, API for predictions)
    games = games_from_fanmatch(today)
    if not games:
        print("No games to project. Exiting.")
        return

    # 5. Run your model on every game
    results = []
    print(f"\n Projections for {today}\n{'─'*50}")

    for game in games:
        try:
            r = project_game(
                team1=game["team1"],
                team2=game["team2"],
                team1_is_home=game["team1_is_home"],
                data=data,
                game_time=game.get("game_time"),
            )

            # Attach KenPom's own predictions for easy side-by-side comparison
            r["kp_home_score"] = game["kp_home_score"]
            r["kp_away_score"] = game["kp_away_score"]
            r["kp_home_wp"]    = game["kp_home_wp"]
            r["kp_tempo"]      = game["kp_tempo"]
            r["game_id"]       = game["game_id"]

            results.append(r)

            # Print side-by-side: your model vs KenPom
            location_tag = " [N]" if game["team1_is_home"] is None else ""
            print(f"   {r['team1']:22} YOUR: {r['team1_score']:5.1f}   KP: {game['kp_home_score']}{location_tag}")
            print(f"    {r['team2']:22} YOUR: {r['team2_score']:5.1f}   KP: {game['kp_away_score']}")

            spread_team = r['team1'] if r['spread'] > 0 else r['team2']
            print(f"      Spread: {spread_team} -{abs(r['spread']):.1f}  |  Total: {r['total']:.1f}  |  KP Tempo: {game['kp_tempo']}")
            print()

        except Exception as e:
            print(f"    {game['team1']} vs {game['team2']}: {e}\n")

    # 6. Match Vegas lines to results
    print(" Fetching Vegas lines...")
    vegas_df = fetch_vegas_lines()
    results = [match_vegas_to_game(r, vegas_df) for r in results]

    # Print edge scores sorted best to worst
    if any(r.get("edge_score") is not None for r in results):
        print(f"\n EDGE REPORT — sorted by disagreement with Vegas")
        print(f"  {'Game':<32} {'My Fav':<14} {'VGS Fav':<14} {'Swing':>6} {'Edge':>8} {'Agree?'}")
        print(f"  {'─'*85}")
        sorted_results = sorted(results, key=lambda r: r.get("edge_score") or 0, reverse=True)
        for r in sorted_results:
            if r.get("vegas_spread") is not None:
                game      = f"{r['team1']} vs {r['team2']}"[:31]
                my_fav    = (r.get('my_fav') or '')[:13]
                vgs_fav   = (r.get('vegas_fav') or '')[:13]
                swing     = r.get('spread_edge', 0)
                edge      = r.get('edge_score', 0)
                agree     = "" if r.get('sides_agree') else " ⚠ DIFFER"
                neutral   = " [N]" if r.get("location") == "neutral" else ""
                print(f"  {game+neutral:<32} {my_fav:<14} {vgs_fav:<14} {swing:>6.1f} {edge:>8.4f} {agree}")

    # 7. Write debug Excel log
    write_debug_excel(results, today)

    # 8. Save results to CSV
    os.makedirs("outputs", exist_ok=True)
    if results:
        out = pd.DataFrame(results)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        proj_path = f"outputs/projections_{today}_{ts}.csv"
        out.to_csv(proj_path, index=False)
        print(f" Saved {len(results)} projections → {proj_path}")


if __name__ == "__main__":
    run(refresh_data=True)

    # To run for a specific past date (useful for backtesting):
    # run(refresh_data=False, target_date="2025-02-15")
