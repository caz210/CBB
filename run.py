"""
run.py
Main entry point.
- Pulls today's games automatically from the KenPom fanmatch API
- Fetches fresh ratings, four factors, height, and NET rankings
- Runs your model on every game and saves results to outputs/projections.csv
"""

import os
import pandas as pd
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
from kenpom_fetcher import fetch_all, fetch_fanmatch, save_data
from net_fetcher import fetch_net_rankings
from model import load_data, project_game
from debug_logger import write_debug_excel
from odds_fetcher import fetch_vegas_lines, match_vegas_to_game

SEASON = 2026  # Current season (2025-26)
CENTRAL = ZoneInfo("America/Chicago")


#  Parse fanmatch into game dicts 

def is_neutral_site(row) -> bool:
    """
    Detect neutral site games from fanmatch data.
    KenPom sets HomeWP to exactly 0.5 for neutral site games (no home court advantage).
    We also check if HomePred and VisitorPred are symmetric as a secondary signal.
    """
    try:
        home_wp = float(row.get("HomeWP", 0))
        # KenPom uses exactly 0.5 for neutral sites
        if home_wp == 0.5:
            return True
        # Secondary check: if predicted scores are within 0.1 of each other
        # and HomeWP is very close to 0.5, likely neutral
        home_pred    = float(row.get("HomePred", 0))
        visitor_pred = float(row.get("VisitorPred", 0))
        if abs(home_wp - 0.5) < 0.01 and abs(home_pred - visitor_pred) < 1.0:
            return True
    except (TypeError, ValueError):
        pass
    return False


def games_from_fanmatch(today: str) -> list[dict]:
    """
    Pulls today's schedule from the KenPom fanmatch endpoint.
    Also checks tomorrow's date when running after 8 PM CT — a 10 PM CT game
    is 4 AM UTC the next day, so KenPom may store it under tomorrow's date.
    Detects neutral site games via HomeWP = 0.5 (KenPom convention).
    team1 = Home team, team2 = Visitor.
    team1_is_home = True (home game), False (away), None (neutral site).
    """
    print(f" Fetching today's games from fanmatch ({today})...")

    frames = []
    try:
        fm = fetch_fanmatch(today)
        if not fm.empty:
            frames.append(fm)
            fm.to_csv(f"data/fanmatch_{today}.csv", index=False)
    except Exception as e:
        print(f"    fanmatch fetch failed for {today}: {e}")

    # After 8 PM CT, also check tomorrow — catches late games logged under UTC next day
    now_ct = datetime.now(CENTRAL)
    if now_ct.hour >= 20:
        tomorrow = (date.fromisoformat(today) + timedelta(days=1)).isoformat()
        print(f"    After 8 PM CT — also checking {tomorrow} for late-night games...")
        try:
            fm_tomorrow = fetch_fanmatch(tomorrow)
            if not fm_tomorrow.empty:
                frames.append(fm_tomorrow)
                fm_tomorrow.to_csv(f"data/fanmatch_{tomorrow}.csv", index=False)
        except Exception as e:
            print(f"    fanmatch fetch failed for {tomorrow}: {e}")

    if not frames:
        print("     No games found for today in fanmatch.")
        return []

    combined = pd.concat(frames, ignore_index=True)
    if "GameID" in combined.columns:
        combined = combined.drop_duplicates(subset="GameID")

    games = []
    neutral_count = 0
    for _, row in combined.iterrows():
        neutral = is_neutral_site(row)
        if neutral:
            neutral_count += 1

        games.append({
            "team1":          row["Home"],
            "team2":          row["Visitor"],
            "team1_is_home":  None if neutral else True,
            "kp_home_score":  row.get("HomePred",    None),
            "kp_away_score":  row.get("VisitorPred", None),
            "kp_home_wp":     row.get("HomeWP",      None),
            "kp_tempo":       row.get("PredTempo",   None),
            "game_id":        row.get("GameID",      None),
            "game_time":      row.get("GameTime",    row.get("Time", None)),
        })

    home_count = len(games) - neutral_count
    print(f"    {len(games)} games found ({home_count} home/away, {neutral_count} neutral site)")
    return games


#  Main runner 

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

    # 4. Get today's games from fanmatch (no manual entry needed!)
    games = games_from_fanmatch(today)
    if not games:
        print("No games to project. Exiting.")
        return

    # 5. Run your model on every game
    results = []
    print(f"\n Projections for {today}\n{''*50}")

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
            print(f"   {r['team1']:22} YOUR: {r['team1_score']:5.1f}   KP: {game['kp_home_score']}")
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
        print(f"\n EDGE REPORT  sorted by disagreement with Vegas")
        print(f"  {'Game':<32} {'My Fav':<14} {'VGS Fav':<14} {'Swing':>6} {'Edge':>8} {'Agree?'}")
        print(f"  {''*85}")
        sorted_results = sorted(results, key=lambda r: r.get("edge_score") or 0, reverse=True)
        for r in sorted_results:
            if r.get("vegas_spread") is not None:
                game      = f"{r['team1']} vs {r['team2']}"[:31]
                my_fav    = (r.get('my_fav') or '')[:13]
                vgs_fav   = (r.get('vegas_fav') or '')[:13]
                swing     = r.get('spread_edge', 0)
                edge      = r.get('edge_score', 0)
                agree     = "" if r.get('sides_agree') else " DIFFER"
                neutral   = " [N]" if r.get("location") == "neutral" else ""
                print(f"  {game+neutral:<32} {my_fav:<14} {vgs_fav:<14} {swing:>6.1f} {edge:>8.4f} {agree}")

    # 7. Write debug Excel log
    write_debug_excel(results, today)

    # 7. Save results to CSV
    os.makedirs("outputs", exist_ok=True)
    if results:
        out = pd.DataFrame(results)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        proj_path = f"outputs/projections_{today}_{ts}.csv"
        out.to_csv(proj_path, index=False)
        print(f" Saved {len(results)} projections  {proj_path}")


if __name__ == "__main__":
    run(refresh_data=True)

    # To run for a specific past date (useful for backtesting):
    # run(refresh_data=False, target_date="2025-02-15")

