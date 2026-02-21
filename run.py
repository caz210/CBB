"""
run.py
Main entry point.
- Pulls today's games automatically from the KenPom fanmatch API
- Fetches fresh ratings, four factors, height, and NET rankings
- Runs your model on every game and saves results to outputs/projections.csv
"""

import os
import pandas as pd
from datetime import date, datetime
from kenpom_fetcher import fetch_all, fetch_fanmatch, save_data
from net_fetcher import fetch_net_rankings
from model import load_data, project_game
from debug_logger import write_debug_excel
from odds_fetcher import fetch_vegas_lines, match_vegas_to_game

SEASON = 2026  # Current season (2025-26)


# ── Parse fanmatch into game dicts ───────────────────────────────────────────

def games_from_fanmatch(today: str) -> list[dict]:
    """
    Pulls today's schedule from the KenPom fanmatch endpoint.
    Returns a list of game dicts ready for project_game().

    Fanmatch fields used:
      Home     → team playing at home
      Visitor  → team playing away
      HomePred / VisitorPred → KenPom's own score predictions (saved for reference)
      HomeWP   → KenPom win probability for home team
    """
    print(f"📡 Fetching today's games from fanmatch ({today})...")
    fm = fetch_fanmatch(today)

    if fm.empty:
        print("   ⚠️  No games found for today in fanmatch.")
        return []

    games = []
    for _, row in fm.iterrows():
        games.append({
            "team1":          row["Home"],       # team1 = home team
            "team2":          row["Visitor"],    # team2 = away team
            "team1_is_home":  True,              # Home is always team1 here
            "kp_home_score":  row.get("HomePred",    None),
            "kp_away_score":  row.get("VisitorPred", None),
            "kp_home_wp":     row.get("HomeWP",      None),
            "kp_tempo":       row.get("PredTempo",   None),
            "game_id":        row.get("GameID",      None),
        })

    print(f"   ✅ {len(games)} games found today")
    return games


# ── Main runner ───────────────────────────────────────────────────────────────

def run(refresh_data: bool = True, target_date: str = None):
    today = target_date or str(date.today())

    if refresh_data:
        # 1. Pull fresh KenPom ratings, four factors, height
        kp_data = fetch_all(year=SEASON)
        save_data(kp_data)

        # 2. Pull NCAA NET rankings
        print("📡 Fetching NCAA NET rankings...")
        net_df = fetch_net_rankings()
        net_df.to_csv("data/net.csv", index=False)
        print(f"   ✅ NET rankings  ({len(net_df)} teams)")

    # 3. Load all model data
    data = load_data()

    # 4. Get today's games from fanmatch (no manual entry needed!)
    games = games_from_fanmatch(today)
    if not games:
        print("No games to project. Exiting.")
        return

    # 5. Run your model on every game
    results = []
    print(f"\n🏀 Projections for {today}\n{'─'*50}")

    for game in games:
        try:
            r = project_game(
                team1=game["team1"],
                team2=game["team2"],
                team1_is_home=game["team1_is_home"],
                data=data,
            )

            # Attach KenPom's own predictions for easy side-by-side comparison
            r["kp_home_score"] = game["kp_home_score"]
            r["kp_away_score"] = game["kp_away_score"]
            r["kp_home_wp"]    = game["kp_home_wp"]
            r["kp_tempo"]      = game["kp_tempo"]
            r["game_id"]       = game["game_id"]

            results.append(r)

            # Print side-by-side: your model vs KenPom
            print(f"  🏠 {r['team1']:22} YOUR: {r['team1_score']:5.1f}   KP: {game['kp_home_score']}")
            print(f"  ✈️  {r['team2']:22} YOUR: {r['team2_score']:5.1f}   KP: {game['kp_away_score']}")

            spread_team = r['team1'] if r['spread'] > 0 else r['team2']
            print(f"      Spread: {spread_team} -{abs(r['spread']):.1f}  |  Total: {r['total']:.1f}  |  KP Tempo: {game['kp_tempo']}")
            print()

        except Exception as e:
            print(f"  ⚠️  {game['team1']} vs {game['team2']}: {e}\n")

    # 6. Match Vegas lines to results
    print("📡 Fetching Vegas lines...")
    vegas_df = fetch_vegas_lines()
    results = [match_vegas_to_game(r, vegas_df) for r in results]

    # Print edge scores
    if any(r.get("edge_score") is not None for r in results):
        print(f"\n{'─'*50}")
        print(f"  {'Game':<35} {'My Sprd':>8} {'VGS Sprd':>9} {'Edge':>7} {'Scr':>7}")
        print(f"{'─'*50}")
        sorted_results = sorted(results, key=lambda r: r.get("edge_score") or 0, reverse=True)
        for r in sorted_results:
            if r.get("vegas_spread") is not None:
                game = f"{r['team1']} vs {r['team2']}"[:34]
                print(f"  {game:<35} {r['spread']:>+7.1f} {r['vegas_spread']:>+9.1f} {r.get('spread_edge', 0):>7.2f} {r.get('edge_score', 0):>7.4f}")

    # 7. Write debug Excel log
    write_debug_excel(results, today)

    # 7. Save results to CSV
    os.makedirs("outputs", exist_ok=True)
    if results:
        out = pd.DataFrame(results)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        proj_path = f"outputs/projections_{today}_{ts}.csv"
        out.to_csv(proj_path, index=False)
        print(f"✅ Saved {len(results)} projections → {proj_path}")


if __name__ == "__main__":
    run(refresh_data=True)

    # To run for a specific past date (useful for backtesting):
    # run(refresh_data=False, target_date="2025-02-15")

