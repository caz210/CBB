"""Parallel season pull: ESPN results + DK open/close lines (cached) for a date range.
    python fetch_history.py 2025-11-03 2026-04-06 [workers]
"""
import os, sys
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
import pandas as pd
import espn_history as eh

def main(s, e, workers=6):
    days = [s + timedelta(n) for n in range((e - s).days + 1)]
    with ThreadPoolExecutor(workers) as ex:
        boards = list(ex.map(eh.scoreboard, days))
    games = [g for b in boards for g in b if g["completed"]]
    print(f"{len(games)} completed games; fetching lines", flush=True)
    with ThreadPoolExecutor(workers) as ex:
        lines = list(ex.map(lambda g: eh.dk_lines(g["event_id"]), games))
    for g, l in zip(games, lines):
        g.update(l)
    df = pd.DataFrame(games).drop_duplicates("event_id")
    out = os.path.join("data", "history", f"espn_{s:%Y%m%d}_{e:%Y%m%d}.csv")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    df.to_csv(out, index=False)
    print(f"Saved {len(df)} -> {out}; with close spread: {df.get('close_home_spread', pd.Series()).notna().sum()}")

if __name__ == "__main__":
    main(date.fromisoformat(sys.argv[1]), date.fromisoformat(sys.argv[2]), int(sys.argv[3]) if len(sys.argv) > 3 else 6)
