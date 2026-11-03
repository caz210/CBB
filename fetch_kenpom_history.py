"""Pull KenPom history used by walkforward.py (season tables, daily archive
ratings, daily fanmatch) into data/history/. Needs KENPOM_API_KEY.
    python fetch_kenpom_history.py
"""
import os, pandas as pd
from datetime import date, timedelta
import kenpom_fetcher as kf
from backtest import archive_ratings
os.makedirs("data/history/kp_fanmatch", exist_ok=True)
for y in (2024, 2025, 2026):
    d = f"data/history/season{y}"; os.makedirs(d, exist_ok=True)
    for name, fn in [("four_factors", kf.fetch_four_factors), ("height", kf.fetch_height), ("misc", kf.fetch_misc), ("ratings", kf.fetch_ratings)]:
        p = f"{d}/{name}.csv"
        if not os.path.exists(p):
            fn(y).to_csv(p, index=False)
    print("season", y, "tables ok", flush=True)
for s, e in [(date(2025,11,3), date(2026,4,6)), (date(2024,11,4), date(2025,4,7))]:
    d = s
    while d <= e:
        ds = d.isoformat()
        try:
            archive_ratings(ds)
            p = f"data/history/kp_fanmatch/{ds}.csv"
            if not os.path.exists(p):
                pd.DataFrame(kf._get("fanmatch", {"d": ds})).to_csv(p, index=False)
        except Exception as ex:
            print(ds, "ERR", ex, flush=True)
        d += timedelta(1)
    print("done", s, flush=True)
