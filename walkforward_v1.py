"""Point-in-time projections from the ORIGINAL v1 formula (main:model.py), using the
same per-date inputs as walkforward.py build. Writes data/history/v1_preds.pkl.
    python walkforward_v1.py && python edge_regrade.py data/history/v1_preds.pkl
"""
import os, subprocess, sys, tempfile
import pandas as pd
_tmp = tempfile.mkdtemp()
with open(os.path.join(_tmp, "model_v1.py"), "w") as fh:
    fh.write(subprocess.check_output(["git", "show", "main:model.py"], text=True))
sys.path.insert(0, _tmp)
import model_v1 as V1
import walkforward as W
from backtest import NameMatcher
from czarp import teamstats as T
from czarp.config import EarlySeasonConfig

def build(S):
    games = W.season_games(S).dropna(subset=["home_score", "away_score"])
    box = W.box_rows(S)
    prior = {k: W.season_table(S - 1, k) for k in ("four_factors", "height", "misc", "ratings")}
    prior_tt = T._raw_table(prior); prior_tt["Opp_OR"] = prior_tt["D_OR_raw"]
    cur_height = W.season_table(S, "height")
    early = EarlySeasonConfig()
    match = NameMatcher(W.archive(W.SEASONS[S][1])["TeamName"].astype(str))
    box["kp"] = box["team"].map(match)
    rows, fails = [], 0
    for d, day in games.groupby("date"):
        r = W.archive(d)
        if r is None: continue
        tt = W.team_table_for_date(d, r, box, prior_tt, cur_height, early)
        tt = tt.reset_index().rename(columns={"index": "TeamName"})
        data = {
            "ratings": tt[["TeamName", "AdjOE", "AdjDE", "AdjTempo", "RankAdjEM"]],
            "four_factors": tt[["TeamName"]].assign(TO_Pct=tt["TO"], DTO_Pct=tt["D_TO"], OR_Pct=tt["OR"],
                                                    DOR_Pct=tt["Opp_OR"], FT_Rate=tt["FTR"], DFT_Rate=tt["D_FTR"]),
            "height": tt[["TeamName", "AvgHgt", "Exp", "Bench"]],
            "net": pd.DataFrame(columns=["TeamName", "Rank"]), "misc": pd.DataFrame(),
        }
        for _, g in day.iterrows():
            h, a = match(g["home"]), match(g["away"])
            if not h or not a: continue
            try:
                res = V1.project_game(a, h, None if g.neutral else False, data)   # team1 = away
            except Exception:
                fails += 1; continue
            rows.append({"event_id": g.event_id, "v1_home_margin": -res["spread"], "v1_total": res["total"]})
    print(S, len(rows), "fails", fails, flush=True)
    return pd.DataFrame(rows)

out = pd.concat([build(2025), build(2026)], ignore_index=True)
out.to_pickle("data/history/v1_preds.pkl")
print(len(out), out.v1_home_margin.describe().round(2).to_dict())
