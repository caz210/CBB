"""coach_lock_study.py — backtest the 'NEW COACH' lock (and the >=3-pt line-move skips).

New-coach teams = KenPom head coach this season != last season (data/history/season*/ratings.csv,
or data/coaches/teams_<season>.csv), plus data/coach_changes.csv if present.
Games: ESPN/DK history 2024-25 + 2025-26 (data/history/features_*.pkl, point-in-time KenPom
ratings, games_h/games_a = games each team had played before tip). Sides model = the live v2
formula (1.137 x efficiency margin + 1.109 x HCA + unit score, fitted coefficients, so a mild
look-ahead in the coefficients only); totals = original v1 (data/history/v1_preds.pkl).
Bets graded at the OPEN line, -110 (that is where the backtested edge lives).

    python coach_lock_study.py   -> outputs/walkforward/coach_lock_study.txt
"""
import json
import os

import numpy as np
import pandas as pd

from czarp.coach import coach_changes_between
from walkforward import clean_lines

NS = (0, 4, 6, 8, 10, 12)
SIDE_THR, TOT_THR, MOVE = 0.04, 0.03, 3.0


def load_coaches(season):
    for p in (f"data/history/season{season}/ratings.csv", f"data/coaches/teams_{season}.csv"):
        if os.path.exists(p):
            d = pd.read_csv(p)
            if "Coach" in d:
                return d[["TeamName", "Coach"]]
    return None


def new_coach_sets():
    out = {}
    for s in (2025, 2026):
        cur, prev = load_coaches(s), load_coaches(s - 1)
        if cur is None or prev is None:
            prev = pd.read_csv(f"data/coaches/teams_{s - 1}.csv")[["TeamName", "Coach"]]
        out[s] = coach_changes_between(cur, prev)
    if os.path.exists("data/coach_changes.csv"):
        x = pd.read_csv("data/coach_changes.csv")
        for s, g in x.groupby("season"):
            out.setdefault(int(s), set()).update(g.team.astype(str))
    return out


def rec(g):
    n = len(g)
    if not n:
        return "n=   0"
    w = g.win.dropna()
    return (f"n={n:4d}  {w.mean()*100:5.1f}% ({int(w.sum())}-{int(len(w)-w.sum())})  "
            f"ROI {g.units.mean()*100:+6.1f}%  CLV {g.clv.mean():+.2f}")


def grade_cols(t, res):
    t["win"] = np.where(res == 0, np.nan, (res > 0).astype(float))
    t["units"] = np.where(res == 0, 0.0, np.where(res > 0, 100 / 110, -1.0))
    return t


def build():
    f = pd.concat([pd.read_pickle(f"data/history/features_{y}.pkl") for y in (2025, 2026)], ignore_index=True)
    f["event_id"] = f.event_id.astype(str)
    v1 = pd.read_pickle("data/history/v1_preds.pkl").drop_duplicates("event_id")
    v1["event_id"] = v1.event_id.astype(str)
    f = clean_lines(f.merge(v1, on="event_id", how="left"))
    c = json.load(open("outputs/walkforward/fit.json"))["coef"]
    f["model"] = (f.base_margin * c["base_margin"] + f.hca.fillna(0) * c["hca"] + f.hgt.fillna(0) * c["hgt"]
                  + f.exp.fillna(0) * c["exp"] + f.bench.fillna(0) * c["bench"])
    nc = new_coach_sets()
    f["nc_h"] = [h in nc.get(s, set()) for h, s in zip(f.home, f.season)]
    f["nc_a"] = [a in nc.get(s, set()) for a, s in zip(f.away, f.season)]
    # fewest games played by a new-coach team in the game (inf if none)
    f["nc_games"] = np.fmin(np.where(f.nc_h, f.games_h, np.inf), np.where(f.nc_a, f.games_a, np.inf))
    return f, nc


def sides(f):
    t = f[f.open_home_spread.notna() & f.open_total.notna() & f.close_home_spread.notna()].copy()
    gap = t.model + t.open_home_spread
    t = t[gap != 0]; gap = gap[gap != 0]
    home = gap > 0
    t["edge_open"] = gap.abs() / t.open_total
    gc = t.model + t.close_home_spread
    t["edge_close"] = gc.abs() / t.close_total.fillna(t.open_total)
    t["same_side"] = (gc > 0) == home
    res = np.where(home, t.margin + t.open_home_spread, -(t.margin + t.open_home_spread))
    t["clv"] = np.where(home, t.open_home_spread - t.close_home_spread, t.close_home_spread - t.open_home_spread)
    t["mae_model"] = (t.margin - t.model).abs()
    t["mae_mkt"] = (t.margin + t.open_home_spread).abs()
    return grade_cols(t, res)


def totals(f):
    t = f[f.open_total.notna() & f.close_total.notna() & f.v1_total.notna()].copy()
    gap = t.v1_total - t.open_total
    t = t[gap != 0]; gap = gap[gap != 0]
    over = gap > 0
    t["edge_open"] = gap.abs() / t.open_total
    gc = t.v1_total - t.close_total
    t["edge_close"] = gc.abs() / t.close_total
    t["same_side"] = (gc > 0) == over
    res = np.where(over, t.total - t.open_total, t.open_total - t.total)
    t["clv"] = np.where(over, t.close_total - t.open_total, t.open_total - t.close_total)
    t["mae_model"] = (t.total - t.v1_total).abs()
    t["mae_mkt"] = (t.total - t.open_total).abs()
    return grade_cols(t, res)


def lock_table(out, picks, title):
    out.append(f"\n-- {title} --")
    out.append(f"  all picks                                {rec(picks)}")
    nc = picks[np.isfinite(picks.nc_games)]
    out.append(f"  picks involving a new-coach team         {rec(nc)}")
    for lo, hi in ((0, 4), (4, 6), (6, 8), (8, 10), (10, 12), (12, 99)):
        g = nc[(nc.nc_games >= lo) & (nc.nc_games < hi)]
        out.append(f"    new-coach team had played {lo:2d}-{min(hi, 99) - 1:2d} games  {rec(g)}")
    out.append("  N (lock until N games) -> kept picks | excluded picks")
    for n in NS:
        ex = picks.nc_games < n
        out.append(f"    N={n:2d}  kept {rec(picks[~ex])} | excluded {rec(picks[ex])}")


def mae_table(out, t, label):
    out.append(f"\n-- {label}: model vs market accuracy (MAE, all games) by new-coach games played --")
    out.append("  (model MAE − open-line MAE; > 0 means the model was worse than the market)")
    base = t[~np.isfinite(t.nc_games)]
    for lo, hi in ((0, 4), (4, 6), (6, 8), (8, 10), (10, 12), (12, 99)):
        g = t[(t.nc_games >= lo) & (t.nc_games < hi)]
        b = base[(base[["games_h", "games_a"]].min(axis=1) >= lo) & (base[["games_h", "games_a"]].min(axis=1) < hi)]
        out.append(f"  games {lo:2d}-{min(hi, 99) - 1:2d}: new-coach n={len(g):4d} gap {(g.mae_model - g.mae_mkt).mean():+.2f}"
                   f"  | other games n={len(b):5d} gap {(b.mae_model - b.mae_mkt).mean():+.2f}")


def move_table(out, picks, title):
    mv = -picks.clv          # >0 = line moved AGAINST our side (pts)
    out.append(f"\n-- {title}: line move open->close vs our side --")
    for lab, m in ((f"moved >= {MOVE:g} AGAINST us", mv >= MOVE), ("moved 2-2.5 against", (mv >= 2) & (mv < MOVE)),
                   ("moved < 2 either way", mv.abs() < 2), (f"moved >= 2 WITH us", mv <= -2)):
        out.append(f"  {lab:<26} {rec(picks[m])}")
    out.append(f"  rule with skip (drop >= {MOVE:g} against) {rec(picks[~(mv >= MOVE)])}")


def main():
    f, nc = build()
    out = [f"Games: {len(f)} ({f.date.min()}..{f.date.max()}); new-coach teams: "
           + ", ".join(f"{s}: {len(v)}" for s, v in sorted(nc.items()))]
    s, t = sides(f), totals(f)
    sa = s[s.edge_open >= SIDE_THR]
    sb = sa[(sa.edge_close >= SIDE_THR) & sa.same_side]
    ta = t[t.edge_open >= TOT_THR]
    tb = ta[(ta.edge_close >= TOT_THR) & ta.same_side]
    out.append("\n######## NEW-COACH LOCK")
    mae_table(out, s, "SIDES margin")
    mae_table(out, t, "TOTALS")
    lock_table(out, sa, f"SIDES edge>={SIDE_THR:.0%} vs open (rule A)")
    lock_table(out, sb, "SIDES vs open AND close, same side (rule B, live)")
    lock_table(out, ta, f"TOTALS edge>={TOT_THR:.0%} vs open (rule A)")
    lock_table(out, tb, "TOTALS vs open AND close, same side (rule B, live)")
    for season in (2025, 2026):
        out.append(f"\n  season {season} only, totals rule A:")
        x = ta[ta.season == season]
        for n in NS:
            ex = x.nc_games < n
            out.append(f"    N={n:2d}  kept {rec(x[~ex])} | excluded {rec(x[ex])}")
    out.append(f"\n######## LINE-MOVE SKIP (>= {MOVE:g} pts against our side; 'current' = close in history)")
    move_table(out, sa, "SIDES rule A")
    move_table(out, sb, "SIDES rule B")
    move_table(out, ta, "TOTALS rule A")
    move_table(out, tb, "TOTALS rule B")
    out.append("\n######## PROPOSAL: totals without the 'edge vs current' check (rule A) + skip moves against us")
    for season in (2025, 2026, None):
        x = ta if season is None else ta[ta.season == season]
        xb = x[(x.edge_close >= TOT_THR) & x.same_side]
        m = -x.clv
        out.append(f"  {season or 'both'}: A {rec(x)}\n        B {rec(xb)}\n        A, skip >=3 against {rec(x[~(m >= 3)])}"
                   f"\n        A, skip >=2 against {rec(x[~(m >= 2)])}")
    txt = "\n".join(out)
    print(txt)
    os.makedirs("outputs/walkforward", exist_ok=True)
    with open("outputs/walkforward/coach_lock_study.txt", "w") as fh:
        fh.write(txt + "\n")


if __name__ == "__main__":
    main()
