"""line_move_study.py — does the 'likely news/injury' line-move flag help our edge picks?

Flag: the DK spread moved >= 2 pts from open to close (ESPN open/close history).
Graded on the walk-forward OOS games (outputs/walkforward/oos.pkl, no look-ahead in the model),
-110 units, bets placed at the OPENING line (that is where the backtest edge lives).

    python line_move_study.py [data/history/v1_preds.pkl]   -> outputs/walkforward/line_move_study.txt
"""
import glob
import sys

import numpy as np
import pandas as pd

from walkforward import clean_lines

MOVE = 2.0
SIDE_THR, TOT_THR = 0.04, 0.03


def rec(g):
    n = len(g)
    if not n:
        return "n=0"
    w = g.win.dropna()
    return (f"n={n:4d}  hit {w.mean()*100:5.1f}% ({int(w.sum())}-{int(len(w)-w.sum())})  "
            f"ROI {g.units.mean()*100:+6.1f}%  CLV {g.clv.mean():+.2f}")


def sides(df, col):
    L, T, C = df.open_home_spread, df.open_total, df.close_home_spread
    t = df[L.notna() & T.notna() & C.notna() & df[col].notna()].copy()
    gap = t[col] + t.open_home_spread
    t = t[gap != 0]; gap = gap[gap != 0]
    home = gap > 0
    ats = np.where(home, t.margin + t.open_home_spread, -(t.margin + t.open_home_spread))
    t["edge_open"] = gap.abs() / t.open_total
    gc = t[col] + t.close_home_spread
    t["edge_close"] = gc.abs() / t.close_total.fillna(t.open_total)
    t["same_side"] = (gc > 0) == home
    t["win"] = np.where(ats == 0, np.nan, (ats > 0).astype(float))
    t["units"] = np.where(ats == 0, 0.0, np.where(ats > 0, 100 / 110, -1.0))
    t["clv"] = np.where(home, t.open_home_spread - t.close_home_spread, t.close_home_spread - t.open_home_spread)
    t["move"] = (t.close_home_spread - t.open_home_spread).abs()
    t["flag"] = t.move >= MOVE
    return t


def totals(df, col):
    t = df[df.open_total.notna() & df.close_total.notna() & df[col].notna()].copy()
    gap = t[col] - t.open_total
    t = t[gap != 0]; gap = gap[gap != 0]
    over = gap > 0
    d = np.where(over, t.total - t.open_total, t.open_total - t.total)
    t["edge_open"] = gap.abs() / t.open_total
    gc = t[col] - t.close_total
    t["edge_close"] = gc.abs() / t.close_total
    t["same_side"] = (gc > 0) == over
    t["win"] = np.where(d == 0, np.nan, (d > 0).astype(float))
    t["units"] = np.where(d == 0, 0.0, np.where(d > 0, 100 / 110, -1.0))
    t["clv"] = np.where(over, t.close_total - t.open_total, t.open_total - t.close_total)
    t["smove"] = (t.close_home_spread - t.open_home_spread).abs()
    t["flag"] = t.smove >= MOVE
    t["tmove"] = (t.close_total - t.open_total).abs()
    return t


def block(out, title, picks, thr):
    out.append(f"\n-- {title} --")
    out.append(f"  all picks            {rec(picks)}")
    out.append(f"  unflagged (<{MOVE:g} pt)   {rec(picks[~picks.flag])}")
    out.append(f"  flagged  (>={MOVE:g} pt)   {rec(picks[picks.flag])}")
    f = picks[picks.flag]
    out.append(f"    flagged, moved WITH us   {rec(f[f.clv > 0])}")
    out.append(f"    flagged, moved AGAINST us {rec(f[f.clv < 0])}")


def main():
    out = []
    # 1) how often do >= 2 pt spread moves happen? (all ESPN games with open+close, both seasons)
    hist = pd.concat([pd.read_csv(p) for p in sorted(glob.glob("data/history/espn_2*_2*.csv"))
                      if not p.endswith("20260115_20260115.csv")], ignore_index=True).drop_duplicates("event_id")
    h = clean_lines(hist)
    h = h[h.open_home_spread.notna() & h.close_home_spread.notna()]
    mv = (h.close_home_spread - h.open_home_spread).abs()
    tmv = (h.close_total - h.open_total).abs()
    out.append(f"ESPN/DK history: {len(h)} games with open+close spread ({hist.date.min()}..{hist.date.max()})")
    for k in (1, 1.5, 2, 3, 4):
        out.append(f"  spread move >= {k:g} pts: {(mv >= k).mean()*100:5.1f}%  ({int((mv >= k).sum())})")
    out.append(f"  total move >= 2 pts: {(tmv >= 2).mean()*100:.1f}%;  >= 4 pts: {(tmv >= 4).mean()*100:.1f}%")
    out.append(f"  spread >=2 AND total moved < 1 pt: {((mv >= 2) & (tmv < 1)).mean()*100:.1f}%")
    nov = h.date.str[5:7] == "11"
    out.append(f"  >=2 pt spread moves in November: {(mv[nov] >= 2).mean()*100:.1f}% vs Dec-Apr {(mv[~nov] >= 2).mean()*100:.1f}%")

    oos = pd.read_pickle("outputs/walkforward/oos.pkl")
    if len(sys.argv) > 1:
        oos = oos.merge(pd.read_pickle(sys.argv[1]).drop_duplicates("event_id"), on="event_id", how="left")
    oos = clean_lines(oos)
    out.append(f"\nWalk-forward OOS games: {len(oos)} ({oos.date.min()}..{oos.date.max()}); bets graded at the OPEN line, -110")

    s = sides(oos, "model")
    out.append(f"\n######## SIDES (v2 model-only), edge% = |model - open spread| / open total >= {SIDE_THR:.0%}")
    out.append(f"  share of all OOS side candidates flagged: {s.flag.mean()*100:.1f}%")
    po = s[s.edge_open >= SIDE_THR]
    block(out, "rule A: edge vs OPEN only", po, SIDE_THR)
    live = po[(po.edge_close >= SIDE_THR) & po.same_side]
    block(out, "rule B (live rule): edge vs open AND vs close/current, same side", live, SIDE_THR)
    for th in (0.03, 0.05):
        g = s[s.edge_open >= th]
        out.append(f"  [sensitivity edge>={th:.0%} vs open] unflagged {rec(g[~g.flag])} | flagged {rec(g[g.flag])}")

    if "v1_total" in oos:
        t = totals(oos, "v1_total")
        out.append(f"\n######## TOTALS (v1), edge% = |model - open total| / open total >= {TOT_THR:.0%}; flag = SPREAD moved >= {MOVE:g}")
        pt = t[t.edge_open >= TOT_THR]
        block(out, "rule A: edge vs OPEN only", pt, TOT_THR)
        lt = pt[(pt.edge_close >= TOT_THR) & pt.same_side]
        block(out, "rule B (live rule): vs open AND vs close, same side", lt, TOT_THR)
        out.append(f"  [total itself moved >= 4 pts] rule B: {rec(lt[lt.tmove >= 4])} | <4: {rec(lt[lt.tmove < 4])}")
    txt = "\n".join(out)
    print(txt)
    with open("outputs/walkforward/line_move_study.txt", "w") as fh:
        fh.write(txt + "\n")


if __name__ == "__main__":
    main()
