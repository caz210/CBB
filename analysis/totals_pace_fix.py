"""totals_pace_fix.py — does an early-season / market pace fix help the v1 totals rule?

Walk-forward by season (fit on one season, test on the other, both directions).
Rule = live totals rule: v1 total vs DK OPEN total, edge% >= 3%, skip if the total moved >= 3 pts
against our side (open->close in history), NEW COACH lock 4 games.

Options (all applied to the v1 total T1 with v1 additive pace P1, PPP held fixed: T' = T1 * P'/P1):
  base      : as-is
  pace_bias : P' = P1 + b·[Nov] + c·[either team < 8 games] + d·[neutral]   (OLS on train-season possessions)
  pts_bias  : T' = T1 + same indicators fit on train-season (actual total − T1)
  mkt_w     : P' = (1−w)·P1 + w·(open_total ÷ PPP)  ==  T' = (1−w)·T1 + w·open_total   (OPEN only, no leakage)
  combo     : pace_bias then mkt_w
    python analysis/totals_pace_fix.py  -> outputs/walkforward/totals_pace_fix.txt
"""
import os, sys
import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from coach_lock_study import build as cl_build, totals as cl_totals
from pace_audit import archive, load_box

THR, MOVE, LOCK = 0.03, 3.0, 4
W_GRID = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5]


def frame():
    f, _ = cl_build()
    ph, pa, av = [], [], []
    for d, h, a in zip(f.date, f.home, f.away):
        ar = archive(d)
        if ar is None:
            ph.append(np.nan); pa.append(np.nan); av.append(np.nan)
        else:
            ph.append(ar[0].get(h, np.nan)); pa.append(ar[0].get(a, np.nan)); av.append(ar[1])
    f["pace_v1"] = np.array(ph) + np.array(pa) - np.array(av)
    box = load_box(f.event_id)
    f = f.merge(box[["event_id", "tempo"]], on="event_id", how="left")
    f.loc[(f.tempo < 45) | (f.tempo > 100), "tempo"] = np.nan
    f["nov"] = (f.date.str[5:7] == "11").astype(float)
    f["early"] = (f[["games_h", "games_a"]].min(axis=1) < 8).astype(float)
    f["neu"] = f.neutral.astype(float)
    f["pace_adj"] = f.pace_v1
    return f[f.pace_v1.notna() & f.v1_total.notna() & f.open_total.notna() & f.close_total.notna()].copy()


X = ["nov", "early", "neu"]


def fit_bias(tr, y):
    A = tr[X].values
    m = np.isfinite(y)
    coef, *_ = np.linalg.lstsq(A[m], y[m], rcond=None)   # no intercept: Dec-Mar non-neutral stays as-is
    return dict(zip(X, coef))


def apply(f, pace_b=None, pts_b=None, w=0.0):
    g = f.copy()
    P = g.pace_v1.copy()
    if pace_b:
        P = P + sum(pace_b[k] * g[k] for k in X)
    T = g.v1_total * P / g.pace_v1
    if pts_b:
        T = T + sum(pts_b[k] * g[k] for k in X)
    T = (1 - w) * T + w * g.open_total
    g["v1_total"] = T
    g["pace_adj"] = P if w == 0 else (1 - w) * P + w * g.open_total / (T / P)
    return g


def rule(g):
    t = cl_totals(g)
    over = t.v1_total > t.open_total
    t["side"] = np.where(over, "O", "U")
    t["mv_against"] = np.where(over, t.open_total - t.close_total, t.close_total - t.open_total)
    lock = t.nc_games.fillna(np.inf) < LOCK
    return t[(t.edge_open >= THR) & (t.mv_against < MOVE) & ~lock]


def summ(p, g):
    w = p.win.dropna()
    n = len(p)
    o = (p.side == "O").sum()
    pm = (g.tempo - g.pace_adj).abs().mean()
    if n == 0:
        return f"n=   0 | poss MAE {pm:.2f}"
    return (f"n={n:4d} {w.mean()*100:5.1f}% ROI {p.units.mean()*100:+5.1f}%  O/U {o}/{n-o} "
            f"(O {p[p.side=='O'].win.mean()*100:4.1f}% U {p[p.side=='U'].win.mean()*100:4.1f}%) | poss MAE {pm:.2f}")


def main():
    f = frame()
    out = []
    P = lambda s: out.append(s)
    P(f"games {len(f)} (2024-25 + 2025-26), with box tempo {f.tempo.notna().sum()}")
    # 1) bias of v1 totals
    P("\n######## BIAS of v1 total (positive = actual/close HIGHER than model, i.e. model leans Under)")
    seg = {"all": f.index == f.index, "November": f.nov == 1, "either team < 8 games": f.early == 1,
           "neutral": f.neu == 1, "Dec-Apr, >=8 games, true home": (f.nov == 0) & (f.early == 0) & (f.neu == 0)}
    for k, m in seg.items():
        s = f[m]
        P(f"  {k:32s} n={len(s):5d}  actual−model {np.mean(s.total - s.v1_total):+5.2f}  close−model "
          f"{np.mean(s.close_total - s.v1_total):+5.2f}  open−model {np.mean(s.open_total - s.v1_total):+5.2f}  "
          f"actual−close {np.mean(s.total - s.close_total):+5.2f}  poss actual−v1 pace {np.nanmean(s.tempo - s.pace_v1):+5.2f}")
    base = rule(f)
    P("\n  current rule picks: " + summ(base, f))
    for k, m in seg.items():
        b = base[m.reindex(base.index, fill_value=False)] if False else base[base.index.isin(f.index[m])]
        P(f"    {k:30s} " + summ(b, f[m]))
    # 2) walk-forward
    P("\n######## WALK-FORWARD (fit on train season, test on the other)")
    res = {}
    for tr_s, te_s in ((2025, 2026), (2026, 2025)):
        tr, te = f[f.season == tr_s], f[f.season == te_s]
        pb = fit_bias(tr, (tr.tempo - tr.pace_v1).values)
        tb = fit_bias(tr, (tr.total - tr.v1_total).values)
        # choose w on train by ROI (n >= 200)
        def best_w(pace_b):
            sc = []
            for w in W_GRID:
                p = rule(apply(tr, pace_b=pace_b, w=w))
                sc.append((p.units.mean() if len(p) >= 200 else -9, w))
            return max(sc)[1]
        w_m, w_c = best_w(None), best_w(pb)
        P(f"\n  train {tr_s} -> test {te_s}: pace bias {', '.join(f'{k} {v:+.2f}' for k, v in pb.items())} poss; "
          f"pts bias {', '.join(f'{k} {v:+.2f}' for k, v in tb.items())}; w(mkt) {w_m}, w(combo) {w_c}")
        opts = {"base": apply(te), "pace_bias": apply(te, pace_b=pb), "pts_bias": apply(te, pts_b=tb),
                f"mkt_w={w_m}": apply(te, w=w_m), f"combo w={w_c}": apply(te, pace_b=pb, w=w_c)}
        for name, g in opts.items():
            p = rule(g)
            res.setdefault(name.split("=")[0].split(" ")[0], []).append(p)
            P(f"    {name:14s} " + summ(p, g))
        P("    fixed market weights (for reference):")
        for w in W_GRID[1:]:
            g = apply(te, w=w)
            P(f"      w={w:.1f}       " + summ(rule(g), g))
    P("\n######## POOLED OUT-OF-SAMPLE (both test seasons)")
    for k, ps in res.items():
        p = pd.concat(ps)
        P(f"  {k:10s} " + summ(p, f).split(' | ')[0])
    txt = "\n".join(out)
    print(txt)
    os.makedirs("outputs/walkforward", exist_ok=True)
    open("outputs/walkforward/totals_pace_fix.txt", "w").write(txt + "\n")


if __name__ == "__main__":
    main()
