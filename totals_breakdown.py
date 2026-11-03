"""Totals breakdown (overs vs unders) on the walk-forward OOS games, v1 and v2 model-only.
    python totals_breakdown.py [data/history/v1_preds.pkl]
"""
import sys
import numpy as np
import pandas as pd
from walkforward import norm_cdf_v, BE

PCT = [0.02, 0.03, 0.04, 0.05, 0.07, 0.10]
PROB = [0.03, 0.05, 0.08]


def graded(df, pred, sd, when):
    T = df[f"{when}_total"]
    t = df[T.notna() & (T > 0) & df[pred].notna()].copy()
    T = t[f"{when}_total"]
    gap = t[pred] - T
    t = t[gap != 0]; T = T[gap != 0]; gap = gap[gap != 0]
    over = gap > 0
    sdv = sd.reindex(t.index) if isinstance(sd, pd.Series) else sd
    p_over = 1 - norm_cdf_v((T - t[pred]) / sdv)
    d = np.where(over, t.total - T, T - t.total)
    t["side"] = np.where(over, "over", "under")
    t["pct"] = gap.abs() / T
    t["pedge"] = np.where(over, p_over, 1 - p_over) - BE
    t["win"] = np.where(d == 0, np.nan, (d > 0).astype(float))
    t["units"] = np.where(d == 0, 0.0, np.where(d > 0, 100 / 110, -1.0))
    t["clv"] = np.where(over, t.close_total - T, T - t.close_total) if when == "open" else np.nan
    return t


def s(g):
    n = len(g)
    if not n:
        return "0"
    c = "" if g.clv.isna().all() else f" clv{g.clv.mean():+.2f}"
    return f"{n} {g.win.mean()*100:.1f}% {g.units.mean()*100:+.1f}%{c}"


def block(t, col, cuts, lab):
    rows = []
    for th in cuts:
        g = t[t[col] >= th]
        r = {lab: f">={th:.0%}"}
        for side in ("over", "under"):
            gs = g[g.side == side]
            r[f"{side} all"] = s(gs)
            r[f"{side} 24-25"] = s(gs[gs.season == 2025])
            r[f"{side} 25-26"] = s(gs[gs.season == 2026])
        rows.append(r)
    return pd.DataFrame(rows)


def main():
    oos = pd.read_pickle("outputs/walkforward/oos.pkl")
    if len(sys.argv) > 1:
        oos = oos.merge(pd.read_pickle(sys.argv[1]).drop_duplicates("event_id"), on="event_id", how="left")
    preds = [("v2 model-only", "model_total")] + ([("v1 (main)", "v1_total")] if "v1_total" in oos else [])
    base = oos.dropna(subset=[c for _, c in preds])
    pd.set_option("display.width", 300); pd.set_option("display.max_columns", 20)
    print(f"games {len(base)}  {base.date.min()}..{base.date.max()}   cells: n hit% ROI@-110 [CLV pts vs open]")
    print(f"actual mean total {base.total.mean():.2f} | open line mean {base.open_total.mean():.2f} | close line mean {base.close_total.mean():.2f}")
    print(f"overs hit vs open {(base.total > base.open_total).mean()*100:.1f}% / vs close {(base.total > base.close_total).mean()*100:.1f}% (pushes counted as not-over)")
    for name, col in preds:
        sd = (base.total - base[col]).std() if col == "v1_total" else base.sd_total
        bias = (base[col] - base.total).mean()
        print(f"\n######## {name}: mean(model - actual) {bias:+.2f}, mean(model - close) {(base[col]-base.close_total).mean():+.2f}, "
              f"RMSE {np.sqrt(((base[col]-base.total)**2).mean()):.2f}")
        for when in ("open", "close"):
            t = graded(base, col, sd, when)
            print(f"\n== vs {when.upper()} total: over picks {(t.side=='over').mean()*100:.0f}% of {len(t)} ==")
            print(block(t, "pct", PCT, "edge%").to_string(index=False))
            print(block(t, "pedge", PROB, "prob edge").to_string(index=False))


if __name__ == "__main__":
    main()
