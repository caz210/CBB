"""Re-grade walk-forward OOS games with Christian's original edge metric:
    edge% = |CZarp spread - Vegas spread| / Vegas total
Usage: python edge_regrade.py [data/history/v1_preds.pkl]   (v1 preds: python walkforward_v1.py)
"""
import sys
import numpy as np
import pandas as pd

TH = [0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.10]


def graded(df, pred, when):
    """One row per game with a side, |gap| pts, edge%, ATS result, -110 units, CLV."""
    L, T = df[f"{when}_home_spread"], df[f"{when}_total"]
    ok = L.notna() & T.notna() & (T > 0) & df[pred].notna()
    t = df[ok].copy()
    L, T = L[ok], T[ok]
    gap = t[pred] + L                       # model home margin - market home margin (-L)
    t = t[gap != 0]; L, T, gap = L[gap != 0], T[gap != 0], gap[gap != 0]
    home = gap > 0
    ats = np.where(home, t.margin + L, -(t.margin + L))
    t["gap"], t["edge"] = gap.abs(), gap.abs() / T
    t["win"] = np.where(ats == 0, np.nan, (ats > 0).astype(float))
    t["units"] = np.where(ats == 0, 0.0, np.where(ats > 0, 100 / 110, -1.0))
    if when == "open":
        cl = t["close_home_spread"]
        t["clv"] = np.where(home, L - cl, cl - L)
    else:
        t["clv"] = np.nan
    return t


def summ(g):
    n = len(g)
    return {"n": n, "ATS%": g.win.mean() * 100 if n else np.nan,
            "ROI%": g.units.sum() / n * 100 if n else np.nan,
            "CLV": g.clv.mean() if n else np.nan}


def table(t):
    rows = []
    for th in TH:
        g = t[t.edge >= th]
        r = {"thr": f">={th:.0%}"}
        for lab, sub in (("all", g), ("24-25", g[g.season == 2025]), ("25-26", g[g.season == 2026])):
            s = summ(sub)
            r.update({f"{lab} n": s["n"], f"{lab} ATS%": s["ATS%"], f"{lab} ROI%": s["ROI%"]})
        r["CLV"] = summ(g)["CLV"]
        # same bet count ranked by raw point gap instead of gap/total
        k = len(g)
        raw = t.nlargest(k, "gap") if k else t.iloc[:0]
        s = summ(raw)
        r.update({"rawgap>=": raw.gap.min() if k else np.nan, "raw ATS%": s["ATS%"], "raw ROI%": s["ROI%"],
                  "raw CLV": s["CLV"], "overlap%": len(set(g.index) & set(raw.index)) / k * 100 if k else np.nan})
        rows.append(r)
    return pd.DataFrame(rows)


def main():
    oos = pd.read_pickle("outputs/walkforward/oos.pkl")
    if len(sys.argv) > 1:
        v1 = pd.read_pickle(sys.argv[1]).drop_duplicates("event_id")
        oos = oos.merge(v1, on="event_id", how="left")
    preds = [("v2 model-only", "model")] + ([("v1 (main)", "v1_home_margin")] if "v1_home_margin" in oos else [])
    pd.set_option("display.width", 250); pd.set_option("display.max_columns", 40)
    base = oos.dropna(subset=[c for _, c in preds])
    print(f"games: {len(base)} (OOS {base.date.min()}..{base.date.max()}; same games for all models)")
    for name, col in preds:
        rmse = np.sqrt(((base[col] - base.margin) ** 2).mean())
        print(f"\n######## {name}   margin RMSE {rmse:.3f}")
        for when in ("open", "close"):
            t = graded(base, col, when)
            print(f"\n== vs {when.upper()} line ({len(t)} games with a side; edge = |gap|/{when} total) ==")
            print(table(t).round(2).to_string(index=False))
            # logistic-free check: does edge% rank covers better than raw gap? (Spearman of win vs each)
            w = t.dropna(subset=["win"])
            print(f"  corr(win, edge%) {w.win.corr(w.edge.rank()):+.4f}   "
                  f"corr(win, |gap|) {w.win.corr(w.gap.rank()):+.4f}   "
                  f"corr(edge%,|gap|) {w.edge.rank().corr(w.gap.rank()):.3f}")


if __name__ == "__main__":
    main()
