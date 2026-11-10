"""
walkforward.py — point-in-time feature build, walk-forward fit and out-of-sample
evaluation for CZarp.

No look-ahead, except where noted:
  * efficiency / tempo     KenPom archive ratings as of each game date
  * KenPom prediction      KenPom fanmatch (HomePred - VisitorPred) for that date
  * style stats            to-date box score aggregates from ESPN (eFG%, 3PA rate,
                           FT rate, FT%, opponent eFG%/3PA rate/FT rate), blended
                           with last season's KenPom values regressed toward the
                           mean (EarlySeasonConfig "regressed" logic). TO% / OR% and
                           their defensive versions are NOT in ESPN scoreboards, so
                           they come from last season (regressed) all year.
  * unit score inputs      KenPom season height table (AvgHgt/Exp/Bench) - roster
                           based but minutes-weighted over the season: mild look-ahead.
  * per-team HCA           current KenPom hca.php (multi-season estimate): mild look-ahead.
  * market                 DraftKings open/close spread, total, ML via ESPN.

    python walkforward.py build 2025 2026        # features -> data/history/features_<S>.pkl
    python walkforward.py fit                    # walk-forward fit + report -> outputs/walkforward/
"""
from __future__ import annotations

import glob
import json
import os
import sys
from datetime import date, timedelta

import numpy as np
import pandas as pd

from backtest import NameMatcher
from czarp import teamstats as T
from czarp.config import ClutchConfig, EarlySeasonConfig, MatchupConfig
from czarp.probability import american_to_prob, devig_two_way, norm_cdf

H = "data/history"
SEASONS = {2025: ("2024-11-04", "2025-04-07"), 2026: ("2025-11-03", "2026-04-06")}
PAIRS = ["Volume 3s|Crowd 3s", "Crash|Control Glass",
         "Fast|Wall", "At the Line|Wall"]
WEAK = T.WEAKNESS_OF


# ── loaders ───────────────────────────────────────────────────────────────────
def season_games(S):
    s, e = SEASONS[S]
    p = f"{H}/espn_{s.replace('-', '')}_{e.replace('-', '')}.csv"
    return pd.read_csv(p)


def archive(d: str) -> pd.DataFrame | None:
    """Archive ratings for d, else the most recent earlier cached date."""
    dd = date.fromisoformat(d)
    for k in range(0, 6):
        p = f"{H}/kp_archive/{(dd - timedelta(k)).isoformat()}.csv"
        if os.path.exists(p):
            return pd.read_csv(p)
    return None


def fanmatch(d: str) -> pd.DataFrame:
    p = f"{H}/kp_fanmatch/{d}.csv"
    if os.path.exists(p) and os.path.getsize(p) > 5:
        try:
            return pd.read_csv(p)
        except pd.errors.EmptyDataError:
            pass
    return pd.DataFrame()


def box_rows(S) -> pd.DataFrame:
    """Per team-game box aggregates from cached ESPN scoreboards (completed games)."""
    s, e = (date.fromisoformat(x) for x in SEASONS[S])
    rows = []
    d = s
    while d <= e:
        p = f"{H}/espn_cache/sb_{d:%Y%m%d}.json"
        if os.path.exists(p):
            j = json.load(open(p))
            for ev in j.get("events", []):
                c = ev["competitions"][0]
                if not c.get("status", {}).get("type", {}).get("completed"):
                    continue
                comps = c["competitors"]
                if len(comps) != 2:
                    continue
                st = []
                for t in comps:
                    m = {x["name"]: x.get("displayValue") for x in t.get("statistics", [])}
                    st.append({"team": t["team"].get("location"), "FGA": m.get("fieldGoalsAttempted"),
                               "FGM": m.get("fieldGoalsMade"), "TPA": m.get("threePointFieldGoalsAttempted"),
                               "TPM": m.get("threePointFieldGoalsMade"), "FTA": m.get("freeThrowsAttempted"),
                               "FTM": m.get("freeThrowsMade")})
                for i in (0, 1):
                    a, b = st[i], st[1 - i]
                    r = {"date": d.isoformat(), "team": a["team"]}
                    for k in ("FGA", "FGM", "TPA", "TPM", "FTA", "FTM"):
                        r[k] = pd.to_numeric(a[k], errors="coerce")
                        r["o" + k] = pd.to_numeric(b[k], errors="coerce")
                    rows.append(r)
        d += timedelta(1)
    return pd.DataFrame(rows)


def season_table(S, name):
    p = f"{H}/season{S}/{name}.csv"
    return pd.read_csv(p) if os.path.exists(p) else pd.DataFrame()


# ── per-date team table ───────────────────────────────────────────────────────
TODATE = {  # canonical -> function of summed box stats
    "eFG": lambda x: (x.FGM + 0.5 * x.TPM) / x.FGA * 100,
    "FG3A_Rate": lambda x: x.TPA / x.FGA * 100,
    "FTR": lambda x: x.FTA / x.FGA * 100,
    "FTPct": lambda x: x.FTM / x.FTA * 100,
    "D_eFG": lambda x: (x.oFGM + 0.5 * x.oTPM) / x.oFGA * 100,
    "Opp_FG3A_Rate": lambda x: x.oTPA / x.oFGA * 100,
    "D_FTR": lambda x: x.oFTA / x.oFGA * 100,
}


def team_table_for_date(d, ratings, box, prior_tt, cur_height, early: EarlySeasonConfig):
    names = ratings["TeamName"].astype(str)
    tt = pd.DataFrame(index=names.values)
    for c in ("AdjOE", "AdjDE", "AdjTempo", "RankAdjEM"):
        tt[c] = pd.to_numeric(ratings[c], errors="coerce").values
    tt["APL_Off"] = np.nan
    past = box[box.date < d]
    agg = past.groupby("kp").sum(numeric_only=True)
    games = past.groupby("kp").size().reindex(tt.index).fillna(0)
    tt["Games"] = games
    w = (games / early.full_weight_games).clip(0, 1)
    h = cur_height.set_index("TeamName").reindex(tt.index) if not cur_height.empty else pd.DataFrame(index=tt.index)
    cont = pd.to_numeric(h.get("Continuity"), errors="coerce") if "Continuity" in h else pd.Series(np.nan, index=tt.index)
    keep = (early.prior_keep_max * cont).fillna(early.prior_keep_default)
    for c in T.STYLE_COLS + ["Opp_OR"]:
        if c in ("D_OR_raw",):
            continue
        p = prior_tt[c].reindex(tt.index) if c in prior_tt else pd.Series(np.nan, index=tt.index)
        mu = p.mean()
        reg = (mu + keep * (p - mu)).fillna(mu)
        if c in TODATE and not agg.empty:
            cur = TODATE[c](agg).reindex(tt.index)
            wc = w.where(cur.notna(), 0)
            tt[c] = wc * cur.fillna(0) + (1 - wc) * reg
        else:
            tt[c] = reg
    for c in ("AvgHgt", "Exp", "Bench"):
        tt[c] = pd.to_numeric(h.get(c), errors="coerce") if c in h else np.nan
    return tt


def build(S):
    games = season_games(S).dropna(subset=["home_score", "away_score"])
    box = box_rows(S)
    prior = {k: season_table(S - 1, k) for k in ("four_factors", "height", "misc")}
    prior["ratings"] = season_table(S - 1, "ratings")
    prior_tt = T._raw_table(prior)
    prior_tt["Opp_OR"] = prior_tt["D_OR_raw"]
    cur_height = season_table(S, "height")
    hca = pd.read_csv("data/hca.csv") if os.path.exists("data/hca.csv") else pd.DataFrame()
    hca_map = dict(zip(hca.TeamName.astype(str), pd.to_numeric(hca.HCA, errors="coerce"))) if not hca.empty else {}
    hca_avg = float(np.nanmean(list(hca_map.values()))) if hca_map else 3.5
    early, mcfg, ccfg = EarlySeasonConfig(), MatchupConfig(), ClutchConfig()
    first = archive(SEASONS[S][1])
    match = NameMatcher(first["TeamName"].astype(str))
    box["kp"] = box["team"].map(match)
    rows = []
    for d, day in games.groupby("date"):
        r = archive(d)
        if r is None:
            continue
        tt = team_table_for_date(d, r, box, prior_tt, cur_height, early)
        tags = T.tag_teams(tt, mcfg)
        clutch = T.clutch_composite(tt, 1, 1, 1)
        n = len(tt)
        pct = (n - tt["RankAdjEM"]) / n
        avg_eff, avg_pace = tt["AdjOE"].mean(), tt["AdjTempo"].mean()
        avg_h, avg_e, avg_b = tt["AvgHgt"].mean(), tt["Exp"].mean(), tt["Bench"].mean()
        fm = fanmatch(d)
        fmk = {}
        if not fm.empty:
            for _, f in fm.iterrows():
                fmk[(str(f.Home), str(f.Visitor))] = (f.HomePred - f.VisitorPred, f.HomePred + f.VisitorPred)
        for _, g in day.iterrows():
            h, a = match(g["home"]), match(g["away"])
            if not h or not a or h not in tt.index or a not in tt.index:
                continue
            th, ta = tt.loc[h], tt.loc[a]
            pace = th.AdjTempo * ta.AdjTempo / avg_pace
            ph, pa = th.AdjOE * ta.AdjDE / avg_eff / 100, ta.AdjOE * th.AdjDE / avg_eff / 100
            gh, ga = tags.loc[h], tags.loc[a]
            row = {"season": S, "date": d, "event_id": g.event_id, "home": h, "away": a,
                   "neutral": bool(g.neutral), "margin": g.home_score - g.away_score,
                   "total": g.home_score + g.away_score,
                   "base_margin": (ph - pa) * pace, "base_total": (ph + pa) * pace,
                   "hca": 0.0 if g.neutral else hca_map.get(h, hca_avg),
                   "qg": (pct[h] - pct[a]) * 10,
                   "games_h": th.Games, "games_a": ta.Games,
                   "clutch_diff": clutch[h] - clutch[a],
                   "hgt": (th.AvgHgt - ta.AvgHgt) / avg_h / 10, "exp": (th.Exp - ta.Exp) / avg_e / 10,
                   "bench": (th.Bench - ta.Bench) / avg_b / 10,
                   "tags_h": "|".join(gh["tags"]), "tags_a": "|".join(ga["tags"])}
            for key in PAIRS:
                o, dd = key.split("|")
                cnt = float(gh[o] and ga[dd]) - float(ga[o] and gh[dd])           # home offense countered (+ = home hurt)
                exp_ = float(gh[o] and ga[WEAK[dd]]) - float(ga[o] and gh[WEAK[dd]])
                row["ctr:" + key] = -cnt
                row["exp:" + key] = exp_
            row["ctr:Disrupt|Careless"] = -(float(ga["Disrupt"] and gh["Careless"])
                                       - float(gh["Disrupt"] and ga["Careless"]))
            kp = fmk.get((h, a))
            row["kp_margin"], row["kp_total"] = (kp if kp else (np.nan, np.nan))
            for k in ("open_home_spread", "close_home_spread", "open_total", "close_total",
                      "close_home_ml", "close_away_ml", "open_home_spread_price", "open_away_spread_price",
                      "close_home_spread_price", "close_away_spread_price",
                      "open_over_price", "open_under_price", "close_over_price", "close_under_price"):
                row[k] = g.get(k)
            rows.append(row)
    df = pd.DataFrame(rows)
    df.to_pickle(f"{H}/features_{S}.pkl")
    print(f"season {S}: {len(df)} games with features (of {len(games)}); "
          f"KenPom pred {df.kp_margin.notna().mean():.0%}, close spread {df.close_home_spread.notna().mean():.0%}; "
          f"unmatched ESPN names: {len(match.misses)}")
    return df


if __name__ == "__main__" and sys.argv[1] == "build":
    for S in map(int, sys.argv[2:]):
        build(S)


# ── walk-forward fit ─────────────────────────────────────────────────────────
GROUPS = {
    "base": ["base_margin"],
    "hca": ["hca"],
    "quality_gap": ["qg"],
    "matchup": [f"ctr:{k}" for k in PAIRS] + ["ctr:TM|Careless"] + [f"exp:{k}" for k in PAIRS],
    "unit": ["hgt", "exp", "bench"],
    "clutch": ["clutch_x"],
}
RIDGE = {"matchup": 20.0, "unit": 5.0, "clutch": 5.0}   # light shrinkage on noisy groups


def clean_lines(df):
    """ESPN/DK opening lines are sometimes posted with home/away swapped (mostly
    neutral-site November events). Flip an open spread whose sign disagrees with
    the close when the flipped value is within 3 pts of the close; then drop any
    open line still > 8 pts from the close, and totals > 12 apart."""
    df = df.copy()
    o, c = df.open_home_spread, df.close_home_spread
    flip = o.notna() & c.notna() & ((o + c).abs() < (o - c).abs()) & ((o + c).abs() <= 3)
    df.loc[flip, "open_home_spread"] = -o[flip]
    hp, ap = df.open_home_spread_price.copy(), df.open_away_spread_price.copy()
    df.loc[flip, "open_home_spread_price"] = ap[flip]
    df.loc[flip, "open_away_spread_price"] = hp[flip]
    bad = (df.open_home_spread - c).abs() > 8
    df.loc[bad, "open_home_spread"] = np.nan
    badt = (df.open_total - df.close_total).abs() > 12
    df.loc[badt, "open_total"] = np.nan
    df.attrs["line_fixes"] = {"open_flipped": int(flip.sum()), "open_dropped": int(bad.sum()), "open_total_dropped": int(badt.sum())}
    return df


def prep(df):
    df = clean_lines(df)
    df["close_x"] = ((df.base_margin + df.hca).abs() <= 5).astype(float)
    df["clutch_x"] = df.clutch_diff * df.close_x
    df["mkt_close"] = -df.close_home_spread
    df["mkt_open"] = -df.open_home_spread
    df["month"] = df.date.str[:7]
    df["late"] = (pd.to_datetime(df.date).dt.month.isin([2, 3, 4])).astype(float)
    for c in sum(GROUPS.values(), []):
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
    return df


def ridge(X, y, pen):
    """Ridge on standardized columns (penalty is scale-free), no intercept."""
    sd = X.std(0)
    sd[sd == 0] = 1.0
    Z = X / sd
    A = Z.T @ Z + np.diag(pen)
    return np.linalg.solve(A, Z.T @ y) / sd


def fit_margin(tr, groups):
    cols = sum((GROUPS[g] for g in groups), [])
    pen = np.array([RIDGE.get(g, 0.0) for g in groups for _ in GROUPS[g]]) * len(tr) / 1000
    beta = ridge(tr[cols].values, tr["margin"].values, pen)
    return cols, beta


def fit_total(tr):
    X = np.c_[np.ones(len(tr)), tr.base_total.values]
    return np.linalg.lstsq(X, tr.total.values, rcond=None)[0]


def blend_fit(tr, cols, line_col=None, step=0.05):
    """Convex blend weights (>=0, sum to 1) minimising squared error; grid search.
    (Unconstrained OLS is unstable: model and KenPom correlate at 0.99.)"""
    sub = tr.dropna(subset=cols + ["margin"])
    X, y = sub[cols].values, sub.margin.values
    grid = np.arange(0, 1 + 1e-9, step)
    best, bw = np.inf, None
    if len(cols) == 2:
        cands = [(a, 1 - a) for a in grid]
    else:
        cands = [(a, b, 1 - a - b) for a in grid for b in grid if a + b <= 1 + 1e-9]
    for w in cands:
        e = np.mean((y - X @ np.array(w)) ** 2)
        if e < best:
            best, bw = e, np.array(w)
    return bw


def walk_forward(df, groups=tuple(GROUPS), min_train=1500):
    df = prep(df).sort_values("date").reset_index(drop=True)
    months = sorted(df.month.unique())
    out, fits = [], []
    for m in months:
        tr, te = df[df.month < m], df[df.month == m].copy()
        if len(tr) < min_train or te.empty:
            continue
        cols, beta = fit_margin(tr, groups)
        tr = tr.copy()
        tr["model"] = tr[cols].values @ beta
        te["model"] = te[cols].values @ beta
        tb = fit_total(tr)
        tr["model_total"] = tb[0] + tb[1] * tr.base_total
        te["model_total"] = tb[0] + tb[1] * te.base_total
        sd_model = float(np.std(tr.margin - tr.model))
        # blends
        wk = blend_fit(tr, ["model", "kp_margin"], None)
        wm = blend_fit(tr, ["model", "kp_margin", "mkt_close"], None)
        wmo = blend_fit(tr, ["model", "kp_margin", "mkt_open"], None)
        trk = tr.dropna(subset=["kp_margin"])
        sd_kp = float(np.std(trk.margin - trk[["model", "kp_margin"]].values @ wk))
        trm = tr.dropna(subset=["kp_margin", "mkt_close"])
        sd_mkt = float(np.std(trm.margin - trm[["model", "kp_margin", "mkt_close"]].values @ wm))
        te["model_kp"] = np.where(te.kp_margin.notna(), te.model * wk[0] + te.kp_margin.fillna(0) * wk[1], te.model)
        te["blend_close"] = te.model * wm[0] + te.kp_margin.fillna(te.model) * wm[1] + te.mkt_close.fillna(te.model) * wm[2]
        te["blend_open"] = te.model * wmo[0] + te.kp_margin.fillna(te.model) * wmo[1] + te.mkt_open * wmo[2]
        te["sd_model"], te["sd_kp"], te["sd_blend"] = sd_model, sd_kp, sd_mkt
        tt_ = tr.dropna(subset=["close_total"])
        te["sd_total"] = float(np.std(tt_.total - tt_.model_total))
        # contribution columns for analysis
        for g in groups:
            gc = GROUPS[g]
            idx = [cols.index(c) for c in gc]
            te[f"pts_{g}"] = te[gc].values @ beta[idx]
        out.append(te)
        fits.append({"month": m, "n_train": len(tr), **dict(zip(cols, beta)),
                     "sd_model": sd_model, "sd_kp": sd_kp, "sd_blend": sd_mkt,
                     "w_model_kp": tuple(wk), "w_blend": tuple(wm), "total_ab": tuple(tb)})
    return pd.concat(out), pd.DataFrame(fits)


# ── grading ───────────────────────────────────────────────────────────────────
STD_PRICE = -110
BE = 110 / 210


def grade_spreads(te, pred_col, sd_col, when):
    line = te[f"{when}_home_spread"]
    ok = line.notna()
    t = te[ok].copy()
    L = t[f"{when}_home_spread"]
    p_home = 1 - norm_cdf_v((-L - t[pred_col]) / t[sd_col])     # P(margin > -L)
    side_home = p_home >= 0.5
    p = np.where(side_home, p_home, 1 - p_home)
    ats = np.where(side_home, t.margin + L, -(t.margin + L))
    t["prob"], t["edge"] = p, p - BE
    t["result"] = np.where(ats == 0, 0.5, (ats > 0).astype(float))
    t["units"] = np.where(ats == 0, 0.0, np.where(ats > 0, 100 / 110, -1.0))
    if when == "open":
        cl = t["close_home_spread"]
        t["clv"] = np.where(side_home, L - cl, cl - L)       # + = got more points than the close
    else:
        t["clv"] = np.nan
    return t


_ERF = np.vectorize(__import__("math").erf)


def norm_cdf_v(x):
    return 0.5 * (1 + _ERF(np.asarray(x, float) / np.sqrt(2)))


def grade_totals(te, when):
    t = te[te[f"{when}_total"].notna()].copy()
    T_ = t[f"{when}_total"]
    p_over = 1 - norm_cdf_v((T_ - t.model_total) / t.sd_total)
    over = p_over >= 0.5
    p = np.where(over, p_over, 1 - p_over)
    d = np.where(over, t.total - T_, T_ - t.total)
    t["prob"], t["edge"] = p, p - BE
    t["result"] = np.where(d == 0, 0.5, (d > 0).astype(float))
    t["units"] = np.where(d == 0, 0.0, np.where(d > 0, 100 / 110, -1.0))
    t["clv"] = np.where(over, t.close_total - T_, T_ - t.close_total) if when == "open" else np.nan
    return t


def tier_table(g, cuts=(0.0, 0.03, 0.05, 0.08)):
    rows = []
    for lo, hi in zip(cuts, list(cuts[1:]) + [1]):
        s = g[(g.edge >= lo) & (g.edge < hi)]
        rows.append(_summ(s, f"{lo:.0%}-{hi:.0%}" if hi < 1 else f">={lo:.0%}"))
    for lo in cuts[1:]:
        rows.append(_summ(g[g.edge >= lo], f">={lo:.0%} (cum)"))
    return pd.DataFrame(rows)


def _summ(s, label):
    nd = s[s.result != 0.5]
    w, l = int((nd.result == 1).sum()), int((nd.result == 0).sum())
    return {"bucket": label, "n": len(s), "W-L": f"{w}-{l}", "ATS%": w / max(1, w + l),
            "ROI@-110": s.units.sum() / max(1, len(s)), "CLV_pts": s.clv.mean() if s.clv.notna().any() else np.nan,
            "beat_close%": (s.clv > 0).mean() if s.clv.notna().any() else np.nan}


def brier(p, y):
    p, y = np.asarray(p, float), np.asarray(y, float)
    return float(np.mean((p - y) ** 2))


def _rmse(a, b):
    return float(np.sqrt(np.mean((np.asarray(a) - np.asarray(b)) ** 2)))


def boot_delta(e_full, e_drop, n=2000, seed=0):
    """Paired bootstrap of MSE(drop) - MSE(full); >0 = the group helps."""
    rng = np.random.default_rng(seed)
    d = e_drop ** 2 - e_full ** 2
    idx = rng.integers(0, len(d), (n, len(d)))
    bs = d[idx].mean(1)
    return float(d.mean()), float(np.percentile(bs, 2.5)), float(np.percentile(bs, 97.5))


def ml_brier(te, pred_col, sd_col):
    s = te.dropna(subset=["close_home_ml", "close_away_ml", pred_col]).copy()
    s = s[s.margin != 0]
    mk = np.array([devig_two_way(h, a)[0] for h, a in zip(s.close_home_ml, s.close_away_ml)])
    pm = norm_cdf_v(s[pred_col] / s[sd_col])
    y = (s.margin > 0).astype(float)
    return len(s), brier(pm, y), brier(mk, y)


def report(out_dir="outputs/walkforward"):
    os.makedirs(out_dir, exist_ok=True)
    df = pd.concat([pd.read_pickle(f"{H}/features_{S}.pkl") for S in (2025, 2026)])
    te, fits = walk_forward(df)
    te.to_pickle(os.path.join(out_dir, "oos.pkl"))
    fits.to_csv(os.path.join(out_dir, "fits_by_month.csv"), index=False)
    L = []
    P = L.append
    P(f"Walk-forward: monthly refits, train = all games before the test month (min 1500).")
    P(f"Line cleaning: {clean_lines(df).attrs['line_fixes']}")
    P(f"OOS games: {len(te)}  ({te.date.min()} .. {te.date.max()}); "
      f"2024-25 OOS {int((te.season == 2025).sum())}, 2025-26 OOS {int((te.season == 2026).sum())}")

    # final fit on everything (what goes into config)
    full = prep(df)
    cols, beta = fit_margin(full, tuple(GROUPS))
    full["model"] = full[cols].values @ beta
    b = dict(zip(cols, beta))
    P("\n== Final fit, all games (ridge on matchup/unit/clutch) ==")
    for c in cols:
        P(f"  {c:<45}{b[c]:+.3f}")
    wk = blend_fit(full, ["model", "kp_margin"], None)
    wm = blend_fit(full, ["model", "kp_margin", "mkt_close"], None)
    wmo = blend_fit(full, ["model", "kp_margin", "mkt_open"], None)
    P(f"  blend (no market):   model {wk[0]:.3f}  kenpom {wk[1]:.3f}")
    P(f"  blend (close line):  model {wm[0]:.3f}  kenpom {wm[1]:.3f}  market {wm[2]:.3f}")
    P(f"  blend (open line):   model {wmo[0]:.3f}  kenpom {wmo[1]:.3f}  market {wmo[2]:.3f}")
    s = full.dropna(subset=["kp_margin", "mkt_close"])
    for nm, pred in [("model", s.model), ("kenpom", s.kp_margin), ("market close", s.mkt_close),
                     ("blend close", s[["model", "kp_margin", "mkt_close"]].values @ wm)]:
        P(f"  in-sample SD of margin - {nm:<13}: {np.std(s.margin - pred):.2f}")
    tb = fit_total(full)
    P(f"  total = {tb[0]:.2f} + {tb[1]:.4f} * base_total; SD {np.std(full.total - (tb[0] + tb[1] * full.base_total)):.2f}")

    # OOS accuracy
    P("\n== Out-of-sample margin RMSE (same games) ==")
    s = te.dropna(subset=["kp_margin", "mkt_close", "mkt_open"])
    for nm, c in [("model (fitted features)", "model"), ("model + KenPom", "model_kp"), ("KenPom fanmatch", "kp_margin"),
                  ("market open", "mkt_open"), ("blend w/ open", "blend_open"),
                  ("market close", "mkt_close"), ("blend w/ close", "blend_close")]:
        P(f"  {nm:<26} {_rmse(s.margin, s[c]):.3f}")

    # ablations
    P("\n== Feature value, out of sample (drop one group, refit walk-forward) ==")
    P("   dMSE = MSE(without) - MSE(with); >0 means the group helps. 95% paired bootstrap CI.")
    P("   'vs market' = same test on the residual (margin - close line) using the group's points.")
    base_err = (te.margin - te.model).values
    for g in [g for g in GROUPS if g != "base"]:
        te2, _ = walk_forward(df, tuple(x for x in GROUPS if x != g))
        e2 = (te2.set_index("event_id").reindex(te.event_id).margin - te2.set_index("event_id").reindex(te.event_id).model).values
        ok = ~np.isnan(e2)
        m, lo, hi = boot_delta(base_err[ok], e2[ok])
        # incremental vs market: regress OOS residual (margin - mkt_close) on the group's OOS points
        r = te.dropna(subset=["mkt_close"])
        x, y = r[f"pts_{g}"].values, (r.margin - r.mkt_close).values
        if np.std(x) > 0:
            k = float(np.sum(x * y) / np.sum(x * x)); se = float(np.sqrt(np.sum((y - k * x) ** 2) / (len(x) - 1) / np.sum(x * x)))
        else:
            k = se = float("nan")
        P(f"  {g:<12} dMSE {m:+.3f} [{lo:+.3f}, {hi:+.3f}]   vs market: slope {k:+.2f} (se {se:.2f}, t {k / se if se else 0:+.1f})")

    # unit score deep-dive
    P("\n== Unit score (height / experience / bench), out of sample ==")
    r = te.dropna(subset=["mkt_close"]).copy()
    r["resid"] = r.margin - r.mkt_close
    r["fav_home"] = r.mkt_close > 0
    for nm, mask in [("all", r.index == r.index), ("close |line|<=5", r.mkt_close.abs() <= 5),
                     ("neutral site", r.neutral), ("late season Feb-Apr", r.late == 1),
                     ("postseason (Mar 10+)", r.date.str[5:] >= "03-10")]:
        q = r[mask]
        x, y = q.pts_unit.values, q.resid.values
        if len(q) < 30 or np.std(x) == 0:
            continue
        k = float(np.sum(x * y) / np.sum(x * x)); se = float(np.sqrt(np.sum((y - k * x) ** 2) / (len(x) - 1) / np.sum(x * x)))
        P(f"  {nm:<22} n={len(q):>5}  mean |unit pts| {np.abs(x).mean():.2f}  slope vs market residual {k:+.2f} (se {se:.2f})")
    # upsets: unit score favors the market underdog
    q = r[(r.mkt_close != 0)].copy()
    dog_unit = np.where(q.fav_home, -q.pts_unit, q.pts_unit)      # + = unit favors underdog
    q["dog_unit"] = dog_unit
    q["dog_won"] = np.where(q.fav_home, q.margin < 0, q.margin > 0)
    q["dog_cover"] = np.where(q.fav_home, q.margin < q.mkt_close, q.margin > q.mkt_close)
    ml = q.dropna(subset=["close_home_ml", "close_away_ml"])
    imp = np.array([devig_two_way(h, a)[0] for h, a in zip(ml.close_home_ml, ml.close_away_ml)])
    ml = ml.assign(dog_imp=np.where(ml.fav_home, 1 - imp, imp))
    for nm, m in [("unit favors underdog by >=0.5 pt", ml.dog_unit >= 0.5),
                  ("unit favors underdog by >=1 pt", ml.dog_unit >= 1.0),
                  ("unit favors favorite by >=0.5 pt", ml.dog_unit <= -0.5)]:
        z = ml[m]
        if len(z):
            P(f"  {nm:<34} n={len(z):>5}  dog won {z.dog_won.mean():.1%} vs market {z.dog_imp.mean():.1%}; "
              f"dog covered {z.dog_cover[z.margin != z.mkt_close].mean():.1%}")

    # betting
    for when in ("open", "close"):
        for nm, pc, sc in [("model only", "model", "sd_model"), ("model + KenPom", "model_kp", "sd_kp"),
                           (f"blend w/ {when} line", f"blend_{when}", "sd_blend")]:
            g = grade_spreads(te, pc, sc, when)
            P(f"\n== ATS vs {when.upper()} line, {nm}, -110 (n={len(g)}) ==")
            for S in (2025, 2026, None):
                gg = g if S is None else g[g.season == S]
                tab = tier_table(gg)
                P(f"  -- {'both seasons' if S is None else f'{S-1}-{str(S)[2:]}'} --")
                P("  " + tab.to_string(index=False, float_format=lambda v: f"{v:.3f}").replace("\n", "\n  "))
    for when in ("open", "close"):
        g = grade_totals(te, when)
        P(f"\n== TOTALS vs {when.upper()} line, model (fitted), -110 (n={len(g)}) ==")
        P("  " + tier_table(g).to_string(index=False, float_format=lambda v: f"{v:.3f}").replace("\n", "\n  "))
    # win prob Brier
    P("\n== Win probability Brier (OOS, lower is better; market = de-vigged DK close ML) ==")
    for nm, pc, sc in [("model", "model", "sd_model"), ("model + KenPom", "model_kp", "sd_kp"), ("blend close", "blend_close", "sd_blend")]:
        n, bm, bk = ml_brier(te, pc, sc)
        P(f"  {nm:<16} n={n}  model {bm:.4f}   market {bk:.4f}")
    P("\n== Fitted SDs (last fold) ==")
    lf = fits.iloc[-1]
    P(f"  margin SD: model {lf.sd_model:.2f} | model+KenPom {lf.sd_kp:.2f} | blend {lf.sd_blend:.2f}; total SD {te.sd_total.iloc[-1]:.2f}")
    txt = "\n".join(L)
    open(os.path.join(out_dir, "report.txt"), "w").write(txt)
    json.dump({"coef": b, "blend_no_market": list(wk), "blend_close": list(wm), "blend_open": list(wmo),
               "total_ab": list(tb), "sd": {"model": lf.sd_model, "kp": lf.sd_kp, "blend": lf.sd_blend,
                                              "total": float(te.sd_total.iloc[-1])}},
              open(os.path.join(out_dir, "fit.json"), "w"), indent=1)
    print(txt)
    return te


if __name__ == "__main__" and sys.argv[1] == "fit":
    report()
