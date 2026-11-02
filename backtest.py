"""
backtest.py — grade CZarp against historical lines.

Inputs
  games: CSV from espn_history.py (free: results + DraftKings open/close lines),
         or any CSV with the same columns:
         date, home, away, home_score, away_score, neutral,
         open_home_spread, close_home_spread, open_total, close_total,
         [open|close]_[home|away]_spread_price, [open|close]_[home|away]_ml,
         [open|close]_[over|under]_price
  ratings (pick one):
    --ratings archive  KenPom point-in-time ratings per game date (KENPOM_API_KEY,
                       1 call per date, cached) — no look-ahead for AdjOE/AdjDE/Tempo
    --ratings static   CSVs in --data-dir (e.g. season-end) — LOOK-AHEAD, pipeline test only
    --projections CSV  precomputed model numbers: date, home, away, model_margin (home), model_total
  style stats (four factors / height / misc used by matchup cards, unit, clutch):
    --style DIR        season tables to use (KenPom has no point-in-time version — look-ahead!)
    --style none       disable matchup cards, unit score and clutch (pure efficiency model)

Outputs (to --out, default outputs/backtest/):
  graded.csv  one row per game x market x line (open/close) with model prob, edge, tier, result, CLV
  report.txt  ATS / totals / ML by edge tier, CLV, Brier + calibration table

Examples
  python espn_history.py 2026-01-05 2026-02-28
  python backtest.py data/history/espn_20260105_20260228.csv --ratings archive --style data/season2026
"""
from __future__ import annotations

import argparse
import math
import os
import re
from datetime import date, timedelta

import numpy as np
import pandas as pd

from czarp.config import load_config, ModelConfig
from czarp.probability import (american_to_prob, brier, cover_prob, devig_two_way,
                               evaluate, over_prob, win_prob)

# ── name matching ESPN/odds names -> KenPom TeamName ──────────────────────────
_ESPN_TO_KP = {
    "UConn": "Connecticut", "Ole Miss": "Mississippi", "Miami": "Miami FL",
    "Miami (OH)": "Miami OH", "NC State": "N.C. State", "Pitt": "Pittsburgh",
    "UNC Wilmington": "UNC Wilmington", "Hawai'i": "Hawaii", "San José State": "San Jose St.",
    "Saint Mary's": "Saint Mary's", "St. John's": "St. John's", "UL Monroe": "Louisiana Monroe",
    "Louisiana": "Louisiana", "Texas A&M-Corpus Christi": "Texas A&M Corpus Chris",
    "Southeast Missouri State": "Southeast Missouri", "App State": "Appalachian St.",
    "Omaha": "Nebraska Omaha", "SIU Edwardsville": "SIUE", "UT Rio Grande Valley": "UT Rio Grande Valley",
    "Long Island University": "LIU", "Queens University": "Queens", "Kansas City": "Kansas City",
    "Grambling": "Grambling St.", "McNeese": "McNeese", "Nicholls": "Nicholls",
    "Sam Houston": "Sam Houston", "Seattle U": "Seattle", "Loyola Chicago": "Loyola Chicago",
    "Florida International": "FIU", "East Tennessee State": "East Tennessee St.",
    "Mississippi Valley State": "Mississippi Valley St.", "Arkansas-Pine Bluff": "Arkansas Pine Bluff",
    "Bethune-Cookman": "Bethune Cookman", "Cal State Fullerton": "Cal St. Fullerton",
    "Cal State Northridge": "CSUN", "Cal State Bakersfield": "Cal St. Bakersfield",
    "Long Beach State": "Long Beach St.", "Southern Miss": "Southern Miss",
    "Gardner-Webb": "Gardner Webb", "Tarleton State": "Tarleton St.", "UAlbany": "Albany",
    "California Baptist": "Cal Baptist", "IU Indianapolis": "IU Indy", "UT Martin": "Tennessee Martin",
    "American University": "American", "UIC": "Illinois Chicago", "SE Louisiana": "Southeastern Louisiana",
    "South Carolina Upstate": "USC Upstate", "Pennsylvania": "Penn", "Loyola Maryland": "Loyola MD",
}
# names KenPom changed between seasons: try each
_ESPN_TO_KP_MULTI = {
    "LSU New Orleans": ["LSU New Orleans", "New Orleans"],
    "New Orleans": ["New Orleans", "LSU New Orleans"],
    "Sam Houston": ["Sam Houston St.", "Sam Houston"],
}


class NameMatcher:
    def __init__(self, kp_names):
        self.names = list(kp_names)
        self.lower = {n.lower(): n for n in self.names}
        self.misses: set[str] = set()

    def __call__(self, name: str) -> str | None:
        if not isinstance(name, str):
            return None
        cands = list(_ESPN_TO_KP_MULTI.get(name, [])) + [_ESPN_TO_KP.get(name, name), name]
        cands.append(re.sub(r"\bState$", "St.", name))
        cands.append(re.sub(r"\bState\b", "St.", name))
        cands.append(name.replace("Saint ", "St. "))
        for c in cands:
            if c.lower() in self.lower:
                return self.lower[c.lower()]
        self.misses.add(name)
        return None


# ── ratings providers ─────────────────────────────────────────────────────────
def archive_ratings(d: str, cache_dir="data/history/kp_archive") -> pd.DataFrame:
    os.makedirs(cache_dir, exist_ok=True)
    path = os.path.join(cache_dir, f"{d}.csv")
    if os.path.exists(path):
        return pd.read_csv(path)
    from kenpom_fetcher import fetch_archive
    df = fetch_archive(d)
    df.to_csv(path, index=False)
    return df


def _load_tables(dir_):
    out = {}
    for k in ("four_factors", "height", "misc"):
        p = os.path.join(dir_, f"{k}.csv")
        if os.path.exists(p):
            out[k] = pd.read_csv(p)
    return out


def project_games(games: pd.DataFrame, mode: str, data_dir: str, style: str,
                  cfg: ModelConfig) -> pd.DataFrame:
    from model import load_data, project_game
    if style == "none":
        cfg.matchup.enabled = False
        cfg.clutch.enabled = False
        cfg.unit.scale = 0.0
        style_tables = {}
    else:
        style_tables = _load_tables(style or data_dir)
    hca = pd.read_csv(os.path.join(data_dir, "hca.csv")) if os.path.exists(os.path.join(data_dir, "hca.csv")) else None

    static = load_data(data_dir) if mode == "static" else None
    rows = []
    for d, day in games.groupby("date"):
        if mode == "archive":
            ratings = archive_ratings(d)
        else:
            ratings = static["ratings"]
        data = {"ratings": ratings, "net": pd.DataFrame(columns=["TeamName", "Rank"]),
                "four_factors": style_tables.get("four_factors", pd.DataFrame({"TeamName": ratings["TeamName"]})),
                "height": style_tables.get("height", pd.DataFrame({"TeamName": ratings["TeamName"]})),
                "misc": style_tables.get("misc", pd.DataFrame()), "hca": hca, "config": cfg}
        match = NameMatcher(ratings["TeamName"].astype(str))
        for _, g in day.iterrows():
            h, a = match(g["home"]), match(g["away"])
            if not h or not a:
                continue
            try:
                r = project_game(h, a, None if g["neutral"] else True, data, game_date=d, config=cfg)
            except Exception as e:
                print(f"  skip {g['away']} @ {g['home']}: {e}")
                continue
            rows.append({"event_id": g.get("event_id"), "date": d, "home": g["home"], "away": g["away"],
                         "model_margin": r["spread_raw"], "model_total": r["total_raw"],
                         "home_tags": "|".join(r["team1_tags"]), "away_tags": "|".join(r["team2_tags"])})
        if match.misses:
            print(f"  {d}: unmatched names {sorted(match.misses)}")
    return pd.DataFrame(rows)


# ── grading ───────────────────────────────────────────────────────────────────
def _payout(price: float) -> float:
    return price / 100 if price > 0 else 100 / -price


def grade(df: pd.DataFrame, cfg: ModelConfig) -> pd.DataFrame:
    """One row per game x market x {open, close}: model picks the side with the
    higher edge; result graded at the price; CLV measured open -> close."""
    b = cfg.betting
    out = []
    for _, g in df.iterrows():
        actual = g["home_score"] - g["away_score"]
        tot = g["home_score"] + g["away_score"]
        for when in ("open", "close"):
            L = g.get(f"{when}_home_spread")
            if pd.notna(L):
                hp = g.get(f"{when}_home_spread_price"); ap = g.get(f"{when}_away_spread_price")
                hp = -110 if pd.isna(hp) else hp; ap = -110 if pd.isna(ap) else ap
                p_home = cover_prob(g["model_margin"], L, b.margin_sd)
                eh, ea = evaluate(p_home, b, american=hp), evaluate(1 - p_home, b, american=ap)
                side, ev, price, line = ("home", eh, hp, L) if eh.edge >= ea.edge else ("away", ea, ap, -L)
                sgn = 1 if side == "home" else -1
                ats = sgn * actual + line
                res = 0.5 if ats == 0 else float(ats > 0)
                clv = None
                if when == "open" and pd.notna(g.get("close_home_spread")):
                    close_line = g["close_home_spread"] * (1 if side == "home" else -1)
                    clv = line - close_line          # + = you beat the close
                out.append({"event_id": g.get("event_id"), "date": g["date"], "market": "spread", "when": when,
                            "side": side, "line": line, "price": price, "model_prob": ev.model_prob,
                            "implied": ev.implied_prob, "edge": ev.edge, "tier": ev.tier or "none",
                            "stake_pct": ev.stake_pct, "result": res,
                            "units": (0 if res == 0.5 else (_payout(price) if res else -1.0)), "clv_pts": clv})
            T = g.get(f"{when}_total")
            if pd.notna(T):
                op = g.get(f"{when}_over_price"); up = g.get(f"{when}_under_price")
                op = -110 if pd.isna(op) else op; up = -110 if pd.isna(up) else up
                po = over_prob(g["model_total"], T, b.total_sd)
                eo, eu = evaluate(po, b, american=op), evaluate(1 - po, b, american=up)
                side, ev, price = ("over", eo, op) if eo.edge >= eu.edge else ("under", eu, up)
                diff = (tot - T) * (1 if side == "over" else -1)
                res = 0.5 if diff == 0 else float(diff > 0)
                clv = None
                if when == "open" and pd.notna(g.get("close_total")):
                    clv = (g["close_total"] - T) * (1 if side == "over" else -1)
                out.append({"event_id": g.get("event_id"), "date": g["date"], "market": "total", "when": when,
                            "side": side, "line": T, "price": price, "model_prob": ev.model_prob,
                            "implied": ev.implied_prob, "edge": ev.edge, "tier": ev.tier or "none",
                            "stake_pct": ev.stake_pct, "result": res,
                            "units": (0 if res == 0.5 else (_payout(price) if res else -1.0)), "clv_pts": clv})
        hm, am = g.get("close_home_ml"), g.get("close_away_ml")
        p_home_win = win_prob(g["model_margin"], b.margin_sd)
        mkt = devig_two_way(hm, am)[0] if pd.notna(hm) and pd.notna(am) else None
        out.append({"event_id": g.get("event_id"), "date": g["date"], "market": "winprob", "when": "close",
                    "model_prob": p_home_win, "market_prob": mkt, "result": float(actual > 0)})
    return pd.DataFrame(out)


def summarize(gr: pd.DataFrame) -> str:
    lines = []
    for market in ("spread", "total"):
        for when in ("open", "close"):
            sub = gr[(gr.market == market) & (gr.when == when)]
            if sub.empty:
                continue
            lines.append(f"\n== {market.upper()} vs {when} line  (n={len(sub)}) ==")
            lines.append(f"{'tier':<8}{'n':>6}{'W-L-P':>14}{'win%':>8}{'units':>9}{'ROI':>8}{'avgCLV':>8}")
            for tier in ["STRONG", "VALUE", "LEAN", "none", "ALL tiered"]:
                t = sub[sub.tier != "none"] if tier == "ALL tiered" else sub[sub.tier == tier]
                if t.empty:
                    continue
                w, l, p = (t.result == 1).sum(), (t.result == 0).sum(), (t.result == 0.5).sum()
                wp = w / max(1, w + l)
                u = t.units.sum()
                clv = t.clv_pts.dropna().mean() if "clv_pts" in t and t.clv_pts.notna().any() else float("nan")
                lines.append(f"{tier:<8}{len(t):>6}{f'{w}-{l}-{p}':>14}{wp:>8.1%}{u:>9.2f}{u/len(t):>8.1%}{clv:>8.2f}")
            # calibration of the chosen side's probability
            sub = sub.copy()
            sub["bin"] = pd.cut(sub.model_prob, [0, .5, .55, .6, .65, .7, .8, 1.0])
            cal = sub[sub.result != 0.5].groupby("bin", observed=True).agg(n=("result", "size"),
                                                                           pred=("model_prob", "mean"),
                                                                           actual=("result", "mean"))
            lines.append("calibration (chosen side): " + "; ".join(
                f"{i}: n={r.n} pred={r.pred:.2f} act={r.actual:.2f}" for i, r in cal.iterrows()))
            nb = sub[sub.result != 0.5]
            lines.append(f"Brier (chosen side cover prob): {brier(nb.model_prob, nb.result):.4f}  (0.25 = coin flip)")
        if market == "spread":
            o = gr[(gr.market == "spread") & (gr.when == "open")]
            if o.clv_pts.notna().any():
                c = o.clv_pts.dropna()
                lines.append(f"CLV (spread, open->close): mean {c.mean():+.2f} pts, beat close {(c>0).mean():.1%}, "
                             f"tiered-only mean {o[o.tier!='none'].clv_pts.dropna().mean():+.2f}")
    wp = gr[gr.market == "winprob"]
    if not wp.empty:
        lines.append(f"\n== WIN PROBABILITY (n={len(wp)}) ==")
        lines.append(f"Brier model: {brier(wp.model_prob, wp.result):.4f}")
        m = wp.dropna(subset=["market_prob"])
        if not m.empty:
            lines.append(f"Brier market (de-vigged close ML, same {len(m)} games): {brier(m.market_prob, m.result):.4f}"
                         f" | model on same games: {brier(m.model_prob, m.result):.4f}")
    return "\n".join(lines)


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("games")
    ap.add_argument("--ratings", choices=["archive", "static"], default="archive")
    ap.add_argument("--projections", help="CSV with date,home,away,model_margin,model_total")
    ap.add_argument("--data-dir", default="data")
    ap.add_argument("--style", default=None, help="dir with four_factors/height/misc CSVs, or 'none'")
    ap.add_argument("--out", default=os.path.join("outputs", "backtest"))
    a = ap.parse_args(argv)
    cfg = load_config()
    games = pd.read_csv(a.games)
    games = games.dropna(subset=["home_score", "away_score"])
    if a.projections:
        proj = pd.read_csv(a.projections)
    else:
        proj = project_games(games, a.ratings, a.data_dir, a.style, cfg)
    keys = ["event_id"] if "event_id" in proj and proj["event_id"].notna().all() else ["date", "home", "away"]
    df = games.merge(proj[keys + ["model_margin", "model_total"]], on=keys, how="inner")
    print(f"Projected {len(df)} of {len(games)} games")
    gr = grade(df, cfg)
    os.makedirs(a.out, exist_ok=True)
    gr.to_csv(os.path.join(a.out, "graded.csv"), index=False)
    rep = summarize(gr)
    with open(os.path.join(a.out, "report.txt"), "w") as fh:
        fh.write(rep)
    print(rep)
    return gr


if __name__ == "__main__":
    main()
