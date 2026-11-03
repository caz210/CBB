"""pace_audit.py — projected possessions vs actual, and how much of the totals error is pace.

Analysis only (no model change). Games: 2024-25 + 2025-26 ESPN/DK history (data/history/features_*.pkl).
Actual possessions from ESPN box scores (data/history/espn_box/<event>.json, fetched from the free
summary endpoint): per team FGA − OREB + TO + 0.475·FTA, game = mean of the two teams; tempo per
40 min = poss × 40 / (40 + 5·OT periods). Projected pace = the live v2 formula, point-in-time KenPom
AdjTempo (archive for the game date): T_home × T_away / national average. Also shown: KenPom's own
FanMatch PredTempo, v1's additive pace, a market-implied pace (open total ÷ model points per poss.),
and in-season blends of observed (opponent-adjusted) tempo with KenPom AdjTempo.

Totals error decomposition (exact): T − T̂ = (P − P̂)·Ê + P·(E − Ê), with P = actual possessions
(incl. OT), E = points per possession, P̂ = projected pace, Ê = model total ÷ P̂.

    python pace_audit.py   -> outputs/walkforward/pace_audit.txt
"""
import json
import os
from datetime import date, timedelta

import numpy as np
import pandas as pd

from coach_lock_study import new_coach_sets
from walkforward import clean_lines

H = "data/history"
BOX = f"{H}/espn_box"


def _n(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return np.nan


def team_poss(t):
    fga = _n((t.get("fieldGoalsMade-fieldGoalsAttempted") or "-").split("-")[-1])
    fta = _n((t.get("freeThrowsMade-freeThrowsAttempted") or "-").split("-")[-1])
    to = _n(t.get("totalTurnovers", t.get("turnovers")))
    orb = _n(t.get("offensiveRebounds"))
    return fga - orb + to + 0.475 * fta


def ot_periods():
    import glob
    out = {}
    for p in glob.glob(f"{H}/espn_cache/sb_*.json"):
        for ev in json.load(open(p)).get("events", []):
            per = ev["competitions"][0].get("status", {}).get("period")
            if per:
                out[str(ev["id"])] = max(0, int(per) - 2)
    return out


def load_box(ids):
    ot_map = ot_periods()
    rows = []
    for e in ids:
        p = f"{BOX}/{e}.json"
        if not os.path.exists(p):
            continue
        j = json.load(open(p))
        ts = j.get("teams", [])
        if len(ts) != 2:
            continue
        ps = [team_poss(t) for t in ts]
        ot = ot_map.get(str(e), max(0, int(j.get("period") or 2) - 2))
        poss = float(np.mean(ps))
        rows.append({"event_id": str(e), "poss": poss, "ot": ot, "tempo": poss * 40 / (40 + 5 * ot)})
    return pd.DataFrame(rows)


_arch = {}


def archive(d):
    if d not in _arch:
        dd = date.fromisoformat(d)
        _arch[d] = None
        for k in range(6):
            p = f"{H}/kp_archive/{(dd - timedelta(k)).isoformat()}.csv"
            if os.path.exists(p):
                a = pd.read_csv(p)
                _arch[d] = (dict(zip(a.TeamName, pd.to_numeric(a.AdjTempo, errors="coerce"))),
                            float(pd.to_numeric(a.AdjTempo, errors="coerce").mean()))
                break
    return _arch[d]


def fm_tempo(d):
    p = f"{H}/kp_fanmatch/{d}.csv"
    try:
        f = pd.read_csv(p)
        return {frozenset([a, b]): t for a, b, t in zip(f.Visitor, f.Home, f.PredTempo)}
    except Exception:
        return {}


def stats(err):
    err = pd.Series(err).dropna()
    return f"n={len(err):5d}  MAE {err.abs().mean():5.2f}  bias {err.mean():+5.2f}  RMSE {np.sqrt((err**2).mean()):5.2f}"


def build():
    f = pd.concat([pd.read_pickle(f"{H}/features_{y}.pkl") for y in (2025, 2026)], ignore_index=True)
    f["event_id"] = f.event_id.astype(str)
    v1 = pd.read_pickle(f"{H}/v1_preds.pkl").drop_duplicates("event_id")
    v1["event_id"] = v1.event_id.astype(str)
    f = clean_lines(f.merge(v1, on="event_id", how="left"))
    f = f.merge(load_box(f.event_id), on="event_id", how="inner")
    ph, pa, avg, kpt = [], [], [], []
    fmc = {}
    for d, h, a in zip(f.date, f.home, f.away):
        ar = archive(d)
        if ar is None:
            ph.append(np.nan); pa.append(np.nan); avg.append(np.nan)
        else:
            ph.append(ar[0].get(h, np.nan)); pa.append(ar[0].get(a, np.nan)); avg.append(ar[1])
        if d not in fmc:
            fmc[d] = fm_tempo(d)
        kpt.append(fmc[d].get(frozenset([h, a]), np.nan))
    f["t_h"], f["t_a"], f["t_avg"], f["kp_pred_tempo"] = ph, pa, avg, kpt
    f["pace_v2"] = f.t_h * f.t_a / f.t_avg
    f = f[f.pace_v2.notna()].copy()
    f["pace_v1"] = f.t_h + f.t_a - f.t_avg
    f["v2_total"] = 27.04 + 0.822 * f.base_total
    nc = new_coach_sets()
    f["nc"] = [(h in nc.get(s, set())) or (a in nc.get(s, set())) for h, a, s in zip(f.home, f.away, f.season)]
    f["min_games"] = f[["games_h", "games_a"]].min(axis=1)
    f["nov"] = f.date.str[5:7] == "11"
    f["month"] = f.date.str[5:7]
    f = observed_tempo(f)
    return f[f.pace_v2.notna() & f.tempo.notna() & (f.tempo > 45) & (f.tempo < 100)].copy()


def observed_tempo(f, ks=(2, 4, 8)):
    """Team to-date pace residual (mean of actual tempo ÷ projected v2 pace over its earlier games, i.e. how
    much faster/slower it has played than KenPom's AdjTempo implied), blended in with w = games/(games+k)."""
    f = f.sort_values(["date", "event_id"]).reset_index(drop=True)
    hist = {}
    obs_h, obs_a, n_h, n_a = [], [], [], []
    for r in f.itertuples():
        for side, team in (("h", r.home), ("a", r.away)):
            L = hist.get((r.season, team), [])
            (obs_h if side == "h" else obs_a).append(np.mean(L) if L else 1.0)
            (n_h if side == "h" else n_a).append(len(L))
        if np.isfinite(r.tempo) and np.isfinite(r.pace_v2) and 45 < r.tempo < 100:
            for team in (r.home, r.away):
                hist.setdefault((r.season, team), []).append(r.tempo / r.pace_v2)
    f["obs_h"], f["obs_a"], f["nobs_h"], f["nobs_a"] = obs_h, obs_a, n_h, n_a
    for k in ks:
        wh = f.nobs_h / (f.nobs_h + k)
        wa = f.nobs_a / (f.nobs_a + k)
        f[f"pace_blend{k}"] = f.pace_v2 * (1 + wh * (f.obs_h - 1) + wa * (f.obs_a - 1))
    return f


def main():
    f = build()
    out = [f"Games with box-score possessions + point-in-time KenPom tempo: {len(f)} ({f.date.min()}..{f.date.max()}); "
           f"OT games {int((f.ot > 0).sum())}. Actual tempo mean {f.tempo.mean():.2f}, projected (v2) mean {f.pace_v2.mean():.2f}."]
    err = f.tempo - f.pace_v2          # + = game was faster than projected
    out.append("\n######## PROJECTED vs ACTUAL POSSESSIONS (per 40 min; error = actual − projected)")
    segs = [("all", f.index == f.index), ("November", f.nov), ("Dec-Apr", ~f.nov),
            ("new-coach team in game", f.nc), ("new-coach, team < 8 games", f.nc & (f.min_games < 8)),
            ("no new-coach team", ~f.nc), ("neutral site", f.neutral.astype(bool)), ("true home/away", ~f.neutral.astype(bool)),
            ("either team < 4 games", f.min_games < 4), ("either team < 8 games", f.min_games < 8)]
    for lab, m in segs:
        out.append(f"  {lab:<28} v2 {stats(err[m])}")
    out.append("  by month: " + "; ".join(f"{mo}: MAE {err[f.month == mo].abs().mean():.2f} bias {err[f.month == mo].mean():+.2f}"
                                          for mo in ("11", "12", "01", "02", "03", "04") if (f.month == mo).any()))
    out.append("\n  alternative pace projections (all games | November | new-coach <8 games):")
    m = f.kp_pred_tempo.notna() & f.open_total.notna() & f.v1_total.notna()
    g = f[m].copy()
    ppp = g.v1_total / g.pace_v2
    g["pace_mkt"] = g.open_total / ppp      # market-implied pace at our efficiency
    g["pace_mkt_half"] = 0.5 * g.pace_mkt + 0.5 * g.pace_v2
    for col, lab in [("pace_v2", "v2 (live): Th×Ta/avg"), ("pace_v1", "v1 additive"), ("kp_pred_tempo", "KenPom FanMatch PredTempo"),
                     ("pace_blend2", "observed tempo blend, k=2"), ("pace_blend4", "observed tempo blend, k=4"),
                     ("pace_blend8", "observed tempo blend, k=8"), ("pace_mkt", "market-implied (open total ÷ our PPP)"),
                     ("pace_mkt_half", "½ v2 + ½ market-implied")]:
        e = g.tempo - g[col]
        nov, nce = g.nov, g.nc & (g.min_games < 8)
        out.append(f"    {lab:<38} MAE {e.abs().mean():5.2f} bias {e.mean():+5.2f} | Nov {e[nov].abs().mean():5.2f} "
                   f"{e[nov].mean():+5.2f} | new-coach {e[nce].abs().mean():5.2f} {e[nce].mean():+5.2f}  (n={len(e)})")
    out.append("    (blend: pace × (1 + Σ_team w·(observed residual − 1)), residual = team's mean actual ÷ projected pace so far, w = games/(games+k))")

    out.append("\n######## TOTALS ERROR: PACE vs EFFICIENCY (T − T̂ = (P − P̂)·Ê + P·(E − Ê), P incl. OT)")
    for mcol, lab in (("v1_total", "v1 totals (betting model)"), ("v2_total", "v2 calibrated total")):
        t = f[f[mcol].notna()].copy()
        Ph, Th = t.pace_v2, t[mcol]
        Eh = Th / Ph
        P, T = t.poss, t.total
        E = T / P
        pace_part, eff_part = (P - Ph) * Eh, P * (E - Eh)
        tot = T - Th
        v = np.var(tot)
        out.append(f"  {lab}: total error {stats(tot)}")
        out.append(f"    pace part {stats(pace_part)} | efficiency part {stats(eff_part)}")
        out.append(f"    variance share: pace {np.var(pace_part) / v:5.1%}, efficiency {np.var(eff_part) / v:5.1%}, "
                   f"2·cov {2 * np.cov(pace_part, eff_part)[0, 1] / v:+5.1%}; OT alone adds {((t.poss - t.tempo) * Eh).mean():+.2f} pts/game avg")
        for lb, mm in (("November", t.nov), ("Dec-Apr", ~t.nov), ("new-coach <8 games", t.nc & (t.min_games < 8)),
                       ("neutral", t.neutral.astype(bool))):
            out.append(f"    {lb:<20} total MAE {tot[mm].abs().mean():5.2f} bias {tot[mm].mean():+5.2f} | pace part bias "
                       f"{pace_part[mm].mean():+5.2f} MAE {pace_part[mm].abs().mean():5.2f} | eff part bias {eff_part[mm].mean():+5.2f} "
                       f"MAE {eff_part[mm].abs().mean():5.2f}")
    # how much would perfect pace knowledge help? and how much does the market know about pace?
    t = f[f.v1_total.notna() & f.open_total.notna()].copy()
    Eh = t.v1_total / t.pace_v2
    out.append("\n  hypothetical: v1 total with the ACTUAL tempo (no OT) instead of projected: "
               f"{stats(t.total - t.tempo * Eh)}  vs as-is {stats(t.total - t.v1_total)}; open line {stats(t.total - t.open_total)}")
    for k in (2, 4, 8):
        out.append(f"  v1 total re-paced with observed blend k={k}: {stats(t.total - t[f'pace_blend{k}'] * Eh)}"
                   f"  | Nov {stats((t.total - t[f'pace_blend{k}'] * Eh)[t.nov])}")
    out.append(f"  v1 as-is November: {stats((t.total - t.v1_total)[t.nov])}")
    out.append("""
######## READING (2024-25 + 2025-26, analysis only; no model change made)
  - Pace is ~1/3 of totals error variance; efficiency (shooting variance) ~2/3. Even a PERFECT pace forecast
    only cuts v1 total MAE ~13.3 -> ~11.9, so pace fixes are second-order vs efficiency.
  - KenPom AdjTempo snapshots already update in-season; blending in our own observed tempo (k=2/4/8) makes
    pace WORSE (MAE 3.25 -> 3.33-3.45) and the totals worse. Don't add a faster observed-tempo weight.
  - The consistent miss: games play SLOWER than projected early and at neutral sites: Nov -0.76 poss, neutral -0.73, either team <8 games -0.71
    (Dec-Mar ~0). That is roughly 1.5 points of total.
  - Best simple fix: (1) average v2 pace with the market-implied pace (open total / our PPP): MAE 3.22, best on new-coach <8g (3.49);
    (2) an additive early-season/neutral tempo adjustment of about -0.7 poss (Nov or <8 games, and neutral sites), fit walk-forward.
    Both are small gains. Validate on a walk-forward backtest before turning them on.""")
    txt = "\n".join(out)
    print(txt)
    with open("outputs/walkforward/pace_audit.txt", "w") as fh:
        fh.write(txt + "\n")


if __name__ == "__main__":
    main()
