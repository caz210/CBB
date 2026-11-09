# -*- coding: utf-8 -*-
"""
model.py
CBB betting model  all formulas match your documented Google Sheets logic.
Column names match the official KenPom API response fields exactly.
"""

import os

import numpy as np
import pandas as pd

from czarp.config import ModelConfig, load_config, feature_weights
from czarp.probability import win_prob
from czarp.season import season_progress
from czarp import teamstats


#  Helpers 

def load_data(data_dir: str = "data") -> dict[str, pd.DataFrame]:
    data = {
        "ratings":      pd.read_csv(f"{data_dir}/ratings.csv"),
        "four_factors": pd.read_csv(f"{data_dir}/four_factors.csv"),
        "height":       pd.read_csv(f"{data_dir}/height.csv"),
    }
    try:
        data["net"] = pd.read_csv(f"{data_dir}/net.csv")
        print(f"    NET rankings loaded ({len(data['net'])} teams)")
    except FileNotFoundError:
        print("     net.csv not found  adjustment metric will use KenPom only")
        data["net"] = pd.DataFrame(columns=["TeamName", "Rank"])
    # v2 optional inputs
    misc_path = f"{data_dir}/misc.csv"
    data["misc"] = pd.read_csv(misc_path) if os.path.exists(misc_path) else pd.DataFrame()
    hca_path = f"{data_dir}/hca.csv"
    data["hca"] = pd.read_csv(hca_path) if os.path.exists(hca_path) else pd.DataFrame(columns=["TeamName", "HCA"])
    prior_dir = f"{data_dir}/prior"
    if os.path.exists(f"{prior_dir}/ratings.csv"):
        data["prior"] = {k: pd.read_csv(f"{prior_dir}/{f}.csv")
                         for k, f in (("ratings", "ratings"), ("four_factors", "four_factors"),
                                      ("height", "height"), ("misc", "misc"))
                         if os.path.exists(f"{prior_dir}/{f}.csv")}
    return data


# KenPom name  model name mapping for common mismatches
TEAM_NAME_MAP = {
    "Southern California":  "USC",
    "UT San Antonio":      "UTSA",
    "Miami OH":           "Miami (OH)",
    "Miami FL":           "Miami (FL)",
    "St. Mary's":         "Saint Mary's",
    "UCSB":               "UC Santa Barbara",
    "UNC":                "North Carolina",
    "UNCW":               "UNC Wilmington",
    "LIU":                "LIU Brooklyn",
    "Detroit":            "Detroit Mercy",
    "Pitt":               "Pittsburgh",
    "Ole Miss":           "Mississippi",
    "SIUE":               "SIU Edwardsville",
    "TAM C. Christi":     "Texas A&M Corpus Christi",
    "Tex. A&M Corpus":    "Texas A&M Corpus Christi",
    "Queens":             "Queens NC",
}

def get_team(df: pd.DataFrame, team_name: str) -> pd.Series:
    """Exact-first, then partial match. Applies name normalization for common mismatches."""
    names_lower = df["TeamName"].astype(str).str.lower()
    # Exact match on the name as given (v2: before the alias map, so a real
    # KenPom name like "Queens" isn't remapped to a non-existent "Queens NC")
    exact = df[names_lower == str(team_name).lower()]
    if not exact.empty:
        return exact.iloc[0]

    # Apply known name mappings
    lookup = TEAM_NAME_MAP.get(team_name, team_name)

    exact = df[names_lower == lookup.lower()]
    if not exact.empty:
        return exact.iloc[0]

    # Fall back to partial match
    partial = df[df["TeamName"].astype(str).str.contains(lookup, case=False, na=False, regex=False)]
    if partial.empty:
        raise ValueError(f"Team not found: '{team_name}'. Check KenPom spelling.")
    if len(partial) > 1:
        # Prefer exact word boundary matches to avoid Purdue/Purdue Fort Wayne type issues
        tighter = partial[partial["TeamName"].str.lower().str.startswith(lookup.lower())]
        if len(tighter) == 1:
            return tighter.iloc[0]
    return partial.iloc[0]


def compute_ncaa_averages(ratings: pd.DataFrame, ff: pd.DataFrame, height: pd.DataFrame) -> dict:
    """Compute NCAA-wide averages used throughout the model."""
    def m(df, *cols):
        for c in cols:
            if df is not None and c in df.columns:
                return pd.to_numeric(df[c], errors="coerce").mean()
        return float("nan")
    return {
        # Ratings
        "pace":           ratings["AdjTempo"].mean(),
        "eff":            ratings["AdjOE"].mean(),   # national avg efficiency (pts/100)
        "n_teams":        len(ratings),
        # Four Factors (API field names)
        "to_pct":         m(ff, "TO_Pct"),          # Offensive turnover %
        "dto_pct":        m(ff, "DTO_Pct", "Def_TO_Pct"),          # Defensive turnovers forced %
        "or_pct":         m(ff, "OR_Pct"),           # Offensive rebound %
        "dor_pct":        m(ff, "DOR_Pct", "Def_OR_Pct"),          # Defensive offensive rebound allowed %
        "ft_rate":        m(ff, "FT_Rate"),          # FT rate (offense)
        "dft_rate":       m(ff, "DFT_Rate", "Def_FT_Rate"),         # FT rate allowed (defense)
        # Height
        "avg_hgt":        m(height, "AvgHgt"),
        "avg_exp":        m(height, "Exp"),
        "avg_bench":      m(height, "Bench"),
    }


#  Adjustment Metric (Percentile) 

def compute_team_percentile(
    team_name: str,
    ratings: pd.DataFrame,
    net: pd.DataFrame = None,   # kept for signature compat, no longer used
) -> tuple[float, dict]:
    """
    Percentile = (n_teams - KenPom_rank) / n_teams
    Result is 0-1 where 1.0 = best team (#1), ~0.0 = worst.
    n_teams is derived from the ratings DataFrame so it never needs hardcoding.
    """
    t_kp    = get_team(ratings, team_name)
    n_teams = len(ratings)          # e.g. 365 -- pulled live, never hardcoded
    kp_rank = float(t_kp["RankAdjEM"])
    pct     = (n_teams - kp_rank) / n_teams

    debug = {
        "kp_rank":  kp_rank,
        "net_rank": None,           # NET removed from formula
        "kp_pct":   pct,
        "net_pct":  None,
        "combined": pct,
        "n_teams":  n_teams,
    }
    return pct, debug


def compute_game_adjustment(home_pct: float, away_pct: float) -> tuple[float, float]:
    """
    Computes the per-game adjustment metric used in TO/REB/FT formulas.
    Matches sheet formula: U2_away = (Home_KP_pct - Away_KP_pct) * 0.5
                           U2_home = (Away_KP_pct - Home_KP_pct) * 0.5
    Result is a small signed decimal (e.g. +0.0103 for away, -0.0103 for home).
    Positive = that team is the weaker team in this matchup (gets upward adjustment).
    """
    away_adj = (home_pct - away_pct) * 0.5
    home_adj = (away_pct - home_pct) * 0.5
    return home_adj, away_adj


#  Core Model Formulas 

def projected_pace(t1_tempo: float, t2_tempo: float, avg_pace: float) -> float:
    """Projected Pace = NCAA Avg + (T1 Pace  Avg) + (T2 Pace  Avg)"""
    return avg_pace + (t1_tempo - avg_pace) + (t2_tempo - avg_pace)


def points_per_possession(off_eff: float, opp_def_eff: float, avg_eff: float | None = None) -> float:
    """v2 (KenPom multiplicative): AdjOE * Opp AdjDE / national avg / 100.
    v1 (additive, used when avg_eff is None): AVERAGE(AdjOE, Opp AdjDE) / 100.
    The additive version compresses mismatches (great O vs bad D gets averaged
    toward the middle), which is a big reason v1 lines were too tight."""
    if avg_eff:
        return off_eff * opp_def_eff / avg_eff / 100
    return ((off_eff + opp_def_eff) / 2) / 100


def projected_pace_mult(t1_tempo: float, t2_tempo: float, avg_pace: float) -> float:
    """KenPom-style multiplicative tempo: T1 * T2 / avg."""
    return t1_tempo * t2_tempo / avg_pace


def projected_turnovers(
    team_to_pct: float,   # Team offense TO_Pct (own ball security)
    opp_dto_pct: float,   # Opponent defense DTO_Pct (how often they force TOs)
    avg_to: float,        # NCAA avg offense TO_Pct
    avg_dto: float,       # NCAA avg defense DTO_Pct
    adjustment: float,    # KenPom quality dampener
) -> float:
    """
    Raw = average of two components:
      term1 = avg_TO  - team_TO   : own ball security  (positive = turns over LESS than avg)
      term2 = avg_DTO - opp_DTO   : opp forcing ability (positive = faces WORSE than avg forcing D)

    The old formula used MINUS which inverted the DTO effect:
      elite opp forcing D → avg_DTO - opp_DTO is NEGATIVE → subtracting it ADDED to your advantage
      terrible opp forcing D → positive → subtracting it HURT you
    Both completely backwards. Fix: ADD the terms and average to avoid double-counting.

    Positive result = favorable TO battle (keep possession, face weak forcing D)
    Adj = Raw + abs(Raw) * adjustment
    """
    term1 = avg_to  - team_to_pct   # own ball security vs avg
    term2 = avg_dto - opp_dto_pct   # opp forcing ability vs avg
    raw = (term1 + term2) / 2
    return raw + abs(raw) * adjustment


def projected_rebounds(
    team_or_pct: float,   # Team offensive rebound %
    opp_dor_pct: float,   # Opponent defensive rebound % allowed
    avg_or: float,
    avg_dor: float,
    adjustment: float,
) -> float:
    """
    Raw = average of two components:
      term1 = opp_DOR - avg_DOR  : how much WORSE than avg opp is at defending OReb
                                   (positive = opp gives up more boards than avg)
      term2 = team_OR - avg_OR   : how much BETTER than avg team is at OReb
                                   (positive = team crashes harder than avg)

    Both terms describe the same event (missed shot → OReb), so summing them
    double-counts the magnitude. Averaging gives one clean possession-unit signal.

    Positive = favorable rebounding matchup (more extra possessions from OReb).
    Adj = Raw + abs(Raw) * adjustment
    """
    term1 = opp_dor_pct - avg_dor    # opp weakness at defending OReb
    term2 = team_or_pct - avg_or     # team strength at OReb
    raw = (term1 + term2) / 2
    return raw + abs(raw) * adjustment


def projected_ft(
    team_def_ft: float,   # Team's defensive FT rate (DFT_Rate  how often they foul)
    team_off_ft: float,   # Team's offensive FT rate (FT_Rate  how often they draw FTs)
    adjustment: float,
) -> float:
    """
    Raw = team_def_FTRate - team_off_FTRate  (same team's own stats)
    Measures FT possession imbalance: do they foul more than they draw?
    Negative = high-FT drawing team (Texas: -17.72  gets to line a lot)
    Positive = team defends FTs while drawing fewer (Georgia: +3.81)
    Adj = Raw + abs(Raw) * adjustment
    """
    raw = team_def_ft - team_off_ft
    return raw + abs(raw) * adjustment


def adjusted_possessions(pace: float, proj_reb: float, proj_to: float) -> float:
    """
    adj_poss = pace + pace*(reb*0.01) + pace*(to*0.01)

    FT is NOT included — drawing FTs is a scoring/efficiency edge,
    not a possession count adjustment. It was distorting totals by
    stripping 5+ possessions from high-FT-drawing teams like VCU.
    """
    raw_delta = (
        pace * (proj_reb * 0.01) +
        pace * (proj_to  * 0.01)
    )
    return pace + raw_delta


def unit_score(avg_hgt: float, exp: float, bench: float, avgs: dict, n_teams: float) -> float:
    """
    Unit Score = ((AvgHgt/NCAA_AvgHgt)*40 + (Exp/NCAA_AvgExp)*40 + (Bench/NCAA_AvgBench)*20) / 10
    All three components normalized against NCAA average, not n_teams.
    """
    return (
        (avg_hgt / avgs["avg_hgt"])   * 40 +
        (exp     / avgs["avg_exp"])   * 40 +
        (bench   / avgs["avg_bench"]) * 20
    ) / 10


def unit_score_adjustments(u1: float, u2: float) -> tuple[float, float]:
    """Returns (team1_bonus, team2_bonus) added to final projected scores."""
    return (u1 - u2) * 0.25, (u2 - u1) * 0.25


def hca_adjustments(hca: float, team1_is_home: bool | None) -> tuple[float, float]:
    """
    Home team gets +HCA*0.5, away team gets -HCA*0.5.
    Pass team1_is_home=None for neutral site (no adjustment).
    """
    if team1_is_home is None:
        return 0.0, 0.0
    if team1_is_home:
        return hca * 0.5, -hca * 0.5
    return -hca * 0.5, hca * 0.5


#  Full Game Projection (v2) 

HCA_VALUE = 3.5  # League-average fallback; per-team values come from data["hca"] (hca_fetcher.py)


def mround(value: float, multiple: float = 0.5) -> float:
    """Round to nearest multiple (like Excel MROUND). DISPLAY ONLY in v2 —
    the model keeps full precision internally."""
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return value
    return round(value / multiple) * multiple


def hca_adjustments(hca: float, team1_is_home: bool | None, split: float = 0.5) -> tuple[float, float]:
    """Home team gets +HCA*split, away team -HCA*split (full swing = HCA when split=0.5
    ... i.e. margin moves by HCA). Pass team1_is_home=None for neutral site."""
    if team1_is_home is None:
        return 0.0, 0.0
    if team1_is_home:
        return hca * split, -hca * split
    return -hca * split, hca * split


def team_hca(team_name: str, hca_df: pd.DataFrame | None, default: float = HCA_VALUE) -> tuple[float, str]:
    """Per-team HCA from KenPom hca.php; a team missing from the table gets the
    table average; no table at all -> `default`."""
    if hca_df is not None and not hca_df.empty and "HCA" in hca_df.columns:
        default = float(pd.to_numeric(hca_df["HCA"], errors="coerce").mean())
        try:
            row = get_team(hca_df, team_name)
            v = float(row["HCA"])
            if not np.isnan(v):
                return v, "kenpom_hca"
        except ValueError:
            pass
        return default, "kenpom_avg"
    return default, "default"


# Clutch: v1 looked for a "ClutchScore" column that KenPom never provides (and
# fetched the wrong endpoint), so it was always 0. v2 builds an execution score
# from real fields — eFG% (shot quality), TO% (ball security), FT% (closing at
# the line) — z-scored nationally, applied only to close projected games.
def compute_clutch_score(team_name: str, misc: pd.DataFrame = None, *, ctx: dict | None = None) -> float:
    if ctx is None:
        return 0.0
    try:
        return float(ctx["clutch"].get(team_name, 0.0))
    except Exception:
        return 0.0


def clutch_margin_adjustment(margin: float, c1: float, c2: float, cfg) -> float:
    """Points added to team1's margin from clutch execution (0 unless |margin| <= threshold)."""
    if not cfg.enabled or abs(margin) > cfg.spread_threshold:
        return 0.0
    adj = (c1 - c2) * cfg.points_per_z
    return float(np.clip(adj, -cfg.max_adj, cfg.max_adj))


_CTX_CACHE: dict = {}


def _context(data: dict, cfg: ModelConfig) -> dict:
    """Team table + tags + clutch composite, cached per ratings frame + config."""
    ratings = data["ratings"]
    key = (id(ratings), id(data.get("four_factors")), id(data.get("misc")), id(data.get("height")),
           id(data.get("prior")), cfg.matchup.top_pct, repr(cfg.early),
           cfg.clutch.w_efg, cfg.clutch.w_to, cfg.clutch.w_ft)
    hit = _CTX_CACHE.get(key)
    if hit is not None and hit["ratings"] is ratings:
        return hit
    prior = data.get("prior") if cfg.early.style_fill != "off" else None
    tt = teamstats.build_team_table(data, prior=prior, early=cfg.early)
    tags = teamstats.mark_tentative(teamstats.tag_teams(tt, cfg.matchup), tt, cfg.early)
    clutch = teamstats.clutch_composite(tt, cfg.clutch.w_efg, cfg.clutch.w_to, cfg.clutch.w_ft)
    ctx = {"ratings": ratings, "tt": tt, "tags": tags, "clutch": clutch,
           "prior_filled": list(tt.attrs.get("prior_filled", []))}
    if len(_CTX_CACHE) > 16:
        _CTX_CACHE.clear()
    _CTX_CACHE[key] = ctx
    return ctx


def _f(x) -> float:
    try:
        return float(x)
    except (TypeError, ValueError):
        return float("nan")


def _unit_score_row(row, avgs, cfg) -> float:
    vals = [_f(row.get("AvgHgt")), _f(row.get("Exp")), _f(row.get("Bench"))]
    if any(np.isnan(v) for v in vals) or any(np.isnan(avgs[k]) or avgs[k] == 0
                                             for k in ("avg_hgt", "avg_exp", "avg_bench")):
        return float("nan")
    u = cfg.unit
    return ((vals[0] / avgs["avg_hgt"]) * u.w_height + (vals[1] / avgs["avg_exp"]) * u.w_exp
            + (vals[2] / avgs["avg_bench"]) * u.w_bench) / 10


def _z(tt: pd.DataFrame, col: str, team: str) -> float:
    if col not in tt:
        raise KeyError(f"custom feature column {col!r} not in team table")
    s = pd.to_numeric(tt[col], errors="coerce")
    sd = s.std()
    v = _f(s.get(team))
    if np.isnan(v) or not sd or np.isnan(sd):
        return 0.0
    return (v - s.mean()) / sd


def custom_feature_adjustment(n1: str, n2: str, tt: pd.DataFrame, custom: list) -> tuple[float, float]:
    """(team1 margin points, total points) from config-defined stat features."""
    dm = dt = 0.0
    for f in custom:
        w = float(f.get("weight", 0.0))
        if f["kind"] == "stat_diff":
            z1, z2 = _z(tt, f["stat"], n1), _z(tt, f["stat"], n2)
            if f.get("target", "margin") == "total":
                dt += w * (z1 + z2)
            else:
                dm += w * (z1 - z2)
        else:  # stat_matchup
            o, d = f["off"], f["def"]
            e1 = _z(tt, o, n1) + _z(tt, d, n2)
            e2 = _z(tt, o, n2) + _z(tt, d, n1)
            if f.get("target", "margin") == "total":
                dt += w * (e1 + e2)
            else:
                dm += w * (e1 - e2)
    return dm, dt


def project_game(
    team1: str,
    team2: str,
    team1_is_home,
    data: dict,
    game_time: str = None,
    game_date: str = None,
    config: ModelConfig | None = None,
) -> dict:
    """Project team1 vs team2. team1_is_home: True / False / None (neutral).

    Pipeline (all full precision; rounding happens only in the display fields):
      1. pace = T1*T2/avg (multiplicative)            [config flag -> v1 additive]
      2. PPP  = AdjOE * opp AdjDE / avg_eff / 100      [config flag -> v1 average]
      3. base score = PPP * pace  (four factors are NOT re-added: AdjOE/AdjDE
         already contain them — v1 double counted TO/REB)
      4. quality-gap bump (KenPom percentile gap)
      5. matchup cards (archetype counters / exploits)
      6. unit score (height/exp/bench), optional extra weight close/neutral/late
      7. per-team HCA (KenPom hca.php, fallback avg)
      8. clutch execution, only when |margin| <= 5
      9. win probability from margin ~ Normal(sd)
    """
    cfg = config or data.get("config") or load_config()
    fw = feature_weights(cfg)
    W = fw["builtin"]
    ratings = data["ratings"]
    ff      = data.get("four_factors")
    height  = data.get("height")
    net     = data.get("net")
    ctx = _context(data, cfg)
    tt, tags = ctx["tt"], ctx["tags"]

    avgs = compute_ncaa_averages(ratings, ff, height)
    # prefer averages from the (sanitized / prior-filled) team table
    for k, col in (("avg_hgt", "AvgHgt"), ("avg_exp", "Exp"), ("avg_bench", "Bench"),
                   ("to_pct", "TO"), ("dto_pct", "D_TO"), ("or_pct", "OR"),
                   ("dor_pct", "Opp_OR"), ("ft_rate", "FTR"), ("dft_rate", "D_FTR")):
        if col in tt and tt[col].notna().any():
            avgs[k] = tt[col].mean()

    t1_r = get_team(ratings, team1)
    t2_r = get_team(ratings, team2)
    n1, n2 = str(t1_r["TeamName"]), str(t2_r["TeamName"])
    s1, s2 = tt.loc[n1], tt.loc[n2]
    g1, g2 = tags.loc[n1], tags.loc[n2]

    t1_pct, adj1_debug = compute_team_percentile(n1, ratings, net)
    t2_pct, adj2_debug = compute_team_percentile(n2, ratings, net)
    adj1, adj2 = compute_game_adjustment(t1_pct, t2_pct)

    # 1-3. pace, PPP, base scores
    if cfg.use_multiplicative_efficiency:
        pace = projected_pace_mult(_f(t1_r["AdjTempo"]), _f(t2_r["AdjTempo"]), avgs["pace"])
        t1_ppp = points_per_possession(_f(t1_r["AdjOE"]), _f(t2_r["AdjDE"]), avgs["eff"])
        t2_ppp = points_per_possession(_f(t2_r["AdjOE"]), _f(t1_r["AdjDE"]), avgs["eff"])
    else:
        pace = projected_pace(_f(t1_r["AdjTempo"]), _f(t2_r["AdjTempo"]), avgs["pace"])
        t1_ppp = points_per_possession(_f(t1_r["AdjOE"]), _f(t2_r["AdjDE"]))
        t2_ppp = points_per_possession(_f(t2_r["AdjOE"]), _f(t1_r["AdjDE"]))
    base1, base2 = t1_ppp * pace, t2_ppp * pace
    # fitted calibration: widen the efficiency margin, calibrate the total
    bt = cfg.total_intercept + cfg.total_slope * (base1 + base2)
    bm = cfg.efficiency_margin_scale * (base1 - base2) * W["efficiency"]
    base1, base2 = (bt + bm) / 2, (bt - bm) / 2
    t1_score, t2_score = base1, base2

    # 4. quality gap (replaces v1's TO/REB dampener)
    qg = cfg.quality_gap.coef * (t1_pct - t2_pct) * 10 * W["quality_gap"] if cfg.quality_gap.enabled else 0.0
    t1_score += qg / 2
    t2_score -= qg / 2

    # 5. matchup cards
    m1, notes1 = teamstats.matchup_adjustment(g1, g2, cfg.matchup)
    m2, notes2 = teamstats.matchup_adjustment(g2, g1, cfg.matchup)
    # tentative archetypes (too few current-season games) count for less
    if not getattr(cfg.matchup, "apply_to_score", True):
        m1, m2 = 0.0, 0.0
    tm = cfg.early.tentative_tag_mult
    if bool(g1.get("tentative", False)) or bool(g2.get("tentative", False)):
        m1, m2 = m1 * tm, m2 * tm
    m1, m2 = m1 * W["matchup"], m2 * W["matchup"]
    t1_score += m1
    t2_score += m2

    # 7. HCA (computed before unit score so the close-game test sees it)
    if team1_is_home is None:
        hca_val, hca_src = 0.0, "neutral"
    else:
        home = n1 if team1_is_home else n2
        hca_val, hca_src = team_hca(home, data.get("hca"), cfg.default_hca)
        hca_val *= cfg.hca_scale * W["hca"]
    h1_adj, h2_adj = hca_adjustments(hca_val, team1_is_home, cfg.hca_split)

    # 6. unit score with optional extra weight
    u1 = _unit_score_row(s1, avgs, cfg)
    u2 = _unit_score_row(s2, avgs, cfg)
    scale = cfg.unit.scale
    pre_margin = (t1_score + h1_adj) - (t2_score + h2_adj)
    mult = 1.0
    if abs(pre_margin) <= cfg.unit.close_game_threshold:
        mult *= cfg.unit.close_game_mult
    if team1_is_home is None:
        mult *= cfg.unit.neutral_mult
    if game_date:
        prog = season_progress(game_date)
        if prog > cfg.unit.late_season_start:
            ramp = (prog - cfg.unit.late_season_start) / (1 - cfg.unit.late_season_start)
            mult *= 1 + (cfg.unit.late_season_mult - 1) * ramp
    if np.isnan(u1) or np.isnan(u2):
        u1_adj = u2_adj = 0.0
    else:
        u1_adj = (u1 - u2) * scale * mult * W["unit"]
        u2_adj = -u1_adj
    t1_score += u1_adj + h1_adj
    t2_score += u2_adj + h2_adj

    # custom config-defined features (z-scored team-table stats)
    cust_m, cust_t = custom_feature_adjustment(n1, n2, tt, fw["custom"])
    t1_score += cust_m / 2 + cust_t / 2
    t2_score += -cust_m / 2 + cust_t / 2

    # 8. clutch
    c1 = float(ctx["clutch"].get(n1, 0.0))
    c2 = float(ctx["clutch"].get(n2, 0.0))
    clutch_adj = clutch_margin_adjustment(t1_score - t2_score, c1, c2, cfg.clutch) * W["clutch"]
    t1_score += clutch_adj / 2
    t2_score -= clutch_adj / 2

    margin = t1_score - t2_score
    total = t1_score + t2_score
    p1 = win_prob(margin, cfg.betting.margin_sd)

    # Legacy matchup indicators (display only; NOT used in the score any more)
    def nz(x):
        return 0.0 if np.isnan(x) else x
    t1_to  = projected_turnovers(nz(_f(s1["TO"])), nz(_f(s2["D_TO"])), nz(avgs["to_pct"]), nz(avgs["dto_pct"]), adj1)
    t2_to  = projected_turnovers(nz(_f(s2["TO"])), nz(_f(s1["D_TO"])), nz(avgs["to_pct"]), nz(avgs["dto_pct"]), adj2)
    t1_reb = projected_rebounds(nz(_f(s1["OR"])), nz(_f(s2["Opp_OR"])), nz(avgs["or_pct"]), nz(avgs["dor_pct"]), adj1)
    t2_reb = projected_rebounds(nz(_f(s2["OR"])), nz(_f(s1["Opp_OR"])), nz(avgs["or_pct"]), nz(avgs["dor_pct"]), adj2)
    t1_ft  = projected_ft(nz(_f(s2["D_FTR"])), nz(_f(s1["FTR"])), adj1)
    t2_ft  = projected_ft(nz(_f(s1["D_FTR"])), nz(_f(s2["FTR"])), adj2)

    loc = "home" if team1_is_home else ("away" if team1_is_home is False else "neutral")
    out = {
        "team1":             team1,
        "team2":             team2,
        "game_time":         game_time,
        "model_version":     getattr(cfg, "model_version", "czarp-v2"),
        "custom_margin_adj": float(cust_m),
        "custom_total_adj":  float(cust_t),
        "projected_pace":    round(pace, 1),
        # display (rounded to 0.5)
        "team1_score":       mround(float(t1_score), 0.5),
        "team2_score":       mround(float(t2_score), 0.5),
        "spread":            mround(float(margin), 0.5),
        "total":             mround(float(total), 0.5),
        # full precision (use these for edges / probabilities / backtests)
        "team1_score_raw":   float(t1_score),
        "team2_score_raw":   float(t2_score),
        "spread_raw":        float(margin),
        "total_raw":         float(total),
        "pace_raw":          float(pace),
        "team1_win_prob":    float(p1),
        "team2_win_prob":    float(1 - p1),
        "team1_tags":        list(g1.get("tags_display", g1["tags"])),
        "team2_tags":        list(g2.get("tags_display", g2["tags"])),
        "team1_tag_objs":    list(g1.get("tag_objs_display", g1.get("tag_objs", []))),
        "team2_tag_objs":    list(g2.get("tag_objs_display", g2.get("tag_objs", []))),
        "tags_tentative":    bool(g1.get("tentative", False)) or bool(g2.get("tentative", False)),
        "early_season_fill": cfg.early.style_fill,
        "matchup_notes":     notes1 + [f"{n2}: {x}" for x in notes2] if notes2 else notes1,
        "adjustments": {
            "base_margin": float(base1 - base2), "quality_gap": float(qg), "matchup_t1": m1, "matchup_t2": m2,
            "unit": float(u1_adj - u2_adj), "unit_mult": float(mult), "hca": float(h1_adj - h2_adj), "hca_value": float(hca_val),
            "hca_source": hca_src, "clutch": clutch_adj,
        },
        "stats_from_prior_season": ctx["prior_filled"],
        "team1_clutch":      round(c1, 3),
        "team2_clutch":      round(c2, 3),
        "team1_adj_metric":  round(adj1, 6),
        "team2_adj_metric":  round(adj2, 6),
        "team1_kp_pct":      round(t1_pct, 4),
        "team2_kp_pct":      round(t2_pct, 4),
        "team1_unit_score":  None if np.isnan(u1) else round(u1, 3),
        "team2_unit_score":  None if np.isnan(u2) else round(u2, 3),
        "team1_ppp":         round(t1_ppp, 4),
        "team2_ppp":         round(t2_ppp, 4),
        "location":          loc,
        "debug": {
            "kenpom_rank_t1": adj1_debug["kp_rank"],   "kenpom_rank_t2": adj2_debug["kp_rank"],
            "net_rank_t1":    None,                      "net_rank_t2":    None,
            "kp_pct_t1":      adj1_debug["kp_pct"],     "kp_pct_t2":      adj2_debug["kp_pct"],
            "net_pct_t1":     None,                      "net_pct_t2":     None,
            "avg_pace":       round(avgs["pace"], 2),
            "avg_eff":        round(avgs["eff"], 3),
            "t1_tempo":       _f(t1_r["AdjTempo"]),     "t2_tempo":       _f(t2_r["AdjTempo"]),
            "t1_adjoe":       _f(t1_r["AdjOE"]),        "t2_adjoe":       _f(t2_r["AdjOE"]),
            "t1_adjde":       _f(t1_r["AdjDE"]),        "t2_adjde":       _f(t2_r["AdjDE"]),
            "t1_to_pct":      _f(s1["TO"]),             "t2_to_pct":      _f(s2["TO"]),
            "t1_dto_pct":     _f(s1["D_TO"]),           "t2_dto_pct":     _f(s2["D_TO"]),
            "avg_to_pct":     round(nz(avgs["to_pct"]), 6),
            "t1_to":          round(t1_to, 4),           "t2_to":          round(t2_to, 4),
            "t1_or_pct":      _f(s1["OR"]),             "t2_or_pct":      _f(s2["OR"]),
            "t1_dor_pct":     _f(s1["Opp_OR"]),         "t2_dor_pct":     _f(s2["Opp_OR"]),
            "t1_reb":         round(t1_reb, 4),          "t2_reb":         round(t2_reb, 4),
            "t1_ft_rate":     _f(s1["FTR"]),            "t2_ft_rate":     _f(s2["FTR"]),
            "t1_dft_rate":    _f(s1["D_FTR"]),          "t2_dft_rate":    _f(s2["D_FTR"]),
            "t1_ft":          round(t1_ft, 4),           "t2_ft":          round(t2_ft, 4),
            "t1_poss":        round(pace, 4),            "t2_poss":        round(pace, 4),
            "t1_hgt":         _f(s1["AvgHgt"]),         "t2_hgt":         _f(s2["AvgHgt"]),
            "t1_exp":         _f(s1["Exp"]),            "t2_exp":         _f(s2["Exp"]),
            "t1_bench":       _f(s1["Bench"]),          "t2_bench":       _f(s2["Bench"]),
            "u1_adj":         round(u1_adj, 3),          "u2_adj":         round(u2_adj, 3),
            "h1_adj":         round(h1_adj, 2),          "h2_adj":         round(h2_adj, 2),
            "m1_adj":         round(m1, 3),              "m2_adj":         round(m2, 3),
            "qg_adj":         round(qg, 3),              "clutch_adj":     round(clutch_adj, 3),
        }
    }
    # NaN (stat not available yet) -> None so Excel/JSON consumers don't choke
    out["debug"] = {k: (None if isinstance(v, float) and np.isnan(v) else v) for k, v in out["debug"].items()}
    return out


if __name__ == "__main__":
    data = load_data()
    result = project_game("Duke", "Kentucky", team1_is_home=True, data=data)
    print("\n Game Projection")
    print("=" * 38)
    for k, v in result.items():
        print(f"  {k:<25} {v}")
