# -*- coding: utf-8 -*-
"""
model.py
CBB betting model  all formulas match your documented Google Sheets logic.
Column names match the official KenPom API response fields exactly.
"""

import pandas as pd


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
    return data


# KenPom name  model name mapping for common mismatches
TEAM_NAME_MAP = {
    "Miami OH":           "Miami (OH)",
    "Miami FL":           "Miami (FL)",
    "St. Mary's":         "Saint Mary's",
    "UCSB":               "UC Santa Barbara",
    "UNC":                "North Carolina",
    "UNCW":               "UNC Wilmington",
    "LIU":                "LIU Brooklyn",
    "Detroit":            "Detroit Mercy",
    "UTSA":               "UT San Antonio",
    "Pitt":               "Pittsburgh",
    "USC":                "Southern California",
    "Southern Cal":       "Southern California",
    "Ole Miss":           "Mississippi",
    "SIUE":               "SIU Edwardsville",
    "TAM C. Christi":     "Texas A&M Corpus Christi",
    "Tex. A&M Corpus":    "Texas A&M Corpus Christi",
    "Queens":             "Queens NC",
}

def get_team(df: pd.DataFrame, team_name: str) -> pd.Series:
    """Exact-first, then partial match. Applies name normalization for common mismatches."""
    # Apply known name mappings
    lookup = TEAM_NAME_MAP.get(team_name, team_name)

    # Try exact match first
    exact = df[df["TeamName"].str.lower() == lookup.lower()]
    if not exact.empty:
        return exact.iloc[0]

    # Fall back to partial match
    partial = df[df["TeamName"].str.contains(lookup, case=False, na=False)]
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
    return {
        # Ratings
        "pace":           ratings["AdjTempo"].mean(),
        "n_teams":        len(ratings),
        # Four Factors (API field names)
        "to_pct":         ff["TO_Pct"].mean(),          # Offensive turnover %
        "dto_pct":        ff["DTO_Pct"].mean(),          # Defensive turnovers forced %
        "or_pct":         ff["OR_Pct"].mean(),           # Offensive rebound %
        "dor_pct":        ff["DOR_Pct"].mean(),          # Defensive offensive rebound allowed %
        "ft_rate":        ff["FT_Rate"].mean(),          # FT rate (offense)
        "dft_rate":       ff["DFT_Rate"].mean(),         # FT rate allowed (defense)
        # Height
        "avg_hgt":        height["AvgHgt"].mean(),
        "avg_exp":        height["Exp"].mean(),
        "avg_bench":      height["Bench"].mean(),
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


def compute_game_adjustment(t1_pct: float, t2_pct: float) -> tuple[float, float]:
    """
    Quality-gap dampener used in TO / REB / FT factor formulas.

    adj1 = (t2_pct - t1_pct) * 0.5
    adj2 = (t1_pct - t2_pct) * 0.5

    Always team1 vs team2 — location is irrelevant here.
    HCA is applied separately via hca_adjustments().

    Sign meaning:
      - If t1 is weaker  (lower pct): adj1 > 0 → nudges their raw factors upward
      - If t1 is stronger (higher pct): adj1 < 0 → dampens their raw advantage
    The math self-corrects regardless of who is home.
    """
    adj1 = (t2_pct - t1_pct) * 0.5
    adj2 = (t1_pct - t2_pct) * 0.5
    return adj1, adj2


#  Core Model Formulas 

def projected_pace(t1_tempo: float, t2_tempo: float, avg_pace: float) -> float:
    """Projected Pace = NCAA Avg + (T1 Pace  Avg) + (T2 Pace  Avg)"""
    return avg_pace + (t1_tempo - avg_pace) + (t2_tempo - avg_pace)


def points_per_possession(off_eff: float, opp_def_eff: float) -> float:
    """=AVERAGE(Team AdjOE, Opp AdjDE) / 100"""
    return ((off_eff + opp_def_eff) / 2) / 100


def projected_turnovers(
    team_to_pct: float,   # Opponent offense TO_Pct (we want THEM to turn it over)
    opp_dto_pct: float,   # Our defense DTO_Pct (turnovers we force)
    avg_to: float,        # NCAA avg offense TO_Pct
    avg_dto: float,       # NCAA avg defense DTO_Pct
    adjustment: float,    # Game-level adj: small signed decimal e.g. +0.0103
) -> float:
    """
    Sheet formula: Raw = (Avg TO - Opp TO) - (Avg DTO - Team DTO)
    Adj = Raw + abs(Raw) * adjustment
    Signed  positive means favorable TO matchup (they turn it over more, we force more).
    """
    raw = (avg_to - team_to_pct) - (avg_dto - opp_dto_pct)
    return raw + abs(raw) * adjustment


def projected_rebounds(
    team_or_pct: float,   # Team offensive rebound %
    opp_dor_pct: float,   # Opponent defensive rebound % allowed
    avg_or: float,
    avg_dor: float,
    adjustment: float,
) -> float:
    """
    Sheet formula: Raw = (Opp DOR - Avg DOR) - (Avg OR - Team OR)
    Adj = Raw + abs(Raw) * adjustment
    Signed  positive means favorable rebound matchup.
    """
    raw = (opp_dor_pct - avg_dor) - (avg_or - team_or_pct)
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


def adjusted_possessions(pace: float, proj_reb: float, proj_to: float, proj_ft: float) -> float:
    """
    raw_delta = pace*(reb*0.01) + pace*(to*0.01) + pace*(ft*0.44*0.01)
    adj_poss  = pace + raw_delta

    raw_delta is already in possession units (pace x pct), so we add it
    directly to pace. The previous formula applied pace*(raw_delta*0.01)
    which double-scaled: treating possession units as a percentage again.
    FTs were hit hardest because proj_ft values are largest in magnitude.
    """
    raw_delta = (
        pace * (proj_reb * 0.01) +
        pace * (proj_to  * 0.01) +
        pace * (proj_ft  * 0.44 * 0.01)
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
    return (u1 - u2) * 0.5, (u2 - u1) * 0.5


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


#  Full Game Projection 

HCA_VALUE = 3.5  # KenPom's typical home court advantage (points)
             # Replace with dynamic value from hca.php if you add that endpoint


def mround(value: float, multiple: float = 0.5) -> float:
    """Round to nearest multiple (like Excel MROUND). Default 0.5 for gambling lines."""
    return round(value / multiple) * multiple


def compute_clutch_score(team_name: str, misc: pd.DataFrame) -> float:
    """
    Execution Score (Clutch) from KenPom misc data.
    Formula from sheet: =100-O4+Q4-S4-U4  (OppFG2Pct, OppFG3Pct, OppFTPct, OppBLKpct based)
    Simplified: use ClutchScore or similar field if available, else return 0.
    We store raw clutch score and normalize it for application.
    """
    if misc is None or misc.empty:
        return 0.0
    try:
        row = get_team(misc, team_name)
        # Try common field names from KenPom misc API
        for field in ["ClutchScore", "Clutch", "clutch_score"]:
            if field in row.index:
                return float(row[field])
    except Exception:
        pass
    return 0.0


def apply_clutch_adjustment(t1_score: float, t2_score: float,
                             t1_clutch: float, t2_clutch: float,
                             t1_poss: float, t2_poss: float) -> tuple[float, float]:
    """
    Apply clutch adjustment only when projected game is within 5 pts at 87% of possessions.
    Last 13% of possessions (approx last 5 min of 2H):
      clutch_poss = total_poss * 0.13
      adjusted score uses clutch percentile weighted by possession share.

    clutch_away = clutch_t1 / (clutch_t1 + clutch_t2)  (away perspective)
    clutch_home = clutch_t2 / (clutch_t1 + clutch_t2)  (home perspective)
    Then adjust last 13% of score by clutch win probability shift.
    """
    spread = abs(t1_score - t2_score)
    # Only apply if game is within 5 pts (i.e., close game at 87% mark)
    if spread > 5.0:
        return t1_score, t2_score

    total_clutch = t1_clutch + t2_clutch
    if total_clutch == 0:
        return t1_score, t2_score

    clutch_pct_t1 = t1_clutch / total_clutch  # t1 clutch win share
    clutch_pct_t2 = t2_clutch / total_clutch  # t2 clutch win share

    # Last 13% of possessions
    clutch_poss_t1 = t1_poss * 0.13
    clutch_poss_t2 = t2_poss * 0.13

    # Baseline: each team scores proportionally to their clutch share
    # Adjustment = (clutch_pct - 0.5) * clutch_poss * ppp_proxy
    # We approximate PPP from score/poss
    ppp_t1 = t1_score / t1_poss if t1_poss > 0 else 1.0
    ppp_t2 = t2_score / t2_poss if t2_poss > 0 else 1.0

    t1_clutch_adj = (clutch_pct_t1 - 0.5) * clutch_poss_t1 * ppp_t1
    t2_clutch_adj = (clutch_pct_t2 - 0.5) * clutch_poss_t2 * ppp_t2

    return t1_score + t1_clutch_adj, t2_score + t2_clutch_adj


def project_game(
    team1: str,
    team2: str,
    team1_is_home,
    data: dict,
    game_time: str = None,
) -> dict:
    ratings = data["ratings"]
    ff      = data["four_factors"]
    height  = data["height"]
    net     = data["net"]
    misc    = data.get("misc", pd.DataFrame())

    avgs = compute_ncaa_averages(ratings, ff, height)

    # Pull team rows (correct API column names)
    t1_r  = get_team(ratings, team1)
    t2_r  = get_team(ratings, team2)
    t1_ff = get_team(ff,      team1)
    t2_ff = get_team(ff,      team2)
    t1_h  = get_team(height,  team1)
    t2_h  = get_team(height,  team2)

    # Per-team combined (KP+NET)/2 percentiles (0-1 decimal, 1=best team)
    # Matches sheet Q/T columns: Adjusted %tile = avg of KP and NET percentiles
    t1_pct, adj1_debug = compute_team_percentile(team1, ratings, net)
    t2_pct, adj2_debug = compute_team_percentile(team2, ratings, net)

    # Quality-gap dampener — purely team1 vs team2, no location dependency.
    # HCA is handled separately below via hca_adjustments().
    adj1, adj2 = compute_game_adjustment(t1_pct, t2_pct)

    # Pace
    pace = projected_pace(t1_r["AdjTempo"], t2_r["AdjTempo"], avgs["pace"])

    # Points per possession
    t1_ppp = points_per_possession(t1_r["AdjOE"], t2_r["AdjDE"])
    t2_ppp = points_per_possession(t2_r["AdjOE"], t1_r["AdjDE"])

    # Turnovers  args: (opp off TO, team def DTO, avg_off_to, avg_def_to, adj)
    # "How much does opponent turn it over vs our defense forcing turnovers"
    # Home TO = (AVG_TO - Home_TO) - (AVG_DTO - Away_DTO)  [sheet: E2=home, D2=away]
    # Away TO = (AVG_TO - Away_TO) - (AVG_DTO - Home_DTO)
    t1_to = projected_turnovers(t1_ff["TO_Pct"], t2_ff["DTO_Pct"], avgs["to_pct"], avgs["dto_pct"], adj1)
    t2_to = projected_turnovers(t2_ff["TO_Pct"], t1_ff["DTO_Pct"], avgs["to_pct"], avgs["dto_pct"], adj2)

    # Rebounds  args: (team OR_Pct, opp DOR_Pct, avg_or, avg_dor, adj)
    t1_reb = projected_rebounds(t1_ff["OR_Pct"], t2_ff["DOR_Pct"], avgs["or_pct"], avgs["dor_pct"], adj1)
    t2_reb = projected_rebounds(t2_ff["OR_Pct"], t1_ff["DOR_Pct"], avgs["or_pct"], avgs["dor_pct"], adj2)

    print(f"DEBUG avgs:    or_pct={avgs['or_pct']:.4f}  dor_pct={avgs['dor_pct']:.4f}")
    print(f"DEBUG t1 ({team1}):  OR={t1_ff['OR_Pct']:.4f}  opp_DOR={t2_ff['DOR_Pct']:.4f}  adj={adj1:.4f}  → reb={t1_reb:.4f}")
    print(f"DEBUG t2 ({team2}):  OR={t2_ff['OR_Pct']:.4f}  opp_DOR={t1_ff['DOR_Pct']:.4f}  adj={adj2:.4f}  → reb={t2_reb:.4f}")

    # Free throws  each team's own DFT_Rate - FT_Rate
    # Measures FT possession imbalance for each team independently
        # Free throws: OPP_DFT_Rate - team_FT_Rate (cross-team)
    # WKU_DFT(45.59) - Liberty_FT(30.82) = +14.77 (sheet=15.09) checked and verified
    t1_ft = projected_ft(t2_ff["DFT_Rate"], t1_ff["FT_Rate"], adj1)
    t2_ft = projected_ft(t1_ff["DFT_Rate"], t2_ff["FT_Rate"], adj2)

    # Adjusted possessions
    t1_poss = adjusted_possessions(pace, t1_reb, t1_to, t1_ft)
    t2_poss = adjusted_possessions(pace, t2_reb, t2_to, t2_ft)

    # Base scores
    # t1_poss is already the full possession count (pace + adjustment)
    t1_score = t1_ppp * t1_poss
    t2_score = t2_ppp * t2_poss



    # Unit score (Height/Experience/Bench)   API fields: AvgHgt, Exp, Bench
    u1 = unit_score(t1_h["AvgHgt"], t1_h["Exp"], t1_h["Bench"], avgs, avgs["n_teams"])
    u2 = unit_score(t2_h["AvgHgt"], t2_h["Exp"], t2_h["Bench"], avgs, avgs["n_teams"])
    u1_adj, u2_adj = unit_score_adjustments(u1, u2)
    t1_score += u1_adj
    t2_score += u2_adj

    # Home court advantage
    h1_adj, h2_adj = hca_adjustments(HCA_VALUE, team1_is_home)
    t1_score += h1_adj
    t2_score += h2_adj

    # Clutch adjustment (only applied when game is within 5 pts)
    t1_clutch = compute_clutch_score(team1, misc)
    t2_clutch = compute_clutch_score(team2, misc)
    t1_score, t2_score = apply_clutch_adjustment(t1_score, t2_score, t1_clutch, t2_clutch, t1_poss, t2_poss)

    # Round all final scores to nearest 0.5 (standard for gambling lines)
    t1_score = mround(t1_score, 0.5)
    t2_score = mround(t2_score, 0.5)

    return {
        "team1":             team1,
        "team2":             team2,
        "game_time":         game_time,
        "projected_pace":    round(pace, 1),
        "team1_score":       t1_score,
        "team2_score":       t2_score,
        "spread":            mround(t1_score - t2_score, 0.5),
        "total":             mround(t1_score + t2_score, 0.5),
        "team1_clutch":      round(t1_clutch, 2),
        "team2_clutch":      round(t2_clutch, 2),
        "team1_adj_metric":  round(adj1, 6),
        "team2_adj_metric":  round(adj2, 6),
        "team1_kp_pct":      round(t1_pct, 4),
        "team2_kp_pct":      round(t2_pct, 4),
        "team1_unit_score":  round(u1, 3),
        "team2_unit_score":  round(u2, 3),
        "team1_ppp":         round(t1_ppp, 4),
        "team2_ppp":         round(t2_ppp, 4),
        "location":          "home" if team1_is_home else ("away" if team1_is_home is False else "neutral"),
        # Full debug breakdown for Excel logger
        "debug": {
            "kenpom_rank_t1": adj1_debug["kp_rank"],   "kenpom_rank_t2": adj2_debug["kp_rank"],
            "net_rank_t1":    adj1_debug["net_rank"],    "net_rank_t2":    adj2_debug["net_rank"],
            "kp_pct_t1":      adj1_debug["kp_pct"],     "kp_pct_t2":      adj2_debug["kp_pct"],
            "net_pct_t1":     None,                      "net_pct_t2":     None,
            "avg_pace":       round(avgs["pace"], 2),
            "t1_tempo":       float(t1_r["AdjTempo"]),  "t2_tempo":       float(t2_r["AdjTempo"]),
            "t1_adjoe":       float(t1_r["AdjOE"]),     "t2_adjoe":       float(t2_r["AdjOE"]),
            "t1_adjde":       float(t1_r["AdjDE"]),     "t2_adjde":       float(t2_r["AdjDE"]),
            "t1_to_pct":      float(t1_ff["TO_Pct"]),   "t2_to_pct":      float(t2_ff["TO_Pct"]),
            "t1_dto_pct":     float(t1_ff["DTO_Pct"]),  "t2_dto_pct":     float(t2_ff["DTO_Pct"]),
            "avg_to_pct":     round(avgs["to_pct"], 6),
            "t1_to":          round(t1_to, 4),           "t2_to":          round(t2_to, 4),
            "t1_or_pct":      float(t1_ff["OR_Pct"]),   "t2_or_pct":      float(t2_ff["OR_Pct"]),
            "t1_dor_pct":     float(t1_ff["DOR_Pct"]),  "t2_dor_pct":     float(t2_ff["DOR_Pct"]),
            "t1_reb":         round(t1_reb, 4),          "t2_reb":         round(t2_reb, 4),
            "t1_ft_rate":     float(t1_ff["FT_Rate"]),  "t2_ft_rate":     float(t2_ff["FT_Rate"]),
            "t1_dft_rate":    float(t1_ff["DFT_Rate"]), "t2_dft_rate":    float(t2_ff["DFT_Rate"]),
            "t1_ft":          round(t1_ft, 4),           "t2_ft":          round(t2_ft, 4),
            "t1_poss":        round(t1_poss, 4),         "t2_poss":        round(t2_poss, 4),
            "t1_hgt":         float(t1_h["AvgHgt"]),    "t2_hgt":         float(t2_h["AvgHgt"]),
            "t1_exp":         float(t1_h["Exp"]),        "t2_exp":         float(t2_h["Exp"]),
            "t1_bench":       float(t1_h["Bench"]),      "t2_bench":       float(t2_h["Bench"]),
            "u1_adj":         round(u1_adj, 3),          "u2_adj":         round(u2_adj, 3),
            "h1_adj":         round(h1_adj, 2),          "h2_adj":         round(h2_adj, 2),
        }
    }


if __name__ == "__main__":
    data = load_data()
    result = project_game("Duke", "Kentucky", team1_is_home=True, data=data)
    print("\n Game Projection")
    print("=" * 38)
    for k, v in result.items():
        print(f"  {k:<25} {v}")
