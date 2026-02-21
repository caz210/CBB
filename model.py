"""
model.py
CBB betting model — all formulas match your documented Google Sheets logic.
Column names match the official KenPom API response fields exactly.
"""

import pandas as pd


# ── Helpers ───────────────────────────────────────────────────────────────────

def load_data(data_dir: str = "data") -> dict[str, pd.DataFrame]:
    return {
        "ratings":      pd.read_csv(f"{data_dir}/ratings.csv"),
        "four_factors": pd.read_csv(f"{data_dir}/four_factors.csv"),
        "height":       pd.read_csv(f"{data_dir}/height.csv"),
        "net":          pd.read_csv(f"{data_dir}/net.csv"),
    }


# KenPom name → model name mapping for common mismatches
TEAM_NAME_MAP = {
    "Miami OH":        "Miami (OH)",
    "Miami FL":        "Miami (FL)",
    "St. Mary's":      "Saint Mary's",
    "UCSB":            "UC Santa Barbara",
    "UNC":             "North Carolina",
    "UNCW":            "UNC Wilmington",
    "LIU":             "LIU Brooklyn",
    "Detroit":         "Detroit Mercy",
    "UTSA":            "UT San Antonio",
    "Pitt":            "Pittsburgh",
    "USC":             "Southern California",
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


# ── Adjustment Metric (Percentile) ───────────────────────────────────────────

def compute_adjustment_metric(
    team_name: str,
    ratings: pd.DataFrame,
    net: pd.DataFrame,
) -> tuple[float, dict]:
    """
    Normalized 0–100 percentile score combining KenPom rank and NCAA NET rank.
    Returns (combined_metric, debug_dict) where debug_dict has the breakdown.
    Higher = better team. Used to scale TO, REB, FT adjustments.
    """
    t_kp  = get_team(ratings, team_name)
    t_net = get_team(net, team_name)

    n_kp  = len(ratings)
    n_net = len(net)

    kp_rank  = float(t_kp["RankAdjEM"])
    net_rank = float(t_net["Rank"])

    # Invert ranks so rank 1 → 100, last rank → ~0
    kp_pct  = (n_kp  - kp_rank)  / n_kp  * 100
    net_pct = (n_net - net_rank) / n_net * 100
    combined = (kp_pct + net_pct) / 2

    # Sanity check — flag if rank seems wrong (e.g. team ranked 300+ but should be ~244)
    if kp_rank > 300:
        print(f"   ⚠️  {team_name} KP rank={int(kp_rank)} — if this looks wrong, "
              f"check data/ratings.csv for duplicate TeamName rows or wrong RankAdjEM column.")

    debug = {
        "kp_rank": kp_rank, "net_rank": net_rank,
        "kp_pct": kp_pct / 100, "net_pct": net_pct / 100,  # as decimals for Excel %
    }
    return combined, debug


# ── Core Model Formulas ───────────────────────────────────────────────────────

def projected_pace(t1_tempo: float, t2_tempo: float, avg_pace: float) -> float:
    """Projected Pace = NCAA Avg + (T1 Pace − Avg) + (T2 Pace − Avg)"""
    return avg_pace + (t1_tempo - avg_pace) + (t2_tempo - avg_pace)


def points_per_possession(off_eff: float, opp_def_eff: float) -> float:
    """=AVERAGE(Team AdjOE, Opp AdjDE) / 100"""
    return ((off_eff + opp_def_eff) / 2) / 100


def projected_turnovers(
    team_to_pct: float,   # TO_Pct  (offense)
    opp_dto_pct: float,   # DTO_Pct (opp defense — turnovers forced)
    avg_to: float,
    avg_dto: float,
    adjustment: float,    # 0–100 percentile metric
) -> float:
    """
    Raw = (Avg TO − Team TO) − (Avg DTO Caused − Opp DTO Caused)
    Positive = team turns it over less than avg AND opp forces fewer TOs than avg
    Adj = Raw + abs(Raw) * (adjustment / 100)
    """
    raw = (avg_to - team_to_pct) - (avg_dto - opp_dto_pct)
    adj = raw + abs(raw) * (adjustment / 100)
    return abs(adj)  # always positive — represents possession advantage


def projected_rebounds(
    opp_dor_pct: float,   # DOR_Pct of opponent (allowed offensive rebounds)
    team_or_pct: float,   # OR_Pct  of team
    avg_dor: float,
    avg_or: float,
    adjustment: float,
) -> float:
    """
    Raw = (Opp DOR_Pct − Avg DOR) − (Avg OR − Team OR_Pct)
    Adj = Raw + abs(Raw) * (adjustment / 100)
    Always returned as positive — represents extra possession advantage.
    """
    raw = (opp_dor_pct - avg_dor) - (avg_or - team_or_pct)
    adj = raw + abs(raw) * (adjustment / 100)
    return abs(adj)


def projected_ft(
    team_ft_rate: float,   # FT_Rate  (offense)
    opp_dft_rate: float,   # DFT_Rate (opponent defense)
    adjustment: float,
) -> float:
    """
    Raw = Team FT_Rate − Opp DFT_Rate
    Adj = Raw + abs(Raw) * (adjustment / 100)
    Always returned as positive — represents FT possession contribution.
    """
    raw = team_ft_rate - opp_dft_rate
    adj = raw + abs(raw) * (adjustment / 100)
    return abs(adj)


def adjusted_possessions(pace: float, proj_reb: float, proj_to: float, proj_ft: float) -> float:
    """
    AdjPoss = Pace*(REB*0.01) + Pace*(TO*0.01) + Pace*(FT*0.44*0.01)
    """
    return (
        pace * (proj_reb * 0.01) +
        pace * (proj_to  * 0.01) +
        pace * (proj_ft  * 0.44 * 0.01)
    )


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


# ── Full Game Projection ──────────────────────────────────────────────────────

HCA_VALUE = 3.5  # KenPom's typical home court advantage (points)
             # Replace with dynamic value from hca.php if you add that endpoint


def project_game(
    team1: str,
    team2: str,
    team1_is_home: bool | None,   # True=home, False=away, None=neutral
    data: dict[str, pd.DataFrame],
) -> dict:
    ratings = data["ratings"]
    ff      = data["four_factors"]
    height  = data["height"]
    net     = data["net"]

    avgs = compute_ncaa_averages(ratings, ff, height)

    # Pull team rows (correct API column names)
    t1_r  = get_team(ratings, team1)
    t2_r  = get_team(ratings, team2)
    t1_ff = get_team(ff,      team1)
    t2_ff = get_team(ff,      team2)
    t1_h  = get_team(height,  team1)
    t2_h  = get_team(height,  team2)

    # Adjustment metrics
    adj1, adj1_debug = compute_adjustment_metric(team1, ratings, net)
    adj2, adj2_debug = compute_adjustment_metric(team2, ratings, net)

    # Pace
    pace = projected_pace(t1_r["AdjTempo"], t2_r["AdjTempo"], avgs["pace"])

    # Points per possession
    t1_ppp = points_per_possession(t1_r["AdjOE"], t2_r["AdjDE"])
    t2_ppp = points_per_possession(t2_r["AdjOE"], t1_r["AdjDE"])

    # Turnovers
    t1_to = projected_turnovers(t1_ff["TO_Pct"],  t2_ff["DTO_Pct"], avgs["to_pct"], avgs["dto_pct"], adj1)
    t2_to = projected_turnovers(t2_ff["TO_Pct"],  t1_ff["DTO_Pct"], avgs["to_pct"], avgs["dto_pct"], adj2)

    # Rebounds
    t1_reb = projected_rebounds(t2_ff["DOR_Pct"], t1_ff["OR_Pct"],  avgs["dor_pct"], avgs["or_pct"], adj1)
    t2_reb = projected_rebounds(t1_ff["DOR_Pct"], t2_ff["OR_Pct"],  avgs["dor_pct"], avgs["or_pct"], adj2)

    # Free throws
    t1_ft = projected_ft(t1_ff["FT_Rate"], t2_ff["DFT_Rate"], adj1)
    t2_ft = projected_ft(t2_ff["FT_Rate"], t1_ff["DFT_Rate"], adj2)

    # Adjusted possessions
    t1_poss = adjusted_possessions(pace, t1_reb, t1_to, t1_ft)
    t2_poss = adjusted_possessions(pace, t2_reb, t2_to, t2_ft)

    # Base scores
    t1_score = t1_ppp * (pace + t1_poss)
    t2_score = t2_ppp * (pace + t2_poss)

    # Unit score (Height/Experience/Bench)  — API fields: AvgHgt, Exp, Bench
    u1 = unit_score(t1_h["AvgHgt"], t1_h["Exp"], t1_h["Bench"], avgs, avgs["n_teams"])
    u2 = unit_score(t2_h["AvgHgt"], t2_h["Exp"], t2_h["Bench"], avgs, avgs["n_teams"])
    u1_adj, u2_adj = unit_score_adjustments(u1, u2)
    t1_score += u1_adj
    t2_score += u2_adj

    # Home court advantage
    h1_adj, h2_adj = hca_adjustments(HCA_VALUE, team1_is_home)
    t1_score += h1_adj
    t2_score += h2_adj

    return {
        "team1":             team1,
        "team2":             team2,
        "projected_pace":    round(pace, 1),
        "team1_score":       round(t1_score, 1),
        "team2_score":       round(t2_score, 1),
        "spread":            round(t1_score - t2_score, 1),
        "total":             round(t1_score + t2_score, 1),
        "team1_adj_metric":  round(adj1, 1),
        "team2_adj_metric":  round(adj2, 1),
        "team1_unit_score":  round(u1, 3),
        "team2_unit_score":  round(u2, 3),
        "team1_ppp":         round(t1_ppp, 4),
        "team2_ppp":         round(t2_ppp, 4),
        "location":          "home" if team1_is_home else ("away" if team1_is_home is False else "neutral"),
        # Full debug breakdown for Excel logger
        "debug": {
            "kenpom_rank_t1": adj1_debug["kp_rank"],   "kenpom_rank_t2": adj2_debug["kp_rank"],
            "net_rank_t1":    adj1_debug["net_rank"],   "net_rank_t2":    adj2_debug["net_rank"],
            "kp_pct_t1":      adj1_debug["kp_pct"],     "kp_pct_t2":      adj2_debug["kp_pct"],
            "net_pct_t1":     adj1_debug["net_pct"],    "net_pct_t2":     adj2_debug["net_pct"],
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
    print("\n🏀 Game Projection")
    print("=" * 38)
    for k, v in result.items():
        print(f"  {k:<25} {v}")
