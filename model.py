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

def compute_team_percentile(
    team_name: str,
    ratings: pd.DataFrame,
) -> tuple[float, dict]:
    """
    Returns KenPom percentile for a single team as a 0-1 decimal.
    Rank 1 (best) → ~1.0, last rank → ~0.0
    """
    t_kp    = get_team(ratings, team_name)
    n       = len(ratings)
    kp_rank = float(t_kp["RankAdjEM"])
    kp_pct  = (n - kp_rank) / n   # 0-1 decimal

    if kp_rank > 300:
        print(f"   ⚠️  {team_name} KP rank={int(kp_rank)} — check data/ratings.csv")

    debug = {"kp_rank": kp_rank, "kp_pct": kp_pct}
    return kp_pct, debug


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


# ── Core Model Formulas ───────────────────────────────────────────────────────

def projected_pace(t1_tempo: float, t2_tempo: float, avg_pace: float) -> float:
    """Projected Pace = NCAA Avg + (T1 Pace − Avg) + (T2 Pace − Avg)"""
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
    Signed — positive means favorable TO matchup (they turn it over more, we force more).
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
    Signed — positive means favorable rebound matchup.
    """
    raw = (opp_dor_pct - avg_dor) - (avg_or - team_or_pct)
    return raw + abs(raw) * adjustment


def projected_ft(
    team_def_ft: float,   # Team's DEFENSIVE FT rate (DFT_Rate)
    team_off_ft: float,   # Team's OFFENSIVE FT rate (FT_Rate) — same team!
    adjustment: float,
) -> float:
    """
    Sheet formula: Raw = team_def_FTRate - team_off_FTRate  (same team, not cross-team)
    AE2 = VLOOKUP(Away team, defense sheet, FTRate) - VLOOKUP(Away team, offense sheet, FTRate)
    Positive = team defends FTs better than they draw them
    Negative = team draws more FTs than they prevent (FT-heavy offense)
    Adj = Raw + abs(Raw) * adjustment
    """
    raw = team_def_ft - team_off_ft
    return raw + abs(raw) * adjustment


def adjusted_possessions(pace: float, proj_reb: float, proj_to: float, proj_ft: float) -> float:
    """
    Sheet formula:
      AL2 (raw poss delta) = pace*(reb*0.01) + pace*(to*0.01) + pace*(ft*0.44*0.01)
      AN2 (adj possessions) = pace + pace*(AL2*0.01)

    We return the FULL adjusted possessions (AN2) — the total possession count for the game.
    """
    raw_delta = (
        pace * (proj_reb * 0.01) +
        pace * (proj_to  * 0.01) +
        pace * (proj_ft  * 0.44 * 0.01)
    )
    return pace + pace * (raw_delta * 0.01)


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

    # Per-team KP percentiles (0-1 decimal, 1=best team)
    # team1 = home, team2 = away (fanmatch convention)
    t1_pct, adj1_debug = compute_team_percentile(team1, ratings)
    t2_pct, adj2_debug = compute_team_percentile(team2, ratings)

    # Game-level adjustment: (home_pct - away_pct)*0.5 and inverse
    # Matches sheet: U2_home=(Away_pct - Home_pct)*0.5, U2_away=(Home_pct - Away_pct)*0.5
    if team1_is_home is None:
        adj1, adj2 = 0.0, 0.0   # neutral site — no adjustment
    else:
        # team1 is home, team2 is away
        adj1, adj2 = compute_game_adjustment(t1_pct, t2_pct)

    # Pace
    pace = projected_pace(t1_r["AdjTempo"], t2_r["AdjTempo"], avgs["pace"])

    # Points per possession
    t1_ppp = points_per_possession(t1_r["AdjOE"], t2_r["AdjDE"])
    t2_ppp = points_per_possession(t2_r["AdjOE"], t1_r["AdjDE"])

    # Turnovers — args: (opp off TO, team def DTO, avg_off_to, avg_def_to, adj)
    # "How much does opponent turn it over vs our defense forcing turnovers"
    t1_to = projected_turnovers(t2_ff["TO_Pct"], t1_ff["DTO_Pct"], avgs["to_pct"], avgs["dto_pct"], adj1)
    t2_to = projected_turnovers(t1_ff["TO_Pct"], t2_ff["DTO_Pct"], avgs["to_pct"], avgs["dto_pct"], adj2)

    # Rebounds — args: (team OR_Pct, opp DOR_Pct, avg_or, avg_dor, adj)
    t1_reb = projected_rebounds(t1_ff["OR_Pct"], t2_ff["DOR_Pct"], avgs["or_pct"], avgs["dor_pct"], adj1)
    t2_reb = projected_rebounds(t2_ff["OR_Pct"], t1_ff["DOR_Pct"], avgs["or_pct"], avgs["dor_pct"], adj2)

    # Free throws — args: (team def FT rate, team off FT rate, adj)
    # Sheet: VLOOKUP(team, defense, FTRate) - VLOOKUP(team, offense, FTRate)
    # Same team's own def vs own off — measures FT style/matchup impact
    t1_ft = projected_ft(t1_ff["DFT_Rate"], t1_ff["FT_Rate"], adj1)
    t2_ft = projected_ft(t2_ff["DFT_Rate"], t2_ff["FT_Rate"], adj2)

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
            "net_rank_t1":    None,                      "net_rank_t2":    None,
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
    print("\n🏀 Game Projection")
    print("=" * 38)
    for k, v in result.items():
        print(f"  {k:<25} {v}")
