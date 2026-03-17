"""
bracket_simulator.py
────────────────────
2026 NCAA Tournament bracket simulator.

Two simulation modes:
  1. CZarp Model  — uses project_game() for all matchups (all neutral site)
  2. Trait mode   — CZarp model base, but if margin ≤ 5 pts AND the losing
                    team has a better selected trait, the result is flipped.

Trait options:
  3-Point %, Free Throw %, Adj Off Efficiency, Adj Def Efficiency,
  Height, Experience
"""

from __future__ import annotations
import pandas as pd
from difflib import SequenceMatcher

# ── 2026 Bracket ─────────────────────────────────────────────────────────────

FIRST_FOUR = [
    # team1, team2, region, seed, slot_key (placeholder in main bracket)
    {"team1": "Texas",             "team2": "N.C. State",  "region": "West",    "seed": 11, "slot": "FF_WEST_11"},
    {"team1": "UMBC",              "team2": "Howard",       "region": "Midwest", "seed": 16, "slot": "FF_MID_16"},
    {"team1": "Miami (OH)",        "team2": "SMU",          "region": "Midwest", "seed": 11, "slot": "FF_MID_11"},
    {"team1": "Prairie View A&M",  "team2": "Lehigh",       "region": "South",   "seed": 16, "slot": "FF_SOUTH_16"},
]

# Each entry: (seed1, team1, seed2, team2)
# FF slot placeholders will be replaced with winners before simulation.
# Order follows standard bracket pairing:
#   Index 0: 1v16,  1: 8v9,  2: 5v12,  3: 4v13
#   Index 4: 6v11,  5: 3v14, 6: 7v10,  7: 2v15
# Round of 32: 0v1, 2v3, 4v5, 6v7
# Sweet 16:    (0v1) vs (2v3),  (4v5) vs (6v7)
# Elite 8:     S16-top vs S16-bottom
REGIONS: dict[str, list[tuple]] = {
    "East": [
        (1,  "Duke",             16, "Siena"),
        (8,  "Ohio St.",          9, "TCU"),
        (5,  "St. John's",       12, "Northern Iowa"),
        (4,  "Kansas",           13, "California Baptist"),
        (6,  "Louisville",       11, "South Florida"),
        (3,  "Michigan St.",     14, "North Dakota St."),
        (7,  "UCLA",             10, "UCF"),
        (2,  "Connecticut",      15, "Furman"),
    ],
    "West": [
        (1,  "Arizona",          16, "LIU"),
        (8,  "Villanova",         9, "Utah St."),
        (5,  "Wisconsin",        12, "High Point"),
        (4,  "Arkansas",         13, "Hawaii"),
        (6,  "BYU",              11, "FF_WEST_11"),   # First Four winner
        (3,  "Gonzaga",          14, "Kennesaw St."),
        (7,  "Miami (FL)",       10, "Missouri"),
        (2,  "Purdue",           15, "Queens (NC)"),
    ],
    "Midwest": [
        (1,  "Michigan",         16, "FF_MID_16"),    # First Four winner
        (8,  "Georgia",           9, "Saint Louis"),
        (5,  "Texas Tech",       12, "Akron"),
        (4,  "Alabama",          13, "Hofstra"),
        (6,  "Tennessee",        11, "FF_MID_11"),    # First Four winner
        (3,  "Virginia",         14, "Wright St."),
        (7,  "Kentucky",         10, "Santa Clara"),
        (2,  "Iowa St.",         15, "Tennessee St."),
    ],
    "South": [
        (1,  "Florida",          16, "FF_SOUTH_16"),  # First Four winner
        (8,  "Clemson",           9, "Iowa"),
        (5,  "Vanderbilt",       12, "McNeese St."),
        (4,  "Nebraska",         13, "Troy"),
        (6,  "North Carolina",   11, "VCU"),
        (3,  "Illinois",         14, "Penn"),
        (7,  "Saint Mary's (CA)", 10, "Texas A&M"),
        (2,  "Houston",          15, "Idaho"),
    ],
}

# Final Four: (region1, region2) — paired by overall seed ranking
# Duke=1 overall (East) vs Florida=4 overall (South)
# Arizona=2 overall (West) vs Michigan=3 overall (Midwest)
FINAL_FOUR_MATCHUPS = [("East", "South"), ("West", "Midwest")]

# ── Trait configuration ───────────────────────────────────────────────────────

TRAITS: dict[str, dict] = {
    "CZarp Model":          {"field": None,      "higher_better": None,  "source": None},
    "3-Point %":            {"field": "FG3Pct",  "higher_better": True,  "source": "misc"},
    "Free Throw %":         {"field": "FTPct",   "higher_better": True,  "source": "misc"},
    "Adj Off Efficiency":   {"field": "AdjOE",   "higher_better": True,  "source": "ratings"},
    "Adj Def Efficiency":   {"field": "AdjDE",   "higher_better": False, "source": "ratings"},
    "Height":               {"field": "AvgHgt",  "higher_better": True,  "source": "ratings"},
    "Experience":           {"field": "Exp",     "higher_better": True,  "source": "ratings"},
}

# KenPom name overrides for bracket names that differ from KenPom database
KENPOM_NAME_OVERRIDES: dict[str, str] = {
    "N.C. State":           "N.C. State",
    "California Baptist":   "Cal Baptist",
    "Northern Iowa":        "Northern Iowa",
    "Connecticut":          "Connecticut",
    "Saint Mary's (CA)":    "Saint Mary's (CA)",
    "North Dakota St.":     "North Dakota St.",
    "Tennessee St.":        "Tennessee St.",
    "Utah St.":             "Utah St.",
    "Iowa St.":             "Iowa St.",
    "Michigan St.":         "Michigan St.",
    "Ohio St.":             "Ohio St.",
    "Wright St.":           "Wright St.",
    "Kennesaw St.":         "Kennesaw St.",
    "Queens (NC)":          "Queens (NC)",
    "McNeese St.":          "McNeese St.",
    "Prairie View A&M":     "Prairie View A&M",
    "Saint Louis":          "Saint Louis",
    "Miami (FL)":           "Miami (FL)",
    "Miami (OH)":           "Miami (OH)",
    "North Carolina":       "North Carolina",
    "BYU":                  "BYU",
    "VCU":                  "VCU",
    "UCF":                  "UCF",
    "SMU":                  "SMU",
    "LIU":                  "LIU",
    "Penn":                 "Penn",
    "TCU":                  "TCU",
}

# ── Data fetching ─────────────────────────────────────────────────────────────

def fetch_misc_stats(y: int = 2026) -> pd.DataFrame:
    """Fetch KenPom miscellaneous stats (3P%, FT%, etc.) for the given season."""
    try:
        from kenpom_scraper import _login
        session = _login()
        resp = session.get(
            "https://kenpom.com/api.php",
            params={"endpoint": "misc-stats", "y": y},
            timeout=20
        )
        resp.raise_for_status()
        data = resp.json()
        df = pd.DataFrame(data)
        print(f"  [misc-stats] loaded {len(df)} teams")
        return df
    except Exception as e:
        print(f"  [misc-stats] failed: {e}")
        return pd.DataFrame()


def fetch_height_stats(y: int = 2026) -> pd.DataFrame:
    """Fetch KenPom height/experience stats for the given season."""
    try:
        from kenpom_scraper import _login
        session = _login()
        resp = session.get(
            "https://kenpom.com/api.php",
            params={"endpoint": "height", "y": y},
            timeout=20
        )
        resp.raise_for_status()
        data = resp.json()
        df = pd.DataFrame(data)
        print(f"  [height-stats] loaded {len(df)} teams")
        return df
    except Exception as e:
        print(f"  [height-stats] failed: {e}")
        return pd.DataFrame()


# ── Name matching ──────────────────────────────────────────────────────────────

def _fuzzy_match(name: str, candidates: list[str], threshold: float = 0.80) -> str | None:
    """Return best fuzzy match from candidates, or None if below threshold."""
    name_lower = name.lower().strip()
    best_score = 0.0
    best_match = None
    for c in candidates:
        score = SequenceMatcher(None, name_lower, c.lower().strip()).ratio()
        if score > best_score:
            best_score = score
            best_match = c
    return best_match if best_score >= threshold else None


def resolve_kp_name(bracket_name: str, kp_names: list[str]) -> str:
    """Map a bracket team name to its KenPom database name."""
    # Use override if available
    candidate = KENPOM_NAME_OVERRIDES.get(bracket_name, bracket_name)
    if candidate in kp_names:
        return candidate
    # Try direct match
    if bracket_name in kp_names:
        return bracket_name
    # Try fuzzy
    fuzzy = _fuzzy_match(bracket_name, kp_names)
    if fuzzy:
        return fuzzy
    print(f"  [bracket] WARNING: no KenPom match for '{bracket_name}'")
    return bracket_name


# ── Trait value lookup ────────────────────────────────────────────────────────

def get_trait_value(
    team_name: str,
    trait_name: str,
    kp_data: dict,
    misc_df: pd.DataFrame,
    height_df: pd.DataFrame,
) -> float | None:
    """Get the trait statistic value for a team."""
    cfg = TRAITS.get(trait_name)
    if not cfg or cfg["field"] is None:
        return None

    source = cfg["source"]
    field  = cfg["field"]

    try:
        if source == "misc":
            if misc_df.empty or "TeamName" not in misc_df.columns:
                return None
            kp_names = misc_df["TeamName"].tolist()
            kp_name  = resolve_kp_name(team_name, kp_names)
            row = misc_df[misc_df["TeamName"] == kp_name]
            if row.empty:
                return None
            return float(row.iloc[0][field])

        elif source == "height":
            if height_df.empty or "TeamName" not in height_df.columns:
                return None
            kp_names = height_df["TeamName"].tolist()
            kp_name  = resolve_kp_name(team_name, kp_names)
            row = height_df[height_df["TeamName"] == kp_name]
            if row.empty:
                return None
            return float(row.iloc[0][field])

        elif source == "ratings":
            # Pull from the ratings DataFrame
            ratings = kp_data.get("ratings")
            if ratings is None or ratings.empty:
                return None
            kp_names = ratings["TeamName"].tolist()
            kp_name  = resolve_kp_name(team_name, kp_names)
            row = ratings[ratings["TeamName"] == kp_name]
            if row.empty:
                return None
            # Try multiple possible column names for each field
            col_map = {
                "AdjOE":   ["AdjOE", "AdjO", "Adj. O", "OE"],
                "AdjDE":   ["AdjDE", "AdjD", "Adj. D", "DE"],
                "AvgHgt":  ["AvgHgt", "Hgt", "AvgHgt", "HgtEff"],
                "Exp":     ["Exp", "Experience"],
            }
            for col in col_map.get(field, [field]):
                if col in row.columns:
                    return float(row.iloc[0][col])
            # Last resort: check all columns case-insensitively
            for col in row.columns:
                if col.lower() == field.lower():
                    return float(row.iloc[0][col])
            return None

    except Exception as e:
        print(f"  [trait] error for {team_name} / {trait_name}: {e}")
        return None


# ── Single game simulation ────────────────────────────────────────────────────

def simulate_game(
    team1: str,
    team2: str,
    seed1: int | None,
    seed2: int | None,
    mode: str,
    trait_name: str,
    kp_data: dict,
    misc_df: pd.DataFrame,
    height_df: pd.DataFrame,
) -> dict:
    """
    Simulate a single game. All tournament games are neutral site.

    Returns dict with: team1, team2, seed1, seed2, t1_score, t2_score,
                       projected_winner, winner, loser, margin, trait_upset,
                       trait_name (if flipped)
    """
    from model import project_game

    kp_names = kp_data.get("ratings", pd.DataFrame()).get("TeamName", pd.Series()).tolist()
    kp_t1 = resolve_kp_name(team1, kp_names)
    kp_t2 = resolve_kp_name(team2, kp_names)

    # Run projection (neutral site → team1_is_home=None)
    try:
        result   = project_game(kp_t1, kp_t2, team1_is_home=None, data=kp_data)
        t1_score = result["team1_score"]
        t2_score = result["team2_score"]
    except Exception as e:
        print(f"  [sim] projection failed {team1} vs {team2}: {e}")
        # Fallback: higher seed wins by 5
        t1_score = 70.0 if (seed1 or 99) < (seed2 or 99) else 65.0
        t2_score = 65.0 if (seed1 or 99) < (seed2 or 99) else 70.0

    margin = abs(t1_score - t2_score)
    projected_winner = team1 if t1_score >= t2_score else team2
    projected_loser  = team2 if projected_winner == team1 else team1

    winner      = projected_winner
    loser       = projected_loser
    trait_upset = False
    flip_trait  = None

    # Trait upset rule: only if margin ≤ 5 AND mode is trait-based
    if mode == "trait" and trait_name != "CZarp Model" and margin <= 5.0:
        w_val = get_trait_value(winner, trait_name, kp_data, misc_df, height_df)
        l_val = get_trait_value(loser,  trait_name, kp_data, misc_df, height_df)

        if w_val is not None and l_val is not None:
            higher_better = TRAITS[trait_name]["higher_better"]
            loser_better  = (l_val > w_val) if higher_better else (l_val < w_val)

            if loser_better:
                winner, loser = loser, winner
                trait_upset   = True
                flip_trait    = trait_name

    return {
        "team1":             team1,
        "team2":             team2,
        "seed1":             seed1,
        "seed2":             seed2,
        "t1_score":          round(t1_score, 1),
        "t2_score":          round(t2_score, 1),
        "projected_winner":  projected_winner,
        "winner":            winner,
        "loser":             loser,
        "margin":            round(margin, 1),
        "trait_upset":       trait_upset,
        "flip_trait":        flip_trait,
    }


# ── Full bracket simulation ───────────────────────────────────────────────────

def _sim(t1, t2, s1, s2, mode, trait, kp_data, misc_df, height_df):
    """Shorthand wrapper."""
    return simulate_game(t1, t2, s1, s2, mode, trait, kp_data, misc_df, height_df)


def run_bracket(
    mode: str,
    trait_name: str,
    kp_data: dict,
    misc_df: pd.DataFrame,
    height_df: pd.DataFrame,
) -> dict:
    """
    Simulate the full 2026 NCAA Tournament bracket.

    Returns nested dict:
      {
        "first_four": [game, ...],
        "regions": {
          "East": {"r64": [...], "r32": [...], "s16": [...], "e8": game},
          ...
        },
        "final_four": [game, game],
        "championship": game,
        "champion": team_name,
      }
    """
    results = {
        "first_four":  [],
        "regions":     {r: {} for r in REGIONS},
        "final_four":  [],
        "championship": None,
        "champion":    None,
    }

    # ── Step 1: First Four ──────────────────────────────────────────────────
    ff_winners: dict[str, str] = {}
    for ff in FIRST_FOUR:
        g = _sim(ff["team1"], ff["team2"], ff["seed"], ff["seed"],
                 mode, trait_name, kp_data, misc_df, height_df)
        ff_winners[ff["slot"]] = g["winner"]
        results["first_four"].append(g)

    # ── Step 2: Build full bracket replacing FF placeholders ───────────────
    full_regions: dict[str, list[tuple]] = {}
    for region, matchups in REGIONS.items():
        resolved = []
        for (s1, t1, s2, t2) in matchups:
            rt1 = ff_winners.get(t1, t1)
            rt2 = ff_winners.get(t2, t2)
            resolved.append((s1, rt1, s2, rt2))
        full_regions[region] = resolved

    # ── Step 3: Simulate each region ───────────────────────────────────────
    region_winners: dict[str, str] = {}

    for region, matchups in full_regions.items():
        # Round of 64
        r64 = []
        for (s1, t1, s2, t2) in matchups:
            r64.append(_sim(t1, t2, s1, s2, mode, trait_name, kp_data, misc_df, height_df))

        # Round of 32 — pair adjacent r64 games: (0,1), (2,3), (4,5), (6,7)
        r32 = []
        for i in range(0, 8, 2):
            w1, s1 = r64[i]["winner"],   r64[i]["seed1"]   if r64[i]["winner"] == r64[i]["team1"] else r64[i]["seed2"]
            w2, s2 = r64[i+1]["winner"], r64[i+1]["seed1"] if r64[i+1]["winner"] == r64[i+1]["team1"] else r64[i+1]["seed2"]
            r32.append(_sim(w1, w2, s1, s2, mode, trait_name, kp_data, misc_df, height_df))

        # Sweet 16 — pair r32 games: (0,1), (2,3)
        s16 = []
        for i in range(0, 4, 2):
            w1, s1 = r32[i]["winner"],   r32[i]["seed1"]   if r32[i]["winner"] == r32[i]["team1"] else r32[i]["seed2"]
            w2, s2 = r32[i+1]["winner"], r32[i+1]["seed1"] if r32[i+1]["winner"] == r32[i+1]["team1"] else r32[i+1]["seed2"]
            s16.append(_sim(w1, w2, s1, s2, mode, trait_name, kp_data, misc_df, height_df))

        # Elite Eight
        w1, s1 = s16[0]["winner"], s16[0]["seed1"] if s16[0]["winner"] == s16[0]["team1"] else s16[0]["seed2"]
        w2, s2 = s16[1]["winner"], s16[1]["seed1"] if s16[1]["winner"] == s16[1]["team1"] else s16[1]["seed2"]
        e8 = _sim(w1, w2, s1, s2, mode, trait_name, kp_data, misc_df, height_df)

        region_winners[region] = e8["winner"]
        results["regions"][region] = {
            "r64": r64, "r32": r32, "s16": s16, "e8": e8
        }

    # ── Step 4: Final Four ──────────────────────────────────────────────────
    ff_games = []
    final_four_winners = []
    for (r1, r2) in FINAL_FOUR_MATCHUPS:
        t1 = region_winners[r1]
        t2 = region_winners[r2]
        g  = _sim(t1, t2, None, None, mode, trait_name, kp_data, misc_df, height_df)
        g["semifinal"] = f"{r1} vs {r2}"
        ff_games.append(g)
        final_four_winners.append(g["winner"])
    results["final_four"] = ff_games

    # ── Step 5: Championship ────────────────────────────────────────────────
    champ_game = _sim(
        final_four_winners[0], final_four_winners[1],
        None, None, mode, trait_name, kp_data, misc_df, height_df
    )
    results["championship"] = champ_game
    results["champion"]     = champ_game["winner"]

    return results
