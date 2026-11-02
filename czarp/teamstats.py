"""Normalize KenPom tables into one per-team frame and tag archetypes.

KenPom's API field names differ between docs/versions (e.g. DTO_Pct vs
Def_TO_Pct), so everything is mapped to canonical names here.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from .config import MatchupConfig

# canonical -> candidate source columns (first found wins)
ALIASES = {
    # ratings
    "AdjOE": ["AdjOE"], "AdjDE": ["AdjDE"], "AdjTempo": ["AdjTempo"],
    "RankAdjEM": ["RankAdjEM"], "APL_Off": ["APL_Off"], "APL_Def": ["APL_Def"],
    # four factors
    "eFG": ["eFG_Pct", "EFG_Pct", "eFGPct"],
    "TO": ["TO_Pct"],
    "OR": ["OR_Pct"],
    "FTR": ["FT_Rate"],
    "D_eFG": ["Def_eFG_Pct", "DeFG_Pct", "DEFG_Pct"],
    "D_TO": ["Def_TO_Pct", "DTO_Pct"],
    "D_OR_raw": ["Def_OR_Pct", "DOR_Pct"],
    "D_FTR": ["Def_FT_Rate", "DFT_Rate"],
    # misc stats
    "FTPct": ["FTPct", "FT_Pct"],
    "FG3A_Rate": ["pFG3Rate", "FG3Rate", "FG3A_Pct", "F3GRate"],
    "Opp_FG3A_Rate": ["OpppFG3Rate", "OppFG3Rate", "OppFG3A_Pct", "OppF3GRate"],
    # height
    "AvgHgt": ["AvgHgt"], "Exp": ["Exp"], "Bench": ["Bench"],
}

OFFENSE_TAGS = ["Addicted to 3s", "Don't Blink", "Board Crashers", "Free Throw Merchants"]
DEFENSE_TAGS = ["Lock Down", "No Rebounds for You", "Arc Wall", "Turnover Machine"]
# defensive weaknesses (bottom top_pct of the same stat) used for "exploit" bonuses
WEAKNESS_OF = {
    "Lock Down": "Sieve",
    "No Rebounds for You": "Glass Leak",
    "Arc Wall": "Open Arc",
    "Turnover Machine": "Passive D",
}


def _pick(df: pd.DataFrame, names: list[str]):
    for n in names:
        if df is not None and n in df.columns:
            return pd.to_numeric(df[n], errors="coerce")
    return None


SENTINELS = {0.0, 100.0, 200.0, -99.0}
# columns that are legitimately preseason-available (KenPom preseason ratings)
ALWAYS_VALID = {"AdjOE", "AdjDE", "AdjTempo", "RankAdjEM"}
MIN_COVERAGE = 0.5   # a column is usable when >= 50% of teams have real values


def _sanitize(col: pd.Series, name: str) -> pd.Series:
    """Early season KenPom returns placeholders (0 / 100 / 200 / -99) for teams
    with no games. Turn those into NaN; if fewer than MIN_COVERAGE of teams have
    real numbers, the whole column is treated as unavailable (all NaN)."""
    if name in ALWAYS_VALID:
        return col
    real = ~col.isin(SENTINELS) & col.notna()
    if real.mean() < MIN_COVERAGE:
        return pd.Series(np.nan, index=col.index)
    return col.where(real | (col.notna() & (real.mean() > 0.95)))


def build_team_table(data: dict, prior: dict | None = None) -> pd.DataFrame:
    """Merge ratings/four_factors/height/misc into one frame keyed by TeamName.

    Early-season placeholders become NaN. If `prior` (last season's data dict,
    e.g. load_data("data/prior")) is supplied, missing stat values are filled
    from it; out.attrs["prior_filled"] lists which columns were filled."""
    frames = {k: data.get(k) for k in ("ratings", "four_factors", "height", "misc")}
    base = frames["ratings"][["TeamName"]].copy()
    base["TeamName"] = base["TeamName"].astype(str)
    out = base.set_index("TeamName")
    for canon, cands in ALIASES.items():
        for key in ("ratings", "four_factors", "misc", "height"):
            df = frames.get(key)
            if df is None or not isinstance(df, pd.DataFrame) or df.empty or "TeamName" not in df.columns:
                continue
            col = _pick(df, cands)
            if col is not None:
                s = pd.Series(col.values, index=df["TeamName"].astype(str))
                s = s[~s.index.duplicated()]
                out[canon] = _sanitize(s.reindex(out.index), canon)
                break
        if canon not in out:
            out[canon] = np.nan
    out.attrs["prior_filled"] = []
    if prior:
        prev = build_team_table(prior)
        for canon in ALIASES:
            if canon in ALWAYS_VALID or canon not in prev:
                continue
            missing = out[canon].isna()
            if missing.any() and prev[canon].notna().any():
                out.loc[missing, canon] = prev[canon].reindex(out.index)[missing]
                if missing.mean() >= MIN_COVERAGE:
                    out.attrs["prior_filled"].append(canon)
    # Defensive OR column: KenPom's defense "OR%" is opponents' offensive-rebound %
    # (lower = better). Some docs call it DR%. Detect by scale: OR% averages ~28-32,
    # DR% averages ~68-72.
    if "D_OR_raw" in out and out["D_OR_raw"].notna().any():
        if out["D_OR_raw"].mean() > 50:
            out["Opp_OR"] = 100 - out["D_OR_raw"]
        else:
            out["Opp_OR"] = out["D_OR_raw"]
        out["DR"] = 100 - out["Opp_OR"]
    else:
        out["Opp_OR"] = np.nan
        out["DR"] = np.nan
    return out


def _top(series: pd.Series | None, pct: float, high: bool = True) -> pd.Series:
    if series is None or series.isna().all():
        return pd.Series(False, index=series.index if series is not None else [])
    r = series.rank(pct=True, ascending=True)  # NaN stays NaN -> compares False
    return (r >= 1 - pct) if high else (r <= pct)


def tag_teams(tt: pd.DataFrame, cfg: MatchupConfig) -> pd.DataFrame:
    """Add boolean archetype columns and a `tags` list column."""
    p = cfg.top_pct
    g = tt.get
    tags = pd.DataFrame(index=tt.index)
    tags["Addicted to 3s"] = _top(g("FG3A_Rate"), p)
    # Don't Blink: fast adjusted tempo and short offensive possessions
    if g("APL_Off") is not None and tt["APL_Off"].notna().mean() >= MIN_COVERAGE:
        z = (tt["AdjTempo"] - tt["AdjTempo"].mean()) / tt["AdjTempo"].std() \
            - (tt["APL_Off"] - tt["APL_Off"].mean()) / tt["APL_Off"].std()
        tags["Don't Blink"] = _top(z, p)
    else:
        tags["Don't Blink"] = _top(g("AdjTempo"), p)
    tags["Board Crashers"] = _top(g("OR"), p)
    tags["Free Throw Merchants"] = _top(g("FTR"), p)
    tags["Lock Down"] = _top(g("AdjDE"), p, high=False)
    tags["No Rebounds for You"] = _top(g("Opp_OR"), p, high=False)
    tags["Arc Wall"] = _top(g("Opp_FG3A_Rate"), p, high=False)
    tags["Turnover Machine"] = _top(g("D_TO"), p)
    # weaknesses
    tags["Sieve"] = _top(g("AdjDE"), p, high=True)
    tags["Glass Leak"] = _top(g("Opp_OR"), p, high=True)
    tags["Open Arc"] = _top(g("Opp_FG3A_Rate"), p, high=True)
    tags["Passive D"] = _top(g("D_TO"), p, high=False)
    tags["Careless"] = _top(g("TO"), p, high=True)
    tags = tags.reindex(tt.index).fillna(False).astype(bool)
    shown = OFFENSE_TAGS + DEFENSE_TAGS
    tags["tags"] = tags[shown].apply(lambda r: [c for c in shown if r[c]], axis=1)
    return tags


def matchup_adjustment(off_tags: pd.Series, def_tags: pd.Series, cfg: MatchupConfig) -> tuple[float, list[str]]:
    """Points added to the OFFENSIVE team's score from its archetypes vs the defense.
    Returns (points, notes)."""
    if not cfg.enabled:
        return 0.0, []
    pts, notes = 0.0, []
    keys = set(cfg.counter) | set(cfg.exploit)
    for key in sorted(keys):
        o, d = key.split("|")
        if not bool(off_tags.get(o, False)):
            continue
        if bool(def_tags.get(d, False)):
            v = -cfg.counter.get(key, 0.0)
            pts += v; notes.append(f"{o} vs {d} ({v:+.2f})")
        elif bool(def_tags.get(WEAKNESS_OF.get(d, ""), False)):
            v = cfg.exploit.get(key, 0.0)
            pts += v; notes.append(f"{o} vs {WEAKNESS_OF[d]} ({v:+.2f})")
    if bool(def_tags.get("Turnover Machine", False)) and bool(off_tags.get("Careless", False)):
        v = -cfg.turnover_machine_vs_careless
        pts += v; notes.append(f"Careless vs Turnover Machine ({v:+.2f})")
    pts = float(np.clip(pts, -cfg.max_adj, cfg.max_adj))
    return pts, notes


def clutch_composite(tt: pd.DataFrame, w_efg: float, w_to: float, w_ft: float) -> pd.Series:
    """Execution score from real fields: good shots (eFG%), ball security (TO%),
    and free throws (FT%). z-scored nationally; missing fields contribute 0."""
    def z(col):
        if col not in tt or tt[col].isna().all():
            return pd.Series(0.0, index=tt.index)
        s = tt[col]
        return ((s - s.mean()) / (s.std() or 1)).fillna(0.0)
    return w_efg * z("eFG") - w_to * z("TO") + w_ft * z("FTPct")
