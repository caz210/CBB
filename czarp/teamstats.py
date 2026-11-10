"""Normalize KenPom tables into one per-team frame and tag archetypes.

KenPom's API field names differ between docs/versions (e.g. DTO_Pct vs
Def_TO_Pct), so everything is mapped to canonical names here.

Style tags (Nov 2026): short plain names, 15th/85th percentile vs D-I,
max 2 chips per team (most extreme tails). Weaknesses are first-class.
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
    "FG3Pct": ["FG3Pct", "FG3_Pct"],
    "Opp_FG3Pct": ["OppFG3Pct", "Opp_FG3_Pct"],
    # height
    "AvgHgt": ["AvgHgt"], "Exp": ["Exp"], "Bench": ["Bench"],
}

# (name, column, high_tail, kind)
# high_tail=True  -> top 15% (pct rank >= 1 - top_pct)
# high_tail=False -> bottom 15% (pct rank <= top_pct)
TAG_DEFS = [
    ("Fast", "AdjTempo", True, "strength"),
    ("Slow", "AdjTempo", False, "style"),
    ("Sharp", "eFG", True, "strength"),
    ("Cold", "eFG", False, "weakness"),
    ("Volume 3s", "FG3A_Rate", True, "strength"),
    ("Paint First", "FG3A_Rate", False, "style"),
    ("Secure", "TO", False, "strength"),       # low TO%
    ("Careless", "TO", True, "weakness"),      # high TO%
    ("Disrupt", "D_TO", True, "strength"),
    ("Crash", "OR", True, "strength"),
    ("Control Glass", "Opp_OR", False, "strength"),  # low opp OR%
    ("At the Line", "FTR", True, "strength"),
    ("Foul Prone", "D_FTR", True, "weakness"),
    ("Wall", "D_eFG", False, "strength"),      # low eFG allowed
    ("Soft D", "D_eFG", True, "weakness"),
    ("Crowd 3s", "Opp_FG3A_Rate", False, "strength"),
]

STRENGTH_TAGS = [n for n, _, _, k in TAG_DEFS if k == "strength"]
WEAKNESS_TAGS = [n for n, _, _, k in TAG_DEFS if k == "weakness"]
STYLE_TAGS = [n for n, _, _, k in TAG_DEFS if k == "style"]
ALL_TAG_NAMES = [n for n, *_ in TAG_DEFS]

# Display cap on slate / projection chips
MAX_TAGS_SHOWN = 2

# Hidden glass-soft signal for upset alerts (not a public chip name)
SOFT_GLASS = "Soft Glass"   # Opp_OR top 15%
OPEN_ARC = "Open Arc"       # Opp_FG3A_Rate top 15% (weak 3pt D volume)

# Defensive counters used by matchup_adjustment (offense_tag|defense_tag)
WEAKNESS_OF = {
    "Crowd 3s": OPEN_ARC,
    "Control Glass": SOFT_GLASS,
    "Wall": "Soft D",
    "Disrupt": "Careless",  # offense Careless is exploited by Disrupt (handled separately)
}


def _pick(df: pd.DataFrame, names: list[str]):
    for n in names:
        if df is not None and n in df.columns:
            return pd.to_numeric(df[n], errors="coerce")
    return None


SENTINELS = {0.0, 100.0, 200.0, -99.0}
ALWAYS_VALID = {"AdjOE", "AdjDE", "AdjTempo", "RankAdjEM"}
MIN_COVERAGE = 0.5


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


STYLE_COLS = [c for c in ALIASES if c not in ALWAYS_VALID and c not in ("APL_Off", "APL_Def")]


def _raw_table(data: dict, sanitize_cols: bool = True) -> pd.DataFrame:
    frames = {k: data.get(k) for k in ("ratings", "four_factors", "height", "misc")}
    base = frames["ratings"][["TeamName"]].copy()
    base["TeamName"] = base["TeamName"].astype(str)
    out = base.set_index("TeamName")
    for canon, cands in list(ALIASES.items()) + [("Continuity", ["Continuity"])]:
        for key in ("ratings", "four_factors", "misc", "height"):
            df = frames.get(key)
            if df is None or not isinstance(df, pd.DataFrame) or df.empty or "TeamName" not in df.columns:
                continue
            col = _pick(df, cands)
            if col is not None:
                s = pd.Series(col.values, index=df["TeamName"].astype(str))
                s = s[~s.index.duplicated()]
                s = s.reindex(out.index)
                if sanitize_cols:
                    s = _sanitize(s, canon)
                else:
                    s = s.where(~s.isin(SENTINELS)) if canon not in ALWAYS_VALID else s
                out[canon] = s
                break
        if canon not in out:
            out[canon] = np.nan
    r = frames["ratings"]
    if {"Wins", "Losses"} <= set(r.columns):
        g = pd.to_numeric(r["Wins"], errors="coerce").fillna(0) + pd.to_numeric(r["Losses"], errors="coerce").fillna(0)
        out["Games"] = pd.Series(g.values, index=r["TeamName"].astype(str)).groupby(level=0).first().reindex(out.index)
    else:
        out["Games"] = np.nan
    return out


def build_team_table(data: dict, prior: dict | None = None, early=None) -> pd.DataFrame:
    """Merge ratings/four_factors/height/misc into one frame keyed by TeamName."""
    mode = getattr(early, "style_fill", "prior" if prior else "off")
    if mode == "regressed" and prior:
        out = _raw_table(data, sanitize_cols=False)
    else:
        out = _raw_table(data)
    out.attrs["prior_filled"] = []
    games = out["Games"]
    full = getattr(early, "full_weight_games", 10) or 1
    w = (games / full).clip(0, 1).fillna(1.0)
    if prior and mode == "prior":
        prev = _raw_table(prior)
        for canon in STYLE_COLS:
            missing = out[canon].isna()
            if missing.any() and prev[canon].notna().any():
                out.loc[missing, canon] = prev[canon].reindex(out.index)[missing]
                if missing.mean() >= MIN_COVERAGE:
                    out.attrs["prior_filled"].append(canon)
        w = pd.Series(np.where(out["Games"].notna() & (out["Games"] < full), w, 1.0), index=out.index)
    elif prior and mode == "regressed":
        prev = _raw_table(prior)
        cont = out["Continuity"].where(out["Continuity"].between(0.0001, 1.0))
        if cont.notna().mean() < MIN_COVERAGE:
            cont = pd.Series(np.nan, index=out.index)
        if cont.notna().any() and cont.max() > 1.5:
            cont = cont / 100
        keep = (early.prior_keep_max * cont).fillna(early.prior_keep_default)
        used = []
        for canon in STYLE_COLS:
            p = prev[canon].reindex(out.index)
            if p.notna().sum() == 0:
                continue
            mu = p.mean()
            reg = mu + keep * (p - mu)
            reg = reg.fillna(mu)
            cur = out[canon]
            wc = w.where(cur.notna(), 0.0)
            out[canon] = (wc * cur.fillna(0) + (1 - wc) * reg)
            if (wc < 1).mean() >= 0.05:
                used.append(canon)
        out.attrs["prior_filled"] = used
        w = w.where(out["Games"].notna(), 1.0)
    out["CurWeight"] = w
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


def _pct_rank(series: pd.Series) -> pd.Series:
    """0–1 percentile rank (NaN stays NaN). Higher value -> higher rank."""
    if series is None or series.isna().all():
        return pd.Series(np.nan, index=series.index if series is not None else [])
    return series.rank(pct=True, ascending=True)


def _top(series: pd.Series | None, pct: float, high: bool = True) -> pd.Series:
    if series is None or series.isna().all():
        return pd.Series(False, index=series.index if series is not None else [])
    r = _pct_rank(series)
    return (r >= 1 - pct) if high else (r <= pct)


def tag_teams(tt: pd.DataFrame, cfg: MatchupConfig, max_shown: int = MAX_TAGS_SHOWN) -> pd.DataFrame:
    """Boolean archetype columns, extremity scores, and capped `tags` / `tag_objs` lists."""
    p = cfg.top_pct
    tags = pd.DataFrame(index=tt.index)
    extremity = pd.DataFrame(index=tt.index)  # how deep in the qualifying tail (0–1)

    for name, col, high_tail, kind in TAG_DEFS:
        s = tt[col] if col in tt else None
        hit = _top(s, p, high=high_tail)
        tags[name] = hit
        pr = _pct_rank(s) if s is not None else pd.Series(np.nan, index=tt.index)
        # extremity: distance past the threshold into the tail
        if high_tail:
            # rank in [1-p, 1] -> map to [0, 1]
            extremity[name] = ((pr - (1 - p)) / p).clip(0, 1).where(hit, 0.0)
        else:
            # rank in [0, p] -> map to [0, 1] (lower = more extreme)
            extremity[name] = ((p - pr) / p).clip(0, 1).where(hit, 0.0)

    # Hidden signals for matchup / upset (not shown as chips unless also a named tag)
    tags[SOFT_GLASS] = _top(tt["Opp_OR"] if "Opp_OR" in tt else None, p, high=True)
    tags[OPEN_ARC] = _top(tt["Opp_FG3A_Rate"] if "Opp_FG3A_Rate" in tt else None, p, high=True)
    tags = tags.reindex(tt.index).fillna(False).astype(bool)
    extremity = extremity.reindex(tt.index).fillna(0.0)

    kinds = {n: k for n, _, _, k in TAG_DEFS}

    def pick_row(i):
        scored = [(n, float(extremity.at[i, n]), kinds[n])
                  for n in ALL_TAG_NAMES if bool(tags.at[i, n])]
        scored.sort(key=lambda x: -x[1])
        top = scored[:max_shown]
        objs = [{"name": n, "kind": k} for n, _, k in top]
        names = [n for n, _, _ in top]
        return names, objs

    picked = [pick_row(i) for i in tags.index]
    tags["tags"] = [x[0] for x in picked]
    tags["tag_objs"] = [x[1] for x in picked]
    tags["_extremity"] = [dict(zip(ALL_TAG_NAMES, [float(extremity.at[i, n]) for n in ALL_TAG_NAMES]))
                          for i in tags.index]
    return tags


def mark_tentative(tags: pd.DataFrame, tt: pd.DataFrame, early) -> pd.DataFrame:
    """Teams with fewer than early.tag_evidence_games current-season games get
    tentative tags (display "(tentative)", matchup points scaled down)."""
    tags = tags.copy()
    n = getattr(early, "tag_evidence_games", 0) if early is not None else 0
    g = tt["Games"].reindex(tags.index) if "Games" in tt else pd.Series(np.nan, index=tags.index)
    tags["tentative"] = g.notna() & (g < n)
    tags["tags_display"] = [
        [f"{t} (tentative)" for t in ts] if tent else list(ts)
        for ts, tent in zip(tags["tags"], tags["tentative"])]
    tags["tag_objs_display"] = [
        [{"name": o["name"], "kind": o["kind"], "tentative": bool(tent)} for o in objs]
        for objs, tent in zip(tags["tag_objs"], tags["tentative"])]
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
            pts += v
            notes.append(f"{o} vs {d} ({v:+.2f})")
        else:
            weak = WEAKNESS_OF.get(d, "")
            if weak and bool(def_tags.get(weak, False)):
                v = cfg.exploit.get(key, 0.0)
                pts += v
                notes.append(f"{o} vs {weak} ({v:+.2f})")
    if bool(def_tags.get("Disrupt", False)) and bool(off_tags.get("Careless", False)):
        v = -cfg.turnover_machine_vs_careless
        pts += v
        notes.append(f"Careless vs Disrupt ({v:+.2f})")
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


def di_percentile(series: pd.Series, high_better: bool = True) -> pd.Series:
    """National percentile 0–100 where 100 = best when high_better else lowest raw is best."""
    if series is None or series.isna().all():
        return pd.Series(np.nan, index=series.index if series is not None else [])
    r = series.rank(pct=True, ascending=high_better)
    return (r * 100).round(1)


# Upset: dog strength that exploits favorite weakness (checked on full boolean tags, not just top-2)
UPSET_PAIRS = [
    # (dog_tag, favorite_tag_or_signal, reason_template)
    ("Crash", SOFT_GLASS, "{dog} Crash vs {fav} soft defensive glass"),
    ("Volume 3s", "Soft D", "{dog} Volume 3s vs {fav} Soft D"),
    ("Sharp", "Soft D", "{dog} Sharp vs {fav} Soft D"),
    ("Volume 3s", OPEN_ARC, "{dog} Volume 3s vs {fav} soft 3-point defense"),
    ("Sharp", OPEN_ARC, "{dog} Sharp vs {fav} soft 3-point defense"),
    ("Disrupt", "Careless", "{dog} Disrupt vs {fav} Careless"),
    ("At the Line", "Foul Prone", "{dog} At the Line vs {fav} Foul Prone"),
]


def upset_alert(home: str, away: str, tags: pd.DataFrame,
                model_home_margin: float, vegas_home_spread: float | None,
                home_win_prob: float | None = None) -> tuple[bool, str]:
    """Game-level flag: underdog has a strength that exploits the favorite's weakness
    AND the model leans toward the dog (vs Vegas spread or outright). Not a bet."""
    if vegas_home_spread is None or home not in tags.index or away not in tags.index:
        return False, ""
    # Favorite = Vegas favorite (home favored when spread < 0)
    if vegas_home_spread < 0:
        fav, dog = home, away
        model_likes_dog = (model_home_margin + vegas_home_spread) < 0
        if home_win_prob is not None and home_win_prob < 0.5:
            model_likes_dog = True
    elif vegas_home_spread > 0:
        fav, dog = away, home
        model_likes_dog = (model_home_margin + vegas_home_spread) > 0
        if home_win_prob is not None and home_win_prob > 0.5:
            model_likes_dog = True
    else:
        return False, ""  # pick'em — no favorite
    if not model_likes_dog:
        return False, ""
    dog_row, fav_row = tags.loc[dog], tags.loc[fav]
    reasons = []
    for dog_tag, fav_tag, tmpl in UPSET_PAIRS:
        if bool(dog_row.get(dog_tag, False)) and bool(fav_row.get(fav_tag, False)):
            reasons.append(tmpl.format(dog=dog, fav=fav))
    if not reasons:
        return False, ""
    return True, "; ".join(reasons)


def projection_reason(home: str, away: str, home_tags: list, away_tags: list,
                      pace: float | None, edge_note: str | None = None,
                      matchup_notes: list | None = None,
                      upset: bool = False, upset_reason: str = "") -> str:
    """1–2 plain sentences for projections.reason. No KenPom ranks."""
    parts = []
    ht = ", ".join(home_tags) if home_tags else "no standout style tags"
    at = ", ".join(away_tags) if away_tags else "no standout style tags"
    parts.append(f"{away} ({at}) at {home} ({ht}).")
    if pace is not None:
        parts.append(f"Projected pace about {pace:.0f} possessions.")
    if matchup_notes:
        # strip point deltas for a cleaner sentence
        clean = []
        for n in matchup_notes[:2]:
            clean.append(n.split(" (")[0] if " (" in n else n)
        parts.append("Matchup notes: " + "; ".join(clean) + ".")
    if upset and upset_reason:
        parts.append(f"Upset alert: {upset_reason}.")
    if edge_note:
        parts.append(edge_note)
    text = " ".join(parts)
    # keep to ~2 sentences worth
    sentences = [s.strip() for s in text.replace("..", ".").split(".") if s.strip()]
    return ". ".join(sentences[:3]) + ("." if sentences else "")


PROFILE_STAT_COLS = [
    # (json_key, tt_col, high_better)
    ("efg", "eFG", True),
    ("d_efg", "D_eFG", False),
    ("to_pct", "TO", False),
    ("d_to_pct", "D_TO", True),
    ("or_pct", "OR", True),
    ("opp_or_pct", "Opp_OR", False),
    ("ftr", "FTR", True),
    ("d_ftr", "D_FTR", False),
    ("tempo", "AdjTempo", True),
    ("fg3a_rate", "FG3A_Rate", True),
    ("fg3_pct", "FG3Pct", True),
    ("opp_fg3_pct", "Opp_FG3Pct", False),
    ("ft_pct", "FTPct", True),
]


def build_team_profiles(tt: pd.DataFrame, tags: pd.DataFrame, *,
                        as_of: str, season: int,
                        hca_df: pd.DataFrame | None = None,
                        new_coach: dict | None = None,
                        lock_sides_games: int | None = None,
                        lock_totals_games: int | None = None) -> list[dict]:
    """One profile dict per team for team_profiles upsert. Percentiles only (no KP ranks).

    new_coach should be the map from czarp.coach.detect_new_coaches (optionally run through
    enrich_coach_profile) — the same source daily_run uses for the NEW COACH lock.
    """
    from czarp.coach import LOCK_SIDES_GAMES, LOCK_TOTALS_GAMES, enrich_coach_profile
    if lock_sides_games is None:
        lock_sides_games = LOCK_SIDES_GAMES
    if lock_totals_games is None:
        lock_totals_games = LOCK_TOTALS_GAMES
    new_coach = enrich_coach_profile(new_coach or {}, lock_sides_games=lock_sides_games,
                                     lock_totals_games=lock_totals_games)
    hca_map = {}
    if hca_df is not None and not hca_df.empty and "TeamName" in hca_df.columns and "HCA" in hca_df.columns:
        for _, r in hca_df.iterrows():
            hca_map[str(r["TeamName"])] = float(pd.to_numeric(r["HCA"], errors="coerce"))

    pcts = {key: di_percentile(tt[col], hib) if col in tt else pd.Series(np.nan, index=tt.index)
            for key, col, hib in PROFILE_STAT_COLS}

    rows = []
    for name in tt.index:
        row = tt.loc[name]
        trow = tags.loc[name] if name in tags.index else None
        factors = {}
        for key, col, _ in PROFILE_STAT_COLS:
            val = float(row[col]) if col in row and pd.notna(row[col]) else None
            pct = float(pcts[key].loc[name]) if name in pcts[key].index and pd.notna(pcts[key].loc[name]) else None
            factors[key] = val
            factors[f"{key}_pctile"] = pct
        tag_list = list(trow["tag_objs_display"]) if trow is not None and "tag_objs_display" in trow else []
        # Also keep full boolean set for grading / research (all fired tags, not just top-2)
        all_fired = [{"name": n, "kind": k} for n, _, _, k in TAG_DEFS
                     if trow is not None and bool(trow.get(n, False))]
        nc = new_coach.get(str(name)) or {}
        flagged = bool(nc)
        rows.append({
            "name": str(name),
            "season": season,
            "as_of": as_of,
            "games_played": int(row["Games"]) if pd.notna(row.get("Games")) else None,
            "tentative": bool(trow["tentative"]) if trow is not None else True,
            "tags": tag_list,
            "tags_all": all_fired,
            "factors": factors,
            "adj_oe": float(row["AdjOE"]) if pd.notna(row.get("AdjOE")) else None,
            "adj_de": float(row["AdjDE"]) if pd.notna(row.get("AdjDE")) else None,
            "hca": hca_map.get(name) if name in hca_map else hca_map.get(str(name)),
            "new_coach": flagged,
            "new_coach_games": nc.get("games") if flagged else None,
            "lock_sides_games": int(nc["lock_sides"]) if flagged and nc.get("lock_sides") is not None else lock_sides_games,
            "lock_totals_games": int(nc["lock_totals"]) if flagged and nc.get("lock_totals") is not None else lock_totals_games,
            "lock_sides_remaining": nc.get("lock_sides_remaining") if flagged else None,
            "lock_totals_remaining": nc.get("lock_totals_remaining") if flagged else None,
        })
    return rows
