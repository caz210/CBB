"""Matchup maker: per-team inputs + constants for the public.project_matchup() SQL function.

The pipeline (daily_run.py) publishes, after projections:
  matchup_inputs     one row per D-I team (current season): the ratings, unit scores and HCA the
                     daily run uses for that team
  matchup_constants  one row per league: national averages + model coefficients
Both tables are private (RLS on, no client grants). Clients only call project_matchup(), which
recomputes a game from these rows with exactly the formulas below.

`project()` here is the Python twin of the SQL function, written with the same operation order
so both agree to the last bit. The parity guard (`parity_check`) recomputes today's slate games
through `project()` and compares them with the daily run's own numbers; a mismatch means the SQL
function no longer matches the model and is logged loudly (picks are never blocked).

What the function returns (team_a perspective):
  total   = CZarp total = v1 totals model, each team score rounded to 0.5 (Python round(), i.e.
            half-to-even; Postgres round(float8) is half-to-even too), then summed
  margin  = v2 margin (efficiency + calibration + unit score + per-team home court)
  score_a = (total + margin) / 2, score_b = (total - margin) / 2   -> add up to the CZarp total
  spread_a = -margin, win_pct_a = Phi(margin / margin_sd), win_pct_b = 1 - win_pct_a
"""
from __future__ import annotations

import math
import re
import unicodedata
from datetime import date

import numpy as np

LEAGUE = "ncaam"
VENUES = ("neutral", "a_home", "b_home")
PARITY_TOL = 1e-6

# v1 constants (czarp/legacy_v1.py: HCA_VALUE 3.5 split ±0.5; unit_score_adjustments 0.25)
V1_HCA = 3.5
V1_UNIT_WEIGHT = 0.25

INPUT_COLS = ("oe", "de", "tempo", "kp_pct", "unit_v2", "unit_v1", "hca",
              "to_pct", "d_to_pct", "or_pct", "opp_or_pct")
CONST_COLS = ("avg_tempo", "avg_eff", "total_intercept", "total_slope", "margin_scale", "w_efficiency",
              "qg_coef", "w_quality_gap", "hca_scale", "w_hca", "hca_split", "unit_scale", "w_unit",
              "unit_close_threshold", "unit_close_mult", "unit_neutral_mult", "unit_late_factor",
              "margin_sd", "v1_avg_tempo", "v1_avg_to", "v1_avg_dto", "v1_avg_or", "v1_avg_dor",
              "v1_hca", "v1_unit_weight")


def _f(x) -> float:
    try:
        v = float(x)
    except (TypeError, ValueError):
        return float("nan")
    return v


def _nan(x) -> bool:
    return x is None or (isinstance(x, float) and math.isnan(x))


# ── what the closed form can't reproduce ──────────────────────────────────────
def unsupported_features(cfg) -> list[str]:
    """Model pieces project_matchup() does not implement. Empty list = the SQL function is exact."""
    from czarp.config import feature_weights
    out = []
    fw = feature_weights(cfg)
    if not cfg.use_multiplicative_efficiency:
        out.append("use_multiplicative_efficiency=false")
    if getattr(cfg.matchup, "apply_to_score", True) and fw["builtin"].get("matchup", 1.0) != 0:
        out.append("matchup cards applied to score")
    if cfg.clutch.enabled and fw["builtin"].get("clutch", 1.0) != 0:
        out.append("clutch enabled")
    if fw["custom"]:
        out.append(f"{len(fw['custom'])} custom features")
    return out


# ── pipeline side: build the rows ─────────────────────────────────────────────
def build_inputs(data: dict, cfg=None, as_of: str | None = None, v1data: dict | None = None) -> tuple[dict, dict]:
    """({kenpom name: input row}, constants row) from the same data the daily run projects with."""
    import model as M
    import czarp.legacy_v1 as V1
    from czarp.config import feature_weights
    from czarp.season import season_progress
    cfg = cfg or data.get("config") or M.load_config()
    as_of = as_of or date.today().isoformat()
    W = feature_weights(cfg)["builtin"]
    ratings = data["ratings"]
    ctx = M._context(data, cfg)
    tt = ctx["tt"]

    # v2 averages, exactly as project_game builds them
    avgs = M.compute_ncaa_averages(ratings, data.get("four_factors"), data.get("height"))
    for k, col in (("avg_hgt", "AvgHgt"), ("avg_exp", "Exp"), ("avg_bench", "Bench"),
                   ("to_pct", "TO"), ("dto_pct", "D_TO"), ("or_pct", "OR"),
                   ("dor_pct", "Opp_OR"), ("ft_rate", "FTR"), ("dft_rate", "D_FTR")):
        if col in tt and tt[col].notna().any():
            avgs[k] = tt[col].mean()

    # v1 frames + averages, exactly as daily_run.project_all builds them
    if v1data is None:
        from daily_run import v1_frames
        v1data = v1_frames(tt)
    v1d = v1data
    a1 = V1.compute_ncaa_averages(v1d["ratings"], v1d["four_factors"], v1d["height"])
    n1 = a1["n_teams"]
    r1 = v1d["ratings"].set_index("TeamName")
    ff1 = v1d["four_factors"].set_index("TeamName")
    h1 = v1d["height"].set_index("TeamName")
    name1 = {str(n): str(n) for n in r1.index}
    name1.update(getattr(v1d["ratings"], "attrs", {}).get("v1_renamed", {}))

    hca_tbl = data.get("hca")
    rows = {}
    for name in ratings["TeamName"].astype(str):
        r = M.get_team(ratings, name)
        if str(r["TeamName"]) != name:
            continue
        s = tt.loc[name]
        vname = next((k for k, v in name1.items() if v == name), name)  # v1 frame row name
        u2 = M._unit_score_row(s, avgs, cfg)
        hca, _src = M.team_hca(name, hca_tbl, cfg.default_hca)
        rk = float(r1.loc[vname, "RankAdjEM"])
        u1 = V1.unit_score(h1.loc[vname, "AvgHgt"], h1.loc[vname, "Exp"], h1.loc[vname, "Bench"], a1, n1)
        rows[name] = {
            "oe": _f(r["AdjOE"]), "de": _f(r["AdjDE"]), "tempo": _f(r["AdjTempo"]),
            "kp_pct": (n1 - rk) / n1,
            "unit_v2": None if np.isnan(u2) else float(u2),
            "unit_v1": _f(u1),
            "hca": float(hca),
            "to_pct": _f(ff1.loc[vname, "TO_Pct"]), "d_to_pct": _f(ff1.loc[vname, "DTO_Pct"]),
            "or_pct": _f(ff1.loc[vname, "OR_Pct"]), "opp_or_pct": _f(ff1.loc[vname, "DOR_Pct"]),
            # v1 reads its own frame; flag if it ever differs from the v2 ratings row
            "_v1_oe": _f(r1.loc[vname, "AdjOE"]), "_v1_de": _f(r1.loc[vname, "AdjDE"]),
            "_v1_tempo": _f(r1.loc[vname, "AdjTempo"]),
        }

    u = cfg.unit
    late = 1.0
    prog = season_progress(as_of)
    if prog > u.late_season_start:
        ramp = (prog - u.late_season_start) / (1 - u.late_season_start)
        late = 1 + (u.late_season_mult - 1) * ramp
    consts = {
        "avg_tempo": float(avgs["pace"]), "avg_eff": float(avgs["eff"]),
        "total_intercept": float(cfg.total_intercept), "total_slope": float(cfg.total_slope),
        "margin_scale": float(cfg.efficiency_margin_scale), "w_efficiency": float(W["efficiency"]),
        "qg_coef": float(cfg.quality_gap.coef) if cfg.quality_gap.enabled else 0.0,
        "w_quality_gap": float(W["quality_gap"]),
        "hca_scale": float(cfg.hca_scale), "w_hca": float(W["hca"]), "hca_split": float(cfg.hca_split),
        "unit_scale": float(u.scale), "w_unit": float(W["unit"]),
        "unit_close_threshold": float(u.close_game_threshold), "unit_close_mult": float(u.close_game_mult),
        "unit_neutral_mult": float(u.neutral_mult), "unit_late_factor": float(late),
        "margin_sd": float(cfg.betting.margin_sd),
        "v1_avg_tempo": float(a1["pace"]), "v1_avg_to": float(a1["to_pct"]), "v1_avg_dto": float(a1["dto_pct"]),
        "v1_avg_or": float(a1["or_pct"]), "v1_avg_dor": float(a1["dor_pct"]),
        "v1_hca": V1_HCA, "v1_unit_weight": V1_UNIT_WEIGHT,
        "model_version": getattr(cfg, "model_version", "czarp-v2"),
        "unsupported": unsupported_features(cfg),
    }
    return rows, consts


# ── the formula (Python twin of public.project_matchup) ───────────────────────
def _v2_margin(t1: dict, t2: dict, t1_home, C: dict) -> float:
    """team1 margin, model.project_game order of operations. t1_home: True/False/None(neutral)."""
    pace = t1["tempo"] * t2["tempo"] / C["avg_tempo"]
    p1 = t1["oe"] * t2["de"] / C["avg_eff"] / 100
    p2 = t2["oe"] * t1["de"] / C["avg_eff"] / 100
    b1, b2 = p1 * pace, p2 * pace
    bt = C["total_intercept"] + C["total_slope"] * (b1 + b2)
    bm = C["margin_scale"] * (b1 - b2) * C["w_efficiency"]
    s1, s2 = (bt + bm) / 2, (bt - bm) / 2
    qg = C["qg_coef"] * (t1["kp_pct"] - t2["kp_pct"]) * 10 * C["w_quality_gap"]
    s1 += qg / 2
    s2 -= qg / 2
    if t1_home is None:
        h1 = h2 = 0.0
    else:
        hv = (t1 if t1_home else t2)["hca"] * C["hca_scale"] * C["w_hca"]
        h1, h2 = (hv * C["hca_split"], -hv * C["hca_split"]) if t1_home else (-hv * C["hca_split"], hv * C["hca_split"])
    pre = (s1 + h1) - (s2 + h2)
    mult = 1.0
    if abs(pre) <= C["unit_close_threshold"]:
        mult *= C["unit_close_mult"]
    if t1_home is None:
        mult *= C["unit_neutral_mult"]
    mult *= C["unit_late_factor"]
    if _nan(t1["unit_v2"]) or _nan(t2["unit_v2"]):
        u1 = u2 = 0.0
    else:
        u1 = (t1["unit_v2"] - t2["unit_v2"]) * C["unit_scale"] * mult * C["w_unit"]
        u2 = -u1
    s1 += u1 + h1
    s2 += u2 + h2
    return s1 - s2


def _v1_scores(t1: dict, t2: dict, t1_home, C: dict) -> tuple[float, float]:
    """(team1, team2) v1 scores before rounding, czarp/legacy_v1.project_game order of operations."""
    a1 = (t2["kp_pct"] - t1["kp_pct"]) * 0.5
    a2 = (t1["kp_pct"] - t2["kp_pct"]) * 0.5
    avg = C["v1_avg_tempo"]
    pace = avg + (t1["tempo"] - avg) + (t2["tempo"] - avg)
    ppp1 = ((t1["oe"] + t2["de"]) / 2) / 100
    ppp2 = ((t2["oe"] + t1["de"]) / 2) / 100

    def to(tm, opp, adj):
        raw = ((C["v1_avg_to"] - tm) + (C["v1_avg_dto"] - opp)) / 2
        return raw + abs(raw) * adj

    def reb(tm_or, opp_dor, adj):
        raw = ((opp_dor - C["v1_avg_dor"]) + (tm_or - C["v1_avg_or"])) / 2
        return raw + abs(raw) * adj

    to1, to2 = to(t1["to_pct"], t2["d_to_pct"], a1), to(t2["to_pct"], t1["d_to_pct"], a2)
    rb1, rb2 = reb(t1["or_pct"], t2["opp_or_pct"], a1), reb(t2["or_pct"], t1["opp_or_pct"], a2)
    poss1 = pace + (pace * (rb1 * 0.01) + pace * (to1 * 0.01))
    poss2 = pace + (pace * (rb2 * 0.01) + pace * (to2 * 0.01))
    c1, c2 = ppp1 * poss1, ppp2 * poss2
    c1 += (t1["unit_v1"] - t2["unit_v1"]) * C["v1_unit_weight"]
    c2 += (t2["unit_v1"] - t1["unit_v1"]) * C["v1_unit_weight"]
    if t1_home is not None:
        h = C["v1_hca"] * 0.5
        c1, c2 = (c1 + h, c2 - h) if t1_home else (c1 - h, c2 + h)
    return c1, c2


def round_half(x: float) -> float:
    """legacy mround: round(x / 0.5) * 0.5 with Python's half-to-even round()."""
    return round(x / 0.5) * 0.5


def project(a: dict, b: dict, venue: str, C: dict) -> dict:
    """Matchup a vs b. venue: 'neutral' | 'a_home' | 'b_home'.

    Mirrors the daily run, which projects (away, home): when a team is home it is team2; at a
    neutral site team_a plays the listed-away (team1) role.
    """
    if venue not in VENUES:
        raise ValueError(f"venue must be one of {VENUES}")
    if venue == "a_home":
        t1, t2, t1_home, a_is_t1 = b, a, False, False
    elif venue == "b_home":
        t1, t2, t1_home, a_is_t1 = a, b, False, True
    else:
        t1, t2, t1_home, a_is_t1 = a, b, None, True
    m1 = _v2_margin(t1, t2, t1_home, C)
    c1, c2 = _v1_scores(t1, t2, t1_home, C)
    v1_total = round_half(c1) + round_half(c2)
    margin_a = m1 if a_is_t1 else -m1
    win_a = 0.5 * (1.0 + math.erf((margin_a / C["margin_sd"]) / math.sqrt(2.0)))
    return {"venue": venue, "total": v1_total, "margin_a": margin_a,
            "score_a": (v1_total + margin_a) / 2, "score_b": (v1_total - margin_a) / 2,
            "spread_a": -margin_a, "win_pct_a": win_a, "win_pct_b": 1 - win_a}


# ── parity guard ──────────────────────────────────────────────────────────────
def parity_check(games: list[dict], rows: dict, consts: dict, tol: float = PARITY_TOL) -> dict:
    """Recompute today's projected games through project() and compare with the daily run.

    Compares the v2 home margin and home win %, and the v1 total (pre pace-fix value when the
    optional pace fix re-wrote it). Returns {'checked', 'mismatches': [...], 'missing': [...],
    'max_margin_diff', 'max_total_diff', 'max_win_diff', 'unsupported'}.
    """
    out = {"checked": 0, "mismatches": [], "missing": [], "max_margin_diff": 0.0,
           "max_total_diff": 0.0, "max_win_diff": 0.0, "unsupported": list(consts.get("unsupported") or [])}
    for g in games:
        h, a = g.get("kp_home"), g.get("kp_away")
        if g.get("skip") or not h or not a or _nan(g.get("v2_home_margin")):
            continue
        if h not in rows or a not in rows:
            out["missing"].append(f"{a} @ {h}")
            continue
        venue = "neutral" if g.get("neutral") else "b_home"
        p = project(rows[a], rows[h], venue, consts)       # a = away = team1, b = home
        dm = abs(-p["margin_a"] - float(g["v2_home_margin"]))
        dw = abs(p["win_pct_b"] - float(g["v2_home_win_prob"])) if not _nan(g.get("v2_home_win_prob")) else 0.0
        v1 = g.get("v1_total_raw", g.get("v1_total"))
        dt = abs(p["total"] - float(v1)) if not _nan(v1) else None
        out["checked"] += 1
        out["max_margin_diff"] = max(out["max_margin_diff"], dm)
        out["max_win_diff"] = max(out["max_win_diff"], dw)
        if dt is not None:
            out["max_total_diff"] = max(out["max_total_diff"], dt)
        if dm > tol or dw > tol or (dt is not None and dt > tol) or dt is None:
            out["mismatches"].append({"game": f"{a} @ {h}", "neutral": bool(g.get("neutral")),
                                      "margin_diff": dm, "win_diff": dw, "total_diff": dt,
                                      "daily_total": None if _nan(v1) else float(v1), "matchup_total": p["total"]})
    return out


def parity_log_lines(res: dict) -> list[str]:
    lines = [f"  matchup parity: {res['checked']} games, max diff margin {res['max_margin_diff']:.2e} / "
             f"total {res['max_total_diff']:.2e} / win% {res['max_win_diff']:.2e}"]
    if res["unsupported"]:
        lines.append("  !!! MATCHUP PARITY WARNING: model uses features project_matchup() does not implement: "
                     + "; ".join(res["unsupported"]))
    if res["mismatches"] or res["missing"]:
        lines.append(f"  !!! MATCHUP PARITY FAILED: {len(res['mismatches'])} mismatched, {len(res['missing'])} "
                     "missing — the matchup maker no longer matches the daily run (picks are unaffected)")
        for m in res["mismatches"][:10]:
            lines.append(f"  !!!   {m['game']}{' (neutral)' if m['neutral'] else ''}: margin diff {m['margin_diff']:.3g}, "
                         f"win% diff {m['win_diff']:.3g}, total {m['daily_total']} vs {m['matchup_total']}")
        for m in res["missing"][:10]:
            lines.append(f"  !!!   no matchup inputs for {m}")
    return lines


# ── Supabase writer (service role) ────────────────────────────────────────────
def write_supabase(client, rows: dict, consts: dict, season: int, as_of_ts: str, log=print) -> int:
    """Upsert matchup_inputs (one row per team) + matchup_constants (one row per league)."""
    teams = client.get("teams", select="id,name", league=f"eq.{LEAGUE}", limit="2000")
    tid = {t["name"]: t["id"] for t in teams}
    missing = sorted(set(rows) - set(tid))
    if missing:
        log(f"  matchup inputs: {len(missing)} teams not in teams table (skipped): {', '.join(missing[:5])}")
    incomplete = sorted(n for n in rows if any(_nan(rows[n][k]) for k in INPUT_COLS if k != "unit_v2"))
    if incomplete:
        log(f"  matchup inputs: {len(incomplete)} teams missing a rating/four-factor value (skipped; the matchup "
            f"maker will report them as unknown): {', '.join(incomplete[:5])}")
    body = [{"team_id": tid[n], "season": season, **{k: rows[n][k] for k in INPUT_COLS}, "as_of": as_of_ts}
            for n in sorted(rows) if n in tid and n not in incomplete]
    client.upsert("matchup_inputs", body, on_conflict="team_id", returning=False)
    crow = {"league": LEAGUE, "season": season, "model_version": consts["model_version"],
            **{k: consts[k] for k in CONST_COLS}, "as_of": as_of_ts}
    client.upsert("matchup_constants", [crow], on_conflict="league", returning=False)
    # rows for teams that dropped out of this season's table would be stale: remove them
    keep = {r["team_id"] for r in body}
    stale = [t["team_id"] for t in client.get("matchup_inputs", select="team_id", limit="2000")
             if t["team_id"] not in keep]
    if stale:
        client.delete("matchup_inputs", team_id=f"in.({','.join(map(str, stale))})")
    return len(body)


# ── alias normalisation (same rules as web/src/lib/teamSearch.ts normalize) ───
def normalize_alias(s: str) -> str:
    s = unicodedata.normalize("NFKD", str(s).lower())
    s = re.sub(r"[\u0300-\u036f]", "", s)
    s = re.sub(r"\b([a-z])\s*&\s*([a-z])\b", r"\1\2", s)
    s = s.replace("&", " ")
    s = re.sub(r"[.'’`]", "", s)
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return " ".join(t for t in s.split(" ") if t and t != "and")
