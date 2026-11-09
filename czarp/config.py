"""All tunable CZarp coefficients in one place.

Defaults are deliberately conservative placeholders. Every coefficient marked
"FIT" should be re-estimated by the backtest before real money rides on it.
Override any value with a JSON file (CZARP_CONFIG env var or czarp_config.json),
e.g. {"margin_sd": 10.5, "unit": {"scale": 0.2}}.
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field, asdict, fields, is_dataclass


@dataclass
class QualityGapConfig:
    # Original sheet logic: (pct_gap * 0.5) used as a dampener on the TO/REB terms.
    # Those terms are gone in v2, so the gap becomes a direct margin bump for the
    # better team:  points = coef * (pct_better - pct_worse) * 10.
    # e.g. coef 0.2, #20 vs #200 of 365 (gap ~0.49) -> ~+1.0 pt to the favorite.
    # Note: the multiplicative efficiency formula already widens lopsided games,
    # which was probably most of the "lines too tight" problem. FIT (may go to 0).
    # Walk-forward fit (2024-25 + 2025-26, 11.5k games): coef -0.32 (t -2.6) once the
    # efficiency margin is scaled; dropping it does not hurt out of sample -> OFF.
    enabled: bool = False
    coef: float = 0.2


@dataclass
class UnitConfig:
    # Fitted (walk-forward, 2 seasons). With scale = 1 the weights read as
    #   ~0.27 pts per inch of average height, ~0.48 pts per year of experience,
    #   ~-0.05 pts per point of bench-minutes % (deeper bench slightly WORSE).
    # Original sheet: 40/40/20 with scale 0.25.
    w_height: float = 208.7
    w_exp: float = 8.66
    w_bench: float = -14.39
    scale: float = 1.0         # points per unit-score point of difference
    # Optional extra weight (multipliers applied to `scale`). 1.0 = off.
    close_game_mult: float = 1.0     # applied when |base margin| <= close_game_threshold
    close_game_threshold: float = 6.0
    neutral_mult: float = 1.0        # neutral-site games (tournaments)
    late_season_mult: float = 1.0    # ramps in linearly with season progress after late_season_start
    late_season_start: float = 0.75  # fraction of season (0=Nov 1, 1=Apr 10); ~Feb 22


@dataclass
class ClutchConfig:
    # Fitted -0.10 pts/z (t -1.4); no out-of-sample value -> OFF by default.
    enabled: bool = False
    spread_threshold: float = 5.0    # only applied when |projected margin| <= this
    # Composite = w_efg*z(eFG%) - w_to*z(TO%) + w_ft*z(FT%)
    w_efg: float = 1.0
    w_to: float = 1.0
    w_ft: float = 1.0
    points_per_z: float = 0.35       # FIT: margin points per 1.0 composite z-score difference
    max_adj: float = 1.5             # cap on total clutch swing (points)


@dataclass
class MatchupConfig:
    enabled: bool = True             # compute + show archetype tags / matchup notes
    # Walk-forward: matchup points made projections WORSE out of sample (dMSE -0.21,
    # CI excludes 0) and moved against the closing line (t -2.0). Tags still shown;
    # points are not added to the score unless this is turned on.
    apply_to_score: bool = False
    top_pct: float = 0.15            # archetype = top 15% nationally
    # Each counter pair: offense archetype vs defense archetype. When an offense
    # archetype meets its counter, its owner loses `counter` pts; when it meets an
    # opponent that is in the BOTTOM top_pct of the same defensive stat ("weakness"),
    # it gains `exploit` pts. FIT all of these.
    counter: dict = field(default_factory=lambda: {
        "Volume 3s|Crowd 3s": 0.75,
        "Crash|Control Glass": 0.75,
        "Fast|Wall": 0.5,
        "At the Line|Wall": 0.25,
    })
    exploit: dict = field(default_factory=lambda: {
        "Volume 3s|Crowd 3s": 0.5,
        "Crash|Control Glass": 0.5,
        "Fast|Wall": 0.25,
        "At the Line|Wall": 0.25,
    })
    # Disrupt defense vs a Careless offense (high TO%)
    turnover_machine_vs_careless: float = 0.5
    max_adj: float = 2.0             # cap on total matchup swing per team (points)


@dataclass
class BettingConfig:
    margin_sd: float = 11.4          # fitted: SD of actual margin around the model projection
    total_sd: float = 16.9           # fitted: SD of actual total around the calibrated model total
    # Blend with the market line before computing edges (sportsbook lines only).
    # Fitted convex weights: close line 0.95 market / 0.05 model, open line
    # 0.85 / 0.15; KenPom's own prediction got weight 0 (model ~= KenPom, r=0.99).
    blend_with_market: bool = True
    blend_w_market: float = 0.9
    blend_w_kenpom: float = 0.0      # needs result["kenpom_margin"] (fanmatch)
    blend_margin_sd: float = 11.2
    # Totals: market weight that minimised OOS RMSE of actual totals was ~0.7 vs
    # the open total and ~1.0 vs the close (single fit on walk-forward preds).
    blend_w_market_total: float = 0.85
    tiers: tuple = (0.03, 0.05, 0.08)   # edge thresholds (probability points)
    tier_names: tuple = ("LEAN", "VALUE", "STRONG")
    kelly_fraction: float = 0.25
    max_stake_pct: float = 0.025     # hard cap: 2.5% of bankroll
    price_source: str = "vegas"      # "vegas" | "kalshi"
    kalshi_fee_profile: str = "robinhood"  # "kalshi_taker" | "kalshi_maker" | "robinhood" (Gold, k=5%) | "robinhood_standard" (k=10%)


@dataclass
class EarlySeasonConfig:
    """How style stats (four factors, misc, height -> archetypes, clutch, unit score)
    are filled before a team has a meaningful current-season sample.
    Efficiency/tempo always come from KenPom's CURRENT ratings (KenPom's preseason
    numbers are already roster-based: transfers, recruits, coaching) - never overridden.

    style_fill:
      "off"       - current-season stats only (missing -> adjustment skipped)
      "prior"     - missing values filled with last season's raw value (v2.0 behaviour)
      "regressed" - DEFAULT. prior_value shrunk toward the national mean:
                       regressed = mean + keep * (prior - mean)
                       keep = prior_keep_max * continuity   (KenPom Continuity, 0-1)
                            = prior_keep_default            when continuity unknown
                    then blended with the current-season value by games played:
                       w = min(1, games / full_weight_games)
                       value = w * current + (1 - w) * regressed
    Archetype tags need current-season evidence: with fewer than tag_evidence_games
    games a team's tags are shown as "(tentative)" and their matchup points are
    multiplied by tentative_tag_mult."""
    style_fill: str = "regressed"
    prior_keep_max: float = 0.5
    prior_keep_default: float = 0.25
    full_weight_games: int = 10
    tag_evidence_games: int = 5
    tentative_tag_mult: float = 0.5


# ── feature list + weights (custom models) ────────────────────────────────────
# Every score component is a named feature with a weight, so a custom model is
# just a different list (JSON: {"model_version": "...", "features": [...]}).
#   built-in kinds (weight = multiplier on that component; 0 or enabled=false turns it off):
#     efficiency  - calibrated AdjOE x AdjDE x tempo base score (weight scales the margin part)
#     hca         - per-team KenPom home-court advantage
#     quality_gap - KenPom percentile gap bump (also needs quality_gap.enabled)
#     matchup     - archetype counter/exploit cards (also needs matchup.apply_to_score)
#     unit        - height / experience / bench unit score
#     clutch      - late-game execution composite (also needs clutch.enabled)
#   generic kinds (any column of the canonical team table, e.g. "TO", "OR", "FTR", "AvgHgt"):
#     stat_diff     - weight x (z(team1.stat) - z(team2.stat)) points to team1's margin
#                     (target "total": weight x (z1 + z2) points to the total)
#     stat_matchup  - weight x [(z(t1.off) + z(t2.def)) - (z(t2.off) + z(t1.def))] points
#                     to team1's margin; "def" is the opponent-allowed column (e.g. off "OR", def "Opp_OR")
BUILTIN_FEATURES = ("efficiency", "hca", "quality_gap", "matchup", "unit", "clutch")


def default_features() -> list[dict]:
    return [{"name": k, "kind": k, "weight": 1.0, "enabled": True} for k in BUILTIN_FEATURES]


def feature_weights(cfg) -> dict:
    """{builtin name: weight} (missing/disabled -> 0) plus a list of custom features."""
    feats = getattr(cfg, "features", None) or default_features()
    w = {k: 0.0 for k in BUILTIN_FEATURES}
    custom = []
    for f in feats:
        if not f.get("enabled", True):
            continue
        kind = f.get("kind", f.get("name"))
        if kind in w:
            w[kind] += float(f.get("weight", 1.0))
        elif kind in ("stat_diff", "stat_matchup"):
            custom.append(f)
        else:
            raise ValueError(f"unknown feature kind {kind!r} in model config")
    return {"builtin": w, "custom": custom}


@dataclass
class ModelConfig:
    model_version: str = "czarp-v2"
    features: list = field(default_factory=default_features)
    use_multiplicative_efficiency: bool = True
    # Fitted: actual margin = 1.137 x efficiency margin (+ HCA). KenPom ratings are
    # deliberately conservative; the multiplicative formula still under-spreads.
    efficiency_margin_scale: float = 1.137
    # Fitted total calibration: total = a + b * model total (pre-calibration)
    total_intercept: float = 27.04
    total_slope: float = 0.822
    default_hca: float = 3.5         # fallback when hca.php is unavailable (else the table mean is used)
    hca_scale: float = 1.109         # fitted multiplier on KenPom per-team HCA (avg 2.84 -> ~3.15)
    hca_split: float = 0.5           # home +hca*split, away -hca*split (full swing = hca)
    quality_gap: QualityGapConfig = field(default_factory=QualityGapConfig)
    unit: UnitConfig = field(default_factory=UnitConfig)
    clutch: ClutchConfig = field(default_factory=ClutchConfig)
    matchup: MatchupConfig = field(default_factory=MatchupConfig)
    early: EarlySeasonConfig = field(default_factory=EarlySeasonConfig)
    betting: BettingConfig = field(default_factory=BettingConfig)

    def to_dict(self) -> dict:
        return asdict(self)

    def weights_dict(self) -> dict:
        """Numeric coefficients only (everything except the feature list) - for model_configs.weights."""
        d = asdict(self)
        d.pop("features", None)
        d.pop("model_version", None)
        return d


def _merge(obj, overrides: dict):
    for f in fields(obj):
        if f.name not in overrides:
            continue
        cur = getattr(obj, f.name)
        val = overrides[f.name]
        if is_dataclass(cur) and isinstance(val, dict):
            _merge(cur, val)
        elif isinstance(cur, dict) and isinstance(val, dict):
            cur.update(val)
        elif isinstance(cur, tuple) and isinstance(val, list):
            setattr(obj, f.name, tuple(val))
        else:
            setattr(obj, f.name, val)
    return obj


def load_config(path: str | None = None, overrides: dict | None = None) -> ModelConfig:
    cfg = ModelConfig()
    path = path or os.getenv("CZARP_CONFIG") or "czarp_config.json"
    if path and os.path.exists(path):
        with open(path) as fh:
            _merge(cfg, json.load(fh))
    if overrides:
        _merge(cfg, overrides)
    return cfg


DEFAULT_CONFIG = ModelConfig()
