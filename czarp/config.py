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
    enabled: bool = True
    coef: float = 0.2


@dataclass
class UnitConfig:
    w_height: float = 40.0
    w_exp: float = 40.0
    w_bench: float = 20.0
    scale: float = 0.25        # points per unit-score point of difference (original 0.25)
    # Optional extra weight (multipliers applied to `scale`). 1.0 = off.
    close_game_mult: float = 1.0     # applied when |base margin| <= close_game_threshold
    close_game_threshold: float = 6.0
    neutral_mult: float = 1.0        # neutral-site games (tournaments)
    late_season_mult: float = 1.0    # ramps in linearly with season progress after late_season_start
    late_season_start: float = 0.75  # fraction of season (0=Nov 1, 1=Apr 10); ~Feb 22


@dataclass
class ClutchConfig:
    enabled: bool = True
    spread_threshold: float = 5.0    # only applied when |projected margin| <= this
    # Composite = w_efg*z(eFG%) - w_to*z(TO%) + w_ft*z(FT%)
    w_efg: float = 1.0
    w_to: float = 1.0
    w_ft: float = 1.0
    points_per_z: float = 0.35       # FIT: margin points per 1.0 composite z-score difference
    max_adj: float = 1.5             # cap on total clutch swing (points)


@dataclass
class MatchupConfig:
    enabled: bool = True
    top_pct: float = 0.15            # archetype = top 15% nationally
    # Each counter pair: offense archetype vs defense archetype. When an offense
    # archetype meets its counter, its owner loses `counter` pts; when it meets an
    # opponent that is in the BOTTOM top_pct of the same defensive stat ("weakness"),
    # it gains `exploit` pts. FIT all of these.
    counter: dict = field(default_factory=lambda: {
        "Addicted to 3s|Arc Wall": 0.75,
        "Board Crashers|No Rebounds for You": 0.75,
        "Don't Blink|Lock Down": 0.5,
        "Free Throw Merchants|Lock Down": 0.25,
    })
    exploit: dict = field(default_factory=lambda: {
        "Addicted to 3s|Arc Wall": 0.5,
        "Board Crashers|No Rebounds for You": 0.5,
        "Don't Blink|Lock Down": 0.25,
        "Free Throw Merchants|Lock Down": 0.25,
    })
    # Turnover Machine defense vs a careless offense (bottom 15% TO%)
    turnover_machine_vs_careless: float = 0.5
    max_adj: float = 2.0             # cap on total matchup swing per team (points)


@dataclass
class BettingConfig:
    margin_sd: float = 11.0          # SD of actual margin around projection (CBB ~10-11.5)
    total_sd: float = 15.0           # FIT: SD of actual total around projected total
    tiers: tuple = (0.03, 0.05, 0.08)   # edge thresholds (probability points)
    tier_names: tuple = ("LEAN", "VALUE", "STRONG")
    kelly_fraction: float = 0.25
    max_stake_pct: float = 0.025     # hard cap: 2.5% of bankroll
    price_source: str = "vegas"      # "vegas" | "kalshi"
    kalshi_fee_profile: str = "robinhood"  # "kalshi_taker" | "kalshi_maker" | "robinhood" | "robinhood_gold"


@dataclass
class ModelConfig:
    use_multiplicative_efficiency: bool = True
    default_hca: float = 3.5         # fallback when a team is missing from hca.php
    hca_split: float = 0.5           # home +hca*split, away -hca*split (full swing = hca)
    quality_gap: QualityGapConfig = field(default_factory=QualityGapConfig)
    unit: UnitConfig = field(default_factory=UnitConfig)
    clutch: ClutchConfig = field(default_factory=ClutchConfig)
    matchup: MatchupConfig = field(default_factory=MatchupConfig)
    betting: BettingConfig = field(default_factory=BettingConfig)

    def to_dict(self) -> dict:
        return asdict(self)


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
