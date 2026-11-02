"""Margin -> probability, odds conversion, de-vig, fees, edge, tiers and Kelly sizing."""
from __future__ import annotations

import math
from dataclasses import dataclass

from .config import BettingConfig


# ── Normal distribution helpers ───────────────────────────────────────────────

def norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def win_prob(margin: float, sd: float = 11.0) -> float:
    """P(team wins) given projected margin (team - opp)."""
    return norm_cdf(margin / sd)


def cover_prob(margin: float, spread: float, sd: float = 11.0) -> float:
    """P(team covers) where `spread` is the team's line in betting notation
    (team -4.5 -> spread=-4.5). Covers when actual margin + spread > 0.
    Half-point lines have no push; for whole numbers this returns P(win ATS)
    ignoring the push sliver (continuity-corrected by 0.5)."""
    threshold = -spread  # need actual margin > threshold
    if float(threshold).is_integer():
        threshold += 0.5
    return 1.0 - norm_cdf((threshold - margin) / sd)


def over_prob(proj_total: float, line: float, sd: float = 15.0) -> float:
    threshold = line + 0.5 if float(line).is_integer() else line
    return 1.0 - norm_cdf((threshold - proj_total) / sd)


def margin_over_prob(margin: float, strike: float, sd: float = 11.0) -> float:
    """Kalshi spread contract: P(team wins by MORE than `strike`)."""
    return 1.0 - norm_cdf((strike - margin) / sd)


# ── Odds conversions ──────────────────────────────────────────────────────────

def american_to_decimal(odds: float) -> float:
    return 1 + (odds / 100.0 if odds > 0 else 100.0 / -odds)


def american_to_prob(odds: float) -> float:
    return 1.0 / american_to_decimal(odds)


def devig_two_way(odds_a: float, odds_b: float, method: str = "multiplicative") -> tuple[float, float]:
    """De-vig a two-way American-odds market. Returns fair (p_a, p_b)."""
    pa, pb = american_to_prob(odds_a), american_to_prob(odds_b)
    if method == "additive":
        over = (pa + pb - 1) / 2
        return pa - over, pb - over
    s = pa + pb
    return pa / s, pb / s


# ── Prediction-market fees (per $1 contract) ─────────────────────────────────

def kalshi_fee_per_contract(price: float, maker: bool = False, multiplier: float = 1.0) -> float:
    """Kalshi fee schedule (effective 2026-07-07):
       taker = 0.07 * P * (1-P), maker = 0.0175 * P * (1-P) (maker only on series
       flagged quadratic_with_maker_fees, which includes KXNCAAMBGAME/SPREAD/TOTAL).
       Per-contract, before order-level rounding (direct accounts round to $0.0001)."""
    rate = 0.0175 if maker else 0.07
    return multiplier * rate * price * (1 - price)


def robinhood_fee_per_contract(price: float, gold: bool = False) -> float:
    """Robinhood event-contract cost (RHD fee schedule, effective 2026-06-01):
       commission = k*P*(1-P), k=10% (5% w/ Gold), capped at $0.01/contract,
       plus KalshiEX exchange fee $0.01/contract/side. Rounding is up to the cent
       per order, so for large orders the per-contract cost approaches this value."""
    k = 0.05 if gold else 0.10
    return min(k * price * (1 - price), 0.01) + 0.01


def fee_per_contract(price: float, profile: str = "robinhood") -> float:
    if profile == "kalshi_taker":
        return kalshi_fee_per_contract(price)
    if profile == "kalshi_maker":
        return kalshi_fee_per_contract(price, maker=True)
    if profile == "robinhood":
        return robinhood_fee_per_contract(price)
    if profile == "robinhood_gold":
        return robinhood_fee_per_contract(price, gold=True)
    if profile in ("none", None):
        return 0.0
    raise ValueError(f"unknown fee profile {profile!r}")


def contract_breakeven(price: float, profile: str = "robinhood") -> float:
    """Effective implied probability of buying a YES contract at `price`:
    you pay price + fee for a $1 payout, so you need p > price + fee."""
    return price + fee_per_contract(price, profile)


# ── Edge, tiers and Kelly ─────────────────────────────────────────────────────

@dataclass
class BetEval:
    model_prob: float
    implied_prob: float      # breakeven probability including vig/fees
    edge: float              # model_prob - implied_prob
    decimal_odds: float      # net payout per $1 staked + 1, after fees
    tier: str | None
    kelly_full: float
    stake_pct: float         # recommended fraction of bankroll


def kelly_fraction(p: float, decimal_odds: float) -> float:
    """Full-Kelly fraction for win prob p at decimal odds. Never negative."""
    b = decimal_odds - 1.0
    if b <= 0:
        return 0.0
    return max(0.0, (b * p - (1 - p)) / b)


def edge_tier(edge: float, cfg: BettingConfig) -> str | None:
    tier = None
    for thr, name in zip(cfg.tiers, cfg.tier_names):
        if edge >= thr:
            tier = name
    return tier


def evaluate(model_prob: float, cfg: BettingConfig, *, american: float | None = None,
             fair_prob: float | None = None, contract_price: float | None = None,
             fee_profile: str | None = None) -> BetEval:
    """Evaluate one side of a bet.

    Vegas mode: pass `american` (the price you'd actually bet at). The breakeven is
      the vigged implied probability; `fair_prob` (de-vigged) is reported for CLV
      but the edge is measured against what you actually pay.
    Prediction-market mode: pass `contract_price` (ask, 0-1) and fee profile.
    """
    if contract_price is not None:
        profile = fee_profile or cfg.kalshi_fee_profile
        implied = contract_breakeven(contract_price, profile)
        dec = 1.0 / implied
    elif american is not None:
        dec = american_to_decimal(american)
        implied = 1.0 / dec
    else:
        raise ValueError("need american odds or contract_price")
    edge = model_prob - implied
    tier = edge_tier(edge, cfg)
    kf = kelly_fraction(model_prob, dec)
    stake = min(kf * cfg.kelly_fraction, cfg.max_stake_pct) if tier else 0.0
    return BetEval(model_prob, implied, edge, dec, tier, kf, stake)


def brier(probs, outcomes) -> float:
    probs = list(probs); outcomes = list(outcomes)
    if not probs:
        return float("nan")
    return sum((p - o) ** 2 for p, o in zip(probs, outcomes)) / len(probs)
