import math

import pytest

from czarp.config import BettingConfig
from czarp import probability as P


def test_win_and_cover_prob():
    assert P.win_prob(0, 11) == pytest.approx(0.5)
    assert P.win_prob(11, 11) == pytest.approx(0.8413, abs=1e-3)
    # model says team wins by 5; line -3.5 -> P(margin > 3.5)
    assert P.cover_prob(5, -3.5, 11) == pytest.approx(1 - P.norm_cdf((3.5 - 5) / 11))
    # both sides of a half-point line sum to 1
    assert P.cover_prob(2, -3.5) + P.cover_prob(-2, 3.5) == pytest.approx(1.0)


def test_odds_and_devig():
    assert P.american_to_prob(-110) == pytest.approx(0.5238, abs=1e-4)
    assert P.american_to_prob(150) == pytest.approx(0.4)
    a, b = P.devig_two_way(-110, -110)
    assert a == pytest.approx(0.5) and b == pytest.approx(0.5)
    a, b = P.devig_two_way(-200, 170)
    assert a + b == pytest.approx(1.0) and a > b


def test_fees():
    assert P.kalshi_fee_per_contract(0.5) == pytest.approx(0.0175)
    assert P.kalshi_fee_per_contract(0.5, maker=True) == pytest.approx(0.004375)
    # Robinhood: 10% * .25 = .025 capped at .01, plus $0.01 exchange fee
    assert P.robinhood_fee_per_contract(0.5) == pytest.approx(0.02)
    assert P.robinhood_fee_per_contract(0.05, gold=True) == pytest.approx(0.05 * 0.05 * 0.95 + 0.01)
    assert P.contract_breakeven(0.40, "kalshi_taker") == pytest.approx(0.40 + 0.07 * 0.4 * 0.6)


def test_kelly_tiers_and_cap():
    cfg = BettingConfig()
    ev = P.evaluate(0.61, cfg, american=-110)
    assert ev.edge == pytest.approx(0.61 - 0.5238, abs=1e-3)
    assert ev.tier == "STRONG"
    assert P.evaluate(0.60, cfg, american=-110).tier == "VALUE"   # 7.6pp
    full = P.kelly_fraction(0.61, P.american_to_decimal(-110))
    assert ev.stake_pct == pytest.approx(min(full * 0.25, 0.025))
    assert ev.stake_pct <= 0.025
    small = P.evaluate(0.555, cfg, american=-110)
    assert small.tier == "LEAN"
    none = P.evaluate(0.53, cfg, american=-110)
    assert none.tier is None and none.stake_pct == 0
    k = P.evaluate(0.55, cfg, contract_price=0.45, fee_profile="kalshi_taker")
    assert k.implied_prob == pytest.approx(0.45 + 0.07 * 0.45 * 0.55)


def test_brier():
    assert P.brier([1, 0], [1, 0]) == 0
    assert P.brier([0.5, 0.5], [1, 0]) == 0.25
