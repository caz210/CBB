import pytest

import model
from czarp.config import ModelConfig
from tests.conftest import make_league


def cfg_plain():
    c = ModelConfig()
    c.quality_gap.enabled = False
    c.matchup.enabled = False
    c.clutch.enabled = False
    c.unit.scale = 0.0
    c.efficiency_margin_scale, c.total_intercept, c.total_slope, c.hca_scale = 1.0, 0.0, 1.0, 1.0
    return c


def test_fitted_calibration():
    lg = make_league()
    raw = model.project_game("Team03", "Team04", None, lg, config=cfg_plain())
    c = cfg_plain(); c.efficiency_margin_scale, c.total_intercept, c.total_slope = 1.137, 27.04, 0.822
    r = model.project_game("Team03", "Team04", None, lg, config=c)
    assert r["spread_raw"] == pytest.approx(1.137 * raw["spread_raw"])
    assert r["total_raw"] == pytest.approx(27.04 + 0.822 * raw["total_raw"])
    c.hca_scale = 1.109
    h = model.project_game("Team00", "Team10", True, lg, config=c)["spread_raw"]
    n = model.project_game("Team00", "Team10", None, lg, config=c)["spread_raw"]
    assert h - n == pytest.approx(5.0 * 1.109)


def test_matchup_points_off_by_default_but_tags_shown():
    lg = make_league(n=100)
    c = ModelConfig()
    assert c.matchup.apply_to_score is False
    for t in ("Team01", "Team02", "Team03", "Team04", "Team05"):
        r = model.project_game(t, "Team50", None, lg, config=c)
        assert r["adjustments"]["matchup_t1"] == 0.0 and r["adjustments"]["matchup_t2"] == 0.0


def test_multiplicative_efficiency():
    lg = make_league()
    c = cfg_plain()
    r = model.project_game("Team03", "Team04", None, lg, config=c)
    rt = lg["ratings"].set_index("TeamName")
    avg_eff, avg_t = rt["AdjOE"].mean(), rt["AdjTempo"].mean()
    pace = rt.loc["Team03", "AdjTempo"] * rt.loc["Team04", "AdjTempo"] / avg_t
    s1 = rt.loc["Team03", "AdjOE"] * rt.loc["Team04", "AdjDE"] / avg_eff / 100 * pace
    s2 = rt.loc["Team04", "AdjOE"] * rt.loc["Team03", "AdjDE"] / avg_eff / 100 * pace
    assert r["team1_score_raw"] == pytest.approx(s1)
    assert r["team2_score_raw"] == pytest.approx(s2)
    assert r["spread_raw"] == pytest.approx(s1 - s2)
    # display rounding only
    assert r["team1_score"] * 2 == int(r["team1_score"] * 2)
    assert r["spread"] == model.mround(s1 - s2)


def test_additive_toggle_matches_v1_formula():
    lg = make_league()
    c = cfg_plain(); c.use_multiplicative_efficiency = False
    r = model.project_game("Team03", "Team04", None, lg, config=c)
    rt = lg["ratings"].set_index("TeamName")
    assert r["team1_ppp"] == pytest.approx((rt.loc["Team03", "AdjOE"] + rt.loc["Team04", "AdjDE"]) / 200, abs=1e-4)


def test_neutral_symmetry_and_full_model():
    lg = make_league()
    c = ModelConfig()
    a = model.project_game("Team05", "Team09", None, lg, config=c)
    b = model.project_game("Team09", "Team05", None, lg, config=c)
    assert a["spread_raw"] == pytest.approx(-b["spread_raw"])
    assert a["total_raw"] == pytest.approx(b["total_raw"])
    assert a["team1_win_prob"] == pytest.approx(b["team2_win_prob"])


def test_per_team_hca_and_fallback():
    lg = make_league()
    c = cfg_plain()
    neutral = model.project_game("Team00", "Team10", None, lg, config=c)["spread_raw"]
    home = model.project_game("Team00", "Team10", True, lg, config=c)
    assert home["spread_raw"] - neutral == pytest.approx(5.0)       # Team00 HCA = 5.0
    assert home["adjustments"]["hca_source"] == "kenpom_hca"
    away = model.project_game("Team00", "Team10", False, lg, config=c)
    assert neutral - away["spread_raw"] == pytest.approx(c.default_hca)  # Team10 not in table
    assert away["adjustments"]["hca_source"] == "kenpom_avg"     # table mean (5+2)/2
    no_table = dict(lg); no_table["hca"] = None
    model._CTX_CACHE.clear()
    assert model.project_game("Team00", "Team10", True, no_table, config=c)["adjustments"]["hca_source"] == "default"


def test_clutch_only_in_close_games():
    lg = make_league()
    c = cfg_plain(); c.clutch.enabled = True
    c.clutch.spread_threshold = 0.0   # never close enough
    r = model.project_game("Team01", "Team02", None, lg, config=c)
    assert r["adjustments"]["clutch"] == 0.0
    c.clutch.spread_threshold = 100.0  # always
    r2 = model.project_game("Team01", "Team02", None, lg, config=c)
    assert r2["adjustments"]["clutch"] != 0.0
    assert abs(r2["adjustments"]["clutch"]) <= c.clutch.max_adj
    assert r2["total_raw"] == pytest.approx(r["total_raw"])  # clutch moves margin, not total


def test_unit_weights_and_multipliers():
    lg = make_league()
    c = cfg_plain(); c.unit.scale = 0.25
    base = model.project_game("Team01", "Team02", None, lg, config=c)["adjustments"]["unit"]
    c.unit.neutral_mult = 2.0
    doubled = model.project_game("Team01", "Team02", None, lg, config=c)["adjustments"]["unit"]
    assert doubled == pytest.approx(2 * base)
    c.unit.neutral_mult = 1.0; c.unit.late_season_mult = 3.0
    early = model.project_game("Team01", "Team02", True, lg, config=c, game_date="2026-12-01")["adjustments"]["unit"]
    late = model.project_game("Team01", "Team02", True, lg, config=c, game_date="2027-04-06")["adjustments"]["unit"]
    assert abs(late) > abs(early)


def test_legacy_keys_present():
    lg = make_league()
    r = model.project_game("Team01", "Team02", True, lg)
    for k in ["team1", "team2", "spread", "total", "team1_score", "team1_ppp", "team1_unit_score",
              "team1_clutch", "location", "team1_tags", "team2_tags", "matchup_notes"]:
        assert k in r
    for k in ["kenpom_rank_t1", "t1_to", "t1_reb", "t1_ft", "t1_poss", "t1_dto_pct", "t1_dor_pct",
              "t1_dft_rate", "u1_adj", "h1_adj", "t1_hgt", "t1_exp", "t1_bench"]:
        assert k in r["debug"]


def test_early_season_missing_stats_does_not_crash():
    lg = make_league()
    for col in ["eFG_Pct", "TO_Pct", "OR_Pct", "FT_Rate", "DTO_Pct", "DFT_Rate"]:
        lg["four_factors"][col] = 0.0
    for col in ["AvgHgt", "Exp", "Bench"]:
        lg["height"][col] = 0.0
    r = model.project_game("Team01", "Team02", True, lg)
    assert r["adjustments"]["unit"] == 0.0
    assert r["team1_unit_score"] is None
    assert r["debug"]["t1_to_pct"] is None
    assert r["spread_raw"] == r["spread_raw"]  # not NaN
