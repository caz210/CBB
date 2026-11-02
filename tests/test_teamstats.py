import numpy as np
import pandas as pd

from czarp import teamstats as T
from czarp.config import MatchupConfig
from tests.conftest import make_league


def test_aliases_and_dr_detection():
    lg = make_league()
    tt = T.build_team_table(lg)
    assert {"eFG", "TO", "D_TO", "Opp_OR", "FG3A_Rate", "Opp_FG3A_Rate", "FTPct"} <= set(tt.columns)
    # DOR_Pct ~29 => interpreted as opponent OR% allowed
    assert np.allclose(tt["Opp_OR"], lg["four_factors"].set_index("TeamName")["DOR_Pct"])
    # If a feed gives DR% (~71) instead, it is converted
    lg2 = make_league()
    lg2["four_factors"]["DOR_Pct"] = 100 - lg2["four_factors"]["DOR_Pct"]
    tt2 = T.build_team_table(lg2)
    assert np.allclose(tt2["Opp_OR"], tt["Opp_OR"])


def test_early_season_placeholders_and_prior_fill():
    lg = make_league()
    early = make_league(seed=9)
    early["four_factors"].loc[:, "eFG_Pct"] = 0.0
    early["four_factors"].loc[:, "DOR_Pct"] = 100.0
    early["four_factors"].loc[0, "eFG_Pct"] = 55.0  # one team has played
    tt = T.build_team_table(early)
    assert tt["eFG"].isna().all() and tt["Opp_OR"].isna().all()
    tt2 = T.build_team_table(early, prior=lg)          # no config -> legacy "prior" fill
    assert tt2["eFG"].notna().all()
    assert "eFG" in tt2.attrs["prior_filled"]


def _early_league(games):
    lg = make_league(seed=9)
    lg["ratings"]["Wins"] = games // 2
    lg["ratings"]["Losses"] = games - games // 2
    return lg


def test_early_season_modes():
    from czarp.config import EarlySeasonConfig
    prior = make_league()
    pe = prior["four_factors"].set_index("TeamName")["eFG_Pct"]
    mu = pe.mean()
    # 0 games: regressed = mean + 0.25*(prior-mean), current ignored
    cur0 = _early_league(0)
    cur0["four_factors"]["eFG_Pct"] = 0.0
    tt = T.build_team_table(cur0, prior=prior, early=EarlySeasonConfig())
    assert np.allclose(tt["eFG"], mu + 0.25 * (pe - mu))
    assert (tt["CurWeight"] == 0).all() and "eFG" in tt.attrs["prior_filled"]
    # continuity scales how much of last season is kept
    cur0["height"]["Continuity"] = 0.8
    tt = T.build_team_table(cur0, prior=prior, early=EarlySeasonConfig())
    assert np.allclose(tt["eFG"], mu + 0.5 * 0.8 * (pe - mu))
    # 5 games: half current, half regressed prior
    cur5 = _early_league(5)
    ce = cur5["four_factors"].set_index("TeamName")["eFG_Pct"]
    tt = T.build_team_table(cur5, prior=prior, early=EarlySeasonConfig())
    assert np.allclose(tt["eFG"], 0.5 * ce + 0.5 * (mu + 0.25 * (pe - mu)))
    # 10+ games: current only
    cur10 = _early_league(12)
    tt = T.build_team_table(cur10, prior=prior, early=EarlySeasonConfig())
    assert np.allclose(tt["eFG"], cur10["four_factors"].set_index("TeamName")["eFG_Pct"])
    # efficiency is never touched by the prior season
    assert np.allclose(tt["AdjOE"], cur10["ratings"].set_index("TeamName")["AdjOE"])
    # off: no prior at all
    tt = T.build_team_table(cur0, prior=None, early=EarlySeasonConfig(style_fill="off"))
    assert tt["eFG"].isna().all()


def test_tentative_tags():
    from czarp.config import EarlySeasonConfig
    lg = _early_league(3)
    tt = T.build_team_table(lg)
    tags = T.mark_tentative(T.tag_teams(tt, MatchupConfig()), tt, EarlySeasonConfig())
    assert tags["tentative"].all()
    tagged = tags[tags["tags"].str.len() > 0].iloc[0]
    assert all(t.endswith("(tentative)") for t in tagged["tags_display"])
    lg = _early_league(8)
    tt = T.build_team_table(lg)
    assert not T.mark_tentative(T.tag_teams(tt, MatchupConfig()), tt, EarlySeasonConfig())["tentative"].any()


def test_tags_top_15pct():
    lg = make_league(n=100)
    tt = T.build_team_table(lg)
    tags = T.tag_teams(tt, MatchupConfig())
    for col in ["Addicted to 3s", "Board Crashers", "Lock Down", "Arc Wall", "Turnover Machine"]:
        assert 13 <= tags[col].sum() <= 17, col
    best_d = tt["AdjDE"].idxmin()
    assert tags.loc[best_d, "Lock Down"]
    assert "Lock Down" in tags.loc[best_d, "tags"]


def test_matchup_counter_and_exploit():
    cfg = MatchupConfig()
    off = pd.Series({"Addicted to 3s": True, "Careless": True})
    wall = pd.Series({"Arc Wall": True, "Turnover Machine": True})
    pts, notes = T.matchup_adjustment(off, wall, cfg)
    assert pts == -(0.75 + 0.5)
    assert len(notes) == 2
    leaky = pd.Series({"Open Arc": True})
    pts, _ = T.matchup_adjustment(off, leaky, cfg)
    assert pts == 0.5
    cfg.enabled = False
    assert T.matchup_adjustment(off, wall, cfg) == (0.0, [])
