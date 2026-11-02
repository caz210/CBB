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
    tt2 = T.build_team_table(early, prior=lg)
    assert tt2["eFG"].notna().all()
    assert "eFG" in tt2.attrs["prior_filled"]


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
