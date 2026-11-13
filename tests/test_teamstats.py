import numpy as np
import pandas as pd

from czarp import teamstats as T
from czarp.config import MatchupConfig
from tests.conftest import make_league


def test_aliases_and_dr_detection():
    lg = make_league()
    tt = T.build_team_table(lg)
    assert {"eFG", "TO", "D_TO", "Opp_OR", "FG3A_Rate", "Opp_FG3A_Rate", "FTPct"} <= set(tt.columns)
    assert np.allclose(tt["Opp_OR"], lg["four_factors"].set_index("TeamName")["DOR_Pct"])
    lg2 = make_league()
    lg2["four_factors"]["DOR_Pct"] = 100 - lg2["four_factors"]["DOR_Pct"]
    tt2 = T.build_team_table(lg2)
    assert np.allclose(tt2["Opp_OR"], tt["Opp_OR"])


def test_fg3_pct_mapped():
    lg = make_league()
    lg["misc"]["FG3Pct"] = 34.0
    lg["misc"]["OppFG3Pct"] = 32.0
    tt = T.build_team_table(lg)
    assert "FG3Pct" in tt.columns and tt["FG3Pct"].notna().all()
    assert "Opp_FG3Pct" in tt.columns and tt["Opp_FG3Pct"].notna().all()


def test_early_season_placeholders_and_prior_fill():
    lg = make_league()
    early = make_league(seed=9)
    early["four_factors"].loc[:, "eFG_Pct"] = 0.0
    early["four_factors"].loc[:, "DOR_Pct"] = 100.0
    early["four_factors"].loc[0, "eFG_Pct"] = 55.0
    tt = T.build_team_table(early)
    assert tt["eFG"].isna().all() and tt["Opp_OR"].isna().all()
    tt2 = T.build_team_table(early, prior=lg)
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
    cur0 = _early_league(0)
    cur0["four_factors"]["eFG_Pct"] = 0.0
    tt = T.build_team_table(cur0, prior=prior, early=EarlySeasonConfig())
    assert np.allclose(tt["eFG"], mu + 0.25 * (pe - mu))
    assert (tt["CurWeight"] == 0).all() and "eFG" in tt.attrs["prior_filled"]
    cur0["height"]["Continuity"] = 0.8
    tt = T.build_team_table(cur0, prior=prior, early=EarlySeasonConfig())
    assert np.allclose(tt["eFG"], mu + 0.5 * 0.8 * (pe - mu))
    cur5 = _early_league(5)
    ce = cur5["four_factors"].set_index("TeamName")["eFG_Pct"]
    tt = T.build_team_table(cur5, prior=prior, early=EarlySeasonConfig())
    assert np.allclose(tt["eFG"], 0.5 * ce + 0.5 * (mu + 0.25 * (pe - mu)))
    cur10 = _early_league(12)
    tt = T.build_team_table(cur10, prior=prior, early=EarlySeasonConfig())
    assert np.allclose(tt["eFG"], cur10["four_factors"].set_index("TeamName")["eFG_Pct"])
    assert np.allclose(tt["AdjOE"], cur10["ratings"].set_index("TeamName")["AdjOE"])
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


def test_tags_top_15pct_and_cap():
    lg = make_league(n=100)
    tt = T.build_team_table(lg)
    tags = T.tag_teams(tt, MatchupConfig())
    # Each named strength/weakness/style fires on ~15% of teams
    for col in ["Volume 3s", "Crash", "Wall", "Soft D", "Careless", "Fast"]:
        assert 10 <= tags[col].sum() <= 20, col
    # Cap at 2 chips
    assert tags["tags"].map(len).max() <= 2
    assert all(len(o) <= 2 for o in tags["tag_objs"])
    # kinds present
    kinds = {o["kind"] for objs in tags["tag_objs"] for o in objs}
    assert kinds <= {"strength", "weakness", "style"}


def test_matchup_counter_and_exploit():
    cfg = MatchupConfig()
    off = pd.Series({"Volume 3s": True, "Careless": True})
    wall = pd.Series({"Crowd 3s": True, "Disrupt": True})
    pts, notes = T.matchup_adjustment(off, wall, cfg)
    assert pts == -(0.75 + 0.5)
    assert len(notes) == 2
    leaky = pd.Series({T.OPEN_ARC: True})
    pts, _ = T.matchup_adjustment(off, leaky, cfg)
    assert pts == 0.5
    cfg.enabled = False
    assert T.matchup_adjustment(off, wall, cfg) == (0.0, [])


def test_upset_alert():
    lg = make_league(n=40)
    tt = T.build_team_table(lg)
    tags = T.tag_teams(tt, MatchupConfig())
    # Force an exploit: away Crash, home Soft Glass; home favored by Vegas; model likes away
    home, away = "Team00", "Team01"
    tags.loc[away, "Crash"] = True
    tags.loc[home, T.SOFT_GLASS] = True
    ok, reason = T.upset_alert(home, away, tags, model_home_margin=2.0, vegas_home_spread=-10.0)
    assert ok and "Crash" in reason and "soft defensive glass" in reason
    # Model does not like dog -> no alert
    ok2, _ = T.upset_alert(home, away, tags, model_home_margin=12.0, vegas_home_spread=-10.0)
    assert not ok2


def test_build_team_profiles():
    from czarp.config import EarlySeasonConfig
    lg = make_league(n=20)
    lg["misc"]["FG3Pct"] = 33.0
    lg["misc"]["OppFG3Pct"] = 31.0
    tt = T.build_team_table(lg)
    tags = T.mark_tentative(T.tag_teams(tt, MatchupConfig()), tt, EarlySeasonConfig())
    rows = T.build_team_profiles(tt, tags, as_of="2026-11-08", season=2027, hca_df=lg["hca"],
                                 new_coach={"Team00": {"games": 2}})
    assert len(rows) == 20
    r0 = next(r for r in rows if r["name"] == "Team00")
    assert "efg" in r0["factors"] and "efg_pctile" in r0["factors"]
    assert isinstance(r0["tags"], list)
    assert r0["new_coach"] is True and r0["new_coach_games"] == 2
    assert r0["lock_sides_games"] == 8 and r0["lock_totals_games"] == 4
    assert r0["lock_sides_remaining"] == 6 and r0["lock_totals_remaining"] == 2
    other = next(r for r in rows if r["name"] == "Team01")
    assert other["new_coach"] is False and other["lock_sides_remaining"] is None
    assert other["lock_sides_games"] == 8 and other["lock_totals_games"] == 4


def test_v1_totals_reason():
    """Short totals line in the same sentence style as v2's projection_reason."""
    s = T.v1_totals_reason("Purdue", "Ohio", 147.0, 68.1)
    assert s == "Ohio at Purdue. Projected total 147.0. Projected pace about 68 possessions."
    s2 = T.v1_totals_reason("Drake", "South Dakota", 155.0, 74.0)
    assert s2 == "South Dakota at Drake. Projected total 155.0. Projected pace about 74 possessions."
    assert T.v1_totals_reason("Purdue", "Ohio", 147.0, None) == "Ohio at Purdue. Projected total 147.0."
    assert T.v1_totals_reason("Purdue", "Ohio", None, 68.1) == "Ohio at Purdue. Projected pace about 68 possessions."
