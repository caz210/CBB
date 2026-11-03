import pandas as pd
import daily_run as dr


def test_kelly_and_min_stake():
    assert dr.kelly_stake(0.545, -110, 1000) > 5
    assert dr.kelly_stake(0.535, -115, 1000) < dr.MIN_STAKE      # no edge at -115
    assert dr.kelly_stake(0.9, -110, 1000) == 25.0                # 2.5% cap


def test_grade_spread_and_total(monkeypatch):
    sb = [{"event_id": "1", "state": "post", "completed": True, "home_score": 80.0, "away_score": 70.0}]
    monkeypatch.setattr(dr, "espn_scoreboard", lambda d: sb)
    monkeypatch.setattr(dr, "espn_dk_lines", lambda e: {"close_home_spread": -8.0, "close_total": 148.0,
                                                         "close_over_price": -110, "close_away_spread_price": -110})
    base = dict(game_date="2026-01-01", event_id="1", tip_ct="2026-01-01 18:00", away="A", home="H",
                price=-110, stake=10.0, status="open")
    led = pd.DataFrame([
        {**base, "pick_id": "1-spread", "market": "spread", "side": "A", "line": 6.5},   # away +6.5, lost by 10
        {**base, "pick_id": "1-total", "market": "total", "side": "Over", "line": 145.5},  # 150 > 145.5
    ], columns=dr.LEDGER_COLS)
    dr.grade(led, pd.Timestamp("2026-01-02", tz=dr.CENTRAL).to_pydatetime())
    s, t = led.iloc[0], led.iloc[1]
    assert s.result == "loss" and s.pnl == -10 and s.clv_pts == -1.5      # took +6.5, closed +8
    assert t.result == "win" and abs(t.pnl - 9.09) < 0.01 and t.clv_pts == 2.5
    assert dr.bankroll(led) == 1000 - 10 + 9.09


def test_feature_config_parsing_and_custom_features():
    import pytest
    from czarp.config import ModelConfig, feature_weights
    from model import custom_feature_adjustment
    fw = feature_weights(ModelConfig())
    assert all(v == 1.0 for v in fw["builtin"].values()) and fw["custom"] == []
    cfg = ModelConfig(features=[{"name": "hca", "kind": "hca", "weight": 0.5},
                                {"name": "x", "kind": "stat_diff", "stat": "S", "weight": 2.0},
                                {"name": "off", "kind": "unit", "enabled": False}])
    fw = feature_weights(cfg)
    assert fw["builtin"]["hca"] == 0.5 and fw["builtin"]["efficiency"] == 0.0 and fw["builtin"]["unit"] == 0.0
    tt = pd.DataFrame({"S": [1.0, 2.0, 3.0]}, index=["A", "B", "C"])
    dm, dt = custom_feature_adjustment("C", "A", tt, fw["custom"])
    assert dm == pytest.approx(4.0) and dt == 0.0          # z(C)-z(A) = 2 sd, x weight 2
    with pytest.raises(ValueError):
        feature_weights(ModelConfig(features=[{"name": "bad", "kind": "nope"}]))


def test_archetypes_strip_tentative():
    a = dr.archetypes({"team1_tags": ["Arc Wall (tentative)", "Track Meet"], "team2_tags": ["Lock Down"],
                       "tags_tentative": True, "matchup_notes": []})
    assert a["away"] == ["Arc Wall", "Track Meet"] and a["home"] == ["Lock Down"]
    assert a["away_tentative"] and not a["home_tentative"] and a["tentative"]


def _game(**kw):
    from datetime import datetime, timezone
    g = dict(event_id="9", state="pre", tip_utc=datetime(2030, 1, 1, tzinfo=timezone.utc), tip_ct="2029-12-31 18:00",
             game_date="2029-12-31", kp_home="H", kp_away="A", neutral=False,
             open_home_spread=-4.0, current_home_spread=-4.5, open_total=140.0, current_total=140.0,
             v2_home_margin=11.0, v1_total=141.0)
    g.update(kw)
    return g


def _now():
    return pd.Timestamp("2029-12-31 08:00", tz=dr.CENTRAL).to_pydatetime()


def test_candidates_reasons_and_line_move_flag(monkeypatch):
    monkeypatch.setattr(dr, "LINE_MOVE_EXCLUDE", False)
    c = {x["market"]: x for x in dr.candidates([_game()], _now())}
    s = c["spread"]                                     # model H by 11 vs H -4.5: gap 6.5 / 140 = 4.6%
    assert s["side"] == "H" and s["line"] == -4.5 and s["open_line"] == -4.0 and s["rule_ok"]
    assert abs(s["edge_pct"] - 6.5 / 140) < 1e-9 and abs(s["edge_pct_vs_open"] - 7 / 140) < 1e-9
    assert not c["total"]["rule_ok"] and "below 3% threshold" in c["total"]["reasons"][0]
    # edge vs open but not vs current -> "edge gone vs current"
    g = _game(current_home_spread=-8.0)
    s = {x["market"]: x for x in dr.candidates([g], _now())}["spread"]
    assert not s["rule_ok"] and s["reasons"][0].startswith("edge gone vs current")
    assert s["flagged"] and s["line_move"] == 4.0
    # 2+ pt move with edge intact: flag only by default, excluded when turned on
    g = _game(open_home_spread=-2.0, current_home_spread=-4.0)
    assert {x["market"]: x for x in dr.candidates([g], _now())}["spread"]["rule_ok"]
    monkeypatch.setattr(dr, "LINE_MOVE_EXCLUDE", True)
    s = {x["market"]: x for x in dr.candidates([g], _now())}["spread"]
    assert not s["rule_ok"] and "likely news/injury" in s["reasons"][0]


def test_flag_plays_matches_candidates_and_top_edges(monkeypatch):
    monkeypatch.setattr(dr, "LINE_MOVE_EXCLUDE", False)
    games = [_game(), _game(event_id="8", v2_home_margin=5.0, v1_total=150.0, kp_home="H2", kp_away="A2"),
             _game(event_id="7", skip="non-D1 / unmatched team")]
    plays = dr.flag_plays(games, 1000.0, {}, _now())
    ids = {p["pick_id"] for p in plays}
    assert ids == {"9-spread", "8-total"}               # 8-total: 150 vs 140 = 7.1%
    top = dr.top_edges(dr.candidates(games, _now()), ids)
    assert len(top) == 4 and top[0]["pick_id"] == "8-total" and top[0]["qualifies"]
    assert [r["rank_edge"] for r in top] == sorted([r["rank_edge"] for r in top], reverse=True)
    md = "\n".join(dr.top_edges_md(top))
    assert "| 1 |" in md and "| 4 |" in md and "below 4% threshold" in md
