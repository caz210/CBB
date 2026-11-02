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
