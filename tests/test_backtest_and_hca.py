import pandas as pd
import pytest

import backtest
from czarp.config import ModelConfig
from hca_fetcher import parse_hca_html


def test_grade_and_summary():
    df = pd.DataFrame([
        # model likes home by 8, open home -3.5, closes -5.5: home covers (won by 10), CLV +2
        {"event_id": 1, "date": "2026-01-15", "home": "H", "away": "A", "home_score": 80, "away_score": 70,
         "model_margin": 8.0, "model_total": 150.0, "open_home_spread": -3.5, "close_home_spread": -5.5,
         "open_total": 145.5, "close_total": 147.5, "close_home_ml": -200, "close_away_ml": 170},
        {"event_id": 2, "date": "2026-01-15", "home": "H2", "away": "A2", "home_score": 60, "away_score": 70,
         "model_margin": -2.0, "model_total": 130.0, "open_home_spread": -1.5, "close_home_spread": -1.0,
         "open_total": 135.5, "close_total": 134.5, "close_home_ml": -120, "close_away_ml": 100},
    ])
    gr = backtest.grade(df, ModelConfig())
    s = gr[(gr.market == "spread") & (gr.when == "open")].set_index("event_id")
    assert s.loc[1, "side"] == "home" and s.loc[1, "result"] == 1.0 and s.loc[1, "clv_pts"] == pytest.approx(2.0)
    assert s.loc[2, "side"] == "away" and s.loc[2, "result"] == 1.0
    t = gr[(gr.market == "total") & (gr.when == "open")].set_index("event_id")
    assert t.loc[1, "side"] == "over" and t.loc[1, "clv_pts"] == pytest.approx(2.0)
    rep = backtest.summarize(gr)
    assert "SPREAD vs open" in rep and "Brier" in rep and "CLV" in rep


def test_name_matcher():
    m = backtest.NameMatcher(["Washington St.", "Connecticut", "Queens"])
    assert m("Washington State") == "Washington St."
    assert m("UConn") == "Connecticut"
    assert m("Queens University") == "Queens"
    assert m("Nowhere") is None and "Nowhere" in m.misses


def test_parse_hca_html():
    html = """<table><thead><tr><th>Team</th><th>Conf</th><th>HCA</th><th>Rk</th><th>PF</th><th>Rk</th>
    </tr></thead><tbody><tr><td>Utah St. 1</td><td>MWC</td><td>4.6</td><td>1</td><td>0.5</td><td>3</td></tr>
    <tr><td>Duke</td><td>ACC</td><td>3.1</td><td>90</td><td>0.2</td><td>8</td></tr></tbody></table>"""
    df = parse_hca_html(html)
    assert list(df["TeamName"]) == ["Utah St.", "Duke"]
    assert list(df["HCA"]) == [4.6, 3.1]
