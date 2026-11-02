import pytest

from czarp import kalshi as kx
from czarp.betting import evaluate_kalshi_game, evaluate_vegas_game, best_bets, evaluate_game
from czarp.config import BettingConfig

RAW = [
    {"ticker": "KXNCAAMBGAME-26NOV05TOWSUTAH-UTAH", "event_ticker": "KXNCAAMBGAME-26NOV05TOWSUTAH",
     "title": "Utah wins", "yes_sub_title": "Utah", "yes_ask_dollars": "0.7000", "no_ask_dollars": "0.3200",
     "rules_primary": "If Utah wins the Towson vs Utah men's college basketball game originally scheduled for Nov 5, 2026"},
    {"ticker": "KXNCAAMBGAME-26NOV05TOWSUTAH-TOWS", "event_ticker": "KXNCAAMBGAME-26NOV05TOWSUTAH",
     "title": "Towson wins", "yes_sub_title": "Towson", "yes_ask_dollars": "0.3200", "no_ask_dollars": "0.7000"},
    {"ticker": "KXNCAAMBSPREAD-26NOV05TOWSUTAH-UTAH5", "event_ticker": "KXNCAAMBSPREAD-26NOV05TOWSUTAH",
     "title": "Utah wins by over 4.5 points", "floor_strike": 4.5, "yes_ask_dollars": "0.5000", "no_ask_dollars": "0.5200"},
    {"ticker": "KXNCAAMBTOTAL-26NOV05TOWSUTAH-140", "event_ticker": "KXNCAAMBTOTAL-26NOV05TOWSUTAH",
     "title": "Full Game: Over 139.5 points scored", "floor_strike": 139.5, "yes_ask_dollars": "0.5000", "no_ask_dollars": "0.5200"},
]


def test_parse_and_find():
    board = kx.parse_markets(RAW)
    assert list(board) == ["26NOV05TOWSUTAH"]
    g = board["26NOV05TOWSUTAH"]
    assert g["teams"] == ["Towson", "Utah"]
    assert g["moneyline"]["Utah"]["yes_ask"] == 0.70
    assert g["spread"][0]["team"] == "Utah" and g["spread"][0]["strike"] == 4.5
    assert g["total"][0]["strike"] == 139.5
    assert kx.find_game(board, "Utah", "Towson") is g
    assert kx.find_game(board, "Utah", "Duke") is None


def test_evaluate_kalshi_game():
    board = kx.parse_markets(RAW)
    res = {"team1": "Utah", "team2": "Towson", "spread_raw": 10.0, "total_raw": 150.0}
    bets = evaluate_kalshi_game(res, board["26NOV05TOWSUTAH"], BettingConfig(), fee_profile="kalshi_taker")
    utah_yes = next(b for b in bets if b["side"] == "Utah YES")
    assert utah_yes["implied_prob"] == pytest.approx(0.70 + 0.07 * 0.7 * 0.3, abs=1e-4)
    sp = next(b for b in bets if b["market"] == "spread" and b["side"].endswith("YES"))
    assert sp["model_prob"] > 0.6   # 10 pt projection vs 4.5 strike
    top = best_bets(bets)
    assert len({b["market"] for b in top}) == len(top)
    assert evaluate_game(res, "kalshi", kalshi_board=board)  # robinhood/kalshi toggle path


def test_evaluate_vegas_game():
    res = {"team1": "A", "team2": "B", "spread_raw": 6.0, "total_raw": 140.0,
           "vegas_spread": -3.5, "t1_spread_price": -110, "t2_spread_price": -110,
           "vegas_t1_ml": -170, "vegas_t2_ml": 145, "vegas_total": 145.5,
           "over_price": -110, "under_price": -110, "source_book": "DraftKings"}
    bets = evaluate_vegas_game(res, BettingConfig())
    a_spread = next(b for b in bets if b["market"] == "spread" and b["side"] == "A")
    b_spread = next(b for b in bets if b["market"] == "spread" and b["side"] == "B")
    assert a_spread["model_prob"] + b_spread["model_prob"] == pytest.approx(1.0)
    assert a_spread["fair_prob"] == pytest.approx(0.5)
    assert a_spread["edge"] > 0 > b_spread["edge"]
    under = next(b for b in bets if b["side"] == "Under")
    assert under["edge"] > 0
