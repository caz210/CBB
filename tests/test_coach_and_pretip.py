import pandas as pd
import pytest

import daily_run as dr
import pretip_check as pc
from czarp.coach import coach_changes_between, detect_new_coaches, games_played, lock_status
from tests.test_daily_run import _game, _now


def test_coach_change_detection(tmp_path):
    cur = pd.DataFrame({"TeamName": ["A", "B", "C"], "Coach": ["Joe Crispin", "Same Guy", "Mike R. Jones Jr."],
                        "Wins": [1, 0, 3], "Losses": [0, 0, 2]})
    prev = pd.DataFrame({"TeamName": ["A", "B", "C"], "Coach": ["Joe Scott", "Same Guy", "Mike R Jones"]})
    assert coach_changes_between(cur, prev) == {"A"}           # punctuation / suffix noise ignored
    (tmp_path / "prior").mkdir()
    cur.to_csv(tmp_path / "ratings.csv", index=False)
    prev.to_csv(tmp_path / "prior" / "ratings.csv", index=False)
    pd.DataFrame({"season": [2027, 2027, 2026], "team": ["B", "A", "C"], "new_coach": [1, 0, 1]}).to_csv(
        tmp_path / "coach_changes.csv", index=False)
    nc = detect_new_coaches(2027, str(tmp_path))
    assert set(nc) == {"B"}                                     # CSV adds B, clears A, ignores other seasons
    played = games_played(cur)
    assert played == {"A": 1, "B": 0, "C": 5}
    st = lock_status("B", nc, played, 8)
    assert st["remaining"] == 8 and st["locked"] and lock_status("C", nc, played, 8) is None


def test_new_coach_lock_blocks_sides_and_totals(monkeypatch):
    monkeypatch.setattr(dr, "COACH_LOCK_GAMES", 8)
    g = _game(v1_total=150.0, coach_home={"team": "H", "games": 3, "coach": "X", "prev": "Y"})
    c = {x["market"]: x for x in dr.candidates([g], _now())}
    for m in ("spread", "total"):
        assert not c[m]["rule_ok"] and any("NEW COACH lock: H has played 3 of 8" in r for r in c[m]["reasons"])
        assert c[m]["coach_tag"] == "NEW COACH H 3/8 (5 left)"
    g["coach_home"]["games"] = 8                                # unlocked after N games
    c = {x["market"]: x for x in dr.candidates([g], _now())}
    assert c["spread"]["rule_ok"] and c["total"]["rule_ok"] and "(unlocked)" in c["total"]["coach_tag"]
    monkeypatch.setattr(dr, "COACH_LOCK_GAMES", 0)
    g["coach_home"]["games"] = 0
    assert all(x["rule_ok"] for x in dr.candidates([g], _now()))
    top = dr.top_edges(dr.candidates([g], _now()), set())
    assert "NEW COACH H" in "\n".join(dr.top_edges_md(top))


def test_total_line_move_skip_and_side_flag(monkeypatch):
    monkeypatch.setattr(dr, "COACH_LOCK_GAMES", 8)
    # Wright St. pattern: model 149.5 Under; total opened 156.5, now 162.5 -> moved 6 away from our Under
    g = _game(open_total=156.5, current_total=162.5, v1_total=149.5)
    t = {x["market"]: x for x in dr.candidates([g], _now())}["total"]
    assert t["side"] == "Under" and t["move_against"] == 6.0 and not t["rule_ok"]
    assert any("total moved 6 pts against our side" in r for r in t["reasons"])
    # moved toward our side -> fine
    g = _game(open_total=162.5, current_total=156.5, v1_total=149.5)
    t = {x["market"]: x for x in dr.candidates([g], _now())}["total"]
    assert t["move_against"] == -6.0 and t["rule_ok"]
    # side: model H by 14; H opened -8, now -5 (3 pts against H) -> flag only by default
    g = _game(open_home_spread=-8.0, current_home_spread=-5.0, v2_home_margin=14.0)
    s = {x["market"]: x for x in dr.candidates([g], _now())}["spread"]
    assert s["move_against"] == 3.0 and s["rule_ok"]
    top = dr.top_edges([s], {"9-spread"})
    assert any("AGAINST our side (flag only)" in r for r in top[0]["reasons"])
    monkeypatch.setattr(dr, "SIDE_MOVE_SKIP", True)
    s = {x["market"]: x for x in dr.candidates([g], _now())}["spread"]
    assert not s["rule_ok"]


def _row(**kw):
    r = dict(pick_id="9-total", event_id="9", game_date="2029-12-31", tip_ct="2029-12-31 18:00", away="A", home="H",
             market="total", side="Under", line=162.5, model_value=149.5, price=-110, stake=8.2, status="open")
    r.update(kw)
    return pd.Series(r)


def test_pretip_check_play():
    assert pc.check_play(_row(), {"current_total": 163.0}) == []
    w = pc.check_play(_row(), {"current_total": 165.0})          # Under 162.5 -> 165: 2.5 against
    assert w and w[0].startswith("line moved 2.5 against us")
    w = pc.check_play(_row(line=152.5), {"current_total": 151.0})  # edge 1.5/151 < 3% but moved toward us
    assert w == []                                                 # totals: no edge-vs-current rule anymore
    sp = _row(pick_id="9-spread", market="spread", side="H", line=-4.5, model_value=11.0)
    assert pc.check_play(sp, {"current_home_spread": -5.0, "current_total": 140.0}) == []
    w = pc.check_play(sp, {"current_home_spread": -7.0, "current_total": 140.0})
    assert any("edge vs current" in x for x in w) and not any("against" in x for x in w)  # moved toward H
    w = pc.check_play(sp, {"current_home_spread": -2.0, "current_total": 140.0})          # 2.5 against H
    assert w[0].startswith("line moved 2.5 against us")
    aw = _row(pick_id="9-spread", market="spread", side="A", line=6.5, model_value=1.0)  # A +6.5, model A wins by 1
    assert pc.check_play(aw, {"current_home_spread": -6.0, "current_total": 140.0}) == []
    assert pc.check_play(aw, {"current_home_spread": -9.0, "current_total": 140.0})[0].startswith("line moved 2.5")
    assert pc.check_play(aw, {"current_home_spread": -4.0, "current_total": 140.0}) == ["edge vs current 3.6% < 4%"]
    assert pc.check_play(sp, {}) and "no current DK line" in pc.check_play(sp, {})[0]


def test_pretip_main_annotates_only_open_unstarted(monkeypatch, tmp_path, capsys):
    led = pd.DataFrame([
        dict(_row(), pretip_flag=None),
        dict(_row(pick_id="8-total", event_id="8", status="graded", result="win", pnl=7.45, game_date="2029-12-31",
                  tip_ct="2029-12-31 07:00")),
        dict(_row(pick_id="7-total", event_id="7", tip_ct="2029-12-31 07:30")),   # already started
    ], columns=dr.LEDGER_COLS)
    path = tmp_path / "ledger.csv"
    led.to_csv(path, index=False)
    monkeypatch.setattr(dr, "LEDGER", str(path))
    monkeypatch.setattr(dr, "LEDGER_DIR", str(tmp_path))
    monkeypatch.setattr(dr, "now_ct", _now)
    monkeypatch.setattr(dr, "espn_dk_lines", lambda e: {"current_total": 166.0})
    pc.main(["--dry-run"])
    out = capsys.readouterr().out
    assert "1 of 1 open plays flagged" in out and "A @ H: Under 162.5" in out and "3.5 against" in out
    assert pd.read_csv(path).pretip_flag.isna().all()              # dry run: untouched
    pc.main([])
    after = pd.read_csv(path, dtype={"event_id": str})
    assert after.pretip_flag.notna().tolist() == [True, False, False]
    pd.testing.assert_frame_equal(after.drop(columns="pretip_flag"), led.drop(columns="pretip_flag").astype(after.drop(columns="pretip_flag").dtypes.to_dict()),
                                  check_dtype=False)
    capsys.readouterr()
    monkeypatch.setattr(dr, "espn_dk_lines", lambda e: {"current_total": 162.5})
    pc.main(["--dry-run"])
    assert capsys.readouterr().out == ""                          # nothing to flag -> empty output


def test_coach_lock_split_totals_4_sides_8(monkeypatch):
    monkeypatch.setattr(dr, "COACH_LOCK_GAMES", None)
    monkeypatch.setattr(dr, "COACH_LOCK_TOTALS", 4)
    monkeypatch.setattr(dr, "COACH_LOCK_SIDES", 8)
    g = _game(v1_total=150.0, coach_home={"team": "H", "games": 3, "coach": "X", "prev": "Y"})
    c = {x["market"]: x for x in dr.candidates([g], _now())}
    assert not c["total"]["rule_ok"] and any("3 of 4" in r for r in c["total"]["reasons"])
    assert not c["spread"]["rule_ok"] and any("3 of 8" in r for r in c["spread"]["reasons"])
    g["coach_home"]["games"] = 4
    c = {x["market"]: x for x in dr.candidates([g], _now())}
    assert c["total"]["rule_ok"] and not c["spread"]["rule_ok"]
    assert c["total"]["coach_tag"] == "NEW COACH H 4/4 (unlocked)" and c["spread"]["coach_tag"] == "NEW COACH H 4/8 (4 left)"


def test_coach_lock_cli_flags(monkeypatch):
    import argparse
    monkeypatch.setattr(dr, "COACH_LOCK_TOTALS", 4)
    monkeypatch.setattr(dr, "COACH_LOCK_SIDES", 8)
    def stop(*a, **k):
        raise SystemExit(0)
    monkeypatch.setattr(dr, "now_ct", stop)
    for argv, want in ((["--coach-lock-games", "6"], (6, 6)),
                       (["--coach-lock-totals", "2", "--coach-lock-sides", "10"], (2, 10)),
                       (["--coach-lock-games", "6", "--coach-lock-sides", "9"], (6, 9))):
        try:
            dr.main(argv)
        except SystemExit:
            pass
        assert (dr.COACH_LOCK_TOTALS, dr.COACH_LOCK_SIDES) == want


def test_totals_edge_vs_open_only(monkeypatch):
    monkeypatch.setattr(dr, "COACH_LOCK_GAMES", 0)
    # model 150 Over; open 145 (3.4% edge), current 147 (2.0%) -> moved 2 toward-ish/against? Over: o-cur=-2 (toward)
    g = _game(open_total=145.0, current_total=147.0, v1_total=150.0)
    t = {x["market"]: x for x in dr.candidates([g], _now())}["total"]
    assert t["edge_pct"] < 0.03 <= t["edge_pct_vs_open"] and t["rule_ok"]
    top = dr.top_edges([t], set())
    assert "2.0%" in "\n".join(dr.top_edges_md(top))               # vs-current edge still shown
