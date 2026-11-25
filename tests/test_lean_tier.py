"""CZarp LEAN tier (sides 2.5% to <4% vs the posted line, flat 1u, never official) + LINE MOVED AWAY flag."""
import pandas as pd
import pytest

import daily_run as dr
from tests.test_daily_run import _game, _now


def _side(g):
    return {x["market"]: x for x in dr.candidates([g], _now())}["spread"]


# model H by 11, totals 140: gap vs H -6.5 = 4.5 pts = 3.2% (lean band); vs -4.5 = 4.6% (official)
LEAN = dict(open_home_spread=-6.5, current_home_spread=-6.5)


@pytest.fixture(autouse=True)
def _defaults(monkeypatch):
    monkeypatch.setattr(dr, "LEAN_ENABLED", True)
    monkeypatch.setattr(dr, "COACH_LOCK_GAMES", 8)
    monkeypatch.setattr(dr, "LINE_MOVE_EXCLUDE", False)
    monkeypatch.setattr(dr, "SIDE_MOVE_SKIP", False)


def test_lean_band_flat_line_is_lean():
    s = _side(_game(**LEAN))
    assert s["edge_pct"] == pytest.approx(4.5 / 140) and s["tier"] == "lean" and not s["rule_ok"]
    assert not s["new_coach"] and s["move_against"] == 0


def test_lean_band_line_moved_toward_czarp_is_lean():
    # opened H -5.5 (5.5 pts), now -6.5 (4.5 pts): picked-side line moved -1 = toward CZarp
    s = _side(_game(open_home_spread=-5.5, current_home_spread=-6.5))
    assert s["move_against"] == -1.0 and s["tier"] == "lean"


def test_edges_outside_the_band_are_not_lean():
    # 3.4 pts / 140 = 2.4% -> below 2.5%
    assert _side(_game(open_home_spread=-7.5, current_home_spread=-7.6))["tier"] is None
    # 4%+ both ways -> official, not lean
    assert _side(_game())["tier"] == "official"
    # exactly 2.5% now (3.5 pts / 140) -> lean
    assert _side(_game(open_home_spread=-7.5, current_home_spread=-7.5))["tier"] == "lean"


def test_line_moved_away_is_never_lean():
    # opened H -7 (4 pts), now -6.5: picked-side line +0.5 = away from CZarp
    s = _side(_game(open_home_spread=-7.0, current_home_spread=-6.5))
    assert s["move_against"] == 0.5 and s["tier"] is None


def test_flipped_side_missing_open_or_started_is_never_lean():
    from datetime import datetime, timezone
    # open H -13 (model on A by 2 pts at open), now -6.5 (H by 4.5) -> flipped side
    assert _side(_game(open_home_spread=-13.0, current_home_spread=-6.5))["tier"] is None
    assert _side(_game(open_home_spread=None, current_home_spread=-6.5))["tier"] is None
    assert _side(_game(**LEAN, state="in", tip_utc=datetime(2020, 1, 1, tzinfo=timezone.utc)))["tier"] is None


def test_track_keeps_priority_over_lean():
    # LSU pattern: open 4.2%, now 2.9% -> track (the existing A/B), not lean
    g = _game(open_home_spread=-21.5, current_home_spread=-23.5, v2_home_margin=28.1, open_total=157.5, current_total=159.5)
    assert _side(g)["tier"] == "track"


def test_new_coach_paused_official_or_lean_goes_to_lean_with_marker():
    paused = {"team": "H", "games": 3}
    s = _side(_game(coach_home=paused))                       # would be official
    assert not s["rule_ok"] and s["tier"] == "lean" and s["new_coach"]
    s = _side(_game(**LEAN, coach_home=paused))               # would be lean
    assert s["tier"] == "lean" and s["new_coach"]
    # paused but below the band -> nothing
    assert _side(_game(open_home_spread=-8.5, current_home_spread=-8.5, coach_home=paused))["tier"] is None
    # paused official whose line moved away -> nothing
    assert _side(_game(open_home_spread=-5.0, current_home_spread=-4.5, coach_home=paused))["tier"] is None


def test_carried_over_coach_holds_from_refresh_count_as_new_coach():
    g = _game(coach_holds={"spread": ["Paused until game 8 (new coach, 3 played)"], "total": []})
    s = _side(g)
    assert s["tier"] == "lean" and s["new_coach"]


def test_lean_disabled_switch():
    import daily_run
    daily_run.LEAN_ENABLED = False
    try:
        assert _side(_game(**LEAN))["tier"] is None
    finally:
        daily_run.LEAN_ENABLED = True


def test_flag_plays_lean_rows_flat_unstaked_and_marked():
    games = [_game(),                                                     # official
             _game(event_id="6", kp_home="P", kp_away="Q", **LEAN),       # lean
             _game(event_id="7", kp_home="R", kp_away="S", coach_home={"team": "R", "games": 2})]  # paused official
    rows = {r["pick_id"]: r for r in dr.flag_plays(games, 1000.0, {}, _now())}
    assert set(rows) == {"9-spread", "6-spread-lean", "7-spread-lean"}
    ln, nc = rows["6-spread-lean"], rows["7-spread-lean"]
    assert ln["tier"] == "lean" and ln["stake"] == 0.0 and "(lean)" in ln["rule"] and ln["new_coach"] is False
    assert nc["tier"] == "lean" and nc["stake"] == 0.0 and nc["rule"].endswith("+new coach") and nc["new_coach"] is True
    assert rows["9-spread"]["tier"] == "official" and rows["9-spread"]["stake"] > 0 and rows["9-spread"]["new_coach"] is False
    assert set(r["pick_id"] for r in dr.flag_plays(games, 1000.0, {}, _now(), include_lean=False)) == {"9-spread"}


def _ledger(tmp_path, monkeypatch):
    monkeypatch.setattr(dr, "LEDGER", str(tmp_path / "ledger.csv"))
    monkeypatch.setattr(dr, "LEDGER_DIR", str(tmp_path))
    games = [_game(),
             _game(event_id="6", kp_home="P", kp_away="Q", **LEAN),
             _game(event_id="7", kp_home="R", kp_away="S", coach_home={"team": "R", "games": 2})]
    rows = dr.flag_plays(games, 1000.0, {}, _now())
    led = dr.append_picks(dr.read_ledger(), rows, _now())
    dr.write_ledger(led)
    return dr.read_ledger()


def _grade_all(led, monkeypatch):
    sb = [{"event_id": e, "state": "post", "completed": True, "home": h, "away": a, "home_score": hs, "away_score": 60.0}
          for e, h, a, hs in (("9", "H", "A", 80.0), ("6", "P", "Q", 70.0), ("7", "R", "S", 61.0))]
    monkeypatch.setattr(dr, "espn_scoreboard", lambda d: sb)
    monkeypatch.setattr(dr, "espn_dk_lines", lambda e: {})
    dr.grade(led, pd.Timestamp("2030-01-01 08:00", tz=dr.CENTRAL).to_pydatetime())
    dr.write_ledger(led)
    return dr.read_ledger()


def test_lean_is_sealed_graded_flat_and_never_official(tmp_path, monkeypatch):
    led = _ledger(tmp_path, monkeypatch)
    assert sorted(led.tier) == ["lean", "lean", "official"] and not dr.verify_ledger(led)
    assert dr._truthy(led.set_index("pick_id").loc["7-spread-lean", "new_coach"])
    assert not dr._truthy(led.set_index("pick_id").loc["6-spread-lean", "new_coach"])
    led = _grade_all(led, monkeypatch)
    by = led.set_index("pick_id")
    assert by.loc["6-spread-lean", "result"] == "win" and by.loc["6-spread-lean", "pnl"] == 0     # P by 10 vs -6.5
    assert by.loc["6-spread-lean", "pnl_units"] == pytest.approx(100 / 110, abs=1e-4)
    assert by.loc["7-spread-lean", "result"] == "loss" and by.loc["7-spread-lean", "pnl_units"] == -1.0
    off = dr.official(led)
    assert list(off.pick_id) == ["9-spread"]
    assert dr.bankroll(led) == pytest.approx(1000 + float(by.loc["9-spread", "pnl"]))  # lean never on the bankroll
    rec = dr.tier_records(led)
    assert rec["official"]["all"]["n"] == 1 and rec["official"]["all"]["w"] == 1
    assert rec["lean"]["all"]["w"] == 1 and rec["lean"]["all"]["l"] == 1
    assert rec["lean"]["all"]["units"] == pytest.approx(100 / 110 - 1, abs=0.01)
    md = "\n".join(dr.tier_record_md(led))
    assert "| LEAN | Sides | 1-1-0 |" in md and "| LEAN | Totals" not in md
    assert not dr.verify_ledger(led)


def test_lean_cannot_be_promoted_or_have_its_rule_changed(tmp_path, monkeypatch):
    _ledger(tmp_path, monkeypatch)
    bad = dr.read_ledger()
    bad.loc[bad.pick_id == "6-spread-lean", "tier"] = "official"
    with pytest.raises(dr.LedgerImmutableError):
        dr.write_ledger(bad)
    bad = dr.read_ledger()
    bad.loc[bad.pick_id == "7-spread-lean", "rule"] = dr.LEAN_RULE
    with pytest.raises(dr.LedgerImmutableError):
        dr.write_ledger(bad)


def test_old_ledgers_without_new_coach_column_still_load_and_verify(tmp_path, monkeypatch):
    led = _ledger(tmp_path, monkeypatch)
    raw = pd.read_csv(dr.LEDGER, dtype={"event_id": str, "pick_id": str}).drop(columns=["new_coach"])
    raw[raw.tier == "official"].to_csv(dr.LEDGER, index=False)
    old = dr.read_ledger()
    assert "new_coach" in old and not dr.verify_ledger(old)


def test_summary_and_chat_header_list_leans_separately(tmp_path, monkeypatch):
    led = _ledger(tmp_path, monkeypatch)
    monkeypatch.setattr(dr, "OUT_DIR", str(tmp_path))
    p = dr.write_summary("2029-12-31", _now(), [], [], led, [], [], [], path=str(tmp_path / "s.md"))
    md = open(p).read()
    assert "## Today's plays (1)" in md and "## CZarp Leans (2)" in md and "| yes |" in md


def test_pretip_official_filter_excludes_shadow_tiers():
    src = open("pretip_check.py").read()
    assert 'led.tier != "track"' not in src and src.count('led.tier.fillna("official") == "official"') == 3


# ── LINE MOVED AWAY flag ─────────────────────────────────────────────────────────
def test_line_moved_away_flag():
    # model H by 11; opened H -8.5, now -6.5: gap 4.5 pts, picked-side line moved +2 away
    assert dr.line_moved_away(_game(open_home_spread=-8.5, current_home_spread=-6.5)) == (True, 2.0)
    # same gap, only 1 pt away -> no flag
    assert dr.line_moved_away(_game(open_home_spread=-7.5, current_home_spread=-6.5)) == (False, 1.0)
    # moved 2 away but the gap is only 3 pts -> no flag
    assert dr.line_moved_away(_game(open_home_spread=-10.0, current_home_spread=-8.0)) == (False, 2.0)
    # moved toward CZarp -> negative points, no flag
    assert dr.line_moved_away(_game(open_home_spread=-4.0, current_home_spread=-6.0)) == (False, -2.0)
    # away-side pick: model A by 6; A opened -3 (H +3), now -1 (H +1): 5-pt gap, A's line moved 2 away from CZarp
    assert dr.line_moved_away(_game(v2_home_margin=-6.0, open_home_spread=3.0, current_home_spread=1.0)) == (True, 2.0)
    # Florida Nov 24: CZarp -39.8, opened -38.5, posted -35.5 -> 4.3 pts, moved 3 away
    assert dr.line_moved_away(_game(v2_home_margin=39.76, open_home_spread=-38.5, current_home_spread=-35.5)) == (True, 3.0)
    assert dr.line_moved_away(_game(open_home_spread=None)) == (None, None)


def test_attach_market_edges_stamps_flag():
    g = _game(open_home_spread=-8.5, current_home_spread=-6.5)
    dr.attach_market_edges([g], _now())
    assert g["line_moved_away"] is True and g["line_moved_away_pts"] == 2.0


# ── Supabase ─────────────────────────────────────────────────────────────────────
def _sync(tmp_path, monkeypatch, lean_env):
    from czarp import supabase_writer as sw
    from tests.test_supabase_writer import FakeClient
    if lean_env:
        monkeypatch.setenv("CZARP_SYNC_LEAN", "1")
    else:
        monkeypatch.delenv("CZARP_SYNC_LEAN", raising=False)
    led = _ledger(tmp_path, monkeypatch)
    c, logs = FakeClient(), []
    sw.sync([], led, None, _now(), log=logs.append, client=c)
    return {r["pick_key"]: r for r in c.calls.get("picks", [])}, logs


def test_supabase_holds_lean_picks_until_enabled(tmp_path, monkeypatch):
    pk, logs = _sync(tmp_path, monkeypatch, lean_env=False)
    assert set(pk) == {"9-spread"} and "new_coach" not in pk["9-spread"]
    assert any("2 CZarp Lean picks kept local" in x for x in logs)


def test_supabase_sends_lean_with_new_coach_when_enabled(tmp_path, monkeypatch):
    pk, _ = _sync(tmp_path, monkeypatch, lean_env=True)
    assert set(pk) == {"9-spread", "6-spread-lean", "7-spread-lean"}
    assert pk["6-spread-lean"]["tier"] == "lean" and pk["6-spread-lean"]["kelly_stake"] == 0
    assert pk["7-spread-lean"]["new_coach"] is True and pk["6-spread-lean"]["new_coach"] is False
    assert pk["9-spread"]["new_coach"] is False                # same keys on every row (PostgREST bulk upsert)


def test_projection_rows_carry_flag_and_fallback_drops_it():
    from czarp import supabase_writer as sw
    g = _game(open_home_spread=-8.5, current_home_spread=-6.5)
    dr.attach_market_edges([g], _now())
    assert sw._flag_fields(g) == {"line_moved_away": True, "line_moved_away_pts": 2.0}
    assert sw._flag_fields({}) == {"line_moved_away": None, "line_moved_away_pts": None}

    class C:
        def __init__(self):
            self.sent = []

        def upsert(self, table, rows, **kw):
            if any("line_moved_away" in r for r in rows):
                raise RuntimeError('400 {"code":"PGRST204","message":"Could not find the \'line_moved_away\' column"}')
            self.sent.extend(rows)

    c, logs = C(), []
    sw.upsert_projections(c, [{"game_id": 1, "line_moved_away": True, "line_moved_away_pts": 2.0, "spread": -11.0}],
                          log=logs.append)
    assert c.sent == [{"game_id": 1, "spread": -11.0}] and "optional columns missing" in logs[0]


def test_refresh_projection_rows_write_flag_only_when_db_has_columns():
    import refresh_lines as RL
    g = _game(open_home_spread=-8.5, current_home_spread=-6.5, v2_total=140.0)
    dr.attach_market_edges([g], _now())
    base = {"id": 1, "game_id": 1, "model_version": "czarp-v2", "created_at": "x", "spread": -11.0, "total": 140.0}
    rows = RL.projection_rows(g, dict(base), None, "2029-12-31T10:00:00-06:00", {"spread"}, sds=(11.0, 17.0))
    assert "line_moved_away" not in rows[0]                    # migration not applied -> column never sent
    rows = RL.projection_rows(g, dict(base, line_moved_away=False, line_moved_away_pts=0.0), None,
                              "2029-12-31T10:00:00-06:00", {"spread"}, sds=(11.0, 17.0))
    assert rows[0]["line_moved_away"] is True and rows[0]["line_moved_away_pts"] == 2.0
    rows = RL.projection_rows(g, dict(base, line_moved_away=False, line_moved_away_pts=0.0), None,
                              "2029-12-31T10:00:00-06:00", {"total"}, sds=(11.0, 17.0))
    assert rows[0]["line_moved_away"] is False                 # spread inputs unchanged -> base value kept
