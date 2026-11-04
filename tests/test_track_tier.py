"""SHADOW 'track' tier + immutable, hash-sealed ledger."""
import pandas as pd
import pytest

import daily_run as dr
from tests.test_daily_run import _game, _now


def _sides(g, monkeypatch=None):
    return {x["market"]: x for x in dr.candidates([g], _now())}["spread"]


def test_tiers_official_track_none(monkeypatch):
    monkeypatch.setattr(dr, "COACH_LOCK_GAMES", 8)
    # model H by 11, total 140. open -4 -> 7/140 = 5%; current -4.5 -> 4.6%: official
    s = _sides(_game())
    assert s["tier"] == "official" and s["rule_ok"] and not s["track_ok"]
    # LSU pattern: open 4.2%, current 2.9% -> track
    g = _game(open_home_spread=-21.5, current_home_spread=-23.5, v2_home_margin=28.1, open_total=157.5, current_total=159.5)
    s = _sides(g)
    assert s["edge_pct_vs_open"] == pytest.approx(6.6 / 157.5) and s["edge_pct"] == pytest.approx(4.6 / 159.5)
    assert s["tier"] == "track" and not s["rule_ok"]
    # current edge below 2% -> nothing
    assert _sides(_game(open_home_spread=-21.5, current_home_spread=-25.5, v2_home_margin=28.1,
                        open_total=157.5, current_total=159.5))["tier"] is None
    # opened under 4% (Missouri 3.4%) -> never track
    assert _sides(_game(open_home_spread=-26.5, current_home_spread=-26.5, v2_home_margin=31.8,
                        open_total=158.5, current_total=160.5))["tier"] is None
    # NEW COACH lock applies to track too ...
    g["coach_home"] = {"team": "H", "games": 0}
    assert _sides(g)["tier"] is None
    # ... unless TRACK_IGNORE_COACH_LOCK
    monkeypatch.setattr(dr, "TRACK_IGNORE_COACH_LOCK", True)
    assert _sides(g)["tier"] == "track"


def test_track_never_on_flipped_side_or_started(monkeypatch):
    monkeypatch.setattr(dr, "COACH_LOCK_GAMES", 0)
    # open: H -4 vs model 11 (H side); current H -14 -> model on A side now
    assert _sides(_game(current_home_spread=-14.0))["tier"] is None
    from datetime import datetime, timezone
    g = _game(open_home_spread=-21.5, current_home_spread=-23.5, v2_home_margin=28.1, open_total=157.5,
              current_total=159.5, state="in", tip_utc=datetime(2020, 1, 1, tzinfo=timezone.utc))
    assert _sides(g)["tier"] is None


def test_flag_plays_track_rows_flat_and_unstaked(monkeypatch):
    monkeypatch.setattr(dr, "COACH_LOCK_GAMES", 0)
    games = [_game(), _game(event_id="5", kp_home="L", kp_away="U", open_home_spread=-21.5, current_home_spread=-23.5,
                            v2_home_margin=28.1, open_total=157.5, current_total=159.5, v1_total=158.0)]
    rows = {r["pick_id"]: r for r in dr.flag_plays(games, 1000.0, {}, _now())}
    assert set(rows) == {"9-spread", "5-spread-track"}
    t = rows["5-spread-track"]
    assert t["tier"] == "track" and t["stake"] == 0.0 and "track" in t["rule"] and t["source"] == "live"
    assert rows["9-spread"]["tier"] == "official" and rows["9-spread"]["stake"] > 0
    assert set(r["pick_id"] for r in dr.flag_plays(games, 1000.0, {}, _now(), include_track=False)) == {"9-spread"}


def _led(tmp_path, monkeypatch):
    monkeypatch.setattr(dr, "LEDGER", str(tmp_path / "ledger.csv"))
    monkeypatch.setattr(dr, "LEDGER_DIR", str(tmp_path))
    monkeypatch.setattr(dr, "COACH_LOCK_GAMES", 0)
    games = [_game(), _game(event_id="5", kp_home="L", kp_away="U", open_home_spread=-21.5, current_home_spread=-23.5,
                            v2_home_margin=28.1, open_total=157.5, current_total=159.5, v1_total=158.0)]
    rows = dr.flag_plays(games, 1000.0, {}, _now())
    return dr.append_picks(dr.read_ledger(), rows, _now())


def test_append_seal_grade_and_records(tmp_path, monkeypatch):
    led = _led(tmp_path, monkeypatch)
    assert list(led.tier) == ["official", "track"] and led.logged_ct.notna().all() and not dr.verify_ledger(led)
    # same game+market again (any tier) is not re-logged
    again = dr.append_picks(led, [dict(led.iloc[1].to_dict(), pick_id="5-spread")], _now())
    assert len(again) == 2
    dr.write_ledger(led)
    sb = [{"event_id": "9", "state": "post", "completed": True, "home_score": 80.0, "away_score": 70.0},
          {"event_id": "5", "state": "post", "completed": True, "home_score": 100.0, "away_score": 57.0}]
    monkeypatch.setattr(dr, "espn_scoreboard", lambda d: sb)
    monkeypatch.setattr(dr, "espn_dk_lines", lambda e: {"close_home_spread": -24.5 if e == "5" else -5.0})
    led = dr.read_ledger()
    dr.grade(led, pd.Timestamp("2030-01-01 08:00", tz=dr.CENTRAL).to_pydatetime())
    dr.write_ledger(led)                                       # grading is allowed (not a frozen field)
    led = dr.read_ledger()
    o, t = led.iloc[0], led.iloc[1]
    assert o.result == "win" and o.pnl > 0 and t.result == "win" and t.pnl == 0
    assert t.pnl_units == pytest.approx(100 / 110, abs=1e-4) and t.clv_pts == 1.0
    assert dr.bankroll(led) == pytest.approx(1000 + o.pnl)    # track never touches the bankroll
    rec = dr.tier_records(led)
    assert rec["track"]["all"]["w"] == 1 and rec["track"]["all"]["units"] == pytest.approx(0.91)
    assert rec["official"]["spread"]["w"] == 1 and rec["official"]["total"]["n"] == 0
    md = "\n".join(dr.tier_record_md(led))
    assert "| TRACK | Sides | 1-0-0 | 100.0% | +0.91u |" in md and "| OFFICIAL | All |" in md
    assert not dr.verify_ledger(led)


def test_write_ledger_refuses_to_change_or_drop_picks(tmp_path, monkeypatch):
    led = _led(tmp_path, monkeypatch)
    dr.write_ledger(led)
    bad = dr.read_ledger()
    bad.loc[0, "line"] = -3.5                                 # moving the number after the fact
    with pytest.raises(dr.LedgerImmutableError):
        dr.write_ledger(bad)
    bad = dr.read_ledger()
    bad.loc[1, "tier"] = "official"                           # promoting a tracked pick
    with pytest.raises(dr.LedgerImmutableError):
        dr.write_ledger(bad)
    with pytest.raises(dr.LedgerImmutableError):
        dr.write_ledger(dr.read_ledger().iloc[[1]])           # deleting a pick
    # tampering with the CSV directly is caught by the hash chain
    raw = pd.read_csv(dr.LEDGER, dtype={"event_id": str, "pick_id": str})
    raw.loc[0, "price"] = 150
    raw.to_csv(dr.LEDGER, index=False)
    assert dr.verify_ledger(dr.read_ledger())[0].startswith("9-spread: pick fields changed")


def test_live_pick_after_tip_is_refused(tmp_path, monkeypatch):
    led = pd.DataFrame(columns=dr.LEDGER_COLS)
    row = dict(pick_id="1-spread", event_id="1", market="spread", tip_ct="2029-12-31 07:00", source="live")
    with pytest.raises(dr.LedgerImmutableError):
        dr.append_picks(led, [row], _now())                   # _now() = 08:00 CT, tip 07:00
    out = dr.append_picks(led, [dict(row, source="backfill", created_ct="2029-12-31T06:00:00-06:00")], _now())
    assert out.iloc[0].source == "backfill" and out.iloc[0].logged_ct.startswith("2029-12-31T08:00")


def test_summary_has_tracked_section_and_tier_table(tmp_path, monkeypatch):
    led = _led(tmp_path, monkeypatch)
    monkeypatch.setattr(dr, "OUT_DIR", str(tmp_path))
    p = dr.write_summary("2029-12-31", _now(), [], [], led, [], [], [], path=str(tmp_path / "s.md"))
    md = open(p).read()
    assert "## Today's plays (1)" in md and "## Tracked (not played) (1)" in md and "L -23.5" in md
    assert "## Running record by tier" in md and "hash chain OK" in md


def test_supabase_sync_sends_tier_and_never_deletes(monkeypatch, tmp_path):
    from czarp import supabase_writer as sw
    from tests.test_supabase_writer import FakeClient
    led = _led(tmp_path, monkeypatch)

    class C(FakeClient):
        def get(self, table, **params):
            return [{"pick_key": "old-total"}] if table == "picks" else super().get(table, **params)

        def delete(self, table, **params):
            raise AssertionError("picks must never be deleted")

    c, logs = C(), []
    out = sw.sync([], led, None, _now(), log=logs.append, client=c)
    pk = {r["pick_key"]: r for r in c.calls["picks"]}
    assert pk["5-spread-track"]["tier"] == "track" and pk["5-spread-track"]["kelly_stake"] == 0
    assert pk["9-spread"]["tier"] == "official" and pk["9-spread"]["source"] == "live"
    assert pk["9-spread"]["pick_hash"] and pk["9-spread"]["logged_at"]
    assert "1 published picks not in ledger" in out and any("immutable" in x for x in logs)


def test_backfill_from_snapshot(tmp_path, monkeypatch):
    import backfill_track as bt
    monkeypatch.setattr(dr, "LEDGER", str(tmp_path / "ledger.csv"))
    monkeypatch.setattr(dr, "LEDGER_DIR", str(tmp_path))
    monkeypatch.setattr(dr, "SNAP_DIR", str(tmp_path))
    monkeypatch.setattr(dr, "tag_new_coaches", lambda games, day: {})
    g = _game(event_id="5", kp_home="LSU", kp_away="UNO", home="LSU", away="UNO", open_home_spread=-21.5,
              current_home_spread=-23.5, current_home_spread_price=-115, v2_home_margin=28.1, open_total=157.5,
              current_total=159.5, v1_total=158.0, tip_ct="2029-12-31 19:00", game_date="2029-12-31")
    g.pop("tip_utc")
    pd.DataFrame([dict(g, snapshot_ct="2029-12-31T07:18:13-06:00")]).to_csv(tmp_path / "2029-12-31_0718.csv", index=False)
    sb = [{"event_id": "5", "state": "post", "completed": True, "home_score": 100.0, "away_score": 57.0}]
    monkeypatch.setattr(dr, "espn_scoreboard", lambda d: sb)
    monkeypatch.setattr(dr, "espn_dk_lines", lambda e: {"close_home_spread": -24.5})
    monkeypatch.setattr(dr, "now_ct", lambda: pd.Timestamp("2030-01-01 08:00", tz=dr.CENTRAL).to_pydatetime())
    tr = bt.backfill("2029-12-31", write=True, log=lambda *_: None)
    assert [r["pick_id"] for r in tr] == ["5-spread-track"]
    led = dr.read_ledger()
    r = led.iloc[0]
    assert r.tier == "track" and r.source == "backfill" and r.created_ct == "2029-12-31T07:18:13-06:00"
    assert r.line == -23.5 and r.price == -115 and r.result == "win" and r.stake == 0
    assert not dr.verify_ledger(led)
