"""Late-line official catch-up (--late-lines-only) and current-as-open fill."""
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

import daily_run as dr


def _game(**kw):
    g = dict(
        event_id="9", state="pre",
        tip_utc=datetime(2030, 1, 1, tzinfo=timezone.utc), tip_ct="2029-12-31 18:00",
        game_date="2029-12-31", kp_home="H", kp_away="A", neutral=False,
        open_home_spread=-4.0, current_home_spread=-4.5, open_total=140.0, current_total=140.0,
        v2_home_margin=11.0, v1_total=141.0,
    )
    g.update(kw)
    return g


def _now(ts="2029-12-31 07:00"):
    return pd.Timestamp(ts, tz=dr.CENTRAL).to_pydatetime()


def _snap_row(event_id="9", **lines):
    row = dict(
        event_id=event_id, state="pre", tip_ct="2029-12-31 18:00",
        game_date="2029-12-31", kp_home="H", kp_away="A",
        v2_home_margin=11.0, v1_total=141.0, v2_total=142.0,
        open_home_spread=None, current_home_spread=None, open_total=None, current_total=None,
    )
    row.update(lines)
    return row


def _write_snapshot(directory: Path, day: str, hhmm: str, rows: list[dict]) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{day}_{hhmm}.csv"
    pd.DataFrame(rows).to_csv(path, index=False)
    return path


# ── snapshot + eligibility ───────────────────────────────────────────────────
def test_first_snapshot_is_the_earliest(tmp_path):
    day = "2026-11-26"
    _write_snapshot(tmp_path, day, "0705", [_snap_row()])
    first = _write_snapshot(tmp_path, day, "0621", [_snap_row()])
    _write_snapshot(tmp_path, day, "1005", [_snap_row()])
    assert Path(dr.first_snapshot(day, str(tmp_path))) == first
    assert Path(dr.latest_snapshot(day, str(tmp_path))).name == "2026-11-26_1005.csv"
    assert dr.first_snapshot("2026-11-25", str(tmp_path)) is None
    assert dr.first_snapshot(day, str(tmp_path / "missing")) is None


def test_eligibility_is_per_market():
    # both missing
    assert dr.late_line_eligible_markets(_snap_row()) == {"spread", "total"}
    # spread missing, total present
    g = _snap_row(open_total=150.5, current_total=150.5)
    assert dr.late_line_eligible_markets(g) == {"spread"}
    # total missing, spread present (current only — a posted line)
    g = _snap_row(current_home_spread=-3.5)
    assert dr.late_line_eligible_markets(g) == {"total"}
    # both present
    g = _snap_row(open_home_spread=-3.5, current_home_spread=-3.5, open_total=150.0, current_total=150.0)
    assert dr.late_line_eligible_markets(g) == set()
    # open dropped by sanity check, current present: NOT eligible
    g = _snap_row(open_home_spread=None, current_home_spread=-3.5, open_total=None, current_total=155.5)
    assert dr.late_line_eligible_markets(g) == set()
    # game missing from the snapshot: first look, both markets
    assert dr.late_line_eligible_markets(None) == {"spread", "total"}


def test_select_late_line_plays_keeps_only_previously_lineless_markets():
    snap = {
        "9": _snap_row("9", open_total=140.0, current_total=140.0),  # total lined, spread missing
        "8": _snap_row("8", open_home_spread=-4.0, current_home_spread=-4.0,
                       open_total=140.0, current_total=140.0),       # both lined
        "7": _snap_row("7"),                                         # both missing
    }
    rows = [
        {"event_id": "9", "market": "spread", "pick_id": "9-spread"},
        {"event_id": "9", "market": "total", "pick_id": "9-total"},
        {"event_id": "8", "market": "spread", "pick_id": "8-spread"},
        {"event_id": "8", "market": "total", "pick_id": "8-total"},
        {"event_id": "7", "market": "total", "pick_id": "7-total"},
    ]
    kept = dr.select_late_line_plays(rows, snap)
    assert {r["pick_id"] for r in kept} == {"9-spread", "7-total"}


def test_already_lined_market_clearing_the_bar_later_does_not_qualify():
    # Morning: small edge, already had a line. 7 AM: line moved, now clears the official bar.
    snap = {"9": _snap_row("9", open_home_spread=-4.0, current_home_spread=-4.0,
                           open_total=140.0, current_total=140.0)}
    later = _game(open_home_spread=-4.0, current_home_spread=-4.5,  # 6.5/140 = 4.6% official
                  open_total=140.0, current_total=140.0, v1_total=141.0)
    plays = dr.flag_plays([later], 1000.0, {}, _now())
    assert any(p["pick_id"] == "9-spread" and p["tier"] == "official" for p in plays)
    assert dr.select_late_line_plays(plays, snap) == []


# ── current-as-open + sanity-check drop ──────────────────────────────────────
def test_drop_unreliable_opens_thresholds():
    kept = dr.drop_unreliable_opens({"open_home_spread": -4.0, "current_home_spread": 4.0,  # 8 pts, not >
                                     "open_total": 140.0, "current_total": 152.0})          # 12 pts, not >
    assert kept["open_home_spread"] == -4.0 and kept["open_total"] == 140.0
    assert kept["open_home_spread_dropped"] is False and kept["open_total_dropped"] is False

    dropped = dr.drop_unreliable_opens({"open_home_spread": -4.0, "current_home_spread": 4.5,
                                        "open_total": 140.0, "current_total": 152.5})
    assert dropped["open_home_spread"] is None and dropped["open_home_spread_dropped"] is True
    assert dropped["open_total"] is None and dropped["open_total_dropped"] is True


def test_current_as_open_fills_missing_open_and_prices():
    g = _game(open_home_spread=None, open_home_spread_price=None, open_away_spread_price=None,
              current_home_spread=-3.5, current_home_spread_price=-108, current_away_spread_price=-112,
              open_total=None, open_over_price=None, open_under_price=None,
              current_total=151.5, current_over_price=-105, current_under_price=-115)
    assert dr.apply_current_as_open(g) == ["spread", "total"]
    assert g["open_home_spread"] == -3.5 and g["open_home_spread_price"] == -108
    assert g["open_away_spread_price"] == -112
    assert g["open_total"] == 151.5 and g["open_over_price"] == -105 and g["open_under_price"] == -115
    # already filled: no-op
    assert dr.apply_current_as_open(g) == []


def test_current_as_open_does_not_undo_sanity_check_drop():
    g = {"open_home_spread": 10.0, "current_home_spread": -5.0,   # 15 pts after no flip
         "open_total": 140.0, "current_total": 160.0}
    dr.drop_unreliable_opens(g)
    assert g["open_home_spread"] is None and g["open_total"] is None
    assert dr.apply_current_as_open(g) == []
    assert g["open_home_spread"] is None and g["open_total"] is None
    # restricting markets does not override the flag either
    assert dr.apply_current_as_open(g, markets=["spread", "total"]) == []


def test_fill_opens_from_current_respects_late_line_eligibility():
    # Already-lined market has current but no open at 7 AM; must not get a synthetic open in late mode.
    lined = _game(event_id="8", open_home_spread=None, current_home_spread=-4.5,
                  open_total=None, current_total=140.0)
    missing = _game(event_id="9", open_home_spread=None, current_home_spread=-4.5,
                    open_total=None, current_total=140.0)
    snap = {
        "8": _snap_row("8", current_home_spread=-3.5, current_total=141.0),  # was lined
        "9": _snap_row("9"),                                                 # was lineless
    }
    n = dr.fill_opens_from_current([lined, missing], snap)
    assert n == 2  # only game 9, both markets
    assert lined["open_home_spread"] is None and lined["open_total"] is None
    assert missing["open_home_spread"] == -4.5 and missing["open_total"] == 140.0
    # morning path (no snap_by_id): both games get the fill
    lined2 = _game(event_id="8", open_home_spread=None, current_home_spread=-4.5,
                   open_total=None, current_total=140.0)
    assert dr.fill_opens_from_current([lined2], None) == 2
    assert lined2["open_home_spread"] == -4.5


def test_morning_odds_api_fill_is_treated_as_open():
    """A current-only Odds API fill on the morning run should get a synthetic open, unless dropped."""
    g = _game(open_home_spread=None, current_home_spread=-2.5,
              open_total=None, current_total=139.5,
              current_home_spread_price=-110, current_away_spread_price=-110,
              current_over_price=-110, current_under_price=-110)
    # simulate post-fallback fill on a normal (non-late) run
    assert dr.fill_opens_from_current([g], None) == 2
    cands = {c["market"]: c for c in dr.candidates([g], _now())}
    assert cands["spread"]["open_line"] == -2.5
    assert cands["total"]["open_line"] == 139.5
    assert "no opening line" not in cands["spread"]["reasons"]
    assert "no opening line" not in cands["total"]["reasons"]


# ── qualification, ledger, started, overlay ──────────────────────────────────
def test_previously_lineless_markets_qualify_and_do_not_alter_morning_picks():
    morning_now = _now("2029-12-31 06:21")
    # Official total already logged this morning on a *different* game.
    morning_game = _game(event_id="1", v1_total=166.0, open_total=155.5, current_total=155.5,
                         v2_home_margin=1.0, open_home_spread=-1.0, current_home_spread=-1.0)
    morning_rows = dr.flag_plays([morning_game], 1000.0, {}, morning_now)
    official_morning = [r for r in morning_rows if r["tier"] == "official"]
    assert official_morning
    ledger = dr.append_picks(pd.DataFrame(columns=dr.LEDGER_COLS), official_morning, morning_now)
    morning_fp = dr.pick_fingerprint(ledger.iloc[0])
    morning_hash = ledger.iloc[0]["pick_hash"]

    snap = {
        "9": _snap_row("9"),   # both markets lineless at 6 AM
        "8": _snap_row("8"),
        "7": _snap_row("7"),
    }
    later = [
        # official spread
        _game(event_id="9", open_home_spread=None, current_home_spread=-4.5,
              open_total=None, current_total=140.0),
        # lean side: 2.625% now, same side, no move away
        _game(event_id="8", kp_home="H2", kp_away="A2",
              v2_home_margin=10.2, current_home_spread=-6.0, open_home_spread=None,
              current_total=160.0, open_total=None, v1_total=161.0),
        # track side: 4%+ vs open, 2–4% vs current
        _game(event_id="7", kp_home="H3", kp_away="A3",
              v2_home_margin=11.0, open_home_spread=None, current_home_spread=-8.0,
              open_total=None, current_total=140.0, current_open_would="set below"),
    ]
    # Current-only at 7 AM → treat current as open (eligible markets).
    assert dr.fill_opens_from_current(later, snap) == 6
    # For the track case we need open and current to *differ*: after the fill, open == current.
    # Set a distinct current (open stays at the first-seen -8 would make it official). Start over:
    later[2]["open_home_spread"] = -2.0
    later[2]["current_home_spread"] = -8.0
    later[2]["open_total"] = 140.0
    later[2]["current_total"] = 140.0

    plays = dr.flag_plays(later, 1000.0, {}, _now())
    plays = dr.select_late_line_plays(plays, snap)
    by_id = {p["pick_id"]: p for p in plays}
    assert by_id["9-spread"]["tier"] == "official"
    assert by_id["8-spread-lean"]["tier"] == "lean"
    assert by_id["7-spread-track"]["tier"] == "track"

    out = dr.append_picks(ledger, plays, _now())
    assert list(out.pick_id.astype(str))[:1] == [str(ledger.iloc[0]["pick_id"])]
    assert dr.pick_fingerprint(out.iloc[0]) == morning_fp
    assert out.iloc[0]["pick_hash"] == morning_hash
    assert set(out.pick_id.astype(str)) >= {"9-spread", "8-spread-lean", "7-spread-track", str(ledger.iloc[0]["pick_id"])}
    assert dr.verify_ledger(out) == []


def test_started_game_is_skipped_even_if_previously_lineless():
    snap = {"9": _snap_row("9")}
    started = _game(state="in", tip_utc=datetime(2026, 11, 26, 12, 0, tzinfo=timezone.utc),
                    tip_ct="2026-11-26 06:00",
                    open_home_spread=None, current_home_spread=-4.5,
                    open_total=None, current_total=140.0)
    dr.fill_opens_from_current([started], snap)
    plays = dr.select_late_line_plays(dr.flag_plays([started], 1000.0, {}, _now("2026-11-26 07:00")), snap)
    assert plays == []
    c = {x["market"]: x for x in dr.candidates([started], _now("2026-11-26 07:00"))}
    assert c["spread"]["started"] and "game started" in c["spread"]["reasons"]


def test_apply_snapshot_projections_copies_model_not_lines():
    live = [{"event_id": "9", "state": "pre", "tip_ct": "2029-12-31 18:00",
             "open_home_spread": -8.0, "current_total": 155.5}]
    snap = {"9": _snap_row("9", v2_home_margin=7.5, v1_total=166.0, v2_total=164.0,
                           reason="pace", proj_possessions=70.0, skip=None,
                           open_home_spread=None, current_total=None)}
    assert dr.apply_snapshot_projections(live, snap) == 1
    g = live[0]
    assert g["v2_home_margin"] == 7.5 and g["v1_total"] == 166.0 and g["reason"] == "pace"
    assert g["proj_possessions"] == 70.0
    # live lines / state are not overwritten by the snapshot
    assert g["open_home_spread"] == -8.0 and g["current_total"] == 155.5
    assert g["state"] == "pre"


# ── CLI + main() integration (tmp ledger / snapshots only) ───────────────────
def test_late_lines_only_rejects_resync_and_chat_only():
    with pytest.raises(SystemExit):
        dr.main(["--late-lines-only", "--resync"])
    with pytest.raises(SystemExit):
        dr.main(["--late-lines-only", "--chat-only"])


def _patch_paths(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(dr, "LEDGER_DIR", str(tmp_path / "ledger"))
    monkeypatch.setattr(dr, "LEDGER", str(tmp_path / "ledger" / "ledger.csv"))
    monkeypatch.setattr(dr, "SNAP_DIR", str(tmp_path / "snaps"))
    monkeypatch.setattr(dr, "OUT_DIR", str(tmp_path / "out"))
    (tmp_path / "ledger").mkdir()
    (tmp_path / "snaps").mkdir()
    (tmp_path / "out").mkdir()


def _logs(monkeypatch):
    lines = []
    monkeypatch.setattr(dr, "log", lambda m="": lines.append(str(m)))
    return lines


def test_late_lines_only_falls_back_without_morning_snapshot(monkeypatch, tmp_path):
    _patch_paths(monkeypatch, tmp_path)
    lines = _logs(monkeypatch)
    called = {"kenpom": 0, "project": 0}

    def load(today, refresh):
        called["kenpom"] += 1
        assert refresh is True
        return {"config": None}

    def project(games, data):
        called["project"] += 1
        for g in games:
            g["kp_home"], g["kp_away"] = "H", "A"
            g["v2_home_margin"] = 11.0
            g["v1_total"] = 141.0

    g = dict(event_id="9", game_date="2029-12-31", state="pre",
             tip_utc=datetime(2030, 1, 1, tzinfo=timezone.utc), tip_ct="2029-12-31 18:00",
             home="H", away="A", kp_home="H", kp_away="A", neutral=False, time_tbd=False)
    monkeypatch.setattr(dr, "now_ct", lambda: _now())
    monkeypatch.setattr(dr, "espn_scoreboard", lambda d: [dict(g)])
    monkeypatch.setattr(dr, "espn_dk_lines", lambda e: {
        "open_home_spread": -4.0, "current_home_spread": -4.5,
        "open_total": 140.0, "current_total": 140.0,
    })
    monkeypatch.setattr(dr, "load_kenpom", load)
    monkeypatch.setattr(dr, "fanmatch_preds", lambda d: {})
    monkeypatch.setattr(dr, "project_all", project)
    monkeypatch.setattr(dr, "tag_new_coaches", lambda *a, **k: {})
    monkeypatch.setattr(dr, "odds_api_fallback", lambda *a, **k: 0)
    monkeypatch.setattr(dr, "grade", lambda ledger, now: [])
    monkeypatch.setattr(dr, "PACE_FIX", False)

    rc = dr.main(["--late-lines-only", "--date", "2029-12-31", "--no-kalshi", "--dry-run"])
    assert rc == 0
    assert called["kenpom"] == 1 and called["project"] == 1
    assert any("falling back to a full run" in x for x in lines)
    assert not any("anchoring to first snapshot" in x for x in lines)
    assert not any("No late-line picks" in x for x in lines)


def test_late_lines_only_e2e_posts_official_and_keeps_morning_pick(monkeypatch, tmp_path):
    _patch_paths(monkeypatch, tmp_path)
    lines = _logs(monkeypatch)
    day = "2029-12-31"

    morning_now = _now("2029-12-31 06:21")
    morning_game = _game(event_id="1", v1_total=166.0, open_total=155.5, current_total=155.5,
                         v2_home_margin=1.0, open_home_spread=-1.0, current_home_spread=-1.0)
    morning_rows = [r for r in dr.flag_plays([morning_game], 1000.0, {}, morning_now)
                    if r["tier"] == "official"]
    led = dr.append_picks(pd.DataFrame(columns=dr.LEDGER_COLS), morning_rows, morning_now)
    led.to_csv(dr.LEDGER, index=False)
    morning_fp = dr.pick_fingerprint(led.iloc[0])
    morning_hash = led.iloc[0]["pick_hash"]
    morning_id = str(led.iloc[0]["pick_id"])

    # 6 AM snapshot: game 9 had no lines; game 2 already had a line (must not re-qualify).
    _write_snapshot(Path(dr.SNAP_DIR), day, "0621", [
        _snap_row("9", v2_home_margin=11.0, v1_total=141.0, v2_total=142.0,
                  kp_home="H", kp_away="A"),
        _snap_row("2", v2_home_margin=11.0, v1_total=141.0, v2_total=142.0,
                  kp_home="H2", kp_away="A2",
                  open_home_spread=-4.0, current_home_spread=-4.0,
                  open_total=140.0, current_total=140.0),
    ])
    # A later snapshot must not become the anchor.
    _write_snapshot(Path(dr.SNAP_DIR), day, "0659", [_snap_row("9")])

    def scoreboard(d):
        return [
            dict(event_id="9", game_date=day, state="pre",
                 tip_utc=datetime(2030, 1, 1, tzinfo=timezone.utc), tip_ct="2029-12-31 18:00",
                 home="H", away="A", neutral=False, time_tbd=False),
            dict(event_id="2", game_date=day, state="pre",
                 tip_utc=datetime(2030, 1, 1, tzinfo=timezone.utc), tip_ct="2029-12-31 18:00",
                 home="H2", away="A2", neutral=False, time_tbd=False),
        ]

    def lines_for(eid):
        # Both games now have a qualifying official spread. Only 9 was lineless at 6 AM.
        return {"open_home_spread": -4.0, "current_home_spread": -4.5,
                "open_total": 140.0, "current_total": 140.0}

    def boom(*a, **k):
        raise AssertionError("KenPom / re-project must not run in --late-lines-only when a snapshot exists")

    monkeypatch.setattr(dr, "now_ct", lambda: _now())
    monkeypatch.setattr(dr, "espn_scoreboard", scoreboard)
    monkeypatch.setattr(dr, "espn_dk_lines", lines_for)
    monkeypatch.setattr(dr, "load_kenpom", boom)
    monkeypatch.setattr(dr, "fanmatch_preds", boom)
    monkeypatch.setattr(dr, "project_all", boom)
    monkeypatch.setattr(dr, "tag_new_coaches", boom)
    monkeypatch.setattr(dr, "odds_api_fallback", lambda *a, **k: 0)
    monkeypatch.setattr(dr, "grade", lambda ledger, now: [])
    monkeypatch.setattr(dr, "PACE_FIX", False)

    rc = dr.main(["--late-lines-only", "--date", day, "--no-kalshi"])
    assert rc == 0
    assert any("anchoring to first snapshot 2029-12-31_0621.csv" in x for x in lines)
    assert any("late-lines: 1 official" in x for x in lines)
    out = pd.read_csv(dr.LEDGER, dtype={"event_id": str, "pick_id": str})
    assert morning_id in set(out.pick_id.astype(str))
    morning = out[out.pick_id.astype(str) == morning_id].iloc[0]
    assert dr.pick_fingerprint(morning) == morning_fp
    assert morning["pick_hash"] == morning_hash
    assert "9-spread" in set(out.pick_id.astype(str))
    assert "2-spread" not in set(out.pick_id.astype(str))  # already lined at 6 AM
    assert dr.verify_ledger(out) == []


def test_late_lines_only_logs_no_picks_marker_when_nothing_qualifies(monkeypatch, tmp_path):
    _patch_paths(monkeypatch, tmp_path)
    lines = _logs(monkeypatch)
    day = "2029-12-31"
    _write_snapshot(Path(dr.SNAP_DIR), day, "0621", [
        _snap_row("9", v2_home_margin=1.0, v1_total=141.0,  # tiny edges, will not qualify
                  kp_home="H", kp_away="A"),
    ])
    g = dict(event_id="9", game_date=day, state="pre",
             tip_utc=datetime(2030, 1, 1, tzinfo=timezone.utc), tip_ct="2029-12-31 18:00",
             home="H", away="A", neutral=False, time_tbd=False)
    monkeypatch.setattr(dr, "now_ct", lambda: _now())
    monkeypatch.setattr(dr, "espn_scoreboard", lambda d: [g])
    monkeypatch.setattr(dr, "espn_dk_lines", lambda e: {
        "open_home_spread": -1.0, "current_home_spread": -1.0,
        "open_total": 140.0, "current_total": 140.0,
    })
    monkeypatch.setattr(dr, "load_kenpom", lambda *a, **k: (_ for _ in ()).throw(AssertionError("no kenpom")))
    monkeypatch.setattr(dr, "odds_api_fallback", lambda *a, **k: 0)
    monkeypatch.setattr(dr, "grade", lambda ledger, now: [])
    monkeypatch.setattr(dr, "PACE_FIX", False)
    # empty ledger
    pd.DataFrame(columns=dr.LEDGER_COLS).to_csv(dr.LEDGER, index=False)

    rc = dr.main(["--late-lines-only", "--date", day, "--no-kalshi", "--dry-run"])
    assert rc == 0
    assert any(x == "  late-lines: No late-line picks" or x.endswith("No late-line picks") for x in lines)
