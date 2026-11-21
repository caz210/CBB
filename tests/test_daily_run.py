import pandas as pd
import daily_run as dr


def test_kelly_and_min_stake():
    assert dr.kelly_stake(0.545, -110, 1000) > 5
    assert dr.kelly_stake(0.535, -115, 1000) < dr.MIN_STAKE      # no edge at -115
    assert dr.kelly_stake(0.9, -110, 1000) == 25.0                # 2.5% cap


def test_grade_spread_and_total(monkeypatch):
    sb = [{"event_id": "1", "state": "post", "completed": True,
           "home": "H", "away": "A", "home_score": 80.0, "away_score": 70.0}]
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


def test_grade_flipped_neutral_spread_wiu_albany(monkeypatch):
    """ESPN flipped homeAway after the morning slate (ledger home is Albany).

    Hypothetical official play: Albany -3.5. Albany won 65-60, so the spread
    wins, and ESPN's close of WIU +6.5 is Albany -6.5 in the ledger frame (CLV +3.0).
    """
    sb = [{"event_id": "401919826", "state": "post", "completed": True, "neutral": True,
           "home": "Western Illinois", "away": "Albany",
           "home_score": 60.0, "away_score": 65.0}]
    monkeypatch.setattr(dr, "espn_scoreboard", lambda d: sb)
    monkeypatch.setattr(dr, "espn_dk_lines", lambda e: {
        "close_home_spread": 6.5,  # ESPN home = WIU +6.5  =>  Albany -6.5
        "close_home_spread_price": -105,
        "close_away_spread_price": -115,
    })
    led = pd.DataFrame([{
        "game_date": "2026-11-20", "event_id": "401919826", "tip_ct": "2026-11-20 12:00",
        "away": "Western Illinois", "home": "Albany", "neutral": True,
        "price": -110, "stake": 10.0, "status": "open",
        "pick_id": "401919826-spread", "market": "spread", "side": "Albany", "line": -3.5,
    }], columns=dr.LEDGER_COLS)
    logs = []
    # grade() uses module-level log
    monkeypatch.setattr(dr, "log", logs.append)
    dr.grade(led, pd.Timestamp("2026-11-21 08:00", tz=dr.CENTRAL).to_pydatetime())
    r = led.iloc[0]
    assert r.status == "graded" and r.result == "win"
    assert int(r.home_score) == 65 and int(r.away_score) == 60  # Albany 65, WIU 60
    assert r.close_line == -6.5 and r.clv_pts == 3.0
    assert r.close_price == -115  # swapped: picked home uses ESPN's away price


def test_grade_leaves_open_when_names_missing_or_unmatched(monkeypatch):
    monkeypatch.setattr(dr, "espn_dk_lines", lambda e: {"close_home_spread": -3.5})
    base = dict(game_date="2026-01-01", event_id="1", tip_ct="2026-01-01 18:00",
                away="A", home="H", price=-110, stake=10.0, status="open",
                pick_id="1-spread", market="spread", side="H", line=-3.5)
    # missing ESPN names: do not fall back to slot order
    monkeypatch.setattr(dr, "espn_scoreboard", lambda d: [
        {"event_id": "1", "state": "post", "completed": True, "home_score": 80.0, "away_score": 70.0}])
    led = pd.DataFrame([base], columns=dr.LEDGER_COLS)
    dr.grade(led, pd.Timestamp("2026-01-02", tz=dr.CENTRAL).to_pydatetime())
    assert led.iloc[0].status == "open" and pd.isna(led.iloc[0].result)
    # unmatched names
    monkeypatch.setattr(dr, "espn_scoreboard", lambda d: [
        {"event_id": "1", "state": "post", "completed": True,
         "home": "X", "away": "Y", "home_score": 80.0, "away_score": 70.0}])
    led = pd.DataFrame([base], columns=dr.LEDGER_COLS)
    dr.grade(led, pd.Timestamp("2026-01-02", tz=dr.CENTRAL).to_pydatetime())
    assert led.iloc[0].status == "open" and pd.isna(led.iloc[0].result)


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
    a = dr.archetypes({
        "team1_tag_objs": [{"name": "Crowd 3s", "kind": "strength", "tentative": True},
                           {"name": "Fast", "kind": "strength", "tentative": True}],
        "team2_tag_objs": [{"name": "Wall", "kind": "strength"}],
        "tags_tentative": True, "matchup_notes": []})
    assert a["away"] == [{"name": "Crowd 3s", "kind": "strength"}, {"name": "Fast", "kind": "strength"}]
    assert a["home"] == [{"name": "Wall", "kind": "strength"}]
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
    assert not c["total"]["rule_ok"] and c["total"]["reasons"] == ["edge vs open below 3%"]
    # edge vs open but not vs current -> "edge vs now below 4%"
    g = _game(current_home_spread=-8.0)
    s = {x["market"]: x for x in dr.candidates([g], _now())}["spread"]
    assert not s["rule_ok"] and s["reasons"] == ["edge vs now below 4%"]
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
    assert "| 1 |" in md and "| 4 |" in md and "edge vs open below 4%" in md


# ── totals pace fix ──────────────────────────────────────────────────────────
def test_pace_fix_off_by_default():
    import daily_run as D
    assert D.PACE_FIX is False


def test_pace_fix_total_math():
    import daily_run as D
    # no bias terms active, w=0 -> unchanged
    assert D.pace_fix_total(150.0, 70.0, 145.0, nov=False, min_games=20, neutral=False, w=0.0) == 150.0
    # November only: P 70 -> 69.65, T scales with pace
    t = D.pace_fix_total(150.0, 70.0, None, nov=True, min_games=20, neutral=False, bias={"nov": -0.35}, w=0.5)
    assert abs(t - 150.0 * 69.65 / 70.0) < 1e-9          # no open total -> no blend
    # blend with the open total only
    t = D.pace_fix_total(150.0, 70.0, 140.0, nov=False, min_games=20, neutral=False, w=0.2)
    assert abs(t - (0.8 * 150 + 0.2 * 140)) < 1e-9
    # all three terms + blend
    b = {"nov": -0.35, "early": -0.30, "neutral": -0.44}
    t = D.pace_fix_total(150.0, 70.0, 148.0, nov=True, min_games=3, neutral=True, bias=b, w=0.2)
    assert abs(t - (0.8 * 150 * (70 - 1.09) / 70 + 0.2 * 148)) < 1e-9
    assert D.pace_fix_total(None, 70.0, 148.0) is None


def test_apply_pace_fix_keeps_raw(tmp_path):
    import pandas as pd
    import daily_run as D
    pd.DataFrame({"TeamName": ["A", "B"], "Wins": [2, 10], "Losses": [1, 5]}).to_csv(tmp_path / "ratings.csv", index=False)
    games = [{"kp_home": "A", "kp_away": "B", "v1_total": 150.0, "v1_pace": 70.0, "open_total": 150.0, "neutral": True},
             {"skip": "non-D1", "v1_total": 150.0, "v1_pace": 70.0}]
    assert D.apply_pace_fix(games, "2026-11-04", str(tmp_path)) == 1
    g = games[0]
    assert g["v1_total_raw"] == 150.0
    exp = D.pace_fix_total(150.0, 70.0, 150.0, nov=True, min_games=3, neutral=True)
    assert abs(g["v1_total"] - exp) < 1e-9 and g["v1_total"] < 150.0
    assert "v1_total_raw" not in games[1]


def test_attach_market_edges_reuses_candidates():
    now = _now()
    games = [_game()]
    dr.attach_market_edges(games, now)
    g = games[0]
    c = {x["market"]: x for x in dr.candidates([_game()], now)}
    assert g["edge_now_pct_spread"] == c["spread"]["edge_pct"]
    assert g["edge_vs_open_pct_spread"] == c["spread"]["edge_pct_vs_open"]
    assert g["edge_now_pct_total"] == c["total"]["edge_pct"]
    assert g["edge_vs_open_pct_total"] == c["total"]["edge_pct_vs_open"]
    assert abs(g["edge_now_pct_spread"] - 6.5 / 140) < 1e-9
    assert abs(g["edge_vs_open_pct_spread"] - 7 / 140) < 1e-9
    # UCF-shaped lean: ~4.5% now, ~2.5% vs open (fractions)
    lean = _game(v2_home_margin=10.2, current_home_spread=-3.0, open_home_spread=-6.2,
                 current_total=160.0, open_total=160.0)
    dr.attach_market_edges([lean], now)
    assert abs(lean["edge_now_pct_spread"] - 0.045) < 1e-9
    assert abs(lean["edge_vs_open_pct_spread"] - 0.025) < 1e-9
    # missing lines stay None, not 0
    bare = _game(current_home_spread=None, open_home_spread=None, current_total=None, open_total=None,
                 v1_total=None)
    dr.attach_market_edges([bare], now)
    assert bare["edge_now_pct_spread"] is None and bare["edge_vs_open_pct_spread"] is None
    assert bare["edge_now_pct_total"] is None and bare["edge_vs_open_pct_total"] is None



def test_reason_text_names_the_bar_without_percentages():
    # numbers stay off the reason string; the card already shows them
    g = _game(v2_home_margin=10.2, current_home_spread=-3.0, open_home_spread=-6.2,
              current_total=160.0, open_total=160.0, v1_total=161.0)
    c = {x["market"]: x for x in dr.candidates([g], _now())}
    assert c["spread"]["reasons"] == ["edge vs open below 4%"]
    assert "2.5" not in c["spread"]["reasons"][0] and "%" not in c["spread"]["reasons"][0].split("below")[0]
    assert c["total"]["reasons"] == ["edge vs open below 3%"]
    # both bars fail on a side: two reasons, still no raw edge figures
    both = _game(v2_home_margin=5.0, current_home_spread=-4.5, open_home_spread=-4.0)
    s = {x["market"]: x for x in dr.candidates([both], _now())}["spread"]
    assert s["reasons"] == ["edge vs open below 4%", "edge vs now below 4%"]
    assert all("(" not in r and r.count("%") == 1 for r in s["reasons"])
    # flipped past the model with current edge still >= 4%
    flip = _game(current_home_spread=-17.0)
    s = {x["market"]: x for x in dr.candidates([flip], _now())}["spread"]
    assert s["reasons"] == ["edge gone vs current (flipped side)"]
    # flipped and current edge also below the bar
    flip_low = _game(current_home_spread=-14.0)
    s = {x["market"]: x for x in dr.candidates([flip_low], _now())}["spread"]
    assert s["reasons"] == ["edge vs now below 4% (flipped side)"]


def test_skip_codes_and_attach_reasons():
    now = _now()
    # H by 11 vs -4.5 is a qualifying side; total 141 vs 140 is below 3%
    games = [_game()]
    # no bankroll pressure
    dr.attach_skip_reasons(games, now, 1000.0)
    g = games[0]
    assert g["skip_reasons_spread"] == [] and g["skip_reason_spread"] is None
    assert "below_open_threshold" in g["skip_reasons_total"]
    assert abs(g["edge_now_pct_spread"] - 6.5 / 140) < 1e-9
    assert g["skip_reason_total"] == "edge vs open below 3%"
    # juiced official side: edge is large but price makes Kelly < $1
    g2 = _game(event_id="2", current_home_spread_price=-1000, current_away_spread_price=-1000)
    # candidates uses price from current_*_spread_price; force a qualifying side with a huge juice
    dr.attach_skip_reasons([g2], now, 1000.0)
    assert "kelly_too_small" in g2["skip_reasons_spread"]
    # new-coach hold on the total
    g3 = _game(event_id="3", v1_total=150.0,
               coach_home={"team": "H", "games": 0, "lock_sides": None, "lock_totals": 4})
    dr.attach_skip_reasons([g3], now, 1000.0)
    assert "new_coach_lock" in g3["skip_reasons_total"]
    assert g3["skip_reason_total"] == "Paused until game 4 (new coach, 0 played)"


def test_resync_never_grades_logs_picks_or_writes_ledger(monkeypatch, tmp_path):
    import daily_run as dr
    import czarp.supabase_writer as sw
    import czarp.sync_finals as sf
    synced = {}

    def boom(*a, **k):
        raise AssertionError("must not run under --resync")

    monkeypatch.chdir(tmp_path)          # cwd-relative writes (data/ledger/big_moves) land in tmp, not the repo
    monkeypatch.setenv("SUPABASE_URL", "http://example.invalid")
    monkeypatch.setenv("SUPABASE_SERVICE_KEY", "x")
    monkeypatch.setattr(dr, "OUT_DIR", str(tmp_path))
    monkeypatch.setattr(dr, "SNAP_DIR", str(tmp_path / "snap"))
    monkeypatch.setattr(dr, "read_ledger", lambda: pd.DataFrame(columns=dr.LEDGER_COLS))
    monkeypatch.setattr(dr, "grade", boom)
    monkeypatch.setattr(dr, "flag_plays", boom)
    monkeypatch.setattr(dr, "append_picks", boom)
    monkeypatch.setattr(dr, "write_ledger", boom)
    monkeypatch.setattr(dr, "espn_scoreboard", lambda d: [dict(event_id="1", game_date="2026-11-10", state="pre")])
    monkeypatch.setattr(dr, "espn_dk_lines", lambda e: {})
    monkeypatch.setattr(dr, "load_kenpom", lambda today, refresh: {"config": None})
    monkeypatch.setattr(dr, "fanmatch_preds", lambda d: {})
    monkeypatch.setattr(dr, "project_all", lambda games, data: None)
    monkeypatch.setattr(dr, "tag_new_coaches", lambda games, today: {})
    monkeypatch.setattr(dr, "odds_api_fallback", lambda *a, **k: 0)
    monkeypatch.setattr(dr, "candidates", lambda games, now: [])
    monkeypatch.setattr(dr, "attach_skip_reasons", lambda games, now, b: synced.setdefault("skips", True))
    monkeypatch.setattr(dr, "PACE_FIX", False)
    monkeypatch.setattr(sw, "sync", lambda games, ledger, cfg, now, **k: synced.setdefault("n", len(ledger)) and "ok")
    monkeypatch.setattr(sf, "sync_finals", lambda **k: {"updated": 0, "matched": 0, "unmatched_espn": 0})
    assert dr.main(["--resync", "--date", "2026-11-10", "--no-kalshi", "--no-refresh"]) == 0
    assert synced.get("skips") and "n" in synced
    assert (tmp_path / "2026-11-10.resync.md").exists()
    assert not (tmp_path / "snap").exists() or not any((tmp_path / "snap").iterdir())


def test_odds_api_fallback_runs_when_any_game_missing_and_keeps_cap_and_reserve(monkeypatch, tmp_path):
    """One D-I game without a DK line (2% of the slate) is enough to call The Odds API, but never more
    than ODDS_API_MAX_CALLS_PER_DAY times a day and never below ODDS_API_RESERVE credits."""
    import json, sys, types
    calls = []
    fake = types.ModuleType("odds_fetcher")
    fake.ODDS_API_KEY = "test-key"
    fake._odds_credits_remaining = 400
    fake._best_score = lambda a, b: 1.0 if a == b else 0.0

    def fetch():
        calls.append(1)
        return pd.DataFrame([dict(vegas_home="Austin Peay Governors", vegas_away="UT Rio Grande Valley Vaqueros",
                                  vegas_spread=-4.5, vegas_total=143.5, home_spread_price=-110,
                                  away_spread_price=-110, over_price=-110, under_price=-110,
                                  source_book="draftkings")])
    fake.fetch_vegas_lines = fetch
    monkeypatch.setitem(sys.modules, "odds_fetcher", fake)
    clock = {"now": dr.datetime(2026, 11, 21, 7, 13, tzinfo=dr.CENTRAL)}
    monkeypatch.setattr(dr, "now_ct", lambda: clock["now"])          # budget day = real CT date
    usage = tmp_path / "usage.json"
    usage.write_text(json.dumps({"calls": {}, "remaining": 500}))
    monkeypatch.setattr(dr, "USAGE", str(usage))
    monkeypatch.setattr(dr, "log", lambda *a, **k: None)

    def slate():
        full = [dict(home_full=f"H{i}", away_full=f"A{i}", current_home_spread=-3.5, current_total=140.5)
                for i in range(50)]
        return full + [dict(home_full="Austin Peay Governors", away_full="UT Rio Grande Valley Vaqueros",
                            current_home_spread=None, current_total=None)]

    g = slate()
    assert dr.odds_api_fallback(g, "2026-11-21", allow=True) == 1
    assert len(calls) == 1 and g[-1]["current_home_spread"] == -4.5 and g[-1]["current_total"] == 143.5
    assert g[-1]["price_source"] == "oddsapi:draftkings"
    assert json.loads(usage.read_text())["calls"]["2026-11-21"] == 1
    # daily cap: a second run the same day does not call again
    assert dr.odds_api_fallback(slate(), "2026-11-21", allow=True) == 0 and len(calls) == 1
    # credit reserve: below it, no call even on a new day
    clock["now"] = dr.datetime(2026, 11, 22, 7, 13, tzinfo=dr.CENTRAL)
    usage.write_text(json.dumps({"calls": {}, "remaining": dr.ODDS_API_RESERVE - 1}))
    assert dr.odds_api_fallback(slate(), "2026-11-22", allow=True) == 0 and len(calls) == 1
    # every game has a line: no call
    usage.write_text(json.dumps({"calls": {}, "remaining": 500}))
    assert dr.odds_api_fallback(slate()[:50], "2026-11-23", allow=True) == 0 and len(calls) == 1
    # --no-odds-api still wins
    assert dr.odds_api_fallback(slate(), "2026-11-23", allow=False) == 0 and len(calls) == 1
