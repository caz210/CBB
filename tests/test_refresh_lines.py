"""Intraday line refresh (refresh_lines.py): hour gating, light-day threshold, Odds API 'refresh' slot,
no-picks guarantee, edge recompute, idempotency, and the pre-tip fill routing through the same recompute."""
import json
import types
import sys
from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

import daily_run as dr
import pretip_check as pc
import refresh_lines as RL
from czarp import supabase_writer as sw

CT = dr.CENTRAL
TODAY = "2026-11-21"


def ct(h, m=5, day=21, month=11):
    return datetime(2026, month, day, h, m, tzinfo=CT)


# ── schedule ────────────────────────────────────────────────────────────────────
def test_light_day_threshold_is_a_constant_under_20():
    assert RL.LIGHT_DAY_MAX_GAMES == 20
    assert RL.is_light_day(19) and RL.is_light_day(1)
    assert not RL.is_light_day(20) and not RL.is_light_day(150)


@pytest.mark.parametrize("hour,expect", [(10, True), (11, False), (12, False), (13, True), (16, True),
                                         (17, False), (19, True), (20, False), (7, False)])
def test_normal_day_runs_10_1_4_7(hour, expect):
    assert RL.should_run(ct(hour), 60)[0] is expect


@pytest.mark.parametrize("hour,expect", [(10, False), (12, True), (13, False), (16, False), (17, True), (19, False)])
def test_light_day_runs_12_and_5(hour, expect):
    assert RL.should_run(ct(hour), 19)[0] is expect


def test_no_games_never_runs():
    assert RL.should_run(ct(10), 0) == (False, "no games on today's slate")


def test_gate_uses_chicago_local_hour_in_cst_and_cdt():
    # 16:05 UTC is 10:05 CST (November) but 11:05 CDT (March): only the CST one is a run hour
    nov = datetime(2026, 11, 21, 16, 5, tzinfo=timezone.utc)
    mar = datetime(2027, 3, 20, 16, 5, tzinfo=timezone.utc)
    assert RL.should_run(nov, 60)[0] and not RL.should_run(mar, 60)[0]
    # 15:05 UTC in March = 10:05 CDT -> run
    assert RL.should_run(datetime(2027, 3, 20, 15, 5, tzinfo=timezone.utc), 60)[0]
    # 01:05 UTC Nov 22 = 7:05 PM CST Nov 21 -> run (CT date, not UTC date)
    assert RL.should_run(datetime(2026, 11, 22, 1, 5, tzinfo=timezone.utc), 60)[0]


def test_cron_hours_cover_every_run_hour_in_cst_and_cdt():
    for offset in (6, 5):                                   # CST, CDT
        for h in RL.ALL_RUN_HOURS_CT:
            assert (h + offset) % 24 in RL.CRON_UTC_HOURS
    # and nothing fires that can never be a run hour
    for u in RL.CRON_UTC_HOURS:
        assert any((u - off) % 24 in RL.ALL_RUN_HOURS_CT for off in (6, 5))


def test_main_off_hour_exits_before_any_network(monkeypatch, capsys):
    monkeypatch.setattr(dr, "now_ct", lambda: ct(11))
    monkeypatch.setattr(sw, "Client", lambda *a, **k: pytest.fail("no Supabase off-hours"))
    assert RL.main([]) == 0
    assert "not a refresh hour" in capsys.readouterr().out


# ── Odds API budget ─────────────────────────────────────────────────────────────
@pytest.fixture
def usage(monkeypatch, tmp_path):
    u = tmp_path / "odds_api_usage.json"
    u.write_text(json.dumps({"calls": {}, "remaining": 500}))
    monkeypatch.setattr(dr, "USAGE", str(u))
    monkeypatch.setattr(dr, "now_ct", lambda: ct(10))
    return u


def test_budget_slots_refresh_four_plus_daily_and_pretip(usage):
    assert dr.ODDS_API_JOB_SLOTS == {"daily": 1, "pretip": 1, "refresh": 4}
    assert dr.ODDS_API_MAX_CALLS_PER_DAY == 6 == sum(dr.ODDS_API_JOB_SLOTS.values())
    assert [dr.odds_api_claim("refresh")[0] for _ in range(5)] == [True] * 4 + [False]
    assert dr.odds_api_claim("refresh") == (False, "refresh slot used (4/4; the other slot is reserved)")
    assert dr.odds_api_claim("daily")[0] and dr.odds_api_claim("pretip")[0]      # their slots are untouched
    assert dr.odds_api_claim("daily")[1].startswith("daily budget used (6/6")
    u = json.loads(usage.read_text())
    assert u["calls"][TODAY] == 6 and u["calls_by_job"][TODAY] == {"refresh": 4, "daily": 1, "pretip": 1}


def test_refresh_cannot_spend_daily_or_pretip_slots(usage):
    for _ in range(4):
        dr.odds_api_claim("refresh")
    assert not dr.odds_api_claim("refresh")[0]
    u = json.loads(usage.read_text())
    assert u["calls_by_job"][TODAY] == {"refresh": 4}


def test_unknown_job_gets_no_slot(usage):
    assert dr.odds_api_claim("something-else")[0] is False


def test_refresh_uses_odds_api_only_for_games_espn_lacks(usage, monkeypatch):
    calls = []
    fake = types.ModuleType("odds_fetcher")
    fake.ODDS_API_KEY = "test-key"
    fake._odds_credits_remaining = 450
    fake._best_score = lambda a, b: 1.0 if a == b else 0.0

    def fetch():
        calls.append(1)
        return pd.DataFrame([dict(vegas_home="B Home", vegas_away="B Away", vegas_spread=-2.5, vegas_total=150.5,
                                  home_spread_price=-110, away_spread_price=-110, over_price=-110,
                                  under_price=-110, source_book="fanduel")])
    fake.fetch_vegas_lines = fetch
    monkeypatch.setitem(sys.modules, "odds_fetcher", fake)
    pre = [dict(id=1, external_id="1", home="A", away="A2"), dict(id=2, external_id="2", home="B", away="B2")]
    espn = {"1": dict(home_full="A Home", away_full="A Away"), "2": dict(home_full="B Home", away_full="B Away")}
    lines = {"1": {"current_home_spread": -3.0, "current_total": 140.0, "book": "DraftKings"}}
    work = RL.fresh_lines(pre, espn, lines_for=lambda e: lines.get(e, {}), today=TODAY, log=lambda m: None)
    assert len(calls) == 1
    assert work[2]["current_total"] == 150.5 and work[2]["price_source"] == "oddsapi:fanduel"
    assert work[1]["price_source"] == "espn:DraftKings"
    assert json.loads(usage.read_text())["calls_by_job"][TODAY] == {"refresh": 1}
    # nothing missing -> no call
    RL.fresh_lines(pre[:1], espn, lines_for=lambda e: lines.get(e, {}), today=TODAY, log=lambda m: None)
    assert len(calls) == 1


# ── fake Supabase ───────────────────────────────────────────────────────────────
class FakeDB:
    """Minimal PostgREST stand-in: records every write; filters only what refresh_lines uses."""

    def __init__(self, games, projections, lines, picks=()):
        self.tables = {"games": list(games), "projections": list(projections), "market_lines": list(lines),
                       "picks": list(picks)}
        self.writes = []

    @staticmethod
    def _ids(params):
        f = params.get("game_id", "")
        return {int(x) for x in f[4:-1].split(",")} if f.startswith("in.(") else None

    def get(self, table, **params):
        rows = [dict(r) for r in self.tables[table]]
        ids = self._ids(params)
        if ids is not None:
            rows = [r for r in rows if int(r["game_id"]) in ids]
        if table == "market_lines":
            lt = params.get("line_type", "")
            if lt.startswith("eq."):
                rows = [r for r in rows if r["line_type"] == lt[3:]]
        if table == "games" and "date" in params:
            rows = [r for r in rows if r["date"] == params["date"][3:]]
        return rows

    def insert(self, table, rows):
        self.writes.append(("insert", table, [dict(r) for r in rows]))
        self.tables[table].extend(dict(r) for r in rows)

    def upsert(self, table, rows, on_conflict=None, returning=True, ignore_duplicates=False):
        self.writes.append(("upsert", table, [dict(r) for r in rows]))
        self.tables[table].extend(dict(r) for r in rows)
        return []

    def patch(self, table, *a, **k):
        self.writes.append(("patch", table, None))

    def delete(self, table, *a, **k):
        self.writes.append(("delete", table, None))


T715 = "2026-11-21T07:15:00-06:00"


def _proj(gid, version, **kw):
    base = dict(id=100 + gid, game_id=gid, sport_id=1, league="ncaam", season=2027, model_version=version,
                proj_home=78.0, proj_away=70.0, spread=-8.0, total=148.0, home_win_prob=0.75,
                home_cover_prob=0.6, over_prob=0.5, archetype_tags={"home": ["x"]}, reason="why",
                proj_possessions=70.1, upset_alert=False, upset_reason=None, tv_rating=55.5,
                skip_reasons_spread=[], skip_reasons_total=[], skip_reason_spread=None, skip_reason_total=None,
                edge_now_pct_spread=None, edge_vs_open_pct_spread=None, edge_now_pct_total=None,
                edge_vs_open_pct_total=None, created_at=T715)
    if version == RL.V1:
        base.update(spread=-7.0, total=152.0, home_cover_prob=None)
    base.update(kw)
    return base


def _line(gid, lt, spread, total, at=T715, prices=None):
    return dict(game_id=gid, source="vegas", line_type=lt, book="DraftKings", spread=spread, total=total,
                prices=prices or {}, fetched_at=at)


def _game(gid, tip="2026-11-22T01:00:00+00:00", status="scheduled"):
    return dict(id=gid, external_id=str(900 + gid), home=f"Home{gid}", away=f"Away{gid}", tipoff=tip,
                status=status, date=TODAY)


def _seed(n_games=25):
    games = [_game(i) for i in range(1, n_games + 1)]
    projs, lines = [], []
    for g in games:
        projs += [_proj(g["id"], RL.V2), _proj(g["id"], RL.V1)]
        lines += [_line(g["id"], "open", -6.0, 148.0), _line(g["id"], "current", -6.0, 148.0)]
    return games, projs, lines


def _scoreboard(games, state="pre"):
    return lambda day: [dict(event_id=g["external_id"], state=state, home_full=g["home"], away_full=g["away"],
                             tip_utc=pd.Timestamp(g["tipoff"]).to_pydatetime()) for g in games]


QUIET = lambda m: None


def _run(db, games, lines_for, now=None, **kw):
    return RL.run(now or ct(10), db, scoreboard=_scoreboard(games), lines_for=lines_for, allow_api=False,
                  log=QUIET, **kw)


# ── no picks, ever ──────────────────────────────────────────────────────────────
def test_refresh_never_writes_picks_or_the_ledger(monkeypatch):
    monkeypatch.setattr(dr, "write_ledger", lambda *a, **k: pytest.fail("ledger write"))
    monkeypatch.setattr(dr, "append_picks", lambda *a, **k: pytest.fail("pick append"))
    monkeypatch.setattr(dr, "flag_plays", lambda *a, **k: pytest.fail("flag_plays"))
    monkeypatch.setattr(dr, "read_ledger", lambda *a, **k: pytest.fail("ledger read"))
    games, projs, lines = _seed()
    db = FakeDB(games, projs, lines)
    out = _run(db, games, lambda e: {"current_home_spread": -2.0, "current_total": 141.0, "book": "DraftKings"})
    assert out["ran"] and out["lines_written"] == 25
    assert {(op, t) for op, t, _ in db.writes} == {("insert", "market_lines"), ("upsert", "projections")}
    for _, _, rows in db.writes:
        for r in rows:
            assert not ({"pick_key", "pick_hash", "bet", "kelly_stake"} & set(r))


def test_refresh_is_idempotent_same_lines_write_nothing():
    games, projs, lines = _seed()
    db = FakeDB(games, projs, lines)
    fresh = lambda e: {"current_home_spread": -2.0, "current_total": 141.0, "book": "DraftKings"}
    _run(db, games, fresh)
    n = len(db.writes)
    out = _run(db, games, fresh, now=ct(10, 40))         # same hour re-run, same lines
    assert len(db.writes) == n and out["lines_written"] == 0 and out["edges_written"] == 0


def test_off_hour_and_light_day_gate_inside_run():
    games, projs, lines = _seed(n_games=10)               # light day
    db = FakeDB(games, projs, lines)
    out = RL.run(ct(10), db, scoreboard=lambda d: pytest.fail("no ESPN when gated"), log=QUIET)
    assert out["ran"] is False and "light day (10 games)" in out["reason"] and db.writes == []
    out = _run(db, games, lambda e: {}, now=ct(12))
    assert out["ran"] is True
    # --force runs off-hour
    games2, p2, l2 = _seed()
    assert RL.run(ct(11), FakeDB(games2, p2, l2), force=True, scoreboard=_scoreboard(games2),
                  lines_for=lambda e: {}, allow_api=False, log=QUIET)["ran"]


def test_dry_run_writes_nothing():
    games, projs, lines = _seed()
    db = FakeDB(games, projs, lines)
    out = _run(db, games, lambda e: {"current_home_spread": -2.0, "current_total": 141.0}, dry_run=True)
    assert db.writes == [] and out["lines_written"] == 25


# ── edge recompute ──────────────────────────────────────────────────────────────
def _written(db, table, gid=None):
    rows = [r for op, t, rs in db.writes if t == table for r in rs]
    return [r for r in rows if gid is None or r["game_id"] == gid]


def test_recompute_matches_daily_run_formulas_and_copies_projections():
    games, projs, lines = _seed()
    db = FakeDB(games, projs, lines)
    _run(db, games, lambda e: {"current_home_spread": -3.0, "current_total": 143.0, "book": "DraftKings",
                               "current_home_spread_price": -110, "current_away_spread_price": -110,
                               "current_over_price": -110, "current_under_price": -110})
    v2 = [r for r in _written(db, "projections", 1) if r["model_version"] == RL.V2][0]
    v1 = [r for r in _written(db, "projections", 1) if r["model_version"] == RL.V1][0]
    # sides: v2 margin 8 vs -3.0 now -> |8-3|/143 ; vs open -6 -> |8-6|/148
    assert v2["edge_now_pct_spread"] == round(5 / 143, 4)
    assert v2["edge_vs_open_pct_spread"] == round(2 / 148, 4)
    # totals use the v1 total (152): now |152-143|/143, open |152-148|/148
    assert v2["edge_now_pct_total"] == round(9 / 143, 4) == v1["edge_now_pct_total"]
    assert v2["edge_vs_open_pct_total"] == round(4 / 148, 4)
    # independent check against daily_run on an equivalent game dict
    g = dict(event_id="x", kp_home="H", kp_away="A", state="pre", tip_utc=datetime(2026, 11, 22, 1, tzinfo=timezone.utc),
             v2_home_margin=8.0, v2_total=148.0, v1_total=152.0, open_home_spread=-6.0, open_total=148.0,
             current_home_spread=-3.0, current_total=143.0)
    dr.attach_skip_reasons([g], ct(10), 1000.0)
    for k in dr.EDGE_FIELDS:
        assert v2[k] == round(g[k], 4)
    assert v2["skip_reasons_spread"] == g["skip_reasons_spread"]
    assert v2["skip_reasons_total"] == g["skip_reasons_total"] == v1["skip_reasons_total"]
    # projections copied unchanged
    for k in ("proj_home", "proj_away", "spread", "total", "home_win_prob", "archetype_tags", "reason",
              "proj_possessions", "upset_alert", "tv_rating", "season", "sport_id", "league"):
        assert v2[k] == projs[0][k]
    assert v1["total"] == 152.0 and v1["spread"] == -7.0
    assert "id" not in v2 and v2["created_at"] == v2["edges_as_of"] == ct(10).isoformat(timespec="seconds")
    assert v1["edges_as_of"] == v2["edges_as_of"]


def test_total_moved_three_against_gets_line_moved_against():
    games, projs, lines = _seed()
    db = FakeDB(games, projs, lines)
    # v1 total 152 -> Over; the total falling 148 -> 144.5 (3.5 pts) is away from CZarp
    _run(db, games, lambda e: {"current_home_spread": -6.0, "current_total": 144.5})
    v2 = [r for r in _written(db, "projections", 1) if r["model_version"] == RL.V2][0]
    assert "line_moved_against" in v2["skip_reasons_total"]
    assert "market moved 3+ pts away from CZarp" in v2["skip_reason_total"]


def test_kelly_too_small_for_every_market_clearing_the_bar_not_only_official():
    games, projs, lines = _seed()
    db = FakeDB(games, projs, lines)
    # side: open edge |8-6|/148 below 4% (not official), but now |8-0|/140 = 5.7% clears the bar; juice kills Kelly
    _run(db, games, lambda e: {"current_home_spread": 0.0, "current_total": 140.0,
                               "current_home_spread_price": -2000, "current_away_spread_price": -2000})
    v2 = [r for r in _written(db, "projections", 1) if r["model_version"] == RL.V2][0]
    assert "below_open_threshold" in v2["skip_reasons_spread"]
    assert "kelly_too_small" in v2["skip_reasons_spread"]
    # a market below the bar never gets kelly_too_small
    g = dict(event_id="y", kp_home="H", kp_away="A", state="pre", tip_utc=datetime(2026, 11, 22, 1, tzinfo=timezone.utc),
             v2_home_margin=8.0, v2_total=148.0, v1_total=149.0, open_home_spread=-7.5, open_total=148.0,
             current_home_spread=-7.5, current_total=148.0, current_home_spread_price=-2000,
             current_away_spread_price=-2000, current_over_price=-2000, current_under_price=-2000)
    dr.attach_skip_reasons([g], ct(10), 1000.0)
    assert "kelly_too_small" not in g["skip_reasons_spread"] + g["skip_reasons_total"]


def test_new_coach_hold_carried_over_from_the_715_row():
    games, projs, lines = _seed()
    projs[0].update(skip_reasons_total=["below_open_threshold", "new_coach_lock"],
                    skip_reason_total="edge vs open below 3%; Paused until game 4 (new coach, 2 played)")
    db = FakeDB(games, projs, lines)
    _run(db, games, lambda e: {"current_home_spread": -6.0, "current_total": 147.0})
    v2 = [r for r in _written(db, "projections", 1) if r["model_version"] == RL.V2][0]
    assert "new_coach_lock" in v2["skip_reasons_total"] and "new_coach_lock" not in v2["skip_reasons_spread"]
    assert "Paused until game 4 (new coach, 2 played)" in v2["skip_reason_total"]
    assert RL.coach_holds_from(projs[0]) == {"spread": [], "total": ["Paused until game 4 (new coach, 2 played)"]}


def test_missing_value_is_carried_forward_and_prices_merge():
    old = _line(1, "current", -6.0, 148.0, prices={"home_spread_price": -105, "over_price": -110})
    row = RL.line_row_if_changed(1, old, {"current_home_spread": -5.5, "current_home_spread_price": -112,
                                          "current_away_spread_price": -108}, "t")
    assert row["spread"] == -5.5 and row["total"] == 148.0
    assert row["prices"] == {"home_spread_price": -112, "away_spread_price": -108, "over_price": -110}
    assert RL.line_row_if_changed(1, old, {}, "t") is None                       # nothing new: no row
    assert RL.line_row_if_changed(1, old, {"current_home_spread": -6.0, "current_total": 148.0}, "t") is None
    # line moved but no fresh price: the stale price for that market is dropped, the other market's kept
    row = RL.line_row_if_changed(1, old, {"current_total": 150.0}, "t")
    assert row["total"] == 150.0 and row["prices"] == {"home_spread_price": -105}


def test_started_final_and_just_picked_games_are_left_alone():
    games, projs, lines = _seed()
    games[0]["tipoff"] = "2026-11-21T15:00:00+00:00"                            # tipped 9 AM CT
    games[1]["status"] = "final"
    db = FakeDB(games, projs, lines, picks=[dict(game_id=3, logged_at=(ct(10) - timedelta(minutes=3)).isoformat(),
                                                 created_at=None, tier="official", result=None, pnl=None)])
    _run(db, games, lambda e: {"current_home_spread": -2.0, "current_total": 141.0})
    touched = {r["game_id"] for r in _written(db, "projections")}
    assert {1, 2, 3}.isdisjoint(touched) and 4 in touched
    assert {1, 2}.isdisjoint({r["game_id"] for r in _written(db, "market_lines")})


def test_bankroll_from_published_picks_is_read_only():
    db = FakeDB([], [], [], picks=[dict(game_id=1, tier="official", result="win", pnl=20.0),
                                   dict(game_id=2, tier="track", result="win", pnl=90.0),
                                   dict(game_id=3, tier=None, result="loss", pnl=-11.0),
                                   dict(game_id=4, tier="official", result=None, pnl=None)])
    assert RL.supabase_bankroll(db) == dr.BANKROLL0 + 9.0 and db.writes == []


# ── pre-tip fill routes through the same recompute ─────────────────────────────
def test_pretip_fill_restamps_edges_for_filled_games(monkeypatch):
    games, projs, lines = _seed(3)
    lines = [l for l in lines if not (l["game_id"] == 2 and l["line_type"] == "current")]
    lines.append(_line(2, "current", -6.0, None))                                 # total missing
    db = FakeDB(games, projs, lines)
    now = ct(15)
    pc.fill_missing_lines(TODAY, now, pd.DataFrame(columns=dr.LEDGER_COLS), allow_api=False, client=db,
                          lines=lambda e: {"current_total": 141.0, "book": "DraftKings"})
    assert [(op, t) for op, t, _ in db.writes] == [("insert", "market_lines"), ("upsert", "projections")]
    rows = _written(db, "projections")
    assert {r["game_id"] for r in rows} == {2}
    v2 = [r for r in rows if r["model_version"] == RL.V2][0]
    assert v2["edge_now_pct_total"] == round(abs(152 - 141) / 141, 4)
    assert v2["edges_as_of"] == now.isoformat(timespec="seconds")


# ── Supabase URL ────────────────────────────────────────────────────────────────
@pytest.mark.parametrize("url", ["https://abc.supabase.co", "https://abc.supabase.co/", "https://abc.supabase.co/rest/v1",
                                 "https://abc.supabase.co/rest/v1/", " https://abc.supabase.co//rest/v1// "])
def test_supabase_url_is_normalized(url, monkeypatch):
    monkeypatch.delenv("SUPABASE_REST_URL", raising=False)
    assert sw.rest_base(url) == "https://abc.supabase.co/rest/v1"
    assert sw.Client(url=url, key="k").base == "https://abc.supabase.co/rest/v1"
