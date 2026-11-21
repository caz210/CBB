"""Odds API daily cap shared across jobs (7 AM run + pre-tip) and pre-tip filling of missing market_lines."""
import json
import multiprocessing as mp
import os
import subprocess
import sys
import types
from datetime import datetime, timedelta

import pandas as pd
import pytest

import daily_run as dr
import pretip_check as pc
from czarp import supabase_writer as sw

TODAY = "2026-11-21"


CLOCK = {"now": datetime(2026, 11, 21, 10, 0, tzinfo=dr.CENTRAL)}


@pytest.fixture
def odds(monkeypatch, tmp_path):
    """Fake odds_fetcher + a temp usage file + a fixed CT clock; returns (calls list, usage path, board rows)."""
    calls, board = [], []
    CLOCK["now"] = datetime(2026, 11, 21, 10, 0, tzinfo=dr.CENTRAL)
    monkeypatch.setattr(dr, "now_ct", lambda: CLOCK["now"])
    fake = types.ModuleType("odds_fetcher")
    fake.ODDS_API_KEY = "test-key"
    fake._odds_credits_remaining = 400
    fake._best_score = lambda a, b: 1.0 if a == b else 0.0

    def fetch():
        calls.append(1)
        print("    Vegas lines fetched (fake) | Credits remaining: 400")      # the real one prints to stdout
        return pd.DataFrame(board, columns=["vegas_home", "vegas_away", "vegas_spread", "vegas_total",
                                            "home_spread_price", "away_spread_price", "over_price",
                                            "under_price", "source_book"])
    fake.fetch_vegas_lines = fetch
    monkeypatch.setitem(sys.modules, "odds_fetcher", fake)
    usage = tmp_path / "odds_api_usage.json"
    usage.write_text(json.dumps({"calls": {}, "remaining": 500}))
    monkeypatch.setattr(dr, "USAGE", str(usage))
    monkeypatch.setattr(dr, "log", lambda *a, **k: None)
    return calls, usage, board


def _missing():
    return [dict(home_full="H", away_full="A", current_home_spread=None, current_total=None)]


def test_cap_two_per_day_daily_cannot_take_pretip_slot_third_refused(odds):
    calls, usage, _ = odds
    assert dr.ODDS_API_MAX_CALLS_PER_DAY == 6 and dr.ODDS_API_JOB_SLOTS == {"daily": 1, "pretip": 1, "refresh": 4}
    dr.odds_api_fallback(_missing(), TODAY, allow=True)                     # 7 AM run: its 1 slot
    dr.odds_api_fallback(_missing(), TODAY, allow=True)                     # 7 AM re-run: refused (pre-tip slot kept)
    assert len(calls) == 1
    dr.odds_api_fallback(_missing(), TODAY, allow=True, job="pretip")       # pre-tip: the reserved slot
    assert len(calls) == 2
    dr.odds_api_fallback(_missing(), TODAY, allow=True, job="pretip")       # third call: refused
    dr.odds_api_fallback(_missing(), TODAY, allow=True)
    assert len(calls) == 2
    u = json.loads(usage.read_text())
    assert u["calls"][TODAY] == 2 and u["calls_by_job"][TODAY] == {"daily": 1, "pretip": 1}
    assert dr.odds_api_claim("pretip") == (False, "pretip slot used (1/1; the other slot is reserved)")
    # a new CT day (real clock) starts fresh
    CLOCK["now"] += timedelta(days=1)
    dr.odds_api_fallback(_missing(), "2026-11-22", allow=True, job="pretip")
    assert len(calls) == 3
    # --no-odds-api still wins
    dr.odds_api_fallback(_missing(), "2026-11-22", allow=False)
    assert len(calls) == 3


def test_total_cap_holds_even_if_slots_were_larger(odds, monkeypatch):
    calls, _, _ = odds
    monkeypatch.setattr(dr, "ODDS_API_MAX_CALLS_PER_DAY", 2)
    monkeypatch.setattr(dr, "ODDS_API_JOB_SLOTS", {"daily": 5, "pretip": 5})
    for job in ("daily", "pretip", "daily", "pretip"):
        dr.odds_api_fallback(_missing(), TODAY, allow=True, job=job)
    assert len(calls) == 2


def test_legacy_single_counter_counts_as_the_7am_call(odds):
    calls, usage, _ = odds
    usage.write_text(json.dumps({"calls": {TODAY: 1}, "remaining": 500}))     # written by the old code at 7 AM
    dr.odds_api_fallback(_missing(), TODAY, allow=True)
    assert len(calls) == 0
    dr.odds_api_fallback(_missing(), TODAY, allow=True, job="pretip")
    assert len(calls) == 1 and json.loads(usage.read_text())["calls"][TODAY] == 2


def test_reserve_respected_and_nothing_claimed(odds):
    calls, usage, _ = odds
    usage.write_text(json.dumps({"calls": {}, "remaining": dr.ODDS_API_RESERVE - 1}))
    for job in ("daily", "pretip"):
        dr.odds_api_fallback(_missing(), TODAY, allow=True, job=job)
    assert calls == [] and json.loads(usage.read_text())["calls"] == {}


def test_no_call_when_nothing_missing(odds):
    calls, usage, _ = odds
    full = [dict(home_full="H", away_full="A", current_home_spread=-3.5, current_total=140.5)]
    assert dr.odds_api_fallback(full, TODAY, allow=True, job="pretip") == 0
    assert calls == [] and json.loads(usage.read_text())["calls"] == {}


def _claim(args):
    usage, job = args
    dr.USAGE = usage
    return dr.odds_api_claim(job)[0]


def test_cap_holds_across_processes(odds):
    _, usage, _ = odds
    ctx = mp.get_context("fork")
    with ctx.Pool(8) as pool:
        got = pool.map(_claim, [(str(usage), j) for j in ["daily", "pretip"] * 8])
    assert sum(got) == 2
    u = json.loads(usage.read_text())
    assert u["calls"][TODAY] == 2 and u["calls_by_job"][TODAY] == {"daily": 1, "pretip": 1}


# ── fail closed: missing / truncated / malformed counter ─────────────────────────
@pytest.mark.parametrize("content", [
    '{"calls": {"2026-11-21": 2}, "rema',                                   # truncated mid-write
    "",                                                                     # empty file
    "not json at all",
    "[]",                                                                   # wrong top-level type
    '{"remaining": 500}',                                                   # no calls
    '{"calls": {"2026-11-21": "2"}, "remaining": 500}',                     # count not an int
    '{"calls": {"tomorrow": 1}}',                                           # bad day key
    '{"calls": {"2026-11-21": -1}}',                                        # negative
    '{"calls": {"2026-11-21": 1}, "calls_by_job": {"2026-11-21": {"daily": 1, "pretip": 1}}}',  # jobs > total
    '{"calls": {}, "remaining": "lots"}',                                   # remaining not numeric
    '{"calls": {}, "remaining": NaN}',                                      # NaN credits
    '{"calls": {}, "remaining": Infinity}',                                 # infinite credits
    '{"calls": {}, "remaining": "-inf"}',
    '{"calls": {}, "remaining": "nan"}',
])
def test_corrupt_counter_refuses_and_is_left_unchanged(odds, content):
    calls, usage, _ = odds
    usage.write_text(content)
    for job in ("daily", "pretip"):
        dr.odds_api_fallback(_missing(), TODAY, allow=True, job=job)
        ok, why = dr.odds_api_claim(job)
        assert not ok and "fail closed" in why
    assert calls == [] and usage.read_text() == content
    assert sorted(os.listdir(usage.parent)) == ["odds_api_usage.json", "odds_api_usage.json.lock"]   # no tmp/new file


def test_deleted_counter_refuses_and_is_not_recreated(odds):
    calls, usage, _ = odds
    usage.unlink()
    for job in ("daily", "pretip"):
        dr.odds_api_fallback(_missing(), TODAY, allow=True, job=job)
    assert calls == [] and not usage.exists()


def test_missing_ledger_dir_refuses_without_creating_it(odds, monkeypatch, tmp_path):
    calls, _, _ = odds
    gone = tmp_path / "nope" / "ledger" / "odds_api_usage.json"
    monkeypatch.setattr(dr, "USAGE", str(gone))
    ok, why = dr.odds_api_claim("daily")
    assert not ok and "fail closed" in why and not (tmp_path / "nope").exists()


def test_counter_corrupted_after_two_calls_still_refuses(odds):
    """Scantron's repro: 2 calls today, then the file is truncated -> no 3rd/4th call, file not rewritten."""
    calls, usage, _ = odds
    dr.odds_api_fallback(_missing(), TODAY, allow=True)
    dr.odds_api_fallback(_missing(), TODAY, allow=True, job="pretip")
    assert len(calls) == 2
    usage.write_text(usage.read_text()[:20])
    snap = usage.read_text()
    dr.odds_api_fallback(_missing(), TODAY, allow=True)
    dr.odds_api_fallback(_missing(), TODAY, allow=True, job="pretip")
    assert len(calls) == 2 and usage.read_text() == snap


def test_init_creates_only_when_absent(tmp_path):
    from czarp import odds_usage as ou
    p = tmp_path / "ledger" / "odds_api_usage.json"
    assert ou.main(["--check", "--path", str(p)]) == 1
    assert ou.main(["--init", "--path", str(p)]) == 0 and ou.load(str(p)) == {"calls": {}, "remaining": None}
    legacy = '{"calls": {"2026-11-08": 1}, "remaining": "19982", "last_call_ct": "2026-11-08T07:20:46-06:00"}'
    p.write_text(legacy)                                         # production's current (pre-PR) format
    assert ou.main(["--init", "--path", str(p)]) == 0 and p.read_text() == legacy    # never overwritten
    assert ou.main(["--check", "--path", str(p)]) == 0
    p.write_text("{broken")
    assert ou.main(["--init", "--path", str(p)]) == 1 and p.read_text() == "{broken"  # init won't "repair" it


def test_legacy_production_format_works_without_migration(odds):
    calls, usage, _ = odds
    usage.write_text('{"calls": {"2026-11-08": 1}, "remaining": "19982", "last_call_ct": "2026-11-08T07:20:46-06:00"}')
    dr.odds_api_fallback(_missing(), TODAY, allow=True)
    u = json.loads(usage.read_text())
    assert len(calls) == 1 and u["calls"] == {"2026-11-08": 1, TODAY: 1}
    assert u["calls_by_job"] == {TODAY: {"daily": 1}} and u["remaining"] == 400


# ── I/O errors on the counter: refuse + log, never crash ────────────────────────
def _assert_refused_untouched(calls, usage, before, n_calls=0):
    assert len(calls) == n_calls and usage.read_text() == before
    assert not [f for f in os.listdir(usage.parent) if ".tmp" in f]        # no stray temp files


@pytest.mark.parametrize("where", ["flock", "open_tmp", "fsync", "replace"])
def test_os_error_on_lock_write_or_replace_refuses_without_crashing(odds, monkeypatch, where):
    import errno
    import fcntl
    import builtins
    from czarp import odds_usage as ou
    calls, usage, _ = odds
    before = usage.read_text()
    boom = OSError(errno.ENOSPC, "No space left on device")

    def raise_(*a, **k):
        raise boom
    if where == "flock":
        monkeypatch.setattr(fcntl, "flock", raise_)
    elif where == "open_tmp":
        real_open = builtins.open
        monkeypatch.setattr(builtins, "open", lambda f, m="r", *a, **k: raise_() if ".tmp" in str(f)
                            else real_open(f, m, *a, **k))
    elif where == "fsync":
        monkeypatch.setattr(ou.os, "fsync", raise_)
    else:
        monkeypatch.setattr(ou.os, "replace", raise_)
    n = len(dr.ODDS_API_COUNTER_ISSUES)
    for job in ("daily", "pretip"):
        assert dr.odds_api_fallback(_missing(), TODAY, allow=True, job=job) == 0     # no exception
    _assert_refused_untouched(calls, usage, before)
    assert len(dr.ODDS_API_COUNTER_ISSUES) == n + 2 and "I/O error" in dr.ODDS_API_COUNTER_ISSUES[-1]


def test_read_only_ledger_folder_refuses_without_crashing(odds):
    if hasattr(os, "geteuid") and os.geteuid() == 0:
        pytest.skip("root ignores directory permissions")
    calls, usage, _ = odds
    before = usage.read_text()
    (usage.parent / "odds_api_usage.json.lock").touch()       # lock file present; temp file can't be created
    os.chmod(usage.parent, 0o555)
    try:
        for job in ("daily", "pretip"):
            assert dr.odds_api_fallback(_missing(), TODAY, allow=True, job=job) == 0
        _assert_refused_untouched(calls, usage, before)
    finally:
        os.chmod(usage.parent, 0o755)
    # read-only folder WITHOUT a lock file: the lock can't be created either -> refused
    os.unlink(usage.parent / "odds_api_usage.json.lock")
    os.chmod(usage.parent, 0o555)
    try:
        ok, why = dr.odds_api_claim("daily")
        assert not ok and "fail closed" in why and usage.read_text() == before
    finally:
        os.chmod(usage.parent, 0o755)


def test_record_os_error_after_call_is_logged_not_raised(odds, monkeypatch):
    from czarp import odds_usage as ou
    calls, usage, board = odds
    board.append(dict(vegas_home="H", vegas_away="A", vegas_spread=-3.5, vegas_total=140.5, home_spread_price=-110,
                      away_spread_price=-110, over_price=-110, under_price=-110, source_book="draftkings"))
    real = ou.os.replace
    n = {"i": 0}

    def flaky(a, b):
        n["i"] += 1
        if n["i"] >= 2:                                   # the claim's write works; the credits write fails
            raise OSError(30, "Read-only file system")
        return real(a, b)
    monkeypatch.setattr(ou.os, "replace", flaky)
    logged = []
    monkeypatch.setattr(dr, "log", lambda m: logged.append(m))
    g = _missing()
    assert dr.odds_api_fallback(g, TODAY, allow=True) == 1 and g[0]["current_total"] == 140.5
    assert len(calls) == 1 and json.loads(usage.read_text())["calls"][TODAY] == 1     # the claim was kept
    assert any("could not record credits" in m for m in logged)


def test_pretip_logs_counter_refusal_without_verbose(odds, capsys):
    calls, usage, _ = odds
    usage.write_text("{broken")
    pc._run_odds_api(_missing(), TODAY, log=lambda m: None)          # non-verbose logger
    cap = capsys.readouterr()
    assert calls == [] and cap.out == "" and "Odds API call refused (fail closed)" in cap.err


def test_daily_run_still_logs_plays_when_counter_write_fails(odds, monkeypatch, tmp_path):
    """7 AM run with an unwritable counter: the Odds API call is refused, plays are still appended + written,
    and the refusal shows up in the report notes."""
    from czarp import odds_usage as ou
    calls, usage, _ = odds
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(ou.os, "replace", lambda *a: (_ for _ in ()).throw(PermissionError(13, "Permission denied")))
    seen = {}
    play = dict(pick_id="9-total", event_id="9", market="total", tier="official", side="Over")
    monkeypatch.setattr(dr, "OUT_DIR", str(tmp_path))
    monkeypatch.setattr(dr, "SNAP_DIR", str(tmp_path / "snap"))
    monkeypatch.setattr(dr, "read_ledger", lambda: pd.DataFrame(columns=dr.LEDGER_COLS))
    monkeypatch.setattr(dr, "seal", lambda led: led)
    monkeypatch.setattr(dr, "verify_ledger", lambda led: [])
    monkeypatch.setattr(dr, "grade", lambda led, now: [])
    monkeypatch.setattr(dr, "espn_scoreboard", lambda d: [dict(event_id="9", game_date=TODAY, state="pre",
                                                                home_full="H", away_full="A", kp_home="H", kp_away="A")])
    monkeypatch.setattr(dr, "espn_dk_lines", lambda e: {})          # ESPN has no line -> Odds API wanted
    monkeypatch.setattr(dr, "load_kenpom", lambda today, refresh: {"config": None})
    monkeypatch.setattr(dr, "fanmatch_preds", lambda d: {})
    monkeypatch.setattr(dr, "project_all", lambda games, data: None)
    monkeypatch.setattr(dr, "matchup_parity", lambda *a, **k: None)
    monkeypatch.setattr(dr, "tag_new_coaches", lambda games, today: {})
    monkeypatch.setattr(dr, "coach_locks", lambda g, market=None: [])
    monkeypatch.setattr(dr, "PACE_FIX", False)
    monkeypatch.setattr(dr, "flag_plays", lambda games, br, board, now: [dict(play)])
    monkeypatch.setattr(dr, "candidates", lambda games, now: [])
    monkeypatch.setattr(dr, "attach_skip_reasons", lambda *a: None)
    monkeypatch.setattr(dr, "info_extras_on", lambda: False)
    monkeypatch.setattr(dr, "append_picks", lambda led, rows, now: seen.setdefault("appended", rows) and led)
    monkeypatch.setattr(dr, "write_ledger", lambda led: seen.setdefault("written", True))
    monkeypatch.setattr(dr, "write_summary", lambda today, now, games, new_rows, ledger, gi, notes, top, **k:
                        seen.setdefault("notes", list(notes)) and str(tmp_path / "s.md"))
    assert dr.main(["--date", TODAY, "--no-kalshi", "--no-refresh"]) == 0
    assert calls == []                                                # refused: no Odds API call
    assert [r["pick_id"] for r in seen["appended"]] == ["9-total"] and seen["written"]
    assert any("Odds API call refused (fail closed)" in n and "I/O error" in n for n in seen["notes"])
    assert json.loads(usage.read_text())["calls"] == {}                # counter untouched


# ── budget day = real CT date, never --date ─────────────────────────────────────
def test_slate_date_does_not_change_the_budget_day(odds):
    calls, usage, _ = odds
    for slate in ("2026-11-22", "2026-11-20", "2026-12-25", TODAY):            # --date backfills / future slates
        dr.odds_api_fallback(_missing(), slate, allow=True)
        dr.odds_api_fallback(_missing(), slate, allow=True, job="pretip")
    u = json.loads(usage.read_text())
    assert len(calls) == 2 and u["calls"] == {TODAY: 2} and list(u["calls_by_job"]) == [TODAY]


def test_caller_slate_date_is_ignored_for_budgeting(odds, monkeypatch):
    """daily_run.main / pretip_check.main pass `today = a.date or now` as the slate date; the counter still keys
    on the real CT date."""
    calls, usage, _ = odds
    seen = []
    real = dr.odds_api_fallback

    def spy(games, today, allow, job="daily"):
        seen.append(today)
        return real(_missing(), today, allow, job)
    monkeypatch.setattr(dr, "odds_api_fallback", spy)
    dr.odds_api_fallback([], "2026-11-23", True)            # direct, with a slate date != today
    assert seen == ["2026-11-23"] and json.loads(usage.read_text())["calls"] == {TODAY: 1}


# ── path anchored to the checkout, not the cwd ──────────────────────────────────
def test_usage_path_is_anchored_to_repo_not_cwd(tmp_path):
    from czarp import odds_usage as ou
    repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    assert ou.USAGE_PATH == os.path.join(repo, "data", "ledger", "odds_api_usage.json")
    assert os.path.isabs(dr.USAGE) and dr.USAGE == ou.USAGE_PATH
    code = "import daily_run as dr; print(dr.USAGE)"
    out = subprocess.run([sys.executable, "-c", code], cwd=tmp_path, capture_output=True, text=True,
                         env={**os.environ, "PYTHONPATH": repo}, check=True).stdout.strip().splitlines()[-1]
    assert out == ou.USAGE_PATH                                  # same file from any working directory


# ── pre-tip: fill missing market_lines ─────────────────────────────────────────
class FakeClient:
    def __init__(self, games, lines):
        self.games, self.lines, self.writes = games, lines, []

    def get(self, table, **params):
        if table == "games":
            return list(self.games)
        if table == "market_lines":
            return [dict(r) for r in self.lines]
        if table in ("projections", "picks"):     # edge recompute after a fill: no stored projections here
            return []
        raise AssertionError(f"unexpected read of {table}")

    def insert(self, table, rows):
        self.writes.append(("insert", table, rows))

    def upsert(self, *a, **k):
        self.writes.append(("upsert", a[0] if a else None, None))

    def patch(self, *a, **k):
        self.writes.append(("patch", a[0] if a else None, None))

    def delete(self, *a, **k):
        self.writes.append(("delete", a[0] if a else None, None))


NOW = datetime(2026, 11, 21, 15, 0, tzinfo=dr.CENTRAL)       # 3 PM CT = 21:00 UTC


def _games():
    g = lambda i, eid, tip, h, a: dict(id=i, external_id=eid, home=h, away=a, tipoff=tip, status="scheduled")
    return [g(1, "101", "2026-11-22T01:00:00+00:00", "Full Home", "Full Away"),       # has both lines
            g(2, "102", "2026-11-22T00:00:00+00:00", "Mercer", "Charleston Southern"),  # spread only
            g(3, "103", "2026-11-22T02:00:00+00:00", "Austin Peay", "UTRGV"),        # no Vegas row at all
            g(4, "104", "2026-11-21T20:00:00+00:00", "Started", "Already"),          # tipped: ignored
            g(5, "105", "2026-11-21T23:00:00+00:00", "Soon Home", "Soon Away")]      # no row, tips soonest


def _lines():
    return [dict(game_id=1, source="vegas", line_type="current", book="DraftKings", spread=-4.5, total=140.5,
                 prices={"over_price": -110}, fetched_at="2026-11-21T13:13:00+00:00"),
            dict(game_id=2, source="vegas", line_type="current", book="DraftKings", spread=-8.5, total=None,
                 prices={"home_spread_price": -110}, fetched_at="2026-11-21T13:13:00+00:00"),
            dict(game_id=2, source="vegas", line_type="current", book="DraftKings", spread=-7.0, total=None,
                 prices={}, fetched_at="2026-11-20T13:13:00+00:00")]                # older row: ignored


def _led(official_eids):
    rows = [dict(pick_id=f"{e}-total", game_date=TODAY, event_id=e, status="open", tier="official",
                 market="total") for e in official_eids]
    return pd.DataFrame(rows, columns=dr.LEDGER_COLS)


def test_pretip_fills_only_missing_lines_and_keeps_existing(odds, monkeypatch, capsys):
    calls, usage, board = odds
    board.append(dict(vegas_home="Austin Peay", vegas_away="UTRGV", vegas_spread=-4.5, vegas_total=143.5,
                      home_spread_price=-110, away_spread_price=-110, over_price=-108, under_price=-112,
                      source_book="draftkings"))
    asked = []

    def espn(eid):
        asked.append(eid)
        return {"102": {"current_total": 161.5, "current_over_price": -105, "current_under_price": -115,
                        "current_home_spread": -9.5, "book": "DraftKings"}}.get(eid, {})
    c = FakeClient(_games(), _lines())
    led = _led(["103"])
    before = led.copy()
    out = pc.fill_missing_lines(TODAY, NOW, led, allow_api=True, client=c, lines=espn)
    assert asked == ["103", "105", "102"]                 # official play first, then by tip time; never 101/104
    assert len(calls) == 1                                # one reserved pre-tip call for what ESPN lacked
    assert [w[:2] for w in c.writes] == [("insert", "market_lines")]       # insert-only, one table
    rows = {r["game_id"]: r for r in c.writes[0][2]}
    assert set(rows) == {2, 3}                            # 1 had both lines; 4 started; 5 found nowhere
    assert rows[2]["spread"] == -8.5 and rows[2]["total"] == 161.5        # existing spread kept, not -9.5
    assert rows[2]["prices"] == {"home_spread_price": -110, "over_price": -105, "under_price": -115}
    assert rows[3]["spread"] == -4.5 and rows[3]["total"] == 143.5 and rows[3]["book"] == "draftkings"
    assert all(r["source"] == "vegas" and r["line_type"] == "current" for r in rows.values())
    assert out["103"]["current_total"] == 143.5 and "105" not in out
    pd.testing.assert_frame_equal(led, before)            # ledger untouched
    assert json.loads(usage.read_text())["calls_by_job"][TODAY] == {"pretip": 1}
    assert capsys.readouterr().out == ""                  # stdout is the alert text: nothing leaks into it


def test_pretip_no_call_and_no_write_when_nothing_missing(odds):
    calls, usage, _ = odds
    full = [dict(game_id=g["id"], source="vegas", line_type="current", spread=-1.5, total=140.5,
                 fetched_at="2026-11-21T13:13:00+00:00") for g in _games()]
    c = FakeClient(_games(), full)
    assert pc.fill_missing_lines(TODAY, NOW, _led([]), allow_api=True, client=c,
                                 lines=lambda e: pytest.fail("no ESPN lookups needed")) == {}
    assert calls == [] and c.writes == [] and json.loads(usage.read_text())["calls"] == {}


def test_pretip_espn_fill_spends_no_credit_and_no_odds_api_flag(odds):
    calls, _, _ = odds
    c = FakeClient(_games(), _lines())
    full = lambda e: {"current_home_spread": -2.5, "current_total": 150.5, "book": "DraftKings"}
    pc.fill_missing_lines(TODAY, NOW, _led([]), allow_api=True, client=c, lines=full)
    assert calls == []                                    # ESPN had everything: Odds API not used
    c2 = FakeClient(_games(), _lines())
    pc.fill_missing_lines(TODAY, NOW, _led([]), allow_api=False, client=c2, lines=lambda e: {})
    assert calls == [] and c2.writes == []                # --no-odds-api and nothing on ESPN: no call, no write


def test_fill_rows_never_replace_existing_values():
    newest = sw.newest_vegas_current(_lines())
    fills = {1: {"current_home_spread": -9.0, "current_total": 150.0},          # has both: skipped
             2: {"current_home_spread": -1.0, "current_total": None}}            # only spread offered: nothing new
    assert sw.missing_line_fill_rows(newest, fills, "t") == []


def test_pretip_main_runs_fill_but_not_on_dry_run(monkeypatch, tmp_path):
    seen = []
    monkeypatch.setattr(pc, "fill_missing_lines", lambda *a, **k: seen.append(k.get("allow_api")) or {})
    monkeypatch.setattr(dr, "read_ledger", lambda: pd.DataFrame(columns=dr.LEDGER_COLS))
    monkeypatch.setattr(dr, "now_ct", lambda: NOW)
    monkeypatch.setattr(dr, "write_ledger", lambda *a, **k: pytest.fail("no ledger write"))
    assert pc.main(["--dry-run", "--big-within", "0"]) == 0 and seen == []
    assert pc.main(["--big-within", "0"]) == 0 and seen == [True]
    assert pc.main(["--no-odds-api", "--big-within", "0"]) == 0 and seen == [True, False]
