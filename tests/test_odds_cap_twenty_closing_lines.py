"""Odds API cap 20/day (daily 2, pretip 10, refresh 6, manual 2), ONE claim gate for every paid call (F1 from #86),
and the pre-tip closing-line snapshot. No network: every request is a fake."""
import importlib
import json
import subprocess
import sys
import types
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest

import daily_run as dr
import pretip_check as pc
from czarp import odds_budget as OB
from tests.odds_fakes import claimed_fetch

ROOT = Path(__file__).resolve().parents[1]
TODAY = "2026-11-21"
NOW = datetime(2026, 11, 21, 18, 40, tzinfo=dr.CENTRAL)
MIGRATION = ROOT / "db/migrations/2026-11-25_odds_api_cap_20_closing_lines.sql"


@pytest.fixture
def usage(monkeypatch, tmp_path):
    u = tmp_path / "odds_api_usage.json"
    u.write_text(json.dumps({"calls": {}, "remaining": 5000, "created_on": "2026-11-01"}))
    monkeypatch.setattr(dr, "USAGE", str(u))
    monkeypatch.setattr(dr, "now_ct", lambda: NOW)
    monkeypatch.setattr(dr, "log", lambda *a, **k: None)
    return u


class _Resp:
    def __init__(self, status=200, body=None, headers=None):
        self.status_code, self._body = status, body if body is not None else []
        self.headers = headers or {"x-requests-remaining": "4990"}

    def json(self):
        return self._body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


@pytest.fixture
def real_of(monkeypatch, usage):
    """The real odds_fetcher with a fake key and a fake requests.get that records every request."""
    monkeypatch.setenv("ODDS_API_KEY", "sekret123")
    sys.modules.pop("odds_fetcher", None)
    of = importlib.import_module("odds_fetcher")
    monkeypatch.setattr(of, "ODDS_API_KEY", "sekret123")
    sent = []

    def get(url, params=None, headers=None, timeout=None):
        sent.append(dict(url=url, params=dict(params or {}), headers=dict(headers or {})))
        return _Resp()
    monkeypatch.setattr(of.requests, "get", get)
    yield of, sent
    sys.modules.pop("odds_fetcher", None)


# ── budget numbers ──────────────────────────────────────────────────────────────
def test_twenty_a_day_split_and_monthly_credits():
    S = dr.ODDS_API_JOB_SLOTS
    assert dr.ODDS_API_MAX_CALLS_PER_DAY == 20 == sum(S.values())
    assert S == {"daily": 2, "pretip": 10, "refresh": 6, "manual": 2}
    import refresh_lines as RL
    assert RL.REFRESH_PAID_CALLS_PER_DAY == S["refresh"]
    # 20 calls x 3 credits x 31 days = 1,860 credits: under 10% of the 20,000-credit plan
    assert 20 * RL.ODDS_CREDITS_PER_CALL * 31 == 1860 < 20_000


def test_total_cap_twenty_holds_across_all_jobs(usage):
    got = [dr.odds_api_claim(j)[0] for j in ["daily", "pretip", "refresh", "manual"] * 12]
    assert sum(got) == 20
    u = json.loads(usage.read_text())
    assert u["calls"][TODAY] == 20 and u["calls_by_job"][TODAY] == dr.ODDS_API_JOB_SLOTS
    assert dr.odds_api_claim("pretip") == (False, "daily budget used (20/20 across all jobs)")


# ── the single gate: every paid call claims first ──────────────────────────────
def test_fetch_vegas_lines_claims_manual_slot_and_refuses_without_request(real_of, usage):
    of, sent = real_of
    assert of.fetch_vegas_lines().empty and of.last_refusal is None        # default job = manual
    assert of.fetch_vegas_lines(job="manual").empty
    assert len(sent) == 2
    assert of.fetch_vegas_lines().empty                                     # manual slot (2) used: refused
    assert len(sent) == 2 and of.last_refusal.startswith("manual slot used (2/2")
    with pytest.raises(OB.OddsApiRefused):
        of.fetch_vegas_lines(raise_refusal=True)
    u = json.loads(usage.read_text())
    assert u["calls_by_job"][TODAY] == {"manual": 2} and u["remaining"] == "4990"   # credits recorded
    for s in sent:                                                          # key only in the header
        assert s["headers"] == {"x-api-key": "sekret123"} and "apiKey" not in s["params"]
        assert "sekret123" not in s["url"]


def test_counter_problem_fails_closed_for_every_caller(real_of, usage):
    of, sent = real_of
    usage.write_text("not json")
    assert of.fetch_vegas_lines().empty and "fail closed" in of.last_refusal
    with pytest.raises(OB.OddsApiRefused):
        of.odds_api_get(of.BASE_URL, {"regions": "us"})
    assert sent == []


def test_odds_api_get_strips_query_key_and_only_talks_to_the_odds_api(real_of):
    of, sent = real_of
    r = of.odds_api_get(of.BASE_URL, {"apiKey": "x", "APIKEY": "y", "regions": "us"}, claim=lambda: (True, ""))
    assert r.status_code == 200
    assert sent[-1]["params"] == {"regions": "us"} and sent[-1]["headers"] == {"x-api-key": "sekret123"}
    with pytest.raises(ValueError):
        of.odds_api_get("https://example.com/v4", claim=lambda: (True, ""))
    with pytest.raises(OB.OddsApiRefused):
        of.odds_api_get(of.BASE_URL, claim=lambda: (False, "nope"))
    assert len(sent) == 1


def test_odds_api_get_errors_never_leak_the_key(real_of, monkeypatch):
    of, _ = real_of

    def boom(url, params=None, headers=None, timeout=None):
        raise ConnectionError(f"pool: {url}?apiKey=sekret123")
    monkeypatch.setattr(of.requests, "get", boom)
    with pytest.raises(RuntimeError) as e:
        of.odds_api_get(of.BASE_URL, claim=lambda: (True, ""))
    assert "sekret123" not in str(e.value)


def test_team_mapper_and_results_tracker_go_through_the_gate(real_of, usage, monkeypatch):
    of, sent = real_of
    import team_mapper as TM
    import results_tracker as RT
    monkeypatch.setattr(TM, "_get_secret", lambda k: "sekret123")
    monkeypatch.setattr(RT, "_get_odds_key", lambda: "sekret123")
    monkeypatch.setattr(RT, "_build_odds_to_kenpom", lambda: {})
    assert TM.fetch_odds_team_names() == []          # 1st manual call (empty board)
    assert RT.fetch_final_scores(TODAY) == []         # 2nd manual call
    assert len(sent) == 2
    assert TM.fetch_odds_team_names() == []           # refused: no request
    with pytest.raises(OB.OddsApiRefused):
        RT.fetch_final_scores(TODAY)
    assert len(sent) == 2
    assert all(s["headers"] == {"x-api-key": "sekret123"} and "apiKey" not in s["params"] for s in sent)
    assert json.loads(usage.read_text())["calls_by_job"][TODAY] == {"manual": 2}


def test_no_source_file_calls_the_odds_api_outside_the_gate():
    """Every tracked non-test .py file: no apiKey query parameter, and requests to the paid Odds API host only in
    odds_fetcher.py (the gate) — daily_run / refresh_lines only read the FREE /v4/sports usage endpoint."""
    files = subprocess.run(["git", "ls-files", "*.py"], cwd=ROOT, capture_output=True, text=True).stdout.split()
    if not files:
        pytest.skip("not a git checkout")
    for f in files:
        if f.startswith("tests/"):
            continue
        src = (ROOT / f).read_text(errors="ignore")
        assert '"apiKey":' not in src and "'apiKey':" not in src and "apiKey=" not in src.replace(
            "apiKey=...", "").replace("(apiKey=)", ""), f
        if "api.the-odds-api.com" in src and f not in ("odds_fetcher.py",):
            # URL constants are fine; the request must go through odds_fetcher.odds_api_get / fetch_vegas_lines
            assert "odds_api_get" in src or "ODDS_API_SPORTS_URL" in src, f


# ── refresh: the Supabase claim is the gate, through fetch_vegas_lines ───────────
def test_refresh_claim_runs_inside_fetch_vegas_lines(usage, monkeypatch):
    import refresh_lines as RL
    calls, claims = [], []
    fake = types.ModuleType("odds_fetcher")
    fake.ODDS_API_KEY = "test-key"
    fake._odds_credits_remaining = 4000
    fake._best_score = lambda a, b: 1.0 if a == b else 0.0
    fake.fetch_vegas_lines = claimed_fetch(fake, lambda: calls.append(1) or pd.DataFrame())
    monkeypatch.setitem(sys.modules, "odds_fetcher", fake)
    pre = [dict(id=1, external_id="1", home="A", away="B")]
    RL.fresh_lines(pre, {}, lines_for=lambda e: {}, today=TODAY, log=lambda m: None,
                   claim=lambda: (claims.append(1) or len(claims) <= 1, "cap"))
    RL.fresh_lines(pre, {}, lines_for=lambda e: {}, today=TODAY, log=lambda m: None,
                   claim=lambda: (claims.append(1) or len(claims) <= 1, "cap"))
    assert len(claims) == 2 and len(calls) == 1
    assert json.loads(usage.read_text())["calls"] == {}          # refresh never touches the box counter


# ── pre-tip closing-line snapshot ───────────────────────────────────────────────
def _ledger(rows):
    cols = ["pick_id", "game_date", "event_id", "tip_ct", "status", "tier", "home", "away", "market", "side", "line"]
    return pd.DataFrame([dict(zip(cols, r)) for r in rows], columns=cols)


LED = _ledger([
    ("1-spread", TODAY, "1", "2026-11-21 19:00", "open", "official", "H1", "A1", "spread", "H1", -3.5),
    ("1-total", TODAY, "1", "2026-11-21 19:00", "open", "official", "H1", "A1", "total", "Under", 140.5),
    ("2-spread-lean", TODAY, "2", "2026-11-21 19:05", "open", "lean", "H2", "A2", "spread", "A2", 4.5),
    ("3-total", TODAY, "3", "2026-11-21 19:00", "open", "track", "H3", "A3", "total", "Over", 150.5),
    ("4-total", TODAY, "4", "2026-11-21 21:00", "open", "official", "H4", "A4", "total", "Over", 150.5),
    ("5-total", TODAY, "5", "2026-11-21 18:30", "open", "official", "H5", "A5", "total", "Over", 150.5),
    ("6-total", TODAY, "6", "2026-11-21 19:00", "void", "official", "H6", "A6", "total", "Over", 150.5),
])


def _utc(h, m=0):
    return pd.Timestamp(datetime(2026, 11, 21, h, m, tzinfo=dr.CENTRAL)).tz_convert("UTC").isoformat()


class FakeClient:
    def __init__(self, closing=(), table_missing=False):
        self.games = [dict(id=10 + i, external_id=str(i), home=f"H{i}", away=f"A{i}", status="scheduled",
                           tipoff=_utc(*t), date=TODAY)
                      for i, t in {1: (19, 0), 2: (19, 5), 3: (19, 0), 4: (21, 0), 5: (18, 30), 6: (19, 0)}.items()]
        self.closing, self.table_missing, self.inserts = list(closing), table_missing, []

    def get_all(self, table, order, **params):
        rows = self.games if table == "games" else self.closing if table == pc.CLOSE_TABLE else []
        if table == pc.CLOSE_TABLE and self.table_missing:
            raise RuntimeError("closing_line_snapshots: HTTP 404")
        for k, v in params.items():
            if k == "select":
                continue
            op, val = v.split(".", 1)
            if op == "eq":
                rows = [r for r in rows if str(r.get(k)) == val]
            elif op == "in":
                vals = set(val.strip("()").split(","))
                rows = [r for r in rows if str(r.get(k)) in vals]
        return [dict(r) for r in rows]

    def insert(self, table, rows):
        assert table == pc.CLOSE_TABLE
        self.inserts.append(list(rows))
        self.closing.extend(rows)


ESPN = {"1": dict(book="DraftKings", current_home_spread=-4.0, current_total=139.5, current_home_spread_price=-110,
                  current_away_spread_price=-110, current_over_price=-112, current_under_price=-108),
        "2": dict(book="DraftKings", current_home_spread=-5.0, current_total=None)}


@pytest.fixture
def board(monkeypatch, usage):
    calls = []
    fake = types.ModuleType("odds_fetcher")
    fake.ODDS_API_KEY = "test-key"
    fake._odds_credits_remaining = 4000
    fake._best_score = lambda a, b: 1.0 if a == b else 0.0
    rows = [dict(vegas_home=f"H{i}", vegas_away=f"A{i}", vegas_spread=-4.5 - i, vegas_total=141.5 + i,
                 home_spread_price=-108, away_spread_price=-112, over_price=-110, under_price=-110,
                 source_book="fanduel") for i in (1, 2, 3, 4)]
    fake.fetch_vegas_lines = claimed_fetch(fake, lambda: calls.append(1) or pd.DataFrame(rows))
    monkeypatch.setitem(sys.modules, "odds_fetcher", fake)
    monkeypatch.setattr(pc, "_snapshot", lambda today: {})
    return calls


def test_closing_targets_official_and_lean_within_window_only():
    t = pc.closing_targets(LED, TODAY, NOW, 30)
    assert set(t) == {"1", "2"}                       # 3 = track tier, 4 = 2h20 away, 5 = tipped, 6 = void
    assert sorted(t["1"]["pick_ids"]) == ["1-spread", "1-total"] and t["2"]["pick_ids"] == ["2-spread-lean"]
    assert set(pc.closing_targets(LED, TODAY, NOW, 150)) == {"1", "2", "4"}
    assert pc.closing_targets(LED.iloc[0:0], TODAY, NOW) == {}


def test_closing_snapshot_writes_espn_and_one_odds_api_call(board, usage):
    db = FakeClient()
    n = pc.closing_line_snapshot(TODAY, NOW, LED, allow_api=True, client=db, lines=lambda e: ESPN.get(e, {}),
                                 cache={})
    rows = db.inserts[0]
    assert n == len(rows) == 4 and len(board) == 1                       # ONE paid call for both games
    by = {(r["event_id"], r["source"]): r for r in rows}
    assert set(by) == {("1", "espn"), ("2", "espn"), ("1", "oddsapi"), ("2", "oddsapi")}
    e1, o1 = by[("1", "espn")], by[("1", "oddsapi")]
    assert (e1["home_spread"], e1["total"], e1["book"], e1["over_price"]) == (-4.0, 139.5, "DraftKings", -112)
    assert (o1["home_spread"], o1["total"], o1["book"]) == (-5.5, 142.5, "fanduel")
    assert e1["pick_ids"] == ["1-spread", "1-total"] and by[("2", "oddsapi")]["pick_ids"] == ["2-spread-lean"]
    assert e1["game_id"] == 11 and e1["minutes_to_tip"] == 20.0 and e1["captured_at"] == NOW.isoformat()
    assert by[("2", "espn")]["total"] is None and by[("2", "espn")]["home_spread"] == -5.0
    assert json.loads(usage.read_text())["calls_by_job"][TODAY] == {"pretip": 1}
    assert all("pick_hash" not in r and "result" not in r for r in rows)       # never pick fields


def test_closing_snapshot_second_run_is_espn_only_unless_board_is_free(board):
    db = FakeClient()
    lines = lambda e: ESPN.get(e, {})
    pc.closing_line_snapshot(TODAY, NOW, LED, allow_api=True, client=db, lines=lines, cache={})
    later = NOW.replace(minute=50)
    pc.closing_line_snapshot(TODAY, later, LED, allow_api=True, client=db, lines=lines, cache={})
    assert len(board) == 1 and {r["source"] for r in db.inserts[1]} == {"espn"}
    # the same run already holds the board (e.g. the missing-line fill paid for it): Odds API rows are free
    cache = {}
    dr.odds_api_board("pretip", cache=cache)
    assert len(board) == 2
    pc.closing_line_snapshot(TODAY, later, LED, allow_api=True, client=db, lines=lines, cache=cache)
    assert len(board) == 2 and {r["source"] for r in db.inserts[2]} == {"espn", "oddsapi"}


def test_closing_snapshot_refused_claim_and_no_api_mean_espn_only(board, usage):
    usage.write_text(json.dumps({"calls": {TODAY: 20}, "remaining": 5000}))     # day's cap used
    db = FakeClient()
    pc.closing_line_snapshot(TODAY, NOW, LED, allow_api=True, client=db, lines=lambda e: ESPN.get(e, {}), cache={})
    assert board == [] and {r["source"] for r in db.inserts[0]} == {"espn"}
    db = FakeClient()
    pc.closing_line_snapshot(TODAY, NOW, LED, allow_api=False, client=db, lines=lambda e: ESPN.get(e, {}))
    assert board == [] and {r["source"] for r in db.inserts[0]} == {"espn"}


def test_closing_snapshot_skips_when_table_missing_or_no_targets(board):
    db = FakeClient(table_missing=True)
    assert pc.closing_line_snapshot(TODAY, NOW, LED, allow_api=True, client=db, lines=lambda e: ESPN.get(e, {})) == 0
    assert db.inserts == [] and board == []
    db = FakeClient()
    early = NOW.replace(hour=12)
    assert pc.closing_line_snapshot(TODAY, early, LED, allow_api=True, client=db, lines=lambda e: {}) == 0
    assert db.inserts == [] and board == []


def test_pretip_run_shares_one_paid_call_between_fill_snapshot_and_checks(board, usage, monkeypatch):
    """main(): missing-line fill, closing snapshot and the play checks all use one 'pretip' call."""
    seen = {}

    def fill(today, now, led, allow_api, log, cache):
        dr.odds_api_board("pretip", cache=cache)                 # the fill pays for the board
        seen["fill"] = cache
        return {}

    def snap(today, now, led, allow_api, log, cache, window):
        seen["snap"] = cache
        dr.odds_api_board("pretip", cache=cache)
        return 0
    monkeypatch.setattr(pc, "fill_missing_lines", fill)
    monkeypatch.setattr(pc, "closing_line_snapshot", snap)
    monkeypatch.setattr(pc, "big_moves_soon", lambda *a, **k: [])
    monkeypatch.setattr(dr, "read_ledger", lambda: LED.assign(model_value=0.0))
    monkeypatch.setattr(dr, "espn_dk_lines", lambda e: {})
    monkeypatch.setattr(pc, "write_flags", lambda notes, wait=None: 0)
    assert pc.main(["--date", TODAY]) == 0
    assert seen["fill"] is seen["snap"] and len(board) == 1
    assert json.loads(usage.read_text())["calls_by_job"][TODAY] == {"pretip": 1}


def test_pretip_dry_run_never_snapshots(board, monkeypatch):
    monkeypatch.setattr(pc, "closing_line_snapshot", lambda *a, **k: pytest.fail("dry run must not snapshot"))
    monkeypatch.setattr(pc, "big_moves_soon", lambda *a, **k: [])
    monkeypatch.setattr(dr, "read_ledger", lambda: LED.assign(model_value=0.0))
    monkeypatch.setattr(dr, "espn_dk_lines", lambda e: {"current_home_spread": -3.5, "current_total": 140.5})
    assert pc.main(["--date", TODAY, "--dry-run"]) == 0 and board == []


# ── migration ───────────────────────────────────────────────────────────────────
def test_migration_tightens_check_and_adds_locked_down_closing_table():
    sql = MIGRATION.read_text().lower()
    code = "\n".join(l.split("--")[0] for l in sql.splitlines()).strip()
    assert code.startswith("begin;") and code.endswith("commit;")
    assert "add constraint odds_api_refresh_claims_n_range check (n between 1 and 6)" in code
    assert "drop constraint" in code and "contype = 'c'" in code
    assert "create table if not exists closing_line_snapshots" in code
    assert "alter table closing_line_snapshots enable row level security" in code
    assert "revoke all on closing_line_snapshots from anon, authenticated" in code
    assert "grant " not in code and "create policy" not in code
    assert "check (source in ('espn', 'oddsapi'))" in code
    assert "unique (game_id, source, captured_at)" in code
    for forbidden in ("picks_immutable", "pick_hash", "alter table picks", "update picks", "delete from picks",
                      "drop trigger", "create or replace view"):
        assert forbidden not in code, forbidden
