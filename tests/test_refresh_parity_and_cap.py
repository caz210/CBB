"""Scantron #65 blockers: F1 edge parity with daily_run (full precision, change-only, per market) and F2 the
Odds API hard cap (server-side claims, API usage read, slot dedupe, --force capped, worst case 5/day)."""
import json
import sys
import types
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest

import daily_run as dr
import refresh_lines as RL
from czarp import supabase_writer as sw
from tests.test_refresh_lines import FakeDB, TODAY, ct, _scoreboard, _seed, QUIET

ROOT = Path(__file__).resolve().parents[1]


# ── F1: parity with daily_run ───────────────────────────────────────────────────
class SyncClient:
    """Captures what supabase_writer.sync (the 7 AM write) sends."""

    def __init__(self):
        self.calls = {}

    def get(self, table, **params):
        return [{"id": 1}] if table == "sports" else []

    def upsert(self, table, rows, on_conflict=None, returning=True, ignore_duplicates=False):
        self.calls.setdefault(table, []).extend(dict(r) for r in rows)
        if table == "teams":
            return [{"name": r["name"], "id": i} for i, r in enumerate(rows)]
        if table == "games":
            return [{"external_id": r["external_id"], "id": 100 + i} for i, r in enumerate(rows)]
        return []

    def insert(self, table, rows):
        self.upsert(table, rows)

    def patch(self, *a, **k):
        pass


TIP = datetime(2026, 11, 22, 2, 0, tzinfo=timezone.utc)        # 8 PM CT
PRICES = dict(current_home_spread_price=-110, current_away_spread_price=-110, current_over_price=-108,
              current_under_price=-112)


def _game715(**kw):
    # full-precision margin 9.46 -> stored spread -9.5. vs open -3.5 / 150: full 5.96/150 = 3.97% (below the 4%
    # bar) while the rounded 9.5 would give 6.0/150 = 4.00% (at the bar): the boundary case
    g = dict(event_id="5", game_date=TODAY, kp_home="H", kp_away="A", neutral=False, state="pre", tip_utc=TIP,
             v2_home_margin=9.46, v2_total=146.04, v2_home_win_prob=0.77, v1_total=151.96, v1_home_margin=8.83,
             open_home_spread=-3.5, open_total=150.0, current_home_spread=-3.5, current_total=150.0,
             open_home_spread_price=-110, open_away_spread_price=-110, open_over_price=-110, open_under_price=-110,
             **PRICES)
    g.update(kw)
    return g


def _daily(g, now):
    g = dict(g)
    dr.attach_skip_reasons([g], now, dr.BANKROLL0)
    return g


def _publish_715(monkeypatch):
    """Run the real supabase_writer.sync on a daily_run-computed game and load what it wrote into a FakeDB."""
    monkeypatch.setattr(dr, "coach_locks", lambda g, market=None: [])
    monkeypatch.setattr(dr, "coach_tag", lambda g, m: "")
    now = datetime(2026, 11, 21, 7, 15, tzinfo=dr.CENTRAL)
    g = _daily(_game715(), now)
    c = SyncClient()
    sw.sync([g], pd.DataFrame(columns=dr.LEDGER_COLS), None, now, log=QUIET, client=c)
    games = [dict(id=100, external_id="5", home="H", away="A", tipoff=TIP.isoformat(), status="scheduled",
                  date=TODAY)]
    lines = [dict(r, id=i + 1) for i, r in enumerate(c.calls["market_lines"])]
    projs = [dict(r, id=500 + i) for i, r in enumerate(c.calls["projections"])]
    return g, FakeDB(games, projs, lines), games


def _espn(**kw):
    out = {"current_home_spread": -3.5, "current_total": 150.0, "book": "DraftKings", **PRICES}
    out.update(kw)
    return lambda eid: out


def _refresh(db, games, lines_for, now):
    return RL.run(now, db, scoreboard=_scoreboard(games), lines_for=lines_for, allow_api=False, log=QUIET,
                  force=True)


def _proj_written(db, version):
    rows = [r for op, t, rs in db.writes if t == "projections" for r in rs if r["model_version"] == version]
    return rows


def test_sync_writes_full_precision_columns(monkeypatch):
    g, db, _ = _publish_715(monkeypatch)
    v2 = next(r for r in db.tables["projections"] if r["model_version"] == RL.V2)
    v1 = next(r for r in db.tables["projections"] if r["model_version"] == RL.V1)
    assert v2["spread"] == -9.5 and v2["home_margin_full"] == 9.46 and v2["total_full"] == 146.04
    assert v1["total"] == 152.0 and v1["total_full"] == 151.96 and v1["home_margin_full"] == 8.83


def test_unchanged_line_writes_nothing(monkeypatch):
    g, db, games = _publish_715(monkeypatch)
    out = _refresh(db, games, _espn(), ct(10))
    assert out["lines_written"] == 0 and out["edges_written"] == 0 and out["edges_unchanged"] == 1
    assert {t for _, t, rows in db.writes if rows} == {RL.RUNS_TABLE}


def test_changed_spread_matches_daily_run_and_total_is_copied(monkeypatch):
    g715, db, games = _publish_715(monkeypatch)
    base2 = next(r for r in db.tables["projections"] if r["model_version"] == RL.V2)
    base1 = next(r for r in db.tables["projections"] if r["model_version"] == RL.V1)
    now = ct(13)
    _refresh(db, games, _espn(current_home_spread=-4.5), now)
    exp = _daily(_game715(current_home_spread=-4.5), now)          # daily_run, identical inputs, full precision
    v2, = _proj_written(db, RL.V2)
    v1, = _proj_written(db, RL.V1)
    for k in ("edge_now_pct_spread", "edge_vs_open_pct_spread"):
        assert v2[k] == v1[k] == round(exp[k], 4)
    assert v2["skip_reasons_spread"] == exp["skip_reasons_spread"]
    assert v2["skip_reason_spread"] == exp["skip_reason_spread"]
    assert "below_open_threshold" in v2["skip_reasons_spread"]     # 3.97% < 4%: full precision decides
    rounded = _daily(_game715(current_home_spread=-4.5, v2_home_margin=9.5), now)
    assert "below_open_threshold" not in rounded["skip_reasons_spread"]   # rounded input would have flipped it
    assert v2["edge_now_pct_spread"] != round(rounded["edge_now_pct_spread"], 4)
    # the total market did not move: copied exactly from the 7:15 rows
    for k in RL.MARKET_FIELDS["total"]:
        assert v2[k] == base2[k]
    for k in ("edge_now_pct_total", "edge_vs_open_pct_total", "skip_reasons_total", "skip_reason_total", "over_prob"):
        assert v1[k] == base1[k]
    # projections themselves unchanged, full-precision columns carried forward
    for k in ("spread", "total", "proj_home", "proj_away", "home_margin_full", "total_full", "home_win_prob"):
        assert v2[k] == base2[k]
    # and a second run with the same (moved) line writes nothing
    n = len(db.writes)
    _refresh(db, games, _espn(current_home_spread=-4.5), now + timedelta(minutes=1))
    assert not [rows for _, t, rows in db.writes[n:] if t != RL.RUNS_TABLE and rows]


def test_changed_total_matches_daily_run_and_spread_is_copied(monkeypatch):
    g715, db, games = _publish_715(monkeypatch)
    base2 = next(r for r in db.tables["projections"] if r["model_version"] == RL.V2)
    now = ct(16)
    _refresh(db, games, _espn(current_total=147.5), now)
    exp = _daily(_game715(current_total=147.5), now)
    v2, = _proj_written(db, RL.V2)
    v1, = _proj_written(db, RL.V1)
    for k in ("edge_now_pct_total", "edge_vs_open_pct_total"):
        assert v2[k] == v1[k] == round(exp[k], 4)
    assert v2["skip_reasons_total"] == v1["skip_reasons_total"] == exp["skip_reasons_total"]
    # a total move changes the spread edge's denominator, so the spread market is re-stamped too (as daily_run)
    assert v2["edge_now_pct_spread"] == round(exp["edge_now_pct_spread"], 4)
    assert v2["skip_reasons_spread"] == exp["skip_reasons_spread"]
    assert v2["edge_vs_open_pct_spread"] == base2["edge_vs_open_pct_spread"]


def test_price_only_change_restamps_only_that_market(monkeypatch):
    g715, db, games = _publish_715(monkeypatch)
    base2 = next(r for r in db.tables["projections"] if r["model_version"] == RL.V2)
    _refresh(db, games, _espn(current_over_price=-125), ct(10))
    v2, = _proj_written(db, RL.V2)
    exp = _daily(_game715(current_over_price=-125), ct(10))
    assert v2["skip_reasons_total"] == exp["skip_reasons_total"]
    for k in RL.MARKET_FIELDS["spread"]:
        assert v2[k] == base2[k]


def test_legacy_rows_without_full_columns_fall_back_and_say_so(monkeypatch):
    g715, db, games = _publish_715(monkeypatch)
    for r in db.tables["projections"]:
        r.pop("home_margin_full"); r.pop("total_full")
    logs = []
    RL.run(ct(10), db, scoreboard=_scoreboard(games), lines_for=_espn(current_home_spread=-4.5), allow_api=False,
           log=logs.append, force=True)
    assert any("pre-migration rounded" in m for m in logs)


def test_upsert_projections_retries_without_full_columns_when_migration_missing():
    class C:
        def __init__(self):
            self.sent = []

        def upsert(self, table, rows, on_conflict=None, returning=True, ignore_duplicates=False):
            self.sent.append([dict(r) for r in rows])
            if any("home_margin_full" in r for r in rows):
                raise RuntimeError('Supabase upsert projections 400: {"code":"PGRST204","message":"Could not '
                                   'find the \'home_margin_full\' column of \'projections\'"}')
            return []
    c, logs = C(), []
    sw.upsert_projections(c, [{"game_id": 1, "home_margin_full": 1.0, "total_full": 2.0, "spread": -1.0}],
                          log=logs.append)
    assert len(c.sent) == 2 and c.sent[1] == [{"game_id": 1, "spread": -1.0}] and logs

    class D(C):
        def upsert(self, *a, **k):
            raise RuntimeError("Supabase upsert projections 500: boom")
    with pytest.raises(RuntimeError):
        sw.upsert_projections(D(), [{"game_id": 1}])


def test_reads_before_writes_and_preflight_before_any_write():
    games, projs, lines = _seed()
    db = FakeDB(games, projs, lines)
    RL.run(ct(10), db, scoreboard=_scoreboard(games), allow_api=False, log=QUIET,
           lines_for=lambda e: {"current_home_spread": -2.0, "current_total": 141.0})
    first_line_write = db.events.index(("write", "market_lines"))
    assert ("get", "picks") in db.events[:first_line_write]
    assert db.events.index(("get", "projections")) < db.events.index(("write", RL.RUNS_TABLE))   # preflight

    class Missing(FakeDB):
        def get(self, table, **params):
            if table == "projections" and "home_margin_full" in params.get("select", ""):
                raise RuntimeError("Supabase GET projections 400: PGRST204 home_margin_full")
            return super().get(table, **params)
    db = Missing(games, projs, lines)
    with pytest.raises(RuntimeError):
        RL.run(ct(10), db, scoreboard=_scoreboard(games), allow_api=False, log=QUIET, lines_for=lambda e: {})
    assert db.writes == []


def test_get_all_pages_past_1000_rows(monkeypatch):
    c = sw.Client(url="https://abc.supabase.co", key="k")
    seen = []

    def get(table, **params):
        seen.append(params)
        off, lim = int(params["offset"]), int(params["limit"])
        return [{"id": i} for i in range(off, min(off + lim, 2345))]
    monkeypatch.setattr(c, "get", get)
    rows = c.get_all("projections", order="created_at.desc,id.desc", game_id="in.(1)")
    assert len(rows) == 2345 and [p["offset"] for p in seen] == ["0", "1000", "2000"]
    assert all(p["order"] == "created_at.desc,id.desc" and p["game_id"] == "in.(1)" for p in seen)


def test_pick_quiet_docstring_matches_constant():
    assert RL.PICK_QUIET_MIN == 10
    assert "PICK_QUIET_MIN (10)" in RL.recompute_edges.__doc__
    assert "PICK_QUIET_MIN = 10" in RL.__doc__ and "5-minute" not in RL.recompute_edges.__doc__


# ── F2: hard cap ────────────────────────────────────────────────────────────────
class FakeAPI:
    """The Odds API account: /v4/sports is free and reports usage; each odds call costs 3 credits."""

    def __init__(self, used=1000.0, remaining=19000.0):
        self.used, self.remaining, self.paid, self.sports_reads, self.fail_reads = used, remaining, 0, [], False

    def get(self, url, headers=None, timeout=None):
        self.sports_reads.append((url, dict(headers or {})))
        if self.fail_reads:
            raise ConnectionError("down")

        class R:
            status_code = 200
        R.headers = {"x-requests-used": str(self.used), "x-requests-remaining": str(self.remaining)}
        return R()

    def odds(self):
        self.paid += 1
        self.used += 3
        self.remaining -= 3
        return pd.DataFrame(columns=["vegas_home", "vegas_away", "vegas_spread", "vegas_total", "source_book"])


@pytest.fixture
def api(monkeypatch, tmp_path):
    a = FakeAPI()
    fake = types.ModuleType("odds_fetcher")
    fake.ODDS_API_KEY = "test-key"
    fake._odds_credits_remaining = None
    fake._best_score = lambda x, y: 0.0
    fake.fetch_vegas_lines = a.odds
    fake.redact_key = lambda t: t.replace("test-key", "***")
    monkeypatch.setitem(sys.modules, "odds_fetcher", fake)
    monkeypatch.setattr(dr._S, "get", a.get)
    monkeypatch.setattr(dr, "log", QUIET)
    # the box's local counter (daily + pretip), restored from an OLD day (stale cache)
    u = tmp_path / "odds_api_usage.json"
    u.write_text(json.dumps({"calls": {"2026-11-19": 6}, "remaining": 19000, "created_on": "2026-11-01"}))
    monkeypatch.setattr(dr, "USAGE", str(u))
    a.usage_file = u
    return a


def _late_games():
    games, projs, lines = _seed()
    for g in games:
        g["tipoff"] = "2026-11-22T04:00:00+00:00"              # 10 PM CT: not started at any slot
    return games, projs, lines


def _paid_run(db, games, now, api, force=False):
    return RL.run(now, db, scoreboard=_scoreboard(games), lines_for=lambda e: {}, allow_api=True, log=QUIET,
                  force=force, odds_key="test-key", usage_get=api.get)


def test_usage_read_is_free_and_keyed_by_header(api):
    assert RL.read_odds_usage("test-key", api.get) == {"used": 1000.0, "remaining": 19000.0}
    url, headers = api.sports_reads[-1]
    assert url == dr.ODDS_API_SPORTS_URL and "test-key" not in url and headers == {"x-api-key": "test-key"}
    assert api.paid == 0
    api.fail_reads = True
    assert RL.read_odds_usage("test-key", api.get) is None
    assert RL.read_odds_usage(None, api.get) is None


def test_eight_runs_stale_cache_and_failed_saves_never_exceed_five_paid_calls(api, monkeypatch):
    games, projs, lines = _late_games()
    db = FakeDB(games, projs, lines)
    stale = api.usage_file.read_text()
    # box jobs on the local file: daily (7 AM) and pretip, one each
    monkeypatch.setattr(dr, "now_ct", lambda: ct(7, 10))
    assert dr.odds_api_fallback([dict(current_home_spread=None, current_total=None)], TODAY, True, job="daily") == 0
    runs = [(ct(10), False), (ct(10, 50), False),          # 10 AM slot fires twice (late duplicate)
            (ct(11, 30), True), (ct(13), False), (ct(14, 30), True), (ct(16), False), (ct(19), False),
            (ct(19, 20), True)]                            # 8 refresh runs incl. 3 x --force
    for now, force in runs:
        api.usage_file.write_text(stale)                   # every run: cache restored from an old day, save lost
        _paid_run(db, games, now, api, force=force)
    monkeypatch.setattr(dr, "now_ct", lambda: ct(18))
    dr.odds_api_fallback([dict(current_home_spread=None, current_total=None)], TODAY, True, job="pretip")
    assert api.paid <= dr.ODDS_API_MAX_CALLS_PER_DAY == 5
    assert len(db.tables[RL.CLAIMS_TABLE]) == 3 == RL.REFRESH_PAID_CALLS_PER_DAY
    assert api.paid == 5                                    # daily 1 + refresh 3 + pretip 1
    slots = sorted(r["slot"] for r in db.tables[RL.RUNS_TABLE])
    assert [s for s in slots if s < RL.FORCED_SLOT_BASE] == [10, 13, 16, 19]   # duplicate 10 AM firing skipped
    # a 9th forced run the same day is refused by the code/database cap
    _paid_run(db, games, ct(19, 40), api, force=True)
    assert api.paid == 5


def test_force_still_respects_cap_from_the_database(api):
    games, projs, lines = _late_games()
    claims = [dict(day=TODAY, n=n, slot=10) for n in range(1, RL.REFRESH_PAID_CALLS_PER_DAY + 1)]
    db = FakeDB(games, projs, lines, claims=claims, runs=[dict(day=TODAY, slot=10, requests_used=1000.0)])
    _paid_run(db, games, ct(11, 30), api, force=True)
    assert api.paid == 0 and len(db.tables[RL.CLAIMS_TABLE]) == RL.REFRESH_PAID_CALLS_PER_DAY


def test_new_day_starts_fresh_from_the_database_not_the_cache(api):
    games, projs, lines = _late_games()
    old = [dict(day="2026-11-20", n=n, slot=10) for n in range(1, 5)]
    db = FakeDB(games, projs, lines, claims=old)
    _paid_run(db, games, ct(10), api)
    assert api.paid == 1 and {r["day"] for r in db.tables[RL.CLAIMS_TABLE] if r not in old} == {TODAY}


def test_usage_read_failure_means_espn_only(api):
    games, projs, lines = _late_games()
    db = FakeDB(games, projs, lines)
    api.fail_reads = True
    out = _paid_run(db, games, ct(10), api)
    assert out["ran"] and api.paid == 0 and db.tables[RL.CLAIMS_TABLE] == []


def test_low_credits_and_api_side_call_count_refuse(api):
    games, projs, lines = _late_games()
    api.remaining = 50
    db = FakeDB(games, projs, lines)
    _paid_run(db, games, ct(10), api)
    assert api.paid == 0
    # baseline 1000 at today's first run; the API now shows 15 credits (5 calls) used since: refuse
    api.remaining, api.used = 19000, 1015
    db = FakeDB(games, projs, lines, runs=[dict(day=TODAY, slot=10, requests_used=1000.0)])
    _paid_run(db, games, ct(13), api)
    assert api.paid == 0 and db.tables[RL.CLAIMS_TABLE] == []


def test_claim_errors_refuse(api):
    games, projs, lines = _late_games()

    class Broken(FakeDB):
        def upsert(self, table, rows, **k):
            if table == RL.CLAIMS_TABLE:
                raise RuntimeError("Supabase 503")
            return super().upsert(table, rows, **k)
    db = Broken(games, projs, lines)
    _paid_run(db, games, ct(10), api)
    assert api.paid == 0


def test_dry_run_never_reads_usage_or_claims(api):
    games, projs, lines = _late_games()
    db = FakeDB(games, projs, lines)
    RL.run(ct(10), db, scoreboard=_scoreboard(games), lines_for=lambda e: {}, allow_api=True, log=QUIET,
           dry_run=True, odds_key="test-key", usage_get=api.get)
    assert api.paid == 0 and api.sports_reads == [] and db.writes == []


def test_budget_constants_add_up_to_five():
    assert dr.ODDS_API_MAX_CALLS_PER_DAY == 5
    assert dr.ODDS_API_JOB_SLOTS == {"daily": 1, "pretip": 1, "refresh": 3}
    assert RL.REFRESH_PAID_CALLS_PER_DAY == 3
    assert sum(dr.ODDS_API_JOB_SLOTS.values()) == dr.ODDS_API_MAX_CALLS_PER_DAY
    # historical #65 migration CHECK still allows n=1..4; the live cap is the Python loop (n=1..3)
    sql = (ROOT / "db/migrations/2026-11-21_intraday_line_refresh.sql").read_text().lower()
    assert "check (n between 1 and 4)" in sql


# ── migration ───────────────────────────────────────────────────────────────────
def test_intraday_migration_is_one_transaction_with_locked_down_tables():
    sql = (ROOT / "db/migrations/2026-11-21_intraday_line_refresh.sql").read_text().lower()
    code = "\n".join(l.split("--")[0] for l in sql.splitlines()).strip()
    assert code.startswith("begin;") and code.endswith("commit;")
    for col in ("edges_as_of timestamptz", "home_margin_full double precision", "total_full double precision"):
        assert f"add column if not exists {col}" in code
    for t in (RL.CLAIMS_TABLE, RL.RUNS_TABLE):
        assert f"create table if not exists {t}" in code
        assert f"alter table {t} enable row level security" in code
        assert f"revoke all on {t} from anon, authenticated" in code
        assert f"grant select on {t}" not in code
    assert "primary key (day, n)" in code and "primary key (day, slot)" in code
    assert "picks " not in code.replace("official_picks_locked", "")


# ── N7: the key never reaches logs ─────────────────────────────────────────────
def test_odds_fetcher_sends_key_in_header_and_redacts_errors(monkeypatch, capsys):
    monkeypatch.setenv("ODDS_API_KEY", "sekret123")
    sys.modules.pop("odds_fetcher", None)
    import importlib
    of = importlib.import_module("odds_fetcher")
    monkeypatch.setattr(of, "ODDS_API_KEY", "sekret123")
    assert of.redact_key("GET https://x/v4?apiKey=sekret123&regions=us failed: sekret123") == \
        "GET https://x/v4?apiKey=***&regions=us failed: ***"
    seen = {}

    def get(url, params=None, headers=None, timeout=None):
        seen.update(url=url, params=params, headers=headers)
        raise ConnectionError(f"HTTPSConnectionPool: url={url}?apiKey=sekret123")
    monkeypatch.setattr(of.requests, "get", get)
    assert of.fetch_vegas_lines().empty
    out = capsys.readouterr().out
    assert "sekret123" not in out and "apiKey=***" in out
    assert "apiKey" not in (seen["params"] or {}) and seen["headers"] == {"x-api-key": "sekret123"}

    class R:
        status_code = 500
    monkeypatch.setattr(of.requests, "get", lambda *a, **k: R())
    with pytest.raises(RuntimeError) as e:
        of.fetch_vegas_lines()
    assert str(e.value) == "Odds API HTTP 500"
