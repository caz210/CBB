"""grade_finals.py: grading between 7 AM runs (ledger copies + mocked ESPN only; no network, no Supabase)."""
import fcntl
import os
import threading
import time

import pandas as pd
import pytest
import requests

import daily_run as dr
import grade_finals as gf
import pretip_check as pc

DAY = "2026-11-22"
# The Nov 22 official plays (all totals). Six were final by ~7:15 PM CT; Utah Valley tipped at 7 and UC San Diego at 8:30.
PLAYS = [  # event_id, tip, away, home, side, line, (away, home) final or None, state
    ("401917540", "15:30", "UC Irvine", "Loyola Marymount", "Over", 138.5, (72, 68), "post"),
    ("401923323", "14:30", "Mercyhurst", "Albany", "Over", 129.5, (70, 83), "post"),
    ("401920736", "12:00", "Dayton", "Northwestern", "Over", 135.5, (60, 74), "post"),
    ("401918988", "13:00", "Evansville", "Memphis", "Under", 158.5, (64, 80), "post"),
    ("401917542", "20:30", "Seattle", "UC San Diego", "Over", 145.5, None, "pre"),
    ("401921206", "19:00", "Tennessee Martin", "Utah Valley", "Over", 146.5, (30, 35), "in"),
    ("401928999", "17:00", "Southern Miss", "Valparaiso", "Over", 145.5, (90, 98), "post"),
    ("401925066", "14:00", "Troy", "Nicholls", "Over", 136.5, (75, 80), "post"),
]
EXPECT = {"401920736": "loss", "401918988": "win", "401925066": "win", "401923323": "win",
          "401917540": "win", "401928999": "win"}
VOID_EVENT = "401999999"           # a postponed track side on the same day
NOW = pd.Timestamp(f"{DAY} 19:45", tz=dr.CENTRAL).to_pydatetime()


def _row(eid, tip, away, home, market, side, line, tier="official", **kw):
    return dict(pick_id=f"{eid}-{market}", created_ct=f"{DAY}T07:12:00-06:00", game_date=DAY, event_id=eid,
                tip_ct=f"{DAY} {tip}", away=away, home=home, neutral=False, market=market, rule="r", side=side,
                line=line, price=-110, open_line=line, model_value=line + 5, edge_pct=0.035, edge_pct_vs_open=0.035,
                stake=10.0 if tier == "official" else 0.0, bankroll_at_pick=1000.0, status="open", tier=tier,
                source="live", logged_ct=f"{DAY}T07:12:00-06:00", **kw)


@pytest.fixture
def ledger(tmp_path, monkeypatch):
    """A sealed ledger copy: one graded Nov 21 play, the eight Nov 22 plays, and a postponed track side."""
    monkeypatch.setattr(dr, "LEDGER", str(tmp_path / "ledger.csv"))
    monkeypatch.setattr(dr, "LEDGER_DIR", str(tmp_path))
    old = _row("401900001", "18:00", "A", "B", "total", "Over", 140.5)
    old.update(game_date="2026-11-21", tip_ct="2026-11-21 18:00", status="graded", result="win", pnl=9.09,
               pnl_units=0.9091, graded_ct="2026-11-22T07:17:14-06:00", close_line=141.5, clv_pts=1.0,
               away_score=70, home_score=75)
    rows = [old] + [_row(e, t, a, h, "total", s, ln) for e, t, a, h, s, ln, _, _ in PLAYS]
    rows.append(_row(VOID_EVENT, "18:00", "Postponed U", "Home State", "spread", "Postponed U", 6.5, tier="track"))
    dr.write_ledger(pd.DataFrame(rows, columns=dr.LEDGER_COLS))
    assert dr.verify_ledger(dr.read_ledger()) == []
    return dr.LEDGER


def _scoreboard(day):
    assert day.isoformat() == DAY, day                     # only the open game date is fetched
    out = []
    for e, _, a, h, _, _, sc, st in PLAYS:
        out.append({"event_id": e, "state": st, "completed": st == "post", "home": h, "away": a,
                    "away_score": None if sc is None else float(sc[0]),
                    "home_score": None if sc is None else float(sc[1])})
    out.append({"event_id": VOID_EVENT, "state": "post", "completed": False, "home": "Home State",
                "away": "Postponed U", "home_score": None, "away_score": None, "status_name": "STATUS_POSTPONED"})
    return out


class Feeds:
    """Mocked ESPN. `no_close` events have no DK close yet. Any other HTTP request fails the test."""
    def __init__(self, monkeypatch, no_close=(), check_lock=True):
        self.no_close = set(no_close)
        self.check_lock = check_lock
        self.sb_calls, self.odds_calls = 0, []
        monkeypatch.setattr(dr, "espn_scoreboard", self.scoreboard)
        monkeypatch.setattr(dr, "espn_dk_lines", self.lines)

        def no_http(*a, **k):
            raise AssertionError(f"unexpected network call {a[:3]}")
        monkeypatch.setattr(requests.Session, "request", no_http)
        monkeypatch.setattr(requests, "get", no_http)
        for name in ("odds_api_fallback", "odds_api_claim", "odds_api_seed_remaining", "load_kenpom", "fanmatch_preds",
                     "flag_plays", "append_picks"):
            monkeypatch.setattr(dr, name, no_http)
        import czarp.kalshi as kx
        monkeypatch.setattr(kx, "fetch_cbb_board", no_http)

    def scoreboard(self, day):
        if self.check_lock:
            assert_lock_free()
        self.sb_calls += 1
        return _scoreboard(day)

    def lines(self, eid):
        if self.check_lock:
            assert_lock_free()
        self.odds_calls.append(eid)
        if eid in self.no_close:
            return {}
        line = next(ln for e, *_r in PLAYS for ln in [_r[4]] if e == eid)
        return {"close_total": line + 1.0, "close_over_price": -112, "close_under_price": -108}


def lock_free() -> bool:
    """Can another process take the ledger lock right now? (a separate open file description, like another job)"""
    with open(dr.ledger_lock_path(), "a") as fh:
        try:
            fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return False
        fcntl.flock(fh, fcntl.LOCK_UN)
        return True


def assert_lock_free():
    assert lock_free(), "network call made while holding the ledger lock"


def _frozen(df):
    return [dr.pick_fingerprint(r) for r in df.to_dict("records")], list(df.pick_hash)


def test_nov22_replay_grades_the_six_finals(ledger, monkeypatch):
    f = Feeds(monkeypatch)
    before = dr.read_ledger()
    s = gf.run(NOW, supabase=False)
    assert (s["graded"], s["voided"], s["written"], s["skipped"]) == (6, 1, True, None)
    led = dr.read_ledger()
    got = {r.event_id: r.result for r in led.itertuples() if r.game_date == DAY and r.tier == "official"
           and r.status == "graded"}
    assert got == EXPECT
    pending = led[(led.tier == "official") & led.result.isna()]
    assert sorted(pending.event_id) == ["401917542", "401921206"]          # site pending drops to 2
    assert dr.verify_ledger(led) == [] and _frozen(led) == _frozen(before)   # hash chain + pick fields untouched
    dayton = led[led.event_id == "401920736"].iloc[0]                        # 134 total vs Over 135.5
    assert (dayton.away_score, dayton.home_score, dayton.pnl, dayton.pnl_units) == (60, 74, -10.0, -1.0)
    assert dayton.close_line == 136.5 and dayton.clv_pts == 1.0
    assert sorted(f.odds_calls) == sorted(EXPECT)       # DK close fetched only for the games being graded
    assert f.sb_calls == 1                              # one free ESPN scoreboard call; no Odds API / KenPom / Kalshi
    assert dr.bankroll(led) == pytest.approx(1000 + 9.09 - 10 + 5 * 9.09, abs=0.02)


def test_double_run_changes_nothing(ledger, monkeypatch):
    Feeds(monkeypatch)
    gf.run(NOW, supabase=False)
    raw, mtime = open(ledger, "rb").read(), os.stat(ledger).st_mtime_ns
    s = gf.run(NOW, supabase=False)
    assert (s["graded"], s["voided"], s["closes"], s["written"]) == (0, 0, 0, False)
    assert open(ledger, "rb").read() == raw and os.stat(ledger).st_mtime_ns == mtime
    assert dr.verify_ledger(dr.read_ledger()) == []


def test_same_numbers_as_the_7am_grading(ledger, monkeypatch, tmp_path):
    """grade_finals uses daily_run.grade itself: a copy graded the 7 AM way ends up byte-identical."""
    Feeds(monkeypatch)
    import shutil
    seven_dir = tmp_path / "seven"
    seven_dir.mkdir()
    shutil.copy(ledger, seven_dir / "ledger.csv")
    gf.run(NOW, supabase=False)
    ours = open(ledger, "rb").read()
    monkeypatch.setattr(dr, "LEDGER", str(seven_dir / "ledger.csv"))
    monkeypatch.setattr(dr, "LEDGER_DIR", str(seven_dir))
    seven = dr.seal(dr.read_ledger())                     # what daily_run.main does at 7 AM
    assert dr.grade(seven, NOW)
    dr.write_ledger(seven)
    assert open(dr.LEDGER, "rb").read() == ours


def test_void_path_for_postponed_games(ledger, monkeypatch):
    Feeds(monkeypatch)
    gf.run(NOW, supabase=False)
    v = dr.read_ledger().set_index("event_id").loc[VOID_EVENT]
    assert (v.status, v.result, v.pnl, v.pnl_units, v.tier) == ("void", "void", 0.0, 0.0, "track")


def test_missing_close_is_filled_later_null_only(ledger, monkeypatch):
    f = Feeds(monkeypatch, no_close={"401925066"})              # Troy @ Nicholls: DK close not posted yet
    gf.run(NOW, supabase=False)
    troy = dr.read_ledger().set_index("event_id").loc["401925066"]
    assert troy.result == "win" and troy.pnl == pytest.approx(9.09, abs=0.01)   # graded now, without CLV
    assert pd.isna(troy.close_line) and pd.isna(troy.clv_pts)
    f.no_close.clear()
    s = gf.run(NOW, supabase=False)
    assert (s["graded"], s["closes"], s["written"]) == (0, 1, True)
    led = dr.read_ledger()
    troy2 = led.set_index("event_id").loc["401925066"]
    assert (troy2.close_line, troy2.close_price, troy2.clv_pts) == (137.5, -112, 1.0)
    assert (troy2.result, troy2.pnl, troy2.graded_ct) == (troy.result, troy.pnl, troy.graded_ct)   # set once
    assert dr.verify_ledger(led) == []
    assert gf.run(NOW, supabase=False)["written"] is False                    # and then a no-op


def test_close_fill_window_and_existing_close_untouched(ledger, monkeypatch):
    f = Feeds(monkeypatch)
    gf.run(NOW, supabase=False)
    led = dr.read_ledger()
    later = NOW + pd.Timedelta(days=5)
    led.loc[led.event_id == "401928999", ["close_line", "close_price", "clv_pts"]] = None
    assert gf.fill_closes(led, later) == []                    # older than CLOSE_FILL_DAYS: left alone
    n = len(f.odds_calls)
    assert gf.fill_closes(dr.read_ledger(), NOW) == [] and len(f.odds_calls) == n   # nothing missing: no calls


def test_lock_busy_skips_the_cycle(ledger, monkeypatch):
    Feeds(monkeypatch)
    raw = open(ledger, "rb").read()
    with open(dr.ledger_lock_path(), "a") as fh:
        fcntl.flock(fh, fcntl.LOCK_EX)                         # e.g. the 7 AM run is mid-way
        s = gf.run(NOW, supabase=False, lock_wait=0)
    assert s["skipped"] and s["graded"] == 0 and open(ledger, "rb").read() == raw
    assert gf.run(NOW, supabase=False)["graded"] == 6          # next cycle grades


def test_broken_hash_chain_writes_nothing(ledger, monkeypatch):
    Feeds(monkeypatch)
    df = pd.read_csv(ledger, dtype=str)
    df.loc[3, "line"] = "130.5"                                 # a posted pick edited after logging
    df.to_csv(ledger, index=False)
    raw = open(ledger, "rb").read()
    with pytest.raises(dr.LedgerImmutableError, match="integrity"):
        gf.run(NOW, supabase=False)
    assert open(ledger, "rb").read() == raw
    assert gf.main(["--no-supabase"]) == 1


def test_dry_run_writes_nothing(ledger, monkeypatch):
    Feeds(monkeypatch)
    raw = open(ledger, "rb").read()
    s = gf.run(NOW, dry_run=True, client=object())
    assert s["graded"] == 6 and not s["written"] and s["patched"] == 0 and open(ledger, "rb").read() == raw


class FakePicks:
    """Supabase picks with the picks_immutable rule: a non-null grading column can never change."""
    def __init__(self, keys):
        self.rows = {k: {"pick_key": k, "result": None, "graded_at": None, "pnl": None, "units_pnl": None,
                         "closing_line": None, "clv": None} for k in keys}
        self.patches = []

    def get(self, table, select=None, league=None, pick_key=None):
        assert_lock_free()
        assert table == "picks" and league == "eq.ncaam" and pick_key.startswith("in.(")
        want = {k.strip('"') for k in pick_key[4:-1].split(",")}
        return [dict(r) for k, r in self.rows.items() if k in want]

    def patch(self, table, row, pick_key=None, **guard):
        assert_lock_free()
        assert table == "picks" and len(guard) == 1
        (col, cond), = guard.items()
        assert cond == "is.null"
        self.patches.append((pick_key, row, col))
        r = self.rows[pick_key[3:]]
        if r[col] is not None:
            return                                              # guarded PATCH matches no row
        for k, v in row.items():
            assert r[k] is None or r[k] == v, f"trigger would refuse {k}"
            r[k] = v

    def upsert(self, *a, **k):
        raise AssertionError("grade_finals never upserts")
    insert = delete = upsert


def test_supabase_mirror_patches_null_columns_only(ledger, monkeypatch):
    f = Feeds(monkeypatch, no_close={"401925066"})
    keys = [k for k in dr.read_ledger().pick_id if k != "401928999-total"]   # Valpo not mirrored yet
    c = FakePicks(keys)
    c.rows["401900001-total"].update(result="win", graded_at="x", pnl=9.09, units_pnl=0.9091,
                                     closing_line=141.5, clv=1.0)          # graded at 7 AM already
    s = gf.run(NOW, client=c)
    assert s["patched"] == len(c.patches) == 5 + 4 + 4 + 1     # 5 results + void, closing_line/clv for 4 of them
    troy = c.rows["401925066-total"]
    assert troy["result"] == "win" and troy["closing_line"] is None and troy["clv"] is None
    dayton = c.rows["401920736-total"]
    assert (dayton["result"], dayton["pnl"], dayton["units_pnl"], dayton["closing_line"], dayton["clv"]) == \
        ("loss", -10.0, -1.0, 136.5, 1.0)
    assert dayton["graded_at"] == dr.read_ledger().set_index("event_id").loc["401920736", "graded_ct"]
    assert c.rows["401999999-spread"]["result"] == "void"
    assert all(c.rows[k]["result"] is None for k in ("401917542-total", "401921206-total"))
    n = len(c.patches)
    assert gf.run(NOW, client=c)["patched"] == 0 and len(c.patches) == n    # re-run: nothing to send
    f.no_close.clear()
    gf.run(NOW, client=c)                                                     # close arrives later
    assert (c.rows["401925066-total"]["closing_line"], c.rows["401925066-total"]["clv"]) == (137.5, 1.0)


def test_supabase_failure_keeps_the_ledger_graded(ledger, monkeypatch):
    Feeds(monkeypatch)

    class Down:
        def get(self, *a, **k):
            raise RuntimeError("picks: HTTP 503")
    s = gf.run(NOW, client=Down())
    assert s["graded"] == 6 and s["written"] and s["sync_error"] == "RuntimeError"


def test_pretip_flag_write_never_overwrites_a_grade(ledger, monkeypatch):
    """pretip_check now re-reads the ledger under the lock: a play graded since its first read stays graded."""
    Feeds(monkeypatch)
    gf.run(NOW, supabase=False)
    n = pc.write_flags({"401920736-total": "x: moved", "401917542-total": "19:45: moved 2 pts against"})
    led = dr.read_ledger().set_index("pick_id")
    assert n == 1 and led.loc["401920736-total", "result"] == "loss"
    assert pd.isna(led.loc["401920736-total", "pretip_flag"])
    assert led.loc["401917542-total", "pretip_flag"] == "19:45: moved 2 pts against"
    assert dr.verify_ledger(dr.read_ledger()) == []


def test_ledger_lock_wait_times_out(tmp_path):
    with open(dr.ledger_lock_path(), "a") as fh:
        fcntl.flock(fh, fcntl.LOCK_EX)
        with pytest.raises(dr.LedgerBusy):
            with dr.ledger_lock(wait=0.2, poll=0.05):
                pass
    with dr.ledger_lock(wait=0):
        pass


def _hold_lock_in_thread(seconds):
    """Hold the ledger lock from another open file description (like another process) for `seconds`."""
    got = threading.Event()

    def hold():
        with open(dr.ledger_lock_path(), "a") as fh:
            fcntl.flock(fh, fcntl.LOCK_EX)
            got.set()
            time.sleep(seconds)
            fcntl.flock(fh, fcntl.LOCK_UN)
    t = threading.Thread(target=hold)
    t.start()
    got.wait(5)
    return t


def test_daily_run_holds_the_ledger_lock_and_grader_skips(ledger, monkeypatch):
    """The 7 AM run holds the lock from its ledger read to its write; a grading cycle during it skips."""
    Feeds(monkeypatch, check_lock=False)                     # daily (not the grader) holds the lock here
    seen = {}
    raw = open(ledger, "rb").read()

    def fake_daily(a):
        seen["held"] = not lock_free()
        t = threading.Thread(target=lambda: seen.update(grader=gf.run(NOW, supabase=False, lock_wait=0)))
        t.start()                                             # grader fires mid-run (another job)
        t.join()
        return 0
    monkeypatch.setattr(dr, "_daily", fake_daily)
    assert dr.main([]) == 0
    assert seen["held"] is True and seen["grader"]["skipped"] and seen["grader"]["graded"] == 0
    assert open(ledger, "rb").read() == raw


def test_daily_run_never_runs_unlocked(ledger, monkeypatch):
    """Scantron B1: if the lock stays busy past DAILY_LOCK_WAIT_S, daily_run reads, grades and writes NOTHING
    (exit 3) instead of running unlocked."""
    monkeypatch.setattr(dr, "DAILY_LOCK_WAIT_S", 0.3)
    called = []
    monkeypatch.setattr(dr, "_daily", lambda a: called.append(1) or 0)
    raw = open(ledger, "rb").read()
    t = _hold_lock_in_thread(1.5)
    try:
        assert dr.main([]) == 3
    finally:
        t.join()
    assert called == [] and open(ledger, "rb").read() == raw
    assert dr.main([]) == 0 and called == [1]                 # lock free again: the re-run goes through


def test_daily_run_waits_for_a_short_holder(ledger, monkeypatch):
    monkeypatch.setattr(dr, "DAILY_LOCK_WAIT_S", 10)
    seen = []
    monkeypatch.setattr(dr, "_daily", lambda a: seen.append(not lock_free()) or 0)
    t = _hold_lock_in_thread(0.5)
    try:
        assert dr.main([]) == 0
    finally:
        t.join()
    assert seen == [True]


def test_dry_run_and_chat_only_take_no_lock(monkeypatch):
    seen = []
    monkeypatch.setattr(dr, "_daily", lambda a: seen.append(lock_free()) or 0)
    assert dr.main(["--dry-run"]) == 0 and dr.main(["--chat-only"]) == 0 and seen == [True, True]


def test_slow_espn_never_holds_the_lock(ledger, monkeypatch):
    """Scantron B1: the grader fetches ESPN BEFORE locking, so a slow feed cannot block daily_run."""
    f = Feeds(monkeypatch)
    fetching = threading.Event()
    fast = f.lines

    def slow(eid):
        fetching.set()
        time.sleep(0.15)
        return fast(eid)
    monkeypatch.setattr(dr, "espn_dk_lines", slow)
    res = {}
    t = threading.Thread(target=lambda: res.update(gf.run(NOW, supabase=False)))
    t.start()
    assert fetching.wait(5)
    t0 = time.monotonic()
    with dr.ledger_lock(wait=0.5, poll=0.01):                 # e.g. daily_run starting mid-fetch: gets it at once
        took = time.monotonic() - t0
    t.join()
    assert took < 0.1 and res["graded"] == 6 and res["lock_ms"] < 2000


def test_grader_under_the_lock_rereads_and_leaves_7am_grades_alone(ledger, monkeypatch):
    """If the 7 AM run grades between the grader's prefetch and its lock, the grader changes nothing."""
    Feeds(monkeypatch)
    real = gf.Prefetched

    def prefetch_then_7am(*a, **k):
        p = real(*a, **k)
        led = dr.seal(dr.read_ledger())                        # 7 AM run grades (an hour later) and writes
        dr.grade(led, NOW + pd.Timedelta(hours=1))
        dr.write_ledger(led)
        return p
    monkeypatch.setattr(gf, "Prefetched", prefetch_then_7am)
    seven = None
    s = gf.run(NOW, supabase=False)
    seven = open(ledger, "rb").read()
    assert (s["graded"], s["voided"], s["written"]) == (0, 0, False)
    assert open(ledger, "rb").read() == seven and dr.verify_ledger(dr.read_ledger()) == []


def test_stale_writer_is_refused_set_once(ledger, monkeypatch):
    """L5A / L5B: a writer holding a copy read before the grader's write can never rewrite or reopen grades."""
    Feeds(monkeypatch)
    stale = dr.seal(dr.read_ledger())
    gf.run(NOW, supabase=False)                               # grader grades and writes
    graded = open(ledger, "rb").read()
    regraded = stale.copy()
    dr.grade(regraded, NOW + pd.Timedelta(hours=11))          # L5A: same results, new graded_ct
    with pytest.raises(dr.LedgerImmutableError, match="graded_ct is set once"):
        dr.write_ledger(regraded)
    with pytest.raises(dr.LedgerImmutableError, match="graded result would change"):
        dr.write_ledger(stale)                                # L5B: rows still open in the stale copy
    assert open(ledger, "rb").read() == graded


@pytest.mark.parametrize("field,new", [("result", "push"), ("graded_ct", "2026-11-23T07:05:00-06:00"),
                                       ("pnl", 1.0), ("pnl_units", 0.5), ("away_score", 61), ("home_score", 1),
                                       ("close_line", 140.0), ("close_price", -105), ("clv_pts", 9.0)])
def test_grading_fields_are_set_once(ledger, monkeypatch, field, new):
    Feeds(monkeypatch)
    gf.run(NOW, supabase=False)
    led = dr.read_ledger()
    i = led.index[led.event_id == "401920736"][0]
    led[field] = led[field].astype(object)
    led.at[i, field] = new
    with pytest.raises(dr.LedgerImmutableError):
        dr.write_ledger(led)
    if field == "pnl_units":
        return                                                # a blank pnl_units is re-derived by _normalize
    led2 = dr.read_ledger()
    led2[field] = led2[field].astype(object)
    led2.at[i, field] = None                                  # blanking a set value is a change too
    with pytest.raises(dr.LedgerImmutableError):
        dr.write_ledger(led2)


def test_set_once_allows_fills_and_equal_numbers(ledger, monkeypatch):
    Feeds(monkeypatch, no_close={"401925066"})
    gf.run(NOW, supabase=False)
    led = dr.read_ledger()
    i = led.index[led.event_id == "401925066"][0]
    for c in ("close_line", "close_price", "clv_pts", "away_score"):
        led[c] = led[c].astype(object)
    led.loc[i, ["close_line", "close_price", "clv_pts"]] = [137.5, -112, 1.0]   # NULL -> value: allowed
    led.at[i, "away_score"] = int(led.at[i, "away_score"])                     # 75.0 -> 75: same value
    dr.write_ledger(led)
    assert dr.read_ledger().at[i, "close_line"] == 137.5


def test_void_and_existing_ledger_rewrite_byte_identical(ledger):
    """write_ledger(read_ledger()) is a byte-identical no-op on an existing ledger (the 7 AM path with no changes)."""
    raw = open(ledger, "rb").read()
    dr.write_ledger(dr.read_ledger())
    assert open(ledger, "rb").read() == raw


def test_post_without_postponed_status_never_voids(ledger, monkeypatch):
    """Scantron N3: a void is permanent, so only ESPN's explicit postponed / canceled status voids a play."""
    Feeds(monkeypatch)
    real = _scoreboard

    def no_status(day):
        out = real(day)
        for g in out:
            if g["event_id"] == VOID_EVENT:
                g.pop("status_name")
        return out
    monkeypatch.setattr(dr, "espn_scoreboard", no_status)
    s = gf.run(NOW, supabase=False)
    assert s["voided"] == 0
    assert dr.read_ledger().set_index("event_id").loc[VOID_EVENT, "status"] == "open"
    for name in ("STATUS_POSTPONED", "STATUS_CANCELED", "STATUS_CANCELLED"):
        assert dr.is_void_game({"state": "post", "completed": False, "status_name": name})
    for g in ({"state": "post", "completed": False, "status_name": "STATUS_FINAL"},
              {"state": "post", "completed": False, "status_name": "STATUS_SUSPENDED"},
              {"state": "in", "completed": False, "status_name": "STATUS_POSTPONED"}):
        assert not dr.is_void_game(g)


def test_fetch_budget_leaves_the_rest_for_next_cycle(ledger, monkeypatch):
    Feeds(monkeypatch)
    ticks = iter(range(0, 1000, 100))                         # each clock read jumps 100 s
    p = gf.Prefetched(dr.read_ledger(), NOW, budget_s=250, log=lambda m: None, clock=lambda: next(ticks))
    assert 0 < len(p.lines) < 6
    led = dr.seal(dr.read_ledger())
    gi = dr.grade(led, NOW, scoreboard=p.scoreboard, lines=p.line)
    graded = [e for e in led.loc[gi, "event_id"] if e != VOID_EVENT]       # a void needs no close
    assert sorted(graded) == sorted(p.lines)                     # only games whose close was looked up
