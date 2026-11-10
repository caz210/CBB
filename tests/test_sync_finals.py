from datetime import date, datetime

from czarp.sync_finals import (
    DEFAULT_LOOKBACK, date_window, game_status, main, should_apply, sync_finals, today_ct,
)
import czarp.sync_finals as sf
import daily_run as dr


class FakeClient:
    def __init__(self, games=None):
        self.existing = games or []
        self.calls = {}

    def get(self, table, **params):
        assert table == "games"
        want = None
        if "date" in params:
            # eq.YYYY-MM-DD
            want = params["date"].split(".", 1)[-1]
        return [g for g in self.existing if want is None or g["date"] == want]

    def upsert(self, table, rows, on_conflict=None, returning=True, ignore_duplicates=False):
        self.calls.setdefault(table, []).extend(rows)
        return []

    def patch(self, table, row, **params):
        rec = dict(row)
        # recover identity filters so tests can key by external_id
        for k, v in params.items():
            if isinstance(v, str) and v.startswith("eq."):
                rec.setdefault(k, v[3:])
        self.calls.setdefault(table, []).append(rec)


def _espn(**kw):
    g = dict(event_id="10", game_date="2026-11-08", state="post", completed=True,
             home_score=80.0, away_score=70.0, status_name="STATUS_FINAL")
    g.update(kw)
    return g


def test_date_window_and_today_ct():
    assert date_window(date(2026, 11, 9), 7)[0] == date(2026, 11, 2)
    assert date_window(date(2026, 11, 9), 7)[-1] == date(2026, 11, 9)
    assert len(date_window(date(2026, 11, 9), 7)) == 8
    assert date_window(date(2026, 11, 9), 0) == [date(2026, 11, 9)]
    now = datetime(2026, 11, 10, 0, 30, tzinfo=dr.CENTRAL)  # still Nov 9? 00:30 CT Nov 10 is Nov 10
    assert today_ct(now) == date(2026, 11, 10)
    # 11:30 PM CT Nov 9
    assert today_ct(datetime(2026, 11, 9, 23, 30, tzinfo=dr.CENTRAL)) == date(2026, 11, 9)
    assert DEFAULT_LOOKBACK == 7


def test_game_status_and_no_downgrade():
    assert game_status(_espn()) == "final"
    assert game_status(_espn(completed=False, state="in", status_name="STATUS_IN_PROGRESS")) == "in_progress"
    assert game_status(_espn(completed=False, state="post", status_name="STATUS_POSTPONED")) == "postponed"
    assert game_status(_espn(completed=False, state="post", status_name="STATUS_CANCELED")) == "canceled"
    assert game_status(_espn(completed=False, state="post", status_name=None)) == "postponed"
    assert game_status(_espn(completed=False, state="pre", status_name="STATUS_SCHEDULED",
                             home_score=None, away_score=None)) == "scheduled"
    cur = {"status": "final", "home_score": 80, "away_score": 70}
    assert should_apply(cur, "scheduled", None, None) is False
    assert should_apply(cur, "final", 80, 70) is False
    assert should_apply(cur, "final", 81, 70) is True  # score correction
    assert should_apply({"status": "scheduled", "home_score": None, "away_score": None},
                        "final", 80, 70) is True


def test_sync_updates_finals_skips_unknown_and_does_not_downgrade():
    existing = [
        dict(external_id="10", date="2026-11-08", status="scheduled", home_score=None, away_score=None, league="ncaam"),
        dict(external_id="11", date="2026-11-08", status="final", home_score=99, away_score=88, league="ncaam"),
        dict(external_id="12", date="2026-11-08", status="scheduled", home_score=None, away_score=None, league="ncaam"),
    ]
    espn = [
        _espn(event_id="10"),
        _espn(event_id="11", home_score=1, away_score=2, completed=False, state="pre",
              status_name="STATUS_SCHEDULED"),  # would downgrade a final — skipped
        _espn(event_id="99"),                  # not in db
        _espn(event_id="12", completed=False, state="in", status_name="STATUS_IN_PROGRESS",
              home_score=40, away_score=38),
    ]
    c = FakeClient(existing)
    s = sync_finals(lookback=0, day=date(2026, 11, 8), client=c, log=lambda *_: None,
                    scoreboard=lambda d: espn)
    rows = {r["external_id"]: r for r in c.calls["games"]}
    assert s["updated"] == 2 and s["matched"] == 3 and s["unmatched_espn"] == 1
    assert rows["10"]["status"] == "final" and rows["10"]["home_score"] == 80 and rows["10"]["away_score"] == 70
    assert "11" not in rows
    assert rows["12"]["status"] == "in_progress" and rows["12"]["home_score"] == 40


def test_sync_leaves_final_without_scores_and_marks_postponed():
    existing = [
        dict(external_id="1", date="2026-11-07", status="scheduled", home_score=None, away_score=None, league="ncaam"),
        dict(external_id="2", date="2026-11-07", status="scheduled", home_score=None, away_score=None, league="ncaam"),
    ]
    espn = [
        _espn(event_id="1", game_date="2026-11-07", completed=True, home_score=None, away_score=None),
        _espn(event_id="2", game_date="2026-11-07", completed=False, state="post",
              status_name="STATUS_POSTPONED", home_score=None, away_score=None),
    ]
    c = FakeClient(existing)
    s = sync_finals(lookback=0, day=date(2026, 11, 7), client=c, log=lambda *_: None,
                    scoreboard=lambda d: espn)
    rows = {r["external_id"]: r for r in c.calls.get("games", [])}
    assert "1" not in rows and s["unchanged"] == 1
    assert rows["2"]["status"] == "postponed" and rows["2"]["home_score"] is None


def test_sync_lookback_fetches_each_day():
    seen = []

    def sb(d):
        seen.append(d)
        return []

    c = FakeClient([])
    sync_finals(lookback=7, day=date(2026, 11, 9), client=c, log=lambda *_: None, scoreboard=sb)
    assert seen[0] == date(2026, 11, 2) and seen[-1] == date(2026, 11, 9) and len(seen) == 8


def test_cli_parses_date_and_lookback(monkeypatch):
    calls = []
    monkeypatch.setattr(sf, "sync_finals", lambda **kw: calls.append(kw) or {"updated": 0})
    assert main(["--date", "2026-11-08"]) == 0
    assert calls[0]["day"] == date(2026, 11, 8) and calls[0]["lookback"] == 0
    calls.clear()
    assert main(["--date", "2026-11-09", "--lookback", "7"]) == 0
    assert calls[0]["day"] == date(2026, 11, 9) and calls[0]["lookback"] == 7
    assert main(["--date", "not-a-date"]) == 2
