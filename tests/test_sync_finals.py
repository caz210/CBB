from datetime import date, datetime, timedelta, timezone

from czarp.sync_finals import (
    DEFAULT_LOOKBACK, date_window, espn_end_utc, game_status, main,
    parse_espn_time, resolve_final_at, should_apply, sync_finals, today_ct,
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
    # already final, scores match: only a missing final_at is writable
    assert should_apply(cur, "final", 80, 70, filling_final_at=True) is True


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
    tip = datetime(2026, 11, 8, 18, 0, tzinfo=timezone.utc)
    espn[0] = _espn(event_id="10", tip_utc=tip)
    s = sync_finals(lookback=0, day=date(2026, 11, 8), client=c, log=lambda *_: None,
                    scoreboard=lambda d: espn, end_time=lambda eid: None)
    rows = {r["external_id"]: r for r in c.calls["games"]}
    assert s["updated"] == 2 and s["matched"] == 3 and s["unmatched_espn"] == 1
    assert rows["10"]["status"] == "final" and rows["10"]["home_score"] == 80 and rows["10"]["away_score"] == 70
    assert rows["10"]["final_at"] == (tip + timedelta(hours=2)).astimezone(timezone.utc).isoformat()
    assert rows["10"]["final_at_estimated"] is True
    assert "updated_at" not in rows["10"] and "updated_at" not in rows["12"]
    assert "11" not in rows
    assert rows["12"]["status"] == "in_progress" and rows["12"]["home_score"] == 40
    assert "final_at" not in rows["12"]


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
                    scoreboard=lambda d: espn, end_time=lambda eid: None)
    rows = {r["external_id"]: r for r in c.calls.get("games", [])}
    assert "1" not in rows and s["unchanged"] == 1
    assert rows["2"]["status"] == "postponed" and rows["2"]["home_score"] is None


def test_sync_lookback_fetches_each_day():
    seen = []

    def sb(d):
        seen.append(d)
        return []

    c = FakeClient([])
    sync_finals(lookback=7, day=date(2026, 11, 9), client=c, log=lambda *_: None, scoreboard=sb,
                end_time=lambda eid: None)
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


TIP = datetime(2026, 11, 8, 18, 0, tzinfo=timezone.utc)
END = datetime(2026, 11, 8, 20, 14, 27, tzinfo=timezone.utc)


def test_parse_and_resolve_final_at():
    assert parse_espn_time("2026-11-08T20:14:27Z") == END
    assert parse_espn_time("2026-11-08T20:14:27+00:00") == END
    assert parse_espn_time("2026-11-08 20:14:27+00") == END
    fa, est = resolve_final_at({}, end_utc=END)
    assert fa == END.isoformat() and est is False
    fa, est = resolve_final_at({"tip_utc": TIP})
    assert fa == (TIP + timedelta(hours=2)).isoformat() and est is True
    # end time wins over tip
    fa, est = resolve_final_at({"tip_utc": TIP, "end_utc": END})
    assert fa == END.isoformat() and est is False
    assert resolve_final_at({}) == (None, None)


def test_espn_end_utc_reads_end_game_wallclock():
    class Resp:
        status_code = 200
        def json(self):
            return {"plays": [
                {"type": {"id": "401", "text": "End Period"}, "wallclock": "2026-11-08T19:00:00Z"},
                {"type": {"id": "402", "text": "End Game"}, "text": "End of Game",
                 "wallclock": "2026-11-08T20:14:27Z"},
            ]}
    class Sess:
        def get(self, *a, **k):
            return Resp()
    assert espn_end_utc("401920424", session=Sess()) == END


def test_sync_uses_espn_end_time_and_never_writes_updated_at():
    existing = [dict(external_id="10", date="2026-11-08", status="scheduled",
                     home_score=None, away_score=None, league="ncaam", tipoff=TIP.isoformat())]
    c = FakeClient(existing)
    s = sync_finals(lookback=0, day=date(2026, 11, 8), client=c, log=lambda *_: None,
                    scoreboard=lambda d: [_espn(event_id="10", tip_utc=TIP)],
                    end_time=lambda eid: END)
    row = c.calls["games"][0]
    assert s["final_at_actual"] == 1 and s["final_at_estimated"] == 0
    assert row["final_at"] == END.isoformat() and row["final_at_estimated"] is False
    assert "updated_at" not in row


def test_sync_backfills_final_at_on_already_final_without_bumping_scores_or_updated_at():
    existing = [dict(external_id="10", date="2026-11-08", status="final",
                     home_score=80, away_score=70, league="ncaam", tipoff=TIP.isoformat(),
                     final_at=None)]
    c = FakeClient(existing)
    s = sync_finals(lookback=0, day=date(2026, 11, 8), client=c, log=lambda *_: None,
                    scoreboard=lambda d: [_espn(event_id="10", tip_utc=TIP)],
                    end_time=lambda eid: None)
    row = c.calls["games"][0]
    assert s["updated"] == 1 and s["final_at_estimated"] == 1
    assert row["final_at"] == (TIP + timedelta(hours=2)).isoformat()
    assert row["final_at_estimated"] is True
    assert "status" not in row and "home_score" not in row and "updated_at" not in row


def test_sync_does_not_overwrite_existing_final_at():
    existing = [dict(external_id="10", date="2026-11-08", status="final",
                     home_score=80, away_score=70, league="ncaam",
                     final_at=END.isoformat(), final_at_estimated=False)]
    c = FakeClient(existing)
    s = sync_finals(lookback=0, day=date(2026, 11, 8), client=c, log=lambda *_: None,
                    scoreboard=lambda d: [_espn(event_id="10", tip_utc=TIP)],
                    end_time=lambda eid: datetime(2026, 11, 8, 23, 0, tzinfo=timezone.utc))
    assert s["updated"] == 0 and "games" not in c.calls


def test_sync_upgrades_estimate_to_espn_end_but_not_to_another_estimate():
    existing = [dict(external_id="10", date="2026-11-08", status="final",
                     home_score=80, away_score=70, league="ncaam",
                     final_at=(TIP + timedelta(hours=2)).isoformat(), final_at_estimated=True)]
    c = FakeClient(existing)
    s = sync_finals(lookback=0, day=date(2026, 11, 8), client=c, log=lambda *_: None,
                    scoreboard=lambda d: [_espn(event_id="10", tip_utc=TIP)],
                    end_time=lambda eid: END)
    row = c.calls["games"][0]
    assert s["updated"] == 1 and s["final_at_actual"] == 1
    assert row["final_at"] == END.isoformat() and row["final_at_estimated"] is False
    assert "updated_at" not in row and "status" not in row
    # no ESPN end: leave the estimate alone
    c2 = FakeClient(existing)
    s2 = sync_finals(lookback=0, day=date(2026, 11, 8), client=c2, log=lambda *_: None,
                     scoreboard=lambda d: [_espn(event_id="10", tip_utc=TIP)],
                     end_time=lambda eid: None)
    assert s2["updated"] == 0 and "games" not in c2.calls


def test_migration_exposes_final_at_on_views():
    from pathlib import Path
    sql = (Path(__file__).resolve().parents[1] / "db/migrations/2026-11-10_games_final_at.sql").read_text()
    assert "add column if not exists final_at timestamptz" in sql
    assert "add column if not exists final_at_estimated boolean" in sql
    assert "g.final_at" in sql and "g.final_at_estimated" in sql
    assert "create or replace view slate_games" in sql
    assert "create or replace view model_grades" in sql
    assert "g.status" in sql
