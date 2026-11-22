from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest

from backtest import NameMatcher
from czarp.sync_game_log import classify, parse_scoreboard, sync_game_log

CENTRAL = ZoneInfo("America/Chicago")


class FakeClient:
    def __init__(self, teams=None, games=None, non_di=None, sport_id=1, fail_upsert=None):
        self.teams = teams or []
        self.games = games or []
        self.non_di = non_di or []
        self.sport_id = sport_id
        self.fail_upsert = fail_upsert
        self.upserts = {}
        self.patches = []

    def get(self, table, **params):
        if table == "sports":
            return [{"id": self.sport_id}]
        if table == "teams":
            rows = list(self.teams)
        elif table == "games":
            rows = list(self.games)
        elif table == "non_di_games":
            rows = list(self.non_di)
        else:
            rows = []
        off = int(params.get("offset") or 0)
        lim = int(params.get("limit") or 1000)
        return rows[off:off + lim]

    def upsert(self, table, rows, on_conflict=None, returning=True, ignore_duplicates=False):
        if self.fail_upsert == table:
            raise RuntimeError(f"upsert {table} failed")
        self.upserts.setdefault(table, []).extend(rows)
        return []

    def patch(self, table, row, **params):
        rec = dict(row)
        rec["_filters"] = dict(params)
        self.patches.append(rec)


def _ev(**kw):
    ev = dict(
        event_id="9",
        game_date="2026-11-09",
        tip_ct=datetime(2026, 11, 9, 18, 30, tzinfo=CENTRAL),
        home="McNeese",
        away="College of Biblical Studies",
        home_full="McNeese Cowboys",
        away_full="College of Biblical Studies Ambassadors",
        home_id="2377",
        away_id="128256",
        home_score=135,
        away_score=87,
        neutral=False,
        completed=True,
        state="post",
        status_name="STATUS_FINAL",
    )
    ev.update(kw)
    return ev


DI_IDS = {"2377", "326", "87", "222", "2335", "2272"}
END = datetime(2026, 11, 1, 23, 45, tzinfo=CENTRAL)


def test_classify_nondi_di_and_unmapped_di():
    match = NameMatcher(["McNeese", "Texas St.", "Notre Dame", "Villanova", "Liberty", "High Point"])
    nd = classify(_ev(), match, DI_IDS)
    assert nd["kind"] == "non_di" and nd["home_name"] == "McNeese" and nd["away_name"] is None
    assert match("Notre Dame (MD)") is None
    di = classify(dict(_ev(), home="High Point", away="Liberty", home_id="2272", away_id="2335",
                       home_score=72, away_score=57), match, DI_IDS)
    assert di["kind"] == "di" and di["home_name"] == "High Point" and di["away_name"] == "Liberty"
    # both ESPN D-I but one name missing from the matcher: skip, do not store as non-D-I
    skip = classify(dict(_ev(), home="High Point", away="Mystery Di", home_id="2272", away_id="2335"),
                    match, DI_IDS)
    assert skip["kind"] == "skip"


def test_classify_requires_espn_ids_when_present():
    """Both names matching is not enough if ESPN ids are known and not both D-I."""
    match = NameMatcher(["McNeese", "Liberty"])
    # Name-collision: a non-D-I side whose name also maps, but ESPN id is not D-I
    row = _ev(home="McNeese", away="Liberty", home_id="2377", away_id="999")
    out = classify(row, match, DI_IDS)
    assert out["kind"] == "non_di"
    assert out["home_di"] is True and out["away_di"] is False


def test_parse_scoreboard_uses_chicago_date():
    # 2026-11-10T00:30Z is Nov 9 evening CT
    payload = {"events": [{
        "id": "401928001",
        "date": "2026-11-10T00:30Z",
        "competitions": [{
            "neutralSite": False,
            "status": {"type": {"completed": True, "state": "post", "name": "STATUS_FINAL"}},
            "competitors": [
                {"homeAway": "home", "score": "135", "team": {"id": "2377", "location": "McNeese", "displayName": "McNeese Cowboys"}},
                {"homeAway": "away", "score": "87", "team": {"id": "128256", "location": "College of Biblical Studies", "displayName": "College of Biblical Studies Ambassadors"}},
            ],
        }],
    }]}
    rows = parse_scoreboard(payload, date(2026, 11, 9))
    assert len(rows) == 1 and rows[0]["game_date"] == "2026-11-09"
    assert rows[0]["home_id"] == "2377" and rows[0]["away_score"] == 87
    assert parse_scoreboard(payload, date(2026, 11, 10)) == []


def _run(client, events_by_day, di_ids=None, **kw):
    if di_ids is None:
        di_ids = {"2377", "2272", "2335", "1", "2"}

    def sb(d):
        return events_by_day.get(d, [])

    return sync_game_log(lookback=8, day=date(2026, 11, 9), client=client, scoreboard=sb,
                         di_ids=di_ids, end_time=lambda eid: END, log=lambda *_: None, **kw)


def test_sync_upserts_nondi_inserts_missing_di_and_fills_ids():
    teams = [{"id": 102, "name": "McNeese"}, {"id": 258, "name": "Wright St."},
             {"id": 359, "name": "Air Force"}, {"id": 44, "name": "High Point"},
             {"id": 566, "name": "Liberty"}]
    games = [dict(id=20, external_id="401911333", home="Wright St.", away="Air Force",
                  home_team_id=None, away_team_id=None, status="final")]
    events_by_day = {
        date(2026, 11, 1): [
            _ev(event_id="401920409", game_date="2026-11-01", home="High Point", away="Liberty",
                home_id="2272", away_id="2335", home_score=72, away_score=57,
                tip_ct=datetime(2026, 11, 1, 20, 0, tzinfo=CENTRAL)),
        ],
        date(2026, 11, 2): [
            _ev(event_id="401911333", game_date="2026-11-02", home="Wright St.", away="Air Force",
                home_id="1", away_id="2", home_score=102, away_score=71),
        ],
        date(2026, 11, 9): [
            _ev(),
        ],
    }
    c = FakeClient(teams=teams, games=games)
    s = _run(c, events_by_day)
    assert s["non_di_new"] == 1 and s["non_di_unchanged"] == 0
    assert s["di_inserted"] == 1
    assert s["ids_filled"] == 1
    nd = c.upserts["non_di_games"][0]
    assert nd["home"] == "McNeese" and nd["away"] == "College of Biblical Studies"
    assert nd["home_team_id"] == 102 and nd["away_team_id"] is None
    assert nd["home_score"] == 135 and nd["away_score"] == 87
    assert "games" in c.upserts
    di = c.upserts["games"][0]
    assert di["home"] == "High Point" and di["away"] == "Liberty"
    assert di["home_team_id"] == 44 and di["away_team_id"] == 566
    assert di["home_score"] == 72 and di["external_id"] == "401920409"
    assert di["final_at"] is not None
    assert di["final_at_estimated"] is False
    assert c.patches[0]["home_team_id"] == 258 and c.patches[0]["away_team_id"] == 359
    assert all(r["external_id"] != "9" for r in c.upserts.get("games", []))
    assert "projections" not in c.upserts and "market_lines" not in c.upserts and "picks" not in c.upserts


def test_second_run_counts_unchanged_not_inserted():
    teams = [{"id": 102, "name": "McNeese"}]
    already = [dict(
        external_id="9", home="McNeese", away="College of Biblical Studies",
        home_team_id=102, away_team_id=None, home_score=135, away_score=87,
        neutral=False, status="final", date="2026-11-09",
    )]
    c = FakeClient(teams=teams, non_di=already)
    s = _run(c, {date(2026, 11, 9): [_ev()]}, di_ids={"2377"})
    assert s["non_di_new"] == 0 and s["non_di_unchanged"] == 1 and s["non_di_updated"] == 0
    assert s["di_inserted"] == 0 and s["ids_filled"] == 0
    # still upserts the identical row (idempotent), but the log counts it as unchanged
    assert len(c.upserts.get("non_di_games", [])) == 1


def test_empty_di_ids_skips_nondi_writes():
    teams = [{"id": 102, "name": "McNeese"}, {"id": 44, "name": "High Point"},
             {"id": 566, "name": "Liberty"}]
    events = {
        date(2026, 11, 1): [
            _ev(event_id="401920409", game_date="2026-11-01", home="High Point", away="Liberty",
                home_id="2272", away_id="2335", home_score=72, away_score=57),
        ],
        date(2026, 11, 9): [_ev()],
    }
    c = FakeClient(teams=teams)
    s = _run(c, events, di_ids=set())
    assert s["ids_unavailable"] is True
    assert s["non_di_new"] == 0 and "non_di_games" not in c.upserts
    # both names still map, so the missing D-I opener is still inserted
    assert s["di_inserted"] == 1


def test_team_id_repair_runs_even_if_upsert_fails():
    teams = [{"id": 258, "name": "Wright St."}, {"id": 359, "name": "Air Force"},
             {"id": 102, "name": "McNeese"}]
    games = [dict(id=20, external_id="401911333", home="Wright St.", away="Air Force",
                  home_team_id=None, away_team_id=None, status="final")]
    events = {
        date(2026, 11, 2): [
            _ev(event_id="401911333", game_date="2026-11-02", home="Wright St.", away="Air Force",
                home_id="1", away_id="2", home_score=102, away_score=71),
        ],
        date(2026, 11, 9): [_ev()],
    }
    c = FakeClient(teams=teams, games=games, fail_upsert="non_di_games")
    with pytest.raises(RuntimeError, match="upsert non_di_games failed"):
        _run(c, events, di_ids={"2377", "1", "2"})
    assert c.patches and c.patches[0]["home_team_id"] == 258
    assert c.patches[0]["away_team_id"] == 359


def test_sync_skips_existing_di_game():
    teams = [{"id": 44, "name": "High Point"}, {"id": 566, "name": "Liberty"}]
    games = [dict(id=1, external_id="401920409", home="High Point", away="Liberty",
                  home_team_id=44, away_team_id=566, status="final")]

    def sb(d):
        if d != date(2026, 11, 1):
            return []
        return [_ev(event_id="401920409", game_date="2026-11-01", home="High Point",
                    away="Liberty", home_id="2272", away_id="2335", home_score=72, away_score=57)]

    c = FakeClient(teams=teams, games=games)
    s = sync_game_log(lookback=0, day=date(2026, 11, 1), client=c, scoreboard=sb,
                      di_ids={"2272", "2335"}, end_time=lambda eid: None, log=lambda *_: None)
    assert s["di_inserted"] == 0 and s["non_di_new"] == 0 and s["ids_filled"] == 0
    assert c.upserts == {} and c.patches == []


def test_view_sql_unions_nondi_and_leaves_model_grades():
    sql = open("db/migrations/2026-11-22_team_game_log.sql").read()
    assert sql.lstrip().startswith("--")
    body = "\n".join(l for l in sql.splitlines() if l.strip() and not l.strip().startswith("--"))
    assert body.startswith("begin;")
    assert body.rstrip().endswith("commit;")
    assert "create table if not exists non_di_games" in sql
    assert "create view team_game_log" in sql
    assert "union all" in sql
    assert "from model_grades" in sql or "join model_grades" in sql.lower()
    assert "vs_di" in sql
    assert "create or replace view model_grades" not in sql
    assert "Does not touch picks" in sql
    # R2: slate_games requires a projection (inner JOIN LATERAL), so unprojected
    # D-I finals never become empty slate cards. Market/pick joins stay LEFT.
    assert "drop view if exists slate_games" in sql
    assert "create view slate_games" in sql
    proj = sql[sql.find("join sports"):]
    assert "\njoin lateral (" in proj
    assert "left join lateral (" in proj
    first, rest = proj.split("left join lateral", 1)
    assert "join lateral (" in first and "projections pr" in first
    assert "market_lines" in rest
