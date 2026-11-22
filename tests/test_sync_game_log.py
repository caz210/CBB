from datetime import date, datetime
from zoneinfo import ZoneInfo

from backtest import NameMatcher
from czarp.sync_game_log import classify, parse_scoreboard, sync_game_log

CENTRAL = ZoneInfo("America/Chicago")


class FakeClient:
    def __init__(self, teams=None, games=None, sport_id=1):
        self.teams = teams or []
        self.games = games or []
        self.sport_id = sport_id
        self.upserts = {}
        self.patches = []

    def get(self, table, **params):
        if table == "sports":
            return [{"id": self.sport_id}]
        if table == "teams":
            rows = list(self.teams)
        elif table == "games":
            rows = list(self.games)
        else:
            rows = []
        off = int(params.get("offset") or 0)
        lim = int(params.get("limit") or 1000)
        return rows[off:off + lim]

    def upsert(self, table, rows, on_conflict=None, returning=True, ignore_duplicates=False):
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


def test_classify_nondi_di_and_unmapped_di():
    match = NameMatcher(["McNeese", "Texas St.", "Notre Dame", "Villanova", "Liberty", "High Point"])
    di_ids = {"2377", "326", "87", "222", "2335", "2272"}
    nd = classify(_ev(), match, di_ids)
    assert nd["kind"] == "non_di" and nd["home_name"] == "McNeese" and nd["away_name"] is None
    # Notre Dame (MD) must not be treated as Notre Dame
    assert match("Notre Dame (MD)") is None
    # both D-I names
    di = classify(dict(_ev(), home="High Point", away="Liberty", home_id="2272", away_id="2335",
                       home_score=72, away_score=57), match, di_ids)
    assert di["kind"] == "di" and di["home_name"] == "High Point" and di["away_name"] == "Liberty"
    # both ESPN D-I but one name missing from the matcher: skip, do not store as non-D-I
    skip = classify(dict(_ev(), home="High Point", away="Mystery Di", home_id="2272", away_id="2335"),
                    match, di_ids)
    assert skip["kind"] == "skip"


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


def test_sync_upserts_nondi_inserts_missing_di_and_fills_ids():
    teams = [{"id": 102, "name": "McNeese"}, {"id": 258, "name": "Wright St."},
             {"id": 359, "name": "Air Force"}, {"id": 44, "name": "High Point"},
             {"id": 566, "name": "Liberty"}]
    # Wright St. vs Air Force already stored, but both team_ids are null
    games = [dict(id=20, external_id="401911333", home="Wright St.", away="Air Force",
                  home_team_id=None, away_team_id=None, status="final")]
    events_by_day = {
        date(2026, 11, 1): [
            _ev(event_id="401920409", game_date="2026-11-01", home="High Point", away="Liberty",
                home_id="2272", away_id="2335", home_score=72, away_score=57),
        ],
        date(2026, 11, 2): [
            _ev(event_id="401911333", game_date="2026-11-02", home="Wright St.", away="Air Force",
                home_id="1", away_id="2", home_score=102, away_score=71),
        ],
        date(2026, 11, 9): [
            _ev(),
        ],
    }
    di_ids = {"2377", "2272", "2335", "1", "2"}

    def sb(d):
        return events_by_day.get(d, [])

    c = FakeClient(teams=teams, games=games)
    s = sync_game_log(lookback=8, day=date(2026, 11, 9), client=c, scoreboard=sb,
                      di_ids=di_ids, log=lambda *_: None)
    assert s["non_di_upserted"] == 1
    assert s["di_inserted"] == 1
    assert s["ids_filled"] == 1
    nd = c.upserts["non_di_games"][0]
    assert nd["home"] == "McNeese" and nd["away"] == "College of Biblical Studies"
    assert nd["home_team_id"] == 102 and nd["away_team_id"] is None
    assert nd["home_score"] == 135 and nd["away_score"] == 87
    assert "games" in c.upserts  # missing Nov 1 D-I final
    di = c.upserts["games"][0]
    assert di["home"] == "High Point" and di["away"] == "Liberty"
    assert di["home_team_id"] == 44 and di["away_team_id"] == 566
    assert di["home_score"] == 72 and di["external_id"] == "401920409"
    assert c.patches[0]["home_team_id"] == 258 and c.patches[0]["away_team_id"] == 359
    # never write a non-D-I row into games, and never write projections
    assert all(r["external_id"] != "9" for r in c.upserts.get("games", []))
    assert "projections" not in c.upserts and "market_lines" not in c.upserts and "picks" not in c.upserts


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
                      di_ids={"2272", "2335"}, log=lambda *_: None)
    assert s["di_inserted"] == 0 and s["non_di_upserted"] == 0 and s["ids_filled"] == 0
    assert c.upserts == {} and c.patches == []


def test_view_sql_unions_nondi_and_leaves_model_grades():
    sql = open("db/migrations/2026-11-22_team_game_log.sql").read()
    assert "create table if not exists non_di_games" in sql
    assert "create view team_game_log" in sql
    assert "union all" in sql
    assert "from model_grades" in sql or "join model_grades" in sql.lower()
    assert "vs_di" in sql
    # model_grades itself is not replaced
    assert "create or replace view model_grades" not in sql
    assert "picks" not in sql.lower() or "Does not touch picks" in sql
