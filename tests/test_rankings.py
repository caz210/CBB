"""CZarp weekly rankings: projected margin vs an average D-I team, neutral floor."""
from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo

from czarp.matchup import _v2_margin
from czarp.rankings import (
    average_opponent, build_poll, di_records, monday_on_or_before, rating_vs_average,
)
import weekly_rankings as WR

CT = ZoneInfo("America/Chicago")

# Minimal coefficients; values match the live model closely enough for ordering tests.
C = {
    "avg_tempo": 67.5, "avg_eff": 106.0,
    "total_intercept": 0.0, "total_slope": 1.0,
    "margin_scale": 1.0, "w_efficiency": 1.0,
    "qg_coef": 0.0, "w_quality_gap": 1.0,
    "hca_scale": 1.0, "w_hca": 1.0, "hca_split": 0.5,
    "unit_scale": 1.0, "w_unit": 1.0,
    "unit_close_threshold": 4.0, "unit_close_mult": 1.0,
    "unit_neutral_mult": 1.0, "unit_late_factor": 1.0,
    "margin_sd": 11.0,
}


def _team(oe, de, tempo=67.5, kp_pct=0.5, unit=0.0):
    return {"oe": oe, "de": de, "tempo": tempo, "kp_pct": kp_pct, "unit_v2": unit, "hca": 3.0}


class FakeDB:
    """Minimal Client stand-in. Seed `weeks` with {week_of_iso: [rows]}."""

    def __init__(self, weeks=None, raise_on_get=None, fail_upsert=False):
        self.weeks = {k: list(v) for k, v in (weeks or {}).items()}
        self.raise_on_get = raise_on_get
        self.fail_upsert = fail_upsert
        self.upserts = []
        self.deletes = []

    def get(self, table, **params):
        if table == "ranking_snapshots":
            if self.raise_on_get:
                raise self.raise_on_get
            week = (params.get("week_of") or "").removeprefix("eq.").removeprefix("lt.")
            op_lt = (params.get("week_of") or "").startswith("lt.")
            if op_lt:
                earlier = sorted(w for w in self.weeks if w < week)
                if not earlier:
                    return []
                prev = earlier[-1]
                if params.get("select") == "week_of":
                    return [{"week_of": prev}]
                rows = self.weeks[prev]
            else:
                rows = self.weeks.get(week, [])
            limit = int(params.get("limit") or 1000)
            offset = int(params.get("offset") or 0)
            return rows[offset:offset + limit]
        if table == "teams":
            return [{"id": 1, "name": "Alpha", "league": "ncaam"},
                    {"id": 2, "name": "Beta", "league": "ncaam"}]
        if table == "matchup_inputs":
            return [
                {"team_id": 1, "oe": 118.0, "de": 96.0, "tempo": 67.5, "kp_pct": 0.9, "unit_v2": 0.0, "hca": 3.0},
                {"team_id": 2, "oe": 104.0, "de": 96.0, "tempo": 67.5, "kp_pct": 0.5, "unit_v2": 0.0, "hca": 3.0},
            ]
        if table == "matchup_constants":
            return [dict(C, league="ncaam")]
        if table == "games":
            return []
        return []

    def upsert(self, table, rows, on_conflict=None, returning=True, ignore_duplicates=False):
        self.upserts.append((table, list(rows), on_conflict))
        if self.fail_upsert:
            raise RuntimeError("upsert failed")
        assert on_conflict == "league,week_of,team_id"
        for r in rows:
            self.weeks.setdefault(r["week_of"], []).append(r)
        return rows if returning else []

    def delete(self, table, **params):
        week = (params.get("week_of") or "").removeprefix("eq.")
        self.deletes.append((table, week, params))
        self.weeks.pop(week, None)


def test_average_team_rates_near_zero():
    avg = average_opponent(
        [{"oe": 110, "de": 100, "tempo": 70, "kp_pct": 0.8, "unit_v2": 2.0},
         {"oe": 102, "de": 112, "tempo": 65, "kp_pct": 0.2, "unit_v2": -2.0}],
        C,
    )
    assert avg["oe"] == C["avg_eff"] and avg["de"] == C["avg_eff"]
    assert avg["tempo"] == C["avg_tempo"] and avg["kp_pct"] == 0.5
    assert avg["unit_v2"] == 0.0
    assert abs(rating_vs_average(avg, avg, C)) < 1e-9


def test_rating_is_v2_margin_on_a_neutral_floor():
    t = _team(118, 92, tempo=70, kp_pct=0.9, unit=1.5)
    avg = average_opponent([t, _team(94, 110, unit=-1.5)], C)
    got = rating_vs_average(t, avg, C)
    assert got == _v2_margin(t, avg, None, C)
    home = _v2_margin(t, avg, True, C)
    assert got != home


def test_stronger_offense_ranks_ahead():
    inputs = [
        {"team_id": 1, "oe": 118.0, "de": 96.0, "tempo": 67.5, "kp_pct": 0.9, "unit_v2": 0.0},
        {"team_id": 2, "oe": 104.0, "de": 96.0, "tempo": 67.5, "kp_pct": 0.5, "unit_v2": 0.0},
    ]
    teams = [{"id": 1, "name": "Alpha"}, {"id": 2, "name": "Beta"}]
    poll = build_poll(inputs, C, teams, games=[], as_of=date(2026, 11, 23))
    assert [r["rank"] for r in poll] == [1, 2]
    assert poll[0]["team_id"] == 1 and poll[0]["rating"] > poll[1]["rating"]
    assert poll[0]["week_of"] == "2026-11-23" and poll[0]["season"] == 2027
    assert poll[0]["league"] == "ncaam" and poll[0]["prev_rank"] is None


def test_high_precision_beats_two_decimal_adj_em():
    """team_profiles stores AdjOE/AdjDE at 2 decimals; that swap is why we rate from matchup_inputs."""
    tennessee = {"team_id": 10, "oe": 118.126, "de": 90.124, "tempo": 67.5, "kp_pct": 0.96, "unit_v2": 0.0}
    michigan  = {"team_id": 11, "oe": 118.124, "de": 90.116, "tempo": 67.5, "kp_pct": 0.95, "unit_v2": 0.0}
    teams = [{"id": 10, "name": "Tennessee"}, {"id": 11, "name": "Michigan"}]
    poll = {r["team_id"]: r for r in build_poll([tennessee, michigan], C, teams, [], date(2026, 11, 23))}
    assert poll[11]["rank"] < poll[10]["rank"], "high-precision Michigan should rank ahead"
    t_rounded = (round(118.126, 2) - round(90.124, 2))
    m_rounded = (round(118.124, 2) - round(90.116, 2))
    assert t_rounded > m_rounded


def test_sort_uses_full_precision_then_rounds_storage():
    a = {"team_id": 1, "oe": 110.000, "de": 100.000, "tempo": 67.5, "kp_pct": 0.5, "unit_v2": 0.0}
    b = {"team_id": 2, "oe": 110.003, "de": 100.000, "tempo": 67.5, "kp_pct": 0.5, "unit_v2": 0.0}
    teams = [{"id": 1, "name": "Aaa"}, {"id": 2, "name": "Bbb"}]
    poll = build_poll([a, b], C, teams, [], date(2026, 11, 23))
    assert poll[0]["team_id"] == 2
    assert isinstance(poll[0]["rating"], float)
    assert poll[0]["rating"] == round(poll[0]["rating"], 2)


def test_di_and_kp_records_and_prev_rank():
    games = [
        {"date": "2026-11-08", "status": "final", "home": "Houston", "away": "California",
         "home_score": 86, "away_score": 68},
        {"date": "2026-11-24", "status": "final", "home": "Houston", "away": "X",
         "home_score": 70, "away_score": 60},
        {"date": "2026-11-08", "status": "pre", "home": "Houston", "away": "Y",
         "home_score": None, "away_score": None},
    ]
    rec = di_records(games, date(2026, 11, 23))
    assert rec["Houston"] == (1, 0) and rec["California"] == (0, 1)
    import pandas as pd
    ratings = pd.DataFrame({"TeamName": ["Houston"], "Wins": [5], "Losses": [0]})
    inputs = [{"team_id": 7, "oe": 120, "de": 90, "tempo": 67.5, "kp_pct": 0.99, "unit_v2": 0.0}]
    teams = [{"id": 7, "name": "Houston"}]
    prev = [{"team_id": 7, "rank": 3}]
    poll = build_poll(inputs, C, teams, games, date(2026, 11, 23), ratings=ratings, prev=prev)
    r = poll[0]
    assert r["rank"] == 1 and r["prev_rank"] == 3
    assert r["di_wins"] == 1 and r["di_losses"] == 0
    assert r["kp_wins"] == 5 and r["kp_losses"] == 0 and r["games_played"] == 1


def test_monday_on_or_before():
    assert monday_on_or_before(date(2026, 11, 23)) == date(2026, 11, 23)
    assert monday_on_or_before(date(2026, 11, 21)) == date(2026, 11, 16)
    assert monday_on_or_before(date(2026, 11, 24)) == date(2026, 11, 23)


def test_maybe_publish_catches_up_after_monday_and_is_once_per_week():
    """A missed Monday is published on Tue–Sun; already-published weeks are not rewritten."""
    logs = []
    empty = FakeDB()
    mon_early = datetime(2026, 11, 23, 6, 59, tzinfo=CT)
    mon = datetime(2026, 11, 23, 7, 0, tzinfo=CT)
    tue = datetime(2026, 11, 24, 7, 0, tzinfo=CT)
    sun = datetime(2026, 11, 29, 18, 0, tzinfo=CT)
    next_mon_early = datetime(2026, 11, 30, 6, 59, tzinfo=CT)
    utc_mon_8 = datetime(2026, 11, 23, 8, 0, tzinfo=timezone.utc)  # 02:00 CT

    assert WR.maybe_publish(mon_early, log=logs.append, client=FakeDB()) is None
    assert WR.maybe_publish(utc_mon_8, log=logs.append, client=FakeDB()) is None  # before 7 AM CT
    poll = WR.maybe_publish(tue, log=logs.append, client=empty, apply=True)
    assert poll and all(r["week_of"] == "2026-11-23" for r in poll)
    assert empty.upserts and empty.upserts[0][2] == "league,week_of,team_id"
    # already published this week (even mid-week) — no second snapshot
    already = FakeDB(weeks={"2026-11-23": [{"id": 1, "week_of": "2026-11-23", "as_of": "2026-11-23", "rank": 1}]})
    assert WR.maybe_publish(sun, log=logs.append, client=already) == []
    assert already.upserts == []
    # next Monday before 7 AM is the next week and still gated
    assert WR.maybe_publish(next_mon_early, log=logs.append, client=FakeDB()) is None
    # Monday 7 AM publishes when empty
    assert WR.maybe_publish(mon, log=logs.append, client=FakeDB(), apply=True)


def test_apply_refuses_to_write_a_second_snapshot_for_the_same_week():
    already = FakeDB(weeks={"2026-11-23": [
        {"id": 1, "team_id": 1, "rank": 1, "week_of": "2026-11-23", "as_of": "2026-11-23"}
    ]})
    # Tuesday --apply with as_of=11-24 would have created as_of 11-24 under the old (as_of, team_id) key
    WR.run(as_of=date(2026, 11, 24), apply=True, client=already, log=lambda *a, **k: None)
    assert already.upserts == []
    WR.run(as_of=date(2026, 11, 24), apply=True, force=True, client=already, log=lambda *a, **k: None)
    assert already.upserts and already.upserts[0][2] == "league,week_of,team_id"


def test_previous_snapshot_uses_week_of_not_as_of():
    # Last week published late (as_of Tuesday) must still be "last week" by week_of.
    db = FakeDB(weeks={
        "2026-11-16": [
            {"team_id": 1, "rank": 4, "week_of": "2026-11-16", "as_of": "2026-11-17"},
            {"team_id": 2, "rank": 1, "week_of": "2026-11-16", "as_of": "2026-11-17"},
        ]
    })
    prev = WR.previous_snapshot(db, date(2026, 11, 23))
    assert {r["team_id"]: r["rank"] for r in prev} == {1: 4, 2: 1}


def test_failed_upsert_rolls_back_the_week():
    db = FakeDB(fail_upsert=True)
    poll = [
        {"as_of": "2026-11-23", "week_of": "2026-11-23", "league": "ncaam",
         "team_id": 1, "name": "Alpha", "rank": 1, "rating": 10.0},
        {"as_of": "2026-11-23", "week_of": "2026-11-23", "league": "ncaam",
         "team_id": 2, "name": "Beta", "rank": 2, "rating": 1.0},
    ]
    try:
        WR.write_week(db, poll, date(2026, 11, 23))
    except RuntimeError:
        pass
    else:
        raise AssertionError("write_week should re-raise")
    assert db.deletes and db.deletes[0][1] == "2026-11-23"
    assert "2026-11-23" not in db.weeks


def test_ratings_csv_is_resolved_from_the_module_not_a_hardcoded_box_path():
    src = open(WR.__file__).read()
    assert "/workspace/CBB/data/ratings.csv" not in src
    assert WR.RATINGS_CSV == os_path_join_root()
    # maybe_ratings must not raise if the file is missing
    assert WR.maybe_ratings() is None or hasattr(WR.maybe_ratings(), "empty")


def os_path_join_root():
    import os
    return os.path.join(os.path.dirname(os.path.abspath(WR.__file__)), "data", "ratings.csv")


def test_daily_run_hook_records_rankings_failures_in_notes():
    src = open("daily_run.py").read()
    assert "Official rankings were not published this run; they will retry next run." in src
    assert "notes.append(" in src[src.index("maybe_publish"):src.index("maybe_publish") + 800]


def test_migration_select_only_week_key_and_no_public_kenpom():
    raw = open("db/migrations/2026-11-21_ranking_snapshots.sql").read()
    sql = "\n".join(l for l in raw.lower().splitlines() if not l.strip().startswith("--"))
    assert "create table if not exists ranking_snapshots" in sql
    assert "unique (league, week_of, team_id)" in sql
    create = sql.split("do $$")[0]
    assert "unique (as_of, team_id)" not in create
    assert "drop constraint ranking_snapshots_as_of_team_id_key" in sql
    assert "enable row level security" in sql
    assert "grant select on table ranking_snapshots to anon, authenticated" in sql
    assert "revoke all on table ranking_snapshots from anon, authenticated" in sql
    assert "revoke all on sequence ranking_snapshots_id_seq from anon, authenticated" in sql
    assert "alter default privileges" not in sql
    assert "picks" not in sql and "picks_immutable" not in sql
    assert "grant insert" not in sql and "grant update" not in sql and "grant all" not in sql
    assert "projected margin" in raw.lower() and "neutral" in raw.lower()
    # COMMENT ON text is visible to anon via PostgREST OpenAPI — no source-name branding.
    comments = "\n".join(l for l in raw.lower().splitlines() if l.strip().startswith("comment on"))
    assert "kenpom" not in comments
