"""CZarp weekly rankings: projected margin vs an average D-I team, neutral floor."""
from datetime import date, datetime
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
    # home court must not leak into the official rating
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
    # True AdjEM: Michigan 28.008 > Tennessee 28.002.
    # Rounded-then-subtracted: Tennessee 28.01 > Michigan 28.00.
    tennessee = {"team_id": 10, "oe": 118.126, "de": 90.124, "tempo": 67.5, "kp_pct": 0.96, "unit_v2": 0.0}
    michigan  = {"team_id": 11, "oe": 118.124, "de": 90.116, "tempo": 67.5, "kp_pct": 0.95, "unit_v2": 0.0}
    teams = [{"id": 10, "name": "Tennessee"}, {"id": 11, "name": "Michigan"}]
    poll = {r["team_id"]: r for r in build_poll([tennessee, michigan], C, teams, [], date(2026, 11, 23))}
    assert poll[11]["rank"] < poll[10]["rank"], "high-precision Michigan should rank ahead"
    t_rounded = (round(118.126, 2) - round(90.124, 2))
    m_rounded = (round(118.124, 2) - round(90.116, 2))
    assert t_rounded > m_rounded  # documents the trap


def test_sort_uses_full_precision_then_rounds_storage():
    # Two ratings that would tie at 2 decimals but not before rounding.
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
        {"date": "2026-11-24", "status": "final", "home": "Houston", "away": "X",  # after as_of: ignored
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


def test_maybe_publish_only_monday_after_7am_and_skips_if_already_written():
    calls = []

    class Fake:
        def get(self, table, **params):
            return [{"id": 1}]  # already published
        def upsert(self, *a, **k):
            raise AssertionError("must not upsert when a snapshot already exists")

    sat = datetime(2026, 11, 21, 10, 0, tzinfo=CT)
    mon_early = datetime(2026, 11, 23, 6, 59, tzinfo=CT)
    mon = datetime(2026, 11, 23, 7, 0, tzinfo=CT)
    assert WR.maybe_publish(sat, log=lambda *a, **k: None, client=Fake()) is None
    assert WR.maybe_publish(mon_early, log=lambda *a, **k: None, client=Fake()) is None
    assert WR.maybe_publish(mon, log=lambda *a, **k: calls.append("skip") or None, client=Fake()) == []
    assert calls == ["skip"]


def test_migration_select_only_and_leaves_picks_alone():
    raw = open("db/migrations/2026-11-21_ranking_snapshots.sql").read()
    sql = "\n".join(l for l in raw.lower().splitlines() if not l.strip().startswith("--"))
    assert "create table if not exists ranking_snapshots" in sql
    assert "enable row level security" in sql
    assert "grant select on table ranking_snapshots to anon, authenticated" in sql
    assert "revoke all on table ranking_snapshots from anon, authenticated" in sql
    assert "alter default privileges" not in sql
    assert "picks" not in sql and "picks_immutable" not in sql
    assert "grant insert" not in sql and "grant update" not in sql and "grant all" not in sql
    assert "projected margin" in raw.lower() and "neutral" in raw.lower()
