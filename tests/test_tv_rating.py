"""TV Rating: range, closeness/quality monotonicity, neutral games, missing inputs."""
from pathlib import Path

import pandas as pd

from czarp.tv_rating import (
    closeness,
    game_tv_rating,
    pace_factor,
    quality_by_team,
    quality_from_data,
    tv_rating,
)
from czarp import supabase_writer as sw
import daily_run as dr
from datetime import datetime, timezone

from tests.test_supabase_writer import FakeClient


def _league(n=365):
    """n D-I teams with unique AdjEM so rank i is Team{i:03d} (1 = best)."""
    # AdjEM runs n, n-1, ..., 1 so Team001 is best.
    names = [f"Team{i:03d}" for i in range(1, n + 1)]
    em = list(range(n, 0, -1))
    oe = [100 + e / 2 for e in em]
    de = [100 - e / 2 for e in em]
    return pd.DataFrame({"TeamName": names, "AdjOE": oe, "AdjDE": de})


# ── formula ──────────────────────────────────────────────────────────────────

def test_quality_best_is_one_worst_is_zero_and_uses_high_precision_not_rounded():
    # Two teams that would *tie* if AdjEM were rounded to 2 decimals
    # (35.494 vs 35.486 → both 35.49) must keep distinct ranks at full precision.
    ratings = pd.DataFrame({
        "TeamName": ["High", "Mid", "Low"],
        "AdjOE": [120.001, 110.0, 90.0],
        "AdjDE": [84.507, 100.0, 110.0],  # High AdjEM = 35.494; a 2-decimal stamp would be 35.49
    })
    # Add a near-twin that 2-decimal team_profiles would collapse into High
    ratings = pd.concat([ratings, pd.DataFrame({
        "TeamName": ["AlmostHigh"], "AdjOE": [120.000], "AdjDE": [84.514],  # AdjEM 35.486
    })], ignore_index=True)
    q = quality_by_team(ratings)
    assert q["High"] > q["AlmostHigh"]
    assert q["High"] == 1.0
    assert q["Low"] == 0.0
    assert 0 < q["Mid"] < 1


def test_quality_rank_formula_matches_n_minus_one_denominator():
    q = quality_by_team(_league(365))
    # rank 25 → 1 - 24/364; rank 48 → 1 - 47/364 (the Michigan / Villanova pair)
    assert q["Team025"] == 1 - 24 / 364
    assert q["Team048"] == 1 - 47 / 364
    assert q["Team001"] == 1.0
    assert q["Team365"] == 0.0


def test_tv_rating_range_and_one_decimal():
    for args in [
        (1.0, 0.5, 82),
        (0.0, 0.99, 60),
        (0.5, 0.5, 70),
        (1.0, 0.0, 90),
    ]:
        s = tv_rating(*args)
        assert s is not None
        assert 0 <= s <= 100
        assert s == round(s, 1)


def test_tv_rating_clamped_to_0_100():
    # Pathological quality > 1 should still clamp (defensive).
    assert tv_rating(2.0, 0.5, 100) == 100.0
    assert tv_rating(-1.0, 0.5, 70) == 0.0


def test_monotonic_in_closeness_holding_quality_and_pace():
    q, poss = 0.8, 70
    # closer to 50% must score higher
    a = tv_rating(q, 0.51, poss)
    b = tv_rating(q, 0.60, poss)
    c = tv_rating(q, 0.80, poss)
    d = tv_rating(q, 0.49, poss)  # equally close as 0.51
    assert a > b > c
    assert a == d


def test_monotonic_in_quality_holding_closeness_and_pace():
    mid = tv_rating(0.5, 0.5, 70)
    hi = tv_rating(0.9, 0.5, 70)
    lo = tv_rating(0.2, 0.5, 70)
    assert hi > mid > lo


def test_pace_prefers_possessions_and_falls_back_to_total():
    assert pace_factor(62, 200) == 0.85          # possessions wins even if total is huge
    assert pace_factor(82, 100) == 1.00
    assert pace_factor(None, 130) == 0.85
    assert pace_factor(None, 170) == 1.00
    assert pace_factor(None, None) is None


def test_sample_michigan_villanova_and_new_mexico_wichita():
    """Today's illustrative scores: Michigan @ Villanova 77, New Mexico @ Wichita St. 68."""
    q = quality_by_team(_league(365))
    # Villanova was rank 48, Michigan 25; home win% and possessions from the v2 card
    mich = game_tv_rating(q, "Team048", "Team025", 0.4478, possessions=73.6)
    # Wichita St. rank 74, New Mexico 62
    nmsu = game_tv_rating(q, "Team074", "Team062", 0.5885, possessions=72.4)
    assert mich == 77.3          # integer display: 77
    assert nmsu == 67.6          # integer display: 68
    assert round(mich) == 77
    assert round(nmsu) == 68


def test_neutral_game_scores_the_same_as_home_with_same_card_numbers():
    """Venue is not an input; a neutral game with the same win% and pace scores the same."""
    q = quality_by_team(_league(10))
    home = game_tv_rating(q, "Team003", "Team004", 0.52, possessions=70)
    # Neutral doesn't change the call — the card's win% already has no HCA
    neut = game_tv_rating(q, "Team003", "Team004", 0.52, possessions=70)
    assert home is not None and home == neut
    assert 0 <= home <= 100


def test_missing_inputs_return_null():
    q = quality_by_team(_league(10))
    assert tv_rating(None, 0.5, 70) is None
    assert tv_rating(0.5, None, 70) is None
    assert tv_rating(0.5, 0.5, None, total=None) is None
    assert closeness(None) is None
    assert game_tv_rating(q, "Team001", "Missing", 0.5, possessions=70) is None
    assert game_tv_rating(q, "Missing", "Team001", 0.5, possessions=70) is None
    assert game_tv_rating({}, "Team001", "Team002", 0.5, possessions=70) is None
    assert game_tv_rating(q, "Team001", "Team002", None, possessions=70) is None
    assert quality_from_data(None) == {}
    assert quality_from_data({}) == {}
    assert quality_from_data({"ratings": pd.DataFrame()}) == {}


def test_quality_does_not_use_two_decimal_team_profile_columns():
    """A ratings frame must rank on full-precision AdjOE/AdjDE, even if a
    rounded AdjEM column would have tied the two leading teams."""
    ratings = pd.DataFrame({
        "TeamName": ["A", "B", "C"],
        "AdjOE": [125.885, 125.880, 100.0],
        "AdjDE": [90.4047, 90.4040, 110.0],
        # if someone rounded AdjEM to numeric(6,2) both A and B would be 35.48 / 35.48
    })
    q = quality_by_team(ratings)
    # B's AdjEM (35.476) is actually a hair *below* A's (35.4803)
    assert q["A"] > q["B"] > q["C"]
    assert q["A"] == 1.0


# ── writer wiring ────────────────────────────────────────────────────────────

def _game(**kw):
    g = dict(event_id="5", game_date="2026-11-21", kp_home="Team048", kp_away="Team025",
             neutral=False, state="pre",
             tip_utc=datetime(2026, 11, 22, 0, 0, tzinfo=timezone.utc),
             v2_home_margin=-1.5, v2_total=156.9, v2_home_win_prob=0.4478,
             v1_total=160.5, v1_home_margin=-2.0,
             current_home_spread=-1.5, current_total=160.5, proj_possessions=73.6)
    g.update(kw)
    return g


def test_sync_writes_same_tv_rating_on_v2_and_v1_from_card_numbers():
    now = datetime(2026, 11, 21, 7, 15, tzinfo=dr.CENTRAL)
    data = {"ratings": _league(365)}
    c = FakeClient()
    sw.sync([_game()], pd.DataFrame(columns=dr.LEDGER_COLS), None, now,
            log=lambda *_: None, client=c, data=data)
    rows = c.calls["projections"]
    v2 = next(p for p in rows if p["model_version"] == "czarp-v2")
    v1 = next(p for p in rows if p["model_version"] == sw.V1_VERSION)
    assert v2["home_win_prob"] == 0.4478
    assert v2["proj_possessions"] == 73.6
    assert v2["tv_rating"] == 77.3
    assert v1["tv_rating"] == 77.3          # same card numbers, both rows
    # Does not depend on Vegas lines: edges/cover may use them, tv_rating does not
    assert "current_home_spread" not in (Path(__file__).parents[1] / "czarp" / "tv_rating.py").read_text()


def test_sync_tv_rating_null_when_a_team_is_unrated_and_omitted_without_data():
    now = datetime(2026, 11, 21, 7, 15, tzinfo=dr.CENTRAL)
    data = {"ratings": _league(10)}          # Team025 / Team048 are not in a 10-team field
    c = FakeClient()
    sw.sync([_game()], pd.DataFrame(columns=dr.LEDGER_COLS), None, now,
            log=lambda *_: None, client=c, data=data)
    v2 = next(p for p in c.calls["projections"] if p["model_version"] == "czarp-v2")
    assert "tv_rating" in v2 and v2["tv_rating"] is None

    c2 = FakeClient()
    sw.sync([_game()], pd.DataFrame(columns=dr.LEDGER_COLS), None, now,
            log=lambda *_: None, client=c2, data=None)
    v2b = next(p for p in c2.calls["projections"] if p["model_version"] == "czarp-v2")
    assert "tv_rating" not in v2b           # do not null out an existing score


def test_sync_tv_rating_works_for_neutral_games():
    now = datetime(2026, 11, 21, 7, 15, tzinfo=dr.CENTRAL)
    c = FakeClient()
    sw.sync([_game(neutral=True, kp_home="Team003", kp_away="Team004",
                   v2_home_win_prob=0.52, proj_possessions=70.0, v2_home_margin=0.8, v2_total=140.0)],
            pd.DataFrame(columns=dr.LEDGER_COLS), None, now,
            log=lambda *_: None, client=c, data={"ratings": _league(10)})
    v2 = next(p for p in c.calls["projections"] if p["model_version"] == "czarp-v2")
    assert v2["tv_rating"] is not None
    assert 0 <= v2["tv_rating"] <= 100


def test_tv_rating_sql_adds_nullable_column_and_does_not_widen_grants_or_touch_picks():
    sql = (Path(__file__).parents[1] / "db" / "migrations" / "2026-11-21_tv_rating.sql").read_text()
    assert "add column if not exists tv_rating numeric(5,1)" in sql
    assert "p.tv_rating" in sql
    assert "alter table picks" not in sql.lower()
    assert "official_picks_locked" in sql          # only as a join on the existing view
    assert "hash" not in sql.lower() or "hash chain" in sql.lower()
    # grants: SELECT on the recreated view only; no INSERT/UPDATE/DELETE/TRUNCATE to anon
    assert "grant select on slate_games to anon, authenticated;" in sql
    for bad in ("grant insert", "grant update", "grant delete", "grant all", "grant truncate"):
        assert bad not in sql.lower()
    assert "alter default privileges" not in sql.lower()
    # not a betting input
    assert "vegas" in sql.lower()                  # comment says it does not use Vegas lines


def test_backfill_plan_skips_matching_rows_and_patches_only_tv_rating():
    from backfill_tv_rating import plan, quality_from_inputs

    teams = [{"id": 1, "name": "Team025"}, {"id": 2, "name": "Team048"}]
    # two more teams so n-1 matches a tiny field; we'll use the 365-team map instead
    q = quality_by_team(_league(365))
    games = [{"id": 10, "date": "2026-11-21", "home": "Team048", "away": "Team025",
              "neutral": False, "status": "scheduled"}]
    rating = game_tv_rating(q, "Team048", "Team025", 0.4478, possessions=73.6)
    assert rating == 77.3
    proj = [
        {"id": 100, "game_id": 10, "model_version": "czarp-v2", "created_at": "2026-11-21T13:00:00+00:00",
         "home_win_prob": 0.4478, "proj_possessions": 73.6, "total": 156.9, "tv_rating": None},
        {"id": 101, "game_id": 10, "model_version": "czarp-v1-totals", "created_at": "2026-11-21T13:00:00+00:00",
         "home_win_prob": None, "proj_possessions": None, "total": 160.5, "tv_rating": None},
    ]
    ups = plan(games, proj, q)
    assert {u["id"] for u in ups} == {100, 101}
    assert all(u["new"] == 77.3 for u in ups)
    # already correct → skipped
    for p in proj:
        p["tv_rating"] = 77.3
    assert plan(games, proj, q) == []
    # quality_from_inputs uses oe/de, not rounded profiles
    q2 = quality_from_inputs(
        [{"team_id": 1, "oe": 120.001, "de": 84.507},
         {"team_id": 2, "oe": 120.000, "de": 84.514}],
        [{"id": 1, "name": "High"}, {"id": 2, "name": "AlmostHigh"}],
    )
    assert q2["High"] > q2["AlmostHigh"]
