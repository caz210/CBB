"""Backfill of PR #23 stored edges: same formulas as attach_market_edges, NULL-only."""
from tests.test_daily_run import _game, _now

import backfill_edges as be
import daily_run as dr
from czarp.supabase_writer import DEFAULT_VERSION, V1_VERSION, _edge_fields


def test_build_patches_matches_attach_market_edges_and_skips_openless():
    # UCF-shaped lean: 4.5% now, 2.5% vs open. Latest v2 + v1 both need a fill.
    games = [
        dict(id=1, external_id="9", date="2026-11-09", home="UCF", away="South Alabama",
             tipoff="2026-11-09T23:00:00+00:00"),
        dict(id=2, external_id="8", date="2026-11-09", home="H2", away="A2"),  # no open line
        dict(id=3, external_id="7", date="2026-11-08", home="H3", away="A3"),  # already filled
    ]
    lines = [
        dict(id=10, game_id=1, line_type="open", spread=-6.2, total=160.0, fetched_at="2026-11-09T12:00:00+00:00"),
        dict(id=11, game_id=1, line_type="current", spread=-3.0, total=160.0, fetched_at="2026-11-09T18:00:00+00:00"),
        dict(id=12, game_id=2, line_type="current", spread=-4.5, total=140.0, fetched_at="2026-11-09T18:00:00+00:00"),
        dict(id=13, game_id=3, line_type="open", spread=-4.0, total=140.0, fetched_at="2026-11-08T12:00:00+00:00"),
        dict(id=14, game_id=3, line_type="current", spread=-4.5, total=140.0, fetched_at="2026-11-08T18:00:00+00:00"),
    ]
    # older v2 on game 1 is ignored; latest is filled
    projs = [
        dict(id=100, game_id=1, model_version=DEFAULT_VERSION, spread=-9.0, total=160.0,
             created_at="2026-11-09T13:00:00+00:00",
             edge_now_pct_spread=None, edge_vs_open_pct_spread=None,
             edge_now_pct_total=None, edge_vs_open_pct_total=None),
        dict(id=101, game_id=1, model_version=DEFAULT_VERSION, spread=-10.2, total=160.0,
             created_at="2026-11-09T14:00:00+00:00",
             edge_now_pct_spread=None, edge_vs_open_pct_spread=None,
             edge_now_pct_total=None, edge_vs_open_pct_total=None),
        dict(id=102, game_id=1, model_version=V1_VERSION, spread=None, total=161.0,
             created_at="2026-11-09T14:00:00+00:00",
             edge_now_pct_spread=None, edge_vs_open_pct_spread=None,
             edge_now_pct_total=None, edge_vs_open_pct_total=None),
        dict(id=200, game_id=3, model_version=DEFAULT_VERSION, spread=-11.0, total=140.0,
             created_at="2026-11-08T14:00:00+00:00",
             edge_now_pct_spread=0.0464, edge_vs_open_pct_spread=0.05,
             edge_now_pct_total=0.0071, edge_vs_open_pct_total=0.0071),
    ]
    patches = be.build_patches(games, lines, projs)
    assert {p["proj_id"] for p in patches} == {101, 102}
    assert all(p["date"] == "2026-11-09" for p in patches)
    lean = _game(v2_home_margin=10.2, current_home_spread=-3.0, open_home_spread=-6.2,
                 current_total=160.0, open_total=160.0, v1_total=161.0)
    dr.attach_market_edges([lean], _now())
    expected = _edge_fields(lean)
    for p in patches:
        assert p["fields"] == expected
        assert abs(p["fields"]["edge_now_pct_spread"] - 0.045) < 1e-9
        assert abs(p["fields"]["edge_vs_open_pct_spread"] - 0.025) < 1e-9
    assert be.slate_counts(patches) == {"2026-11-09": 1}


def test_build_patches_fills_only_null_columns():
    games = [dict(id=1, external_id="9", date="2026-11-09", home="H", away="A")]
    lines = [
        dict(id=1, game_id=1, line_type="open", spread=-4.0, total=140.0, fetched_at="2026-11-09T12:00:00+00:00"),
        dict(id=2, game_id=1, line_type="current", spread=-4.5, total=140.0, fetched_at="2026-11-09T18:00:00+00:00"),
    ]
    projs = [dict(id=10, game_id=1, model_version=DEFAULT_VERSION, spread=-11.0, total=140.0,
                  created_at="2026-11-09T14:00:00+00:00",
                  edge_now_pct_spread=0.1111, edge_vs_open_pct_spread=None,
                  edge_now_pct_total=None, edge_vs_open_pct_total=None)]
    patches = be.build_patches(games, lines, projs)
    assert len(patches) == 1
    assert "edge_now_pct_spread" not in patches[0]["fields"]
    assert patches[0]["fields"]["edge_vs_open_pct_spread"] == 0.05   # |11-4|/140


def test_apply_patches_null_guard():
    patches_sent = []

    class C:
        def patch(self, table, row, **params):
            assert table == "projections"
            (col,) = row
            assert params[col] == "is.null"
            patches_sent.append((params["id"], col, row[col]))

    n = be.apply_patches(C(), [
        dict(proj_id=101, fields={"edge_now_pct_spread": 0.045, "edge_vs_open_pct_spread": 0.025}),
    ], log=lambda *_: None)
    assert n == 1
    assert patches_sent == [
        ("eq.101", "edge_now_pct_spread", 0.045),
        ("eq.101", "edge_vs_open_pct_spread", 0.025),
    ]
