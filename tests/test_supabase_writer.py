from datetime import datetime, timezone

import pandas as pd

import daily_run as dr
from czarp import supabase_writer as sw


class FakeClient:
    def __init__(self):
        self.calls = {}

    def get(self, table, **params):
        return [{"id": 1}] if table == "sports" else []

    def upsert(self, table, rows, on_conflict=None, returning=True, ignore_duplicates=False):
        self.calls.setdefault(table, []).extend(rows)
        if table == "teams":
            return [{"name": r["name"], "id": i} for i, r in enumerate(rows)]
        if table == "games":
            return [{"external_id": r["external_id"], "id": 100 + i} for i, r in enumerate(rows)]
        return []

    def insert(self, table, rows):
        self.upsert(table, rows)

    def delete(self, table, **params):
        pass


def test_sync_tipoff_central_and_projection_rounding():
    now = datetime(2026, 11, 3, 7, 15, tzinfo=dr.CENTRAL)
    g = dict(event_id="5", game_date="2026-11-03", kp_home="H", kp_away="A", neutral=False, state="pre",
             tip_utc=datetime(2026, 11, 4, 0, 0, tzinfo=timezone.utc), v2_home_margin=7.2345, v2_total=143.678,
             v2_home_win_prob=0.73, v1_total=145.04, v1_home_margin=6.66, current_home_spread=-6.5, current_total=144.5)
    # an earlier-day ledger game not on today's slate: tipoff must come from tip_ct as Central time
    led = pd.DataFrame([dict(pick_id="4-total", created_ct="2026-11-02T07:36:45-06:00", game_date="2026-11-02",
                             event_id="4", tip_ct="2026-11-02 18:00", away="Air Force", home="Wright St.", neutral=False,
                             market="total", side="Under", line=162.5, price=-108, edge_pct=0.08, stake=8.2,
                             status="graded", home_score=102.0, away_score=71.0, result="loss", pnl=-8.2,
                             model_version="czarp-v1-totals")], columns=dr.LEDGER_COLS)
    c = FakeClient()
    sw.sync([g], led, None, now, log=lambda *_: None, client=c)
    games = {r["external_id"]: r for r in c.calls["games"]}
    assert games["4"]["tipoff"] == "2026-11-02T18:00:00-06:00"          # 6 PM CST, not UTC
    assert pd.Timestamp(games["4"]["tipoff"]).tz_convert("UTC").hour == 0
    assert games["5"]["tipoff"] == "2026-11-04T00:00:00+00:00"
    v2 = next(p for p in c.calls["projections"] if p["model_version"] == "czarp-v2")
    v1 = next(p for p in c.calls["projections"] if p["model_version"] == sw.V1_VERSION)
    assert v2["spread"] == -7.2 and v2["total"] == 143.7 and v2["proj_home"] == 75.5 and v2["proj_away"] == 68.2
    assert v1["total"] == 145.0 and v1["spread"] == -6.7


def test_new_picks_get_locked_post_fields_existing_rows_do_not():
    """New pick_keys get model_line / open_line / edge_vs_open; already-mirrored rows do not."""
    now = datetime(2026, 11, 9, 7, 15, tzinfo=dr.CENTRAL)
    g = dict(event_id="5", game_date="2026-11-09", kp_home="Northwestern", kp_away="Boston University",
             neutral=False, state="pre",
             tip_utc=datetime(2026, 11, 10, 1, 0, tzinfo=timezone.utc),
             v2_home_margin=18.1, v2_total=146.2, v1_total=142.0, v1_home_margin=11.0,
             current_home_spread=-16.5, current_total=144.5)
    # already-mirrored pick: FakeClient.get will list its pick_key
    old = dict(pick_id="4-total", created_ct="2026-11-02T07:36:45-06:00", game_date="2026-11-02",
               event_id="4", tip_ct="2026-11-02 18:00", away="Air Force", home="Wright St.", neutral=False,
               market="total", side="Under", line=162.5, price=-108, edge_pct=0.08, edge_pct_vs_open=0.06,
               open_line=165.5, model_value=149.0, stake=8.2, status="graded",
               home_score=102.0, away_score=71.0, result="loss", pnl=-8.2,
               model_version="czarp-v1-totals")
    new = dict(pick_id="5-total", created_ct="2026-11-09T07:15:53-06:00", game_date="2026-11-09",
               event_id="5", tip_ct="2026-11-09 19:00", away="Boston University", home="Northwestern",
               neutral=False, market="total", side="Under", line=144.5, price=-112,
               edge_pct=0.0173, edge_pct_vs_open=0.0373, open_line=147.5, model_value=142.0,
               stake=3.57, status="open", model_version="czarp-v1-totals")
    # a new side: model_value is picked-side margin, model_line stores the picked-side line
    side = dict(pick_id="5-spread", created_ct="2026-11-09T07:15:53-06:00", game_date="2026-11-09",
                event_id="5", tip_ct="2026-11-09 19:00", away="Boston University", home="Northwestern",
                neutral=False, market="spread", side="Boston University", line=16.5, price=-110,
                edge_pct=0.011, edge_pct_vs_open=0.007, open_line=17.5, model_value=-11.0,
                stake=2.0, status="open", model_version="czarp-v2")
    led = pd.DataFrame([old, new, side], columns=dr.LEDGER_COLS)
    c = FakeClient()
    orig_get = c.get

    def get(table, **params):
        if table == "picks":
            return [{"pick_key": "4-total"}]
        return orig_get(table, **params)

    c.get = get
    sw.sync([g], led, None, now, log=lambda *_: None, client=c)
    picks = {r["pick_key"]: r for r in c.calls["picks"]}
    assert "fair_at_post" not in picks["4-total"]
    assert "open_line" not in picks["4-total"]
    assert "edge_vs_open" not in picks["4-total"]
    bu = picks["5-total"]
    assert bu["fair_at_post"] == 142.0 and bu["line_at_post"] == 144.5 and bu["total_at_post"] == 144.5
    assert bu["open_line"] == 147.5 and bu["edge_vs_open"] == 0.0373
    assert bu["model_version"] == "czarp-v1-totals" and bu["line_or_price"] == 144.5
    sp = picks["5-spread"]
    assert sp["fair_at_post"] == 11.0 and sp["line_at_post"] == 16.5 and sp["open_line"] == 17.5


def test_official_picks_locked_sql_uses_pick_model_not_default():
    from pathlib import Path
    sql = (Path(__file__).parents[1] / "db" / "migrations" / "2026-11-09_fair_locks_skip_reasons.sql").read_text()
    view = sql[sql.index("create view official_picks_locked"):sql.index("grant select on official_picks_locked")]
    assert "pr.model_version = pk.model_version" in view
    assert "is_default" not in view
    for col in ("pick_id", "game_id", "market", "side", "model_name", "fair_at_post",
                "line_at_post", "total_at_post", "open_line", "edge_at_post_pct", "edge_vs_open_pct", "posted_at"):
        assert col in view
    vb = sql[sql.index("create view value_bets"):sql.index("grant select on value_bets")]
    assert "pk.logged_at" in vb and "pk.pick_hash" in vb
    assert "lk.fair_at_post" in vb and "lk.total_at_post" in vb and "lk.model_name" in vb
    assert "lock_sides_games" in sql and "lock_totals_games" in sql
    assert "skip_reasons_spread" in sql and "skip_reason_total" in sql


def test_projections_merge_upsert_on_unique_key():
    """A re-run must fill reason/pace/upset/skip fields, not ignore the existing row."""
    now = datetime(2026, 11, 10, 7, 15, tzinfo=dr.CENTRAL)
    g = dict(event_id="5", game_date="2026-11-10", kp_home="H", kp_away="A", neutral=False, state="pre",
             tip_utc=datetime(2026, 11, 11, 0, 0, tzinfo=timezone.utc), v2_home_margin=3.0, v2_total=140.0,
             v1_total=141.0, current_home_spread=-2.5, current_total=145.5, reason="why", proj_possessions=70.04,
             skip_reasons_total=["line_moved_against"], skip_reason_total="total moved 6 pts against our side")
    seen = []

    class C(FakeClient):
        def upsert(self, table, rows, on_conflict=None, returning=True, ignore_duplicates=False):
            if table == "projections":
                seen.append((on_conflict, ignore_duplicates))
            return super().upsert(table, rows, on_conflict, returning, ignore_duplicates)

        def insert(self, table, rows):
            assert table != "projections", "projections must not use ignore-duplicates insert"
            super().insert(table, rows)

    c = C()
    sw.sync([g], pd.DataFrame(columns=dr.LEDGER_COLS), None, now, log=lambda *_: None, client=c)
    assert seen == [(sw.PROJ_KEY, False)]
    assert sw.PROJ_KEY == "game_id,model_version,created_at"
    v2 = next(p for p in c.calls["projections"] if p["model_version"] == "czarp-v2")
    assert v2["reason"] == "why" and v2["proj_possessions"] == 70.0
    assert v2["skip_reasons_total"] == ["line_moved_against"]


def test_fill_pick_locks_only_null_columns():
    patches = []

    class C:
        def get(self, table, **params):
            if table == "picks":
                return [{"pick_key": "a", "fair_at_post": None, "line_at_post": None, "total_at_post": None,
                         "open_line": None, "edge_vs_open": None},
                        {"pick_key": "b", "fair_at_post": 150.0, "line_at_post": 151.5, "total_at_post": 151.5,
                         "open_line": None, "edge_vs_open": 0.04},
                        {"pick_key": "c", "fair_at_post": 1.0, "line_at_post": 2.0, "total_at_post": 3.0,
                         "open_line": 4.0, "edge_vs_open": 0.05}]
            if table == "official_picks_locked":
                return [{"pick_id": "a", "fair_at_post": 142.0, "line_at_post": 144.5, "total_at_post": 144.5,
                         "open_line": 147.5, "edge_vs_open_pct": 0.0373},
                        {"pick_id": "b", "fair_at_post": 999.0, "line_at_post": 999.0, "total_at_post": 999.0,
                         "open_line": 153.5, "edge_vs_open_pct": 0.9},
                        {"pick_id": "c", "fair_at_post": 9.0, "line_at_post": 9.0, "total_at_post": 9.0,
                         "open_line": 9.0, "edge_vs_open_pct": 0.9}]
            return []

        def patch(self, table, row, **params):
            assert table == "picks"
            (col,) = row
            assert params[col] == "is.null"
            patches.append((params["pick_key"], col, row[col]))

    n = sw.fill_pick_locks(C(), log=lambda *_: None)
    assert n == 2
    assert ("eq.a", "edge_vs_open", 0.0373) in patches and ("eq.a", "fair_at_post", 142.0) in patches
    assert [p for p in patches if p[0] == "eq.b"] == [("eq.b", "open_line", 153.5)]   # stored values never overwritten
    assert not [p for p in patches if p[0] == "eq.c"]
