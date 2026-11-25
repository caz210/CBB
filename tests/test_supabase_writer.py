from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

import daily_run as dr
from czarp import supabase_writer as sw


class FakeClient:
    def __init__(self):
        self.calls = {}
        self._team_ids = {}

    def get(self, table, **params):
        return [{"id": 1}] if table == "sports" else []

    def upsert(self, table, rows, on_conflict=None, returning=True, ignore_duplicates=False):
        self.calls.setdefault(table, []).extend(rows)
        if table == "teams":
            out = []
            for r in rows:
                if r["name"] not in self._team_ids:
                    self._team_ids[r["name"]] = len(self._team_ids) + 1
                out.append({"name": r["name"], "id": self._team_ids[r["name"]]})
            return out
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
    # ledger scores are not mirrored (they may be slot-flipped); sync_finals owns finals
    assert "home_score" not in games["4"] and "away_score" not in games["4"]
    # but team_ids must be set so the game attaches to both team logs
    assert games["4"]["home_team_id"] == c._team_ids["Wright St."]
    assert games["4"]["away_team_id"] == c._team_ids["Air Force"]
    v2 = next(p for p in c.calls["projections"] if p["model_version"] == "czarp-v2")
    v1 = next(p for p in c.calls["projections"] if p["model_version"] == sw.V1_VERSION)
    assert v2["spread"] == -7.2 and v2["total"] == 143.7 and v2["proj_home"] == 76.1 and v2["proj_away"] == 68.9
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
             skip_reasons_total=["line_moved_against"], skip_reason_total="total moved 6 pts against our side",
             edge_now_pct_spread=0.045012, edge_vs_open_pct_spread=0.025, edge_now_pct_total=0.01,
             edge_vs_open_pct_total=0.02)
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
    assert v2["edge_now_pct_spread"] == 0.045 and v2["edge_vs_open_pct_spread"] == 0.025
    assert v2["edge_now_pct_total"] == 0.01 and v2["edge_vs_open_pct_total"] == 0.02
    v1 = next(p for p in c.calls["projections"] if p["model_version"] == sw.V1_VERSION)
    assert v1["edge_now_pct_spread"] == 0.045 and v1["edge_vs_open_pct_total"] == 0.02


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
            if table == "picks_locked":
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


def test_stored_edges_sql_columns_and_view_logic():
    from pathlib import Path
    sql = (Path(__file__).parents[1] / "db" / "migrations" / "2026-11-10_stored_edges.sql").read_text()
    for col in ("edge_now_pct_spread", "edge_vs_open_pct_spread",
                "edge_now_pct_total", "edge_vs_open_pct_total"):
        assert f"add column if not exists {col} numeric(6,4)" in sql
        assert f"p.{col}" in sql
    assert "verdict_spread" in sql and "verdict_total" in sql
    assert "lean_spread" in sql and "lean_total" in sql
    assert "'official'" in sql and "'low_value'" in sql
    assert "pk.tier = 'official'" in sql
    assert "edge_now_pct_spread >= 0.04" in sql and "edge_vs_open_pct_spread < 0.04" in sql
    assert "edge_now_pct_total >= 0.03" in sql and "edge_vs_open_pct_total < 0.03" in sql
    assert "numeric(6,4)" in sql
    # must not rewrite picks / the ledger
    assert "alter table picks" not in sql.lower()


def _cents(x):
    return round(x * 100)


def test_team_scores_sum_to_czarp_total_and_gap_is_czarp_spread():
    # CZarp total = v1 total (rounded as stored), CZarp spread = default-model margin (rounded as stored)
    for hm, t2, t1 in [(9.8, 159.8, 165.0), (-1.9, 140.6, 135.5), (5.7, 164.5, 169.0), (7.25, 150.0, 145.5),
                       (-22.149, 154.3, 154.0), (0.0, 131.8, 130.5), (13.3, 152.2, 152.04)]:
        h, a = sw.team_scores(hm, t2, t1)
        assert _cents(h + a) == _cents(round(t1, 1))
        assert _cents(h - a) == _cents(round(hm, 1))           # home - away == home margin == -spread
    assert sw.team_scores(9.8, 159.8, 165.0) == (87.4, 77.6)   # Mercer / Charleston Southern, Nov 21
    assert sw.team_scores(-1.0, 150.0, 145.5) == (72.25, 73.25)  # .x5 case keeps both identities exact


def test_team_scores_fall_back_to_default_total_without_v1():
    for v1 in (None, float("nan")):
        h, a = sw.team_scores(7.2345, 143.678, v1)
        assert _cents(h + a) == _cents(143.7) and _cents(h - a) == _cents(7.2)


def test_sync_team_scores_match_czarp_total_and_spread_incl_neutral():
    now = datetime(2026, 11, 21, 7, 13, tzinfo=dr.CENTRAL)
    base = dict(game_date="2026-11-21", state="pre", tip_utc=datetime(2026, 11, 22, 0, 0, tzinfo=timezone.utc),
                v2_home_win_prob=0.7, current_home_spread=-8.5, current_total=161.5)
    gs = [dict(base, event_id="1", kp_home="Mercer", kp_away="Charleston Southern", neutral=False,
               v2_home_margin=9.8031, v2_total=159.81, v1_total=165.0, v1_home_margin=9.0),
          # neutral floor: the margin carries no home edge; same rule, home/away as listed
          dict(base, event_id="2", kp_home="San Diego", kp_away="UC Irvine", neutral=True,
               v2_home_margin=-5.6449, v2_total=147.02, v1_total=148.5, v1_home_margin=-5.0),
          # no v1 total: scores follow the default model's own total
          dict(base, event_id="3", kp_home="X", kp_away="Y", neutral=False,
               v2_home_margin=3.33, v2_total=140.04, v1_total=None)]
    c = FakeClient()
    sw.sync(gs, pd.DataFrame(columns=dr.LEDGER_COLS), None, now, log=lambda *_: None, client=c)
    gid = {r["external_id"]: 100 + i for i, r in enumerate(c.calls["games"])}   # FakeClient ids
    pr = c.calls["projections"]
    v2 = {p["game_id"]: p for p in pr if p["model_version"] == "czarp-v2"}
    v1 = {p["game_id"]: p for p in pr if p["model_version"] == sw.V1_VERSION}
    for eid in ("1", "2"):
        p, t = v2[gid[eid]], v1[gid[eid]]["total"]
        assert _cents(p["proj_home"] + p["proj_away"]) == _cents(t)
        assert _cents(p["proj_home"] - p["proj_away"]) == _cents(-p["spread"])
    p3 = v2[gid["3"]]
    assert _cents(p3["proj_home"] + p3["proj_away"]) == _cents(p3["total"])
    assert _cents(p3["proj_home"] - p3["proj_away"]) == _cents(-p3["spread"])
    # stored total / spread columns unchanged by the reconciliation
    assert (v2[gid["1"]]["total"], v2[gid["1"]]["spread"]) == (159.8, -9.8)
    assert (v2[gid["2"]]["total"], v2[gid["2"]]["spread"]) == (147.0, 5.6)


def _lock_client(missing_view: bool, err: str = "PGRST205"):
    calls = []

    class C:
        def get(self, table, **params):
            calls.append(table)
            if table == "picks":
                return [{"pick_key": "lean1", "fair_at_post": None, "line_at_post": -2.5, "total_at_post": 146.0,
                         "open_line": -3.0, "edge_vs_open": 0.03}]
            if table == "picks_locked":
                if missing_view:
                    raise RuntimeError(f"picks_locked: HTTP 404 {{\"code\":\"{err}\",\"message\":\"Could not find the table\"}}")
                return [{"pick_id": "lean1", "fair_at_post": -6.8, "line_at_post": -2.5, "total_at_post": 146.0,
                         "open_line": -3.0, "edge_vs_open_pct": 0.03}]
            if table == "official_picks_locked":
                return [{"pick_id": "lean1", "fair_at_post": -7.0, "line_at_post": -2.5, "total_at_post": 146.0,
                         "open_line": -3.0, "edge_vs_open_pct": 0.03}]
            return []

        def patch(self, table, row, **params):
            calls.append(("patch", params["pick_key"], dict(row)))

    return C(), calls


def test_fill_pick_locks_reads_all_tier_picks_locked():
    """official_picks_locked is official-only (2026-11-25); lean/track locks come from picks_locked."""
    c, calls = _lock_client(missing_view=False)
    assert sw.fill_pick_locks(c, log=lambda *_: None) == 1
    assert "official_picks_locked" not in calls
    assert ("patch", "eq.lean1", {"fair_at_post": -6.8}) in calls


@pytest.mark.parametrize("err", ["PGRST205", "42P01"])
def test_fill_pick_locks_falls_back_before_migration(err):
    c, calls = _lock_client(missing_view=True, err=err)
    assert sw.fill_pick_locks(c, log=lambda *_: None) == 1
    assert calls[:3] == ["picks", "picks_locked", "official_picks_locked"]
    assert ("patch", "eq.lean1", {"fair_at_post": -7.0}) in calls


def test_fill_pick_locks_other_errors_raise():
    class C:
        def get(self, table, **params):
            if table == "picks":
                return [{"pick_key": "x", "fair_at_post": None}]
            raise RuntimeError(f"{table}: HTTP 500 boom")
    with pytest.raises(RuntimeError, match="500"):
        sw.fill_pick_locks(C(), log=lambda *_: None)


def test_official_picks_locked_official_only_migration():
    raw = (Path(__file__).parents[1] / "db" / "migrations" / "2026-11-25_official_picks_locked_official_only.sql").read_text()
    sql = "\n".join(line.split("--")[0] for line in raw.splitlines())   # code only, no comments
    opl = sql[sql.index("create or replace view official_picks_locked"):]
    opl = opl[:opl.index(";")]
    assert "from picks_locked" in opl and "where tier = 'official'" in opl
    vb = sql[sql.index("create or replace view value_bets"):]
    vb = vb[:vb.index(";")]
    assert "left join picks_locked lk" in vb and "official_picks_locked" not in vb
    low = sql.lower()
    for bad in ("drop view", "drop trigger", "picks_guard", "picks_immutable()", "update picks", "delete from picks", "insert into picks"):
        assert bad not in low
