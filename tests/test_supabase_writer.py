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
