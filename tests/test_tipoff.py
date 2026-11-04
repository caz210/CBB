from datetime import datetime, timezone
from pathlib import Path

from czarp.supabase_writer import _tip_ct_iso, tipoff_iso


def test_real_7pm_et_tip_at_midnight_utc_is_kept():
    # 00:00Z = 7 PM ET / 6 PM CT: a normal tip, not a placeholder (games 7, 20)
    assert tipoff_iso(datetime(2026, 11, 4, 0, 0, tzinfo=timezone.utc)) == "2026-11-04T00:00:00+00:00"


def test_tbd_flag_drops_time():
    assert tipoff_iso(datetime(2026, 11, 4, 23, tzinfo=timezone.utc), time_tbd=True) is None


def test_midnight_local_placeholders_dropped():
    assert tipoff_iso(datetime(2026, 11, 4, 6, 0, tzinfo=timezone.utc)) is None  # 00:00 CST
    assert tipoff_iso(datetime(2026, 11, 4, 5, 0, tzinfo=timezone.utc)) is None  # 00:00 EST
    assert tipoff_iso(datetime(2026, 3, 10, 4, 0, tzinfo=timezone.utc)) is None  # 00:00 EDT


def test_date_only_and_naive_dropped():
    assert tipoff_iso("2026-11-04") is None
    assert tipoff_iso(datetime(2026, 11, 4, 19)) is None
    assert tipoff_iso(None) is None
    assert _tip_ct_iso("2026-11-04") is None
    assert _tip_ct_iso(float("nan")) is None


def test_tip_ct_string_converted():
    assert _tip_ct_iso("2026-11-03 18:00") == "2026-11-03T18:00:00-06:00"


def test_views_use_logged_at_and_pick_hash():
    sql = (Path(__file__).parents[1] / "db" / "schema.sql").read_text()
    vb = sql[sql.index("create view value_bets"):sql.index("create view ledger")]
    assert "pk.logged_at" in vb and "pk.pick_hash" in vb and "pk.created_at" not in vb
    ld = sql[sql.index("create view ledger"):]
    ld = ld[:ld.index(";")]
    assert ld.rstrip().split("from picks")[0].strip().endswith("pk.logged_at                                    -- when the pick was written to the ledger")
