from datetime import datetime, timedelta, timezone

import pandas as pd

import daily_run as dr
import pretip_check as pc
from czarp import big_moves as BM


def _g(**kw):
    g = dict(event_id="10", game_date="2026-11-10", tip_ct="2026-11-10 19:00", kp_home="Texas Tech", kp_away="Illinois",
             home="Texas Tech", away="Illinois", neutral=False, state="pre",
             tip_utc=datetime(2026, 11, 11, 1, 0, tzinfo=timezone.utc),
             open_total=161.5, current_total=167.5, open_home_spread=-2.5, current_home_spread=-3.0,
             v1_total=154.5, v2_total=156.0, v2_home_margin=2.0)
    g.update(kw)
    return g


def test_thresholds_either_direction():
    assert BM.is_big("total", 161.5, 165.5) and BM.is_big("total", 161.5, 157.5)
    assert not BM.is_big("total", 161.5, 165.0)
    assert BM.is_big("spread", -17.5, -14.5) and BM.is_big("spread", 3.0, 0.0)
    assert not BM.is_big("spread", -17.5, -15.0)
    assert not BM.is_big("total", None, 170)


def test_build_row_total_against_lean_and_skip_reason():
    g = _g()
    r = BM.build_row(g, "total")
    assert r["move"] == 6.0 and r["direction"] == "up" and r["move_side"] == "Over"
    assert r["czarp_line"] == 154.5 and r["czarp_side"] == "Under" and r["against_czarp"] is True
    assert r["skipped"] and "AGAINST" in r["note"]
    assert BM.build_row(g, "spread") is None                     # 0.5 pt spread move is not big


def test_build_row_spread_direction_and_czarp_side():
    g = _g(kp_home="UCF", kp_away="South Alabama", open_home_spread=-17.5, current_home_spread=-14.5,
           v2_home_margin=21.3, open_total=150, current_total=151)
    r = BM.build_row(g, "spread")
    assert r["move"] == 3.0 and r["direction"] == "toward South Alabama"
    assert r["czarp_line"] == -21.3 and r["czarp_side"] == "UCF" and r["against_czarp"] is True


def test_detect_uses_candidate_reasons_and_official_flag(monkeypatch):
    monkeypatch.setattr(dr, "COACH_LOCK_GAMES", 0)
    monkeypatch.setattr(dr, "COACH_LOCK_TOTALS", 0)
    monkeypatch.setattr(dr, "COACH_LOCK_SIDES", 0)
    g = _g()
    now = datetime(2026, 11, 10, 7, 15, tzinfo=dr.CENTRAL)
    cands = dr.candidates([g], now)
    rows = BM.detect([g], cands, played_ids=())
    assert len(rows) == 1 and "total moved 6 pts against our side" in rows[0]["skip_reason"]
    rows = BM.detect([g], cands, played_ids={"10-total"})
    assert rows[0]["official_pick"] and rows[0]["skip_reason"] is None and "official play" in rows[0]["note"]
    assert BM.detect([dict(g, skip="non-D1")], cands) == []


def test_top10_note_and_report_section(monkeypatch):
    monkeypatch.setattr(dr, "COACH_LOCK_GAMES", 0)
    monkeypatch.setattr(dr, "COACH_LOCK_TOTALS", 0)
    monkeypatch.setattr(dr, "COACH_LOCK_SIDES", 0)
    g = _g()
    now = datetime(2026, 11, 10, 7, 15, tzinfo=dr.CENTRAL)
    top = dr.top_edges(dr.candidates([g], now), set())
    tot = next(r for r in top if r["market"] == "total")
    assert any(x.startswith("Big move: up 6 pts (161.5 → 167.5) — against our lean") for x in tot["reasons"])
    sp = next(r for r in top if r["market"] == "spread")
    assert not any(x.startswith("Big move") for x in sp["reasons"])
    md = "\n".join(BM.report_md(BM.detect([g], dr.candidates([g], now)), {"all": {"graded": 3, "with_move_w": 2, "with_move_l": 1,
            "with_move_p": 0, "with_czarp_w": 1, "with_czarp_l": 2, "against_czarp_w": 1, "against_czarp_l": 1}}))
    assert "## Big moves" in md and "Illinois @ Texas Tech" in md and "6 up" in md and "⚠" in md
    assert "followed the move 2-1" in md and "CZarp's lean 1-2" in md
    assert "No big moves" in "\n".join(BM.report_md([]))


def test_grade_row_vs_current_line():
    r = dict(market="total", current_line=167.5, move_side="Over", czarp_side="Under", home="Texas Tech", away="Illinois")
    out = BM.grade_row(r, 80, 70)                                  # 150 under 167.5
    assert out["landed_with_move"] is False and out["landed_with_czarp"] is True and out["final_value"] == 150
    assert BM.grade_row(dict(r, current_line=150.0), 80, 70)["landed_with_move"] is None   # push
    s = dict(market="spread", current_line=-14.5, move_side="South Alabama", czarp_side="UCF", home="UCF", away="South Alabama")
    out = BM.grade_row(s, 80, 70)                                  # UCF by 10, did not cover -14.5
    assert out["landed_with_move"] is True and out["landed_with_czarp"] is False


class FakeBM:
    def __init__(self, rows):
        self.rows, self.patches, self.upserts = rows, [], []

    def get(self, table, **params):
        assert table == "big_moves" and params["graded_at"] == "is.null"
        return self.rows

    def patch(self, table, row, **params):
        assert table == "big_moves" and params["graded_at"] == "is.null"
        self.patches.append((params["id"], row))

    def upsert(self, table, rows, on_conflict=None, returning=True, ignore_duplicates=False):
        self.upserts.append((table, rows, on_conflict, ignore_duplicates))
        return []


def test_grade_pending_live_gets_close_backfill_does_not():
    now = datetime(2026, 11, 11, 7, 15, tzinfo=dr.CENTRAL)
    gm = dict(home="Texas Tech", away="Illinois", status="final", home_score=90, away_score=81, external_id="10")
    rows = [dict(id=1, market="total", current_line=167.5, move_side="Over", czarp_side="Under", source="live",
                 close_line=None, games=gm),
            dict(id=2, market="total", current_line=160.5, move_side="Over", czarp_side="Under", source="backfill",
                 close_line=None, games=gm),
            dict(id=3, market="total", current_line=160.5, move_side="Over", czarp_side="Under", source="live",
                 close_line=None, games=dict(gm, status="scheduled")),
            dict(id=4, market="spread", current_line=-3.0, move_side="Texas Tech", czarp_side=None, source="live",
                 close_line=None, games=dict(gm, status="postponed"))]
    c = FakeBM(rows)
    seen = []
    n = BM.grade_pending(c, now, lines_for=lambda e: seen.append(e) or {"close_total": 169.5}, log=lambda *_: None)
    assert n == 3 and seen == ["10"]
    p = dict(c.patches)
    assert p["eq.1"]["close_line"] == 169.5 and p["eq.1"]["close_available"] is True
    assert p["eq.1"]["landed_with_move"] is True and p["eq.1"]["landed_with_czarp"] is False   # 171 > 167.5
    assert "close_line" not in p["eq.2"]
    assert "eq.3" not in p and p["eq.4"]["result_text"] == "void"


def test_write_rows_first_report_wins_and_backfill_marks_close():
    c = FakeBM([])
    g = _g()
    rows = BM.detect([g])
    now = datetime(2026, 11, 10, 7, 15, tzinfo=dr.CENTRAL)
    assert BM.write_rows(c, rows, {"10": 277}, now) == 1
    table, sent, key, ign = c.upserts[0]
    assert table == "big_moves" and key == "game_id,market,game_date" and ign is True
    assert sent[0]["game_id"] == 277 and sent[0]["source"] == "live" and "close_available" not in sent[0]
    assert BM.db_row(rows[0], 277, now, source="backfill")["close_available"] is False


def test_pretip_big_moves_any_game_within_window(monkeypatch, tmp_path, capsys):
    now = datetime(2026, 11, 10, 17, 45, tzinfo=dr.CENTRAL)
    soon = dict(event_id="10", game_date="2026-11-10", tip_ct="2026-11-10 19:00", home="Texas Tech", away="Illinois",
                state="pre", neutral=False, tip_utc=(now + timedelta(minutes=75)).astimezone(timezone.utc))
    late = dict(soon, event_id="11", tip_utc=(now + timedelta(minutes=200)).astimezone(timezone.utc))
    path = tmp_path / "ledger.csv"
    pd.DataFrame(columns=dr.LEDGER_COLS).to_csv(path, index=False)
    monkeypatch.setattr(dr, "LEDGER", str(path))
    monkeypatch.setattr(dr, "LEDGER_DIR", str(tmp_path))
    monkeypatch.setattr(dr, "SNAP_DIR", str(tmp_path / "snap"))
    monkeypatch.setattr(BM, "CTX_DIR", str(tmp_path / "ctx"))
    monkeypatch.setattr(dr, "now_ct", lambda: now)
    monkeypatch.setattr(dr, "espn_scoreboard", lambda d: [soon, late])
    monkeypatch.setattr(dr, "espn_dk_lines", lambda e: {"open_total": 161.5, "current_total": 167.5,
                                                         "open_home_spread": -2.5, "current_home_spread": -3.0})
    pc.main(["--dry-run"])
    out = capsys.readouterr().out
    assert "Big moves 2026-11-10" in out and out.count("Illinois @ Texas Tech") == 1 and "161.5 → 167.5 (6 up)" in out
    monkeypatch.setattr(dr, "espn_dk_lines", lambda e: {"open_total": 161.5, "current_total": 163.5})
    pc.main(["--dry-run"])
    assert capsys.readouterr().out == ""                          # nothing big -> empty output


def test_migration_and_view_shape():
    from pathlib import Path
    sql = (Path(__file__).parents[1] / "db" / "migrations" / "2026-11-10_big_moves.sql").read_text()
    for col in ("game_id", "market", "open_line", "current_line", "close_line", "move", "direction", "czarp_line",
                "czarp_side", "skipped", "skip_reason", "result_text", "landed_with_move", "landed_with_czarp", "note",
                "close_available", "unique (game_id, market, game_date)"):
        assert col in sql
    assert "create or replace view big_moves_record" in sql
    assert "picks" not in sql.split("create table")[1].split(";")[0]      # never references the picks ledger


def test_backfill_build_uses_morning_current_and_stored_skip_reason():
    import backfill_big_moves as bf
    games = [dict(id=1, external_id="10", date="2026-11-10", home="Texas Tech", away="Illinois",
                  tipoff="2026-11-11T01:00:00+00:00", neutral=False)]
    lines = [dict(game_id=1, line_type="open", spread=-2.5, total=161.5, fetched_at="2026-11-09T13:15:00+00:00"),
             dict(game_id=1, line_type="current", spread=-2.5, total=163.0, fetched_at="2026-11-09T13:15:00+00:00"),
             dict(game_id=1, line_type="open", spread=-2.5, total=161.5, fetched_at="2026-11-10T13:15:00+00:00"),
             dict(game_id=1, line_type="current", spread=-3.0, total=167.5, fetched_at="2026-11-10T13:15:00+00:00"),
             dict(game_id=1, line_type="current", spread=-3.0, total=170.5, fetched_at="2026-11-10T22:00:00+00:00")]
    projs = [dict(game_id=1, model_version="czarp-v1-totals", spread=None, total=154.5,
                  created_at="2026-11-10T13:15:00+00:00", skip_reason_total="total moved 6 pts against our side"),
             dict(game_id=1, model_version="czarp-v2", spread=-2.0, total=156.0,
                  created_at="2026-11-10T13:15:00+00:00", skip_reason_spread=None, skip_reason_total=None)]
    out = bf.build(games, lines, projs, picks=[])
    assert len(out) == 1
    gid, r = out[0]
    assert gid == 1 and r["current_line"] == 167.5 and r["open_line"] == 161.5   # morning, not the 4 PM line
    assert r["czarp_line"] == 154.5 and r["skip_reason"] == "total moved 6 pts against our side"
    assert "close unavailable" in r["note"]
    out = bf.build(games, lines, projs, picks=[dict(game_id=1, market="total", tier="official", result=None)])
    assert out[0][1]["official_pick"] and out[0][1]["skip_reason"] is None
