"""model_grades grades against the newest pre-tip Vegas current line (not max() over every current row)."""
import re
from pathlib import Path

SQL_PATH = Path(__file__).resolve().parents[1] / "db/migrations/2026-11-21_model_grades_pretip_line.sql"

COLUMNS = ("game_id", "league", "season", "game_date", "home", "away", "home_team_id", "away_team_id",
           "home_score", "away_score", "status", "model_version", "model_spread", "model_total", "proj_home",
           "proj_away", "proj_possessions", "upset_alert", "upset_reason", "reason", "vegas_spread", "vegas_total",
           "open_spread", "open_total", "current_spread", "current_total", "close_spread", "close_total",
           "model_ats_side", "model_ou_side", "actual_ats_cover", "actual_ou", "model_ats_result",
           "model_ou_result", "final_at", "final_at_estimated")


def _code(raw: str) -> str:
    return "\n".join(l for l in raw.lower().splitlines() if not l.strip().startswith("--"))


def test_replaces_view_in_place_with_grants():
    sql = _code(SQL_PATH.read_text()).strip()
    assert sql.startswith("begin;") and sql.endswith("commit;")
    assert "create or replace view model_grades with (security_invoker = true)" in sql
    assert "drop view" not in sql
    assert "revoke all on model_grades from anon, authenticated" in sql
    assert "grant select on model_grades to anon, authenticated" in sql
    assert "picks" not in sql.replace("official_picks_locked", "")      # never reads or writes picks


def test_current_line_is_newest_pre_tip_row_not_max():
    sql = _code(SQL_PATH.read_text())
    assert "filter (where ml.line_type = 'current'" not in sql            # the old max() over current rows
    assert sql.count("ml.fetched_at < g.tipoff") == 2                    # spread and total, both pre-tip
    assert sql.count("g.tipoff is null or ml.fetched_at < g.tipoff") == 2
    assert sql.count("order by ml.fetched_at desc, ml.id desc") == 2   # id breaks fetched_at ties
    assert "ml.spread is not null" in sql and "ml.total is not null" in sql
    assert "cs.spread::numeric as current_spread" in sql and "ct.total::numeric as current_total" in sql
    # fallback order unchanged: close, then current, then open
    assert "coalesce(v.close_spread, v.current_spread, v.open_spread)" in sql
    assert "coalesce(v.close_total, v.current_total, v.open_total)" in sql


def test_column_names_and_order_unchanged():
    sql = _code(SQL_PATH.read_text())
    select = sql[sql.index("\n select g.id as game_id"):]
    names = re.findall(r"(?:as\s+(\w+)\s*,?\s*$)|(?:^\s*(?:[gpv])\.(\w+)\s*,?\s*$)", select, flags=re.M)
    got = [a or b for a, b in names]
    assert tuple(got) == COLUMNS
