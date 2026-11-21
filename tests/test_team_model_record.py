"""team_model_record: model ATS uses every graded final, same as O/U."""
from pathlib import Path

SQL_PATH = (
    Path(__file__).resolve().parents[1]
    / "db/migrations/2026-11-21_team_model_record_all_games_ats.sql"
)


def _code(raw: str) -> str:
    return "\n".join(l for l in raw.lower().splitlines() if not l.strip().startswith("--"))


def test_migration_replaces_view_with_all_games_ats():
    raw = SQL_PATH.read_text()
    sql = _code(raw).strip()
    assert sql.startswith("begin;")
    assert sql.endswith("commit;")
    assert "create or replace view team_model_record with (security_invoker = true)" in sql
    assert "revoke all on team_model_record from anon, authenticated" in sql
    assert "grant select on team_model_record to anon, authenticated" in sql
    # Recreating with OR REPLACE keeps the view identity; do not DROP.
    assert "drop view" not in sql
    # ATS counts every graded final, same filter as O/U — not only when
    # CZarp's spread pick was this team.
    assert "model_picked_this_team_ats" not in sql
    assert "ats_when_picked" not in sql
    assert "where ats is not null" in sql
    assert "where ou is not null" in sql
    # Official-pick attribution stays the live LATERAL match.
    assert "pk.tier = 'official'" in sql
    assert "pk.result is distinct from 'void'" in sql
    assert "cross join lateral" in sql
    assert "pk.bet ~~* (g.home || ' %')" in sql
    assert "pk.bet ~~* (g.away || ' %')" in sql
    for col in (
        "team_id",
        "team_name",
        "league",
        "season",
        "model_ats_wins",
        "model_ats_losses",
        "model_ats_pushes",
        "model_ou_wins",
        "model_ou_losses",
        "model_ou_pushes",
        "official_wins",
        "official_losses",
        "official_pushes",
        "official_ats_wins",
        "official_ats_losses",
        "official_ou_wins",
        "official_ou_losses",
    ):
        assert col in sql
    assert "grant insert" not in sql
    assert "grant update" not in sql
    assert "grant all" not in sql
    assert "alter default privileges" not in sql
