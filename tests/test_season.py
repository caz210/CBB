from datetime import date

from czarp.season import season_for_date, season_label, season_progress


def test_season_from_date():
    assert season_for_date(date(2026, 11, 1)) == 2027
    assert season_for_date("2027-03-15") == 2027
    assert season_for_date(date(2027, 6, 30)) == 2027
    assert season_for_date(date(2027, 7, 1)) == 2028


def test_label_and_progress():
    assert season_label(2027) == "2026-27"
    assert season_progress("2026-11-01") == 0.0
    assert season_progress("2027-04-10") == 1.0
    assert 0.4 < season_progress("2027-01-15") < 0.6
