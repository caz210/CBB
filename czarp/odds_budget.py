"""The ONE gate every paid The Odds API request goes through (Scantron's #86 follow-up F1).

Every paid request (odds_fetcher.fetch_vegas_lines, odds_fetcher.odds_api_get: team_mapper, results_tracker, app.py,
run.py, daily_run, pretip_check, refresh_lines) runs through paid_call(), which claims one call of the shared
per-day budget BEFORE the request is made:

  * claim=None  -> the box's local counter (daily_run.odds_api_claim(job): data/ledger/odds_api_usage.json + flock),
                   after reading the credits left from the FREE /v4/sports endpoint when unknown. Fail closed: a
                   missing/unreadable counter means no request.
  * claim=fn    -> fn() -> (ok, why) replaces the local counter (refresh_lines' Supabase-backed RefreshBudget.claim).

A refused claim never reaches the network. A claimed call counts even if the request then fails. The budget numbers
live in daily_run (ODDS_API_MAX_CALLS_PER_DAY / ODDS_API_JOB_SLOTS); the free /v4/sports usage read is not a paid call
and does not go through here.
"""
from __future__ import annotations


class OddsApiRefused(RuntimeError):
    """The budget refused a paid Odds API call (cap, job slot, credit reserve, or an unusable counter)."""


def claim(job: str, claim=None, key: str | None = None) -> tuple[bool, str]:
    """(ok, why) for one paid call by `job`. Never raises."""
    if claim is not None:
        try:
            return claim()
        except Exception as e:                 # a claim that errors is a refusal
            return False, f"claim failed ({type(e).__name__})"
    try:
        import daily_run as dr
    except Exception as e:                     # no counter code -> no paid call
        return False, f"budget unavailable ({type(e).__name__})"
    if key:
        dr.odds_api_seed_remaining(key)
    return dr.odds_api_claim(job)


def record(remaining) -> None:
    """Record the API's x-requests-remaining on the local counter (local-counter calls only). Never raises."""
    try:
        import daily_run as dr
        dr._odds_api_record(remaining)
    except Exception:
        pass


def paid_call(job: str, request, *, claim_fn=None, key: str | None = None, remaining=lambda: None):
    """Claim one call for `job`, then run request() and return its result. Raises OddsApiRefused (and makes no
    request) when the claim is refused. After a local-counter call the credits left (remaining()) are recorded."""
    ok, why = claim(job, claim_fn, key)
    if not ok:
        raise OddsApiRefused(why)
    try:
        return request()
    finally:
        if claim_fn is None:
            try:
                rem = remaining()
            except Exception:
                rem = None
            record(rem)
