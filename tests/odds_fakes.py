"""Test helper: a fake odds_fetcher.fetch_vegas_lines that claims exactly like the real one.

The real fetch_vegas_lines claims its call through czarp.odds_budget.paid_call (the single gate for paid Odds API
calls), so a fake that replaced it without claiming would bypass the cap. claimed_fetch wraps a fake request body
(no network) in the same gate."""
import pandas as pd

from czarp import odds_budget as OB


def claimed_fetch(fake, request):
    def fetch_vegas_lines(job="manual", claim=None, raise_refusal=False):
        try:
            return OB.paid_call(job, request, claim_fn=claim, key=fake.ODDS_API_KEY,
                                remaining=lambda: getattr(fake, "_odds_credits_remaining", None))
        except OB.OddsApiRefused:
            if raise_refusal:
                raise
            return pd.DataFrame()
    return fetch_vegas_lines
