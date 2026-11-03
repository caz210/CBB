"""
hca_fetcher.py
Per-team home-court advantage from KenPom's hca.php (subscriber page).

The KenPom API has no HCA endpoint (checked: endpoint=hca -> 404), and hca.php
needs a logged-in subscriber session, so this uses kenpom_scraper._login()
(KENPOM_EMAIL / KENPOM_PASSWORD or st.secrets["kenpom"]).

Result is cached to data/hca.csv (TeamName, HCA, ...). If scraping fails, the
model falls back to the league-average HCA in ModelConfig.default_hca.
You can also drop a hand-made data/hca.csv with columns TeamName,HCA.

    python hca_fetcher.py            # scrape + save data/hca.csv
"""
from __future__ import annotations

import io
import os

import pandas as pd

HCA_URL = "https://kenpom.com/hca.php"
HCA_CSV = os.path.join("data", "hca.csv")


def parse_hca_html(html: str) -> pd.DataFrame:
    """Parse the hca.php table. Columns on the page: Team, Conf, HCA, Rk, PF, Rk,
    Pts, Rk, NST, Rk, Blk, Rk, Elev, Rk."""
    tables = pd.read_html(io.StringIO(html))
    if not tables:
        raise ValueError("no tables on hca page")
    df = tables[0]
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[-1] for c in df.columns]
    cols = [str(c) for c in df.columns]
    # de-duplicate the repeated "Rk" columns
    seen, new = {}, []
    for c in cols:
        if c in seen:
            seen[c] += 1
            new.append(f"{new[-1]}_Rk" if c.lower().startswith("rk") and new else f"{c}.{seen[c]}")
        else:
            seen[c] = 0
            new.append(c)
    df.columns = new
    team_col = next(c for c in df.columns if c.lower().startswith("team"))
    hca_col = next(c for c in df.columns if c.upper() == "HCA")
    df = df.rename(columns={team_col: "TeamName", hca_col: "HCA"})
    df["TeamName"] = df["TeamName"].astype(str).str.replace(r"\s*\d+$", "", regex=True).str.strip()
    df["HCA"] = pd.to_numeric(df["HCA"], errors="coerce")
    df = df[df["HCA"].notna() & (df["TeamName"] != "Team")]
    return df.reset_index(drop=True)


def scrape_hca(session=None) -> pd.DataFrame:
    if session is None:
        from kenpom_scraper import _login
        session = _login()
    resp = session.get(HCA_URL, timeout=20)
    resp.raise_for_status()
    if "register" in resp.url or "login" in resp.url.lower():
        raise RuntimeError("hca.php redirected to login/register - subscriber session required")
    return parse_hca_html(resp.text)


def load_hca(path: str = HCA_CSV, refresh: bool = False) -> pd.DataFrame:
    """Return per-team HCA table; scrape when refresh=True or no cache. Never raises:
    returns an empty frame if unavailable (model then uses default_hca)."""
    if not refresh and os.path.exists(path):
        try:
            return pd.read_csv(path)
        except Exception:
            pass
    try:
        df = scrape_hca()
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        df.to_csv(path, index=False)
        return df
    except Exception as e:  # missing creds, network, layout change
        print(f"    HCA scrape unavailable ({e.__class__.__name__}: {e}); using default HCA")
        return pd.DataFrame(columns=["TeamName", "HCA"])


if __name__ == "__main__":
    df = load_hca(refresh=True)
    print(df.head(10).to_string() if not df.empty else "No HCA data")
