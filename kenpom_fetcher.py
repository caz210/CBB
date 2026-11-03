"""
kenpom_fetcher.py
Fetches all data needed by the CBB model via the official KenPom REST API.
API key loaded from .env  never hardcode it here.
"""

import os
import time
import requests
import pandas as pd
from datetime import date, datetime
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

from czarp.season import season_for_date

load_dotenv()

def _get_secret(key: str) -> str:
    """Read from Streamlit secrets if running in cloud, else fall back to .env"""
    try:
        import streamlit as st
        return st.secrets.get(key) or os.getenv(key)
    except Exception:
        return os.getenv(key)

API_KEY  = _get_secret("KENPOM_API_KEY")
BASE_URL = "https://kenpom.com/api.php"
CENTRAL = ZoneInfo("America/Chicago")
# Ending year of the current season, derived from today's Central date
# (Nov 2026 -> 2027). Rolls over on July 1.
SEASON   = season_for_date(datetime.now(CENTRAL).date())

def _today_central() -> str:
    """Return today's date in Central Time as YYYY-MM-DD.
    Avoids UTC drift on cloud servers (e.g. Streamlit Cloud) where
    date.today() would roll to the next date after 6 PM Central."""
    return datetime.now(CENTRAL).strftime("%Y-%m-%d")


def _get(endpoint: str, params: dict = {}) -> list[dict]:
    """Make an authenticated GET request to the KenPom API."""
    if not API_KEY:
        raise RuntimeError("KENPOM_API_KEY not set in .env")

    headers = {"Authorization": f"Bearer {API_KEY}"}
    full_params = {"endpoint": endpoint, **params}

    resp = requests.get(BASE_URL, headers=headers, params=full_params, timeout=30)
    resp.raise_for_status()

    data = resp.json()
    time.sleep(0.5)  # polite rate limiting

    # Handle both list and dict-wrapped responses
    if isinstance(data, list):
        return data
    for v in data.values():
        if isinstance(v, list):
            return v
    return [data]


def fetch_ratings(year: int = SEASON) -> pd.DataFrame:
    """Core ratings: AdjOE, AdjDE, AdjTempo, Luck, SOS, RankAdjEM, etc."""
    return pd.DataFrame(_get("ratings", {"y": year}))


def fetch_four_factors(year: int = SEASON) -> pd.DataFrame:
    """Four Factors: TO_Pct, DTO_Pct, OR_Pct, DOR_Pct, FT_Rate, DFT_Rate."""
    return pd.DataFrame(_get("four-factors", {"y": year}))


def fetch_height(year: int = SEASON) -> pd.DataFrame:
    """Height, Exp (experience), Bench strength, Continuity."""
    return pd.DataFrame(_get("height", {"y": year}))


def fetch_teams(year: int = SEASON) -> pd.DataFrame:
    """Team list with TeamID for resolving team names to IDs."""
    return pd.DataFrame(_get("teams", {"y": year}))


def fetch_misc(year: int = SEASON) -> pd.DataFrame:
    """Misc stats: FG3Pct, FG2Pct, FTPct, F3GRate (3PA rate), OppF3GRate, BlockPct, ...
    The API endpoint is "misc-stats" (the old "misc" name 404s, which is why the
    v1 clutch score was always 0)."""
    return pd.DataFrame(_get("misc-stats", {"y": year}))


def fetch_archive(rating_date: str) -> pd.DataFrame:
    """Point-in-time ratings (AdjOE/AdjDE/AdjTempo as of `rating_date`, YYYY-MM-DD).
    Used by the backtest so historical games are graded with the ratings that
    existed before tip-off (no look-ahead)."""
    return pd.DataFrame(_get("archive", {"d": rating_date}))


def fetch_fanmatch(game_date: str | None = None) -> pd.DataFrame:
    """
    KenPom game predictions for a given date.

    Returns fields per the API spec:
        Season, GameID, DateOfGame, Visitor, Home,
        HomeRank, VisitorRank, HomePred, VisitorPred,
        HomeWP, PredTempo, ThrillScore

    Args:
        game_date: Date string in YYYY-MM-DD format.
                   Defaults to today. Only dates up to
                   and including today are supported.
    """
    if game_date is None:
        game_date = _today_central()

    rows = _get("fanmatch", {"d": game_date})
    df = pd.DataFrame(rows)

    # Enforce expected types if columns are present
    int_cols   = ["Season", "GameID", "HomeRank", "VisitorRank"]
    float_cols = ["HomePred", "VisitorPred", "HomeWP", "PredTempo", "ThrillScore"]

    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    for col in float_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def fetch_all(year: int = SEASON, game_date: str | None = None) -> dict[str, pd.DataFrame]:
    """Pull every dataset the model needs."""
    if game_date is None:
        game_date = _today_central()

    print(f"Fetching KenPom data for {year} season (fanmatch date: {game_date})...")

    data = {
        "ratings":      fetch_ratings(year),
        "four_factors": fetch_four_factors(year),
        "height":       fetch_height(year),
        "teams":        fetch_teams(year),
    }

    # misc is optional — not all KenPom API tiers include it
    try:
        data["misc"] = fetch_misc(year)
        print(f"    {'misc':<15} ({len(data['misc'])} teams)")
    except Exception:
        data["misc"] = None

    # Fanmatch — game-level predictions for today
    try:
        data["fanmatch"] = fetch_fanmatch(game_date)
        print(f"    {'fanmatch':<15} ({len(data['fanmatch'])} games on {game_date})")
    except Exception as e:
        print(f"    fanmatch fetch failed: {e}")
        data["fanmatch"] = None

    for name, df in data.items():
        if name not in ("misc", "fanmatch") and df is not None:
            print(f"    {name:<15} ({len(df)} teams)")

    return data


def fetch_prior(year: int = SEASON) -> dict[str, pd.DataFrame]:
    """Last season's stat tables. Early in a season KenPom returns placeholders
    (0/100/200) for four factors, misc and height; the model fills those from
    the prior season (flagged in output) until enough games are played."""
    prior = {}
    for name, fn in (("ratings", fetch_ratings), ("four_factors", fetch_four_factors),
                     ("height", fetch_height), ("misc", fetch_misc)):
        try:
            prior[name] = fn(year - 1)
        except Exception as e:
            print(f"    prior {name} fetch failed: {e}")
    return prior


def save_data(data: dict, output_dir: str = "data") -> None:
    os.makedirs(output_dir, exist_ok=True)
    for name, df in data.items():
        if df is not None:
            df.to_csv(f"{output_dir}/{name}.csv", index=False)
    print(f"    All CSVs saved to /{output_dir}/")


if __name__ == "__main__":
    data = fetch_all()
    save_data(data)
    save_data(fetch_prior(), os.path.join("data", "prior"))
