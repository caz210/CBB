"""
kenpom_fetcher.py
Fetches all data needed by the CBB model via the official KenPom REST API.
API key loaded from .env  never hardcode it here.
"""

import os
import time
import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

def _get_secret(key: str) -> str:
    """Read from Streamlit secrets if running in cloud, else fall back to .env"""
    try:
        import streamlit as st
        return st.secrets.get(key) or os.getenv(key)
    except Exception:
        return os.getenv(key)

API_KEY = _get_secret("KENPOM_API_KEY")
BASE_URL = "https://kenpom.com/api.php"
SEASON   = 2025  # Update each year


def _get(endpoint: str, params: dict = {}) -> list[dict]:
    """Make an authenticated GET request to the KenPom API."""
    if not API_KEY:
        raise RuntimeError("KENPOM_API_KEY not set in .env")

    headers = {"Authorization": f"Bearer {API_KEY}"}
    full_params = {"endpoint": endpoint, **params}

    resp = requests.get(BASE_URL, headers=headers, params=full_params)
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
    """Team list with TeamID  for resolving team names to IDs."""
    return pd.DataFrame(_get("teams", {"y": year}))


def fetch_misc(year: int = SEASON) -> pd.DataFrame:
    """Misc stats including clutch time performance."""
    return pd.DataFrame(_get("misc", {"y": year}))


def fetch_fanmatch(date: str) -> pd.DataFrame:
    """KenPom's own game predictions for a date (YYYY-MM-DD). Good sanity check."""
    return pd.DataFrame(_get("fanmatch", {"d": date}))


def fetch_all(year: int = SEASON) -> dict[str, pd.DataFrame]:
    """Pull every dataset the model needs."""
    print(f" Fetching KenPom data for {year} season...")
    data = {
        "ratings":      fetch_ratings(year),
        "four_factors": fetch_four_factors(year),
        "height":       fetch_height(year),
        "teams":        fetch_teams(year),
        "misc":         fetch_misc(year),
    }
    for name, df in data.items():
        print(f"    {name:<15} ({len(df)} teams)")
    return data


def save_data(data: dict[str, pd.DataFrame], output_dir: str = "data") -> None:
    os.makedirs(output_dir, exist_ok=True)
    for name, df in data.items():
        df.to_csv(f"{output_dir}/{name}.csv", index=False)
    print(f"    All CSVs saved to /{output_dir}/")


if __name__ == "__main__":
    data = fetch_all()
    save_data(data)
