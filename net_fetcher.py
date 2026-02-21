"""
net_fetcher.py
Scrapes NCAA NET rankings from the public NCAA website.
No login required — this is public data.
"""

import requests
import pandas as pd
from bs4 import BeautifulSoup


NET_URL = "https://www.ncaa.com/rankings/basketball-men/d1/ncaa-mens-basketball-net-rankings"


def fetch_net_rankings() -> pd.DataFrame:
    """Scrape NCAA NET rankings and return as a DataFrame."""
    headers = {"User-Agent": "Mozilla/5.0"}
    resp = requests.get(NET_URL, headers=headers)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    table = soup.find("table")

    if table is None:
        raise RuntimeError("Could not find rankings table on NCAA page. Structure may have changed.")

    rows = []
    headers_row = [th.text.strip() for th in table.find("tr").find_all("th")]

    for tr in table.find_all("tr")[1:]:
        cells = [td.text.strip() for td in tr.find_all("td")]
        if cells:
            rows.append(cells)

    df = pd.DataFrame(rows, columns=headers_row)

    # Normalize column names
    df.columns = [c.strip().replace(" ", "_") for c in df.columns]

    # Try to standardize rank and team name columns
    for col in df.columns:
        if "rank" in col.lower():
            df.rename(columns={col: "Rank"}, inplace=True)
            break
    for col in df.columns:
        if "team" in col.lower() or "school" in col.lower():
            df.rename(columns={col: "TeamName"}, inplace=True)
            break

    df["Rank"] = pd.to_numeric(df["Rank"], errors="coerce")
    df.dropna(subset=["Rank"], inplace=True)

    return df


if __name__ == "__main__":
    df = fetch_net_rankings()
    df.to_csv("data/net.csv", index=False)
    print(f"✅ Saved NET rankings: {len(df)} teams")
    print(df.head())
