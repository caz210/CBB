"""
kenpom_scraper.py
─────────────────
Logs into KenPom and scrapes the FanMatch page to detect neutral-site games.

Logic (same as KenPom's own notation):
  "Team A at Team B"  → home game  (Team B is home)
  "Team A vs Team B"  → neutral site

Credentials come from Streamlit secrets:
    .streamlit/secrets.toml
    ─────────────────────────
    [kenpom]
    email    = "you@example.com"
    password = "yourpassword"

Usage (standalone test):
    python kenpom_scraper.py           # scrapes today
    python kenpom_scraper.py 2026-03-08

Usage (from app.py / kenpom_fetcher.py):
    from kenpom_scraper import get_neutral_pairs, scrape_fanmatch_games

    # Returns set of frozensets — order-independent neutral pair lookup
    neutral = get_neutral_pairs("2026-03-08")
    is_neutral = frozenset([team1.lower(), team2.lower()]) in neutral

    # Returns full game list with home/away/neutral info
    games = scrape_fanmatch_games("2026-03-08")
"""

import re
import sys
from datetime import date, datetime
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup

# ── Constants ─────────────────────────────────────────────────────────────────
LOGIN_URL   = "https://kenpom.com/handlers/login_handler.php"
FANMATCH_URL = "https://kenpom.com/fanmatch.php"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Referer": "https://kenpom.com/",
}

CENTRAL = ZoneInfo("America/Chicago")


# ── Credential helpers ────────────────────────────────────────────────────────

def _get_credentials():
    """
    Pull email/password from Streamlit secrets (production)
    or environment variables (local dev / CI).

    secrets.toml entry:
        [kenpom]
        email    = "you@example.com"
        password = "yourpassword"

    Env-var fallback:
        KENPOM_EMAIL / KENPOM_PASSWORD
    """
    try:
        import streamlit as st
        email    = st.secrets["kenpom"]["email"]
        password = st.secrets["kenpom"]["password"]
        return email, password
    except Exception:
        pass

    import os
    email    = os.environ.get("KENPOM_EMAIL", "")
    password = os.environ.get("KENPOM_PASSWORD", "")
    if email and password:
        return email, password

    raise RuntimeError(
        "KenPom credentials not found.\n"
        "Add them to .streamlit/secrets.toml:\n"
        "  [kenpom]\n"
        "  email    = 'you@example.com'\n"
        "  password = 'yourpassword'\n"
        "Or set KENPOM_EMAIL / KENPOM_PASSWORD environment variables."
    )


# ── Login ─────────────────────────────────────────────────────────────────────

def _login() -> requests.Session:
    """
    Log in to KenPom and return an authenticated Session.
    Raises RuntimeError on failure.
    """
    email, password = _get_credentials()

    session = requests.Session()
    session.headers.update(HEADERS)

    # Grab login page first to pick up any CSRF token / cookies
    session.get("https://kenpom.com/", timeout=15)

    payload = {
        "email":    email,
        "password": password,
        "submit":   "Login",
    }
    resp = session.post(LOGIN_URL, data=payload, timeout=15)

    # KenPom returns 200 on bad login too — check we can actually load a
    # members-only page (fanmatch redirects to login if not authenticated)
    check = session.get("https://kenpom.com/fanmatch.php", timeout=15)
    if "login" in check.url.lower() or "Login" in check.text[:500]:
        raise RuntimeError(
            "KenPom login failed — check your email/password in secrets.toml"
        )

    return session


# ── Scraper ───────────────────────────────────────────────────────────────────

def scrape_fanmatch_games(date_str: str | None = None) -> list[dict]:
    """
    Scrape KenPom FanMatch for *date_str* (YYYY-MM-DD).
    Defaults to today (CT).

    Returns a list of dicts:
        {
            "date":      "2026-03-08",
            "team1":     "Auburn",        # left-side team (visitor or listed first)
            "team2":     "Alabama",       # right-side team (home or listed second)
            "connector": "at" | "vs",     # raw word between the teams
            "neutral":   True | False,
            "home_team": "Alabama" | None,  # None when neutral
            "away_team": "Auburn"  | None,  # None when neutral
        }

    Raises RuntimeError on login failure.
    Raises ValueError if the page structure is unexpected.
    """
    if date_str is None:
        date_str = datetime.now(CENTRAL).date().isoformat()

    session = _login()

    params = {"s": "Time"}
    if date_str != datetime.now(CENTRAL).date().isoformat():
        params["d"] = date_str          # KenPom accepts ?d=YYYY-MM-DD

    resp = session.get(FANMATCH_URL, params=params, timeout=15)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")

    # FanMatch table: each row is a game.
    # The matchup cell looks like:  "Auburn at Alabama"  or  "Auburn vs Alabama"
    # KenPom wraps team names in <a> tags inside a <td class="teamname"> or
    # similar. We look for the text pattern flexibly.

    games = []

    # Strategy: find all <a> links pointing to team pages and scan surrounding text
    # More robust: grab every table row and look for "at" / "vs" between two team links

    table = soup.find("table", id="fanmatch-table")
    if table is None:
        # Fallback: find any table that has game rows
        tables = soup.find_all("table")
        table = tables[0] if tables else None

    if table is None:
        raise ValueError("Could not find FanMatch table on page — structure may have changed")

    rows = table.find_all("tr")

    for row in rows:
        cells = row.find_all("td")
        if not cells:
            continue

        # The matchup is usually in the first meaningful cell
        # Look for a cell containing "at" or "vs" between two anchors
        for cell in cells:
            cell_text = cell.get_text(separator=" ", strip=True)

            # Match pattern: TeamName (at|vs) TeamName
            # Allow for parenthetical rankings like "(1) Kansas vs (4) Iowa St."
            match = re.search(
                r"(.+?)\s+(at|vs\.?)\s+(.+)",
                cell_text,
                re.IGNORECASE,
            )
            if not match:
                continue

            raw_t1    = match.group(1).strip()
            connector = match.group(2).strip().rstrip(".").lower()   # "at" or "vs"
            raw_t2    = match.group(3).strip()

            # Strip leading rank like "(3) " or "#3 "
            def _strip_rank(name: str) -> str:
                name = re.sub(r"^\(?\d{1,3}\)?\s*", "", name)
                name = re.sub(r"^#\d{1,3}\s*", "", name)
                return name.strip()

            team1 = _strip_rank(raw_t1)
            team2 = _strip_rank(raw_t2)

            if not team1 or not team2:
                continue

            neutral   = (connector == "vs")
            home_team = None if neutral else team2
            away_team = None if neutral else team1

            games.append({
                "date":      date_str,
                "team1":     team1,
                "team2":     team2,
                "connector": connector,
                "neutral":   neutral,
                "home_team": home_team,
                "away_team": away_team,
            })
            break   # found the matchup cell for this row — move to next row

    return games


# ── Convenience helper for app integration ────────────────────────────────────

def get_neutral_pairs(date_str: str | None = None) -> set[frozenset]:
    """
    Returns a set of frozensets for fast order-independent neutral-site lookup.

    Example:
        neutral = get_neutral_pairs("2026-03-08")
        if frozenset([t1.lower(), t2.lower()]) in neutral:
            # it's a neutral site game
    """
    games = scrape_fanmatch_games(date_str)
    return {
        frozenset([g["team1"].lower(), g["team2"].lower()])
        for g in games
        if g["neutral"]
    }


def get_home_away_map(date_str: str | None = None) -> dict[tuple, tuple]:
    """
    Returns a dict mapping (team1_lower, team2_lower) → (home, away | None for neutral).

    Keys are stored in BOTH orderings so lookups are order-independent.
    """
    games = scrape_fanmatch_games(date_str)
    result = {}
    for g in games:
        t1, t2 = g["team1"].lower(), g["team2"].lower()
        info = (g["home_team"], g["away_team"], g["neutral"])
        result[(t1, t2)] = info
        result[(t2, t1)] = info
    return result


# ── CLI test ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    target_date = sys.argv[1] if len(sys.argv) > 1 else None

    print(f"\n{'─'*60}")
    print(f"  KenPom FanMatch Scraper — {target_date or 'today'}")
    print(f"{'─'*60}\n")

    try:
        games = scrape_fanmatch_games(target_date)
    except RuntimeError as e:
        print(f"ERROR: {e}")
        sys.exit(1)

    if not games:
        print("No games found (or page structure changed).")
        sys.exit(0)

    neutral_count = sum(1 for g in games if g["neutral"])
    print(f"Found {len(games)} games  ({neutral_count} neutral)\n")

    for g in games:
        site_label = "🏟  NEUTRAL" if g["neutral"] else f"🏠  Home: {g['home_team']}"
        print(f"  {g['team1']:25s}  {g['connector']:2s}  {g['team2']:25s}   {site_label}")

    print()
