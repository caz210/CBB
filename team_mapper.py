# -*- coding: utf-8 -*-
"""
team_mapper.py
One-time (and periodic) utility to build/update data/team_map.csv.

How it works:
  1. Loads KenPom team names via kenpom_fetcher
  2. Loads team names seen in recent Odds API responses
  3. Auto-matches using fuzzy scoring + the seed table below
  4. Saves a CSV with columns: kenpom_name, odds_name, auto_matched, confidence
  5. Prints any unmatched teams so you can fill them in manually

Run this once at the start of the season, or whenever you notice a new team
isn't matching. Then just edit data/team_map.csv for any manual corrections.

Usage:
    python team_mapper.py
"""

import os
import requests
import pandas as pd
from difflib import SequenceMatcher
from dotenv import load_dotenv

load_dotenv()

OUTPUT_PATH  = "data/team_map.csv"
MATCH_CUTOFF = 0.82   # confidence threshold for auto-accept


def _get_secret(key: str) -> str:
    try:
        import streamlit as st
        return st.secrets.get(key) or os.getenv(key)
    except Exception:
        return os.getenv(key)


# ─────────────────────────────────────────────────────────────────
# SEED TABLE  —  known hard mismatches that fuzzy matching gets wrong
# Add rows here any time you spot a new mismatch in the logs.
# ─────────────────────────────────────────────────────────────────
SEED = {
    # KenPom name              : Odds API name
    "Ole Miss":                  "Mississippi",
    "UConn":                     "Connecticut",
    "UNLV":                      "Nevada Las Vegas",
    "Pitt":                      "Pittsburgh",
    "USC":                       "Southern California",
    "UNC":                       "North Carolina",
    "UNCW":                      "UNC Wilmington",
    "UCF":                       "Central Florida",
    "UCSB":                      "UC Santa Barbara",
    "UTEP":                      "Texas El Paso",
    "UTSA":                      "Texas San Antonio",
    "UAB":                       "Alabama Birmingham",
    "VCU":                       "Virginia Commonwealth",
    "SMU":                       "Southern Methodist",
    "TCU":                       "Texas Christian",
    "LSU":                       "Louisiana State",
    "BYU":                       "Brigham Young",
    "Miami FL":                  "Miami (FL)",
    "Miami OH":                  "Miami (OH)",
    "Detroit":                   "Detroit Mercy",
    "St. Mary's":                "Saint Mary's",
    "St. John's":                "St. John's (NY)",
    "St. Bonaventure":           "Saint Bonaventure",
    "St. Francis PA":            "Saint Francis (PA)",
    "St. Francis NY":            "Saint Francis Brooklyn",
    "St. Peter's":               "Saint Peter's",
    "St. Thomas":                "Saint Thomas",
    "Saint Mary's":              "Saint Mary's",
    "Loyola Chicago":            "Loyola (IL)",
    "Loyola MD":                 "Loyola Maryland",
    "Loyola NO":                 "Loyola New Orleans",
    "Texas A&M Corpus Chris":    "Texas A&M Corpus Christi",
    "ETSU":                      "East Tennessee State",
    "FIU":                       "Florida International",
    "FAU":                       "Florida Atlantic",
    "FDU":                       "Fairleigh Dickinson",
    "LIU":                       "Long Island University",
    "SIUE":                      "Southern Illinois Edwardsville",
    "SIU":                       "Southern Illinois",
    "SFA":                       "Stephen F. Austin",
    "IUPUI":                     "IU Indianapolis",
    "UMBC":                      "Maryland Baltimore County",
    "UMKC":                      "Missouri Kansas City",
    "UT Martin":                 "Tennessee Martin",
    "UT Arlington":              "Texas Arlington",
    "UMass":                     "Massachusetts",
    "UMass Lowell":              "Massachusetts Lowell",
    "UIC":                       "Illinois Chicago",
    "UIW":                       "Incarnate Word",
    "UALR":                      "Arkansas Little Rock",
    "ULM":                       "Louisiana Monroe",
    "ULL":                       "Louisiana",
    "North Carolina A&T":        "NC A&T",
    "North Carolina Central":    "NC Central",
    "NC State":                  "North Carolina State",
    "Gardner-Webb":              "Gardner Webb",
    "Miami":                     "Miami (FL)",
    "Cal Poly":                  "California Polytechnic",
    "Cal Baptist":               "California Baptist",
    "Cal State Fullerton":       "CS Fullerton",
    "Cal State Bakersfield":     "CS Bakersfield",
    "Cal State Northridge":      "CS Northridge",
    "CSU Bakersfield":           "CS Bakersfield",
    "CSU Fullerton":             "CS Fullerton",
    "CSU Northridge":            "CS Northridge",
    "CSUN":                      "CS Northridge",
    "CSUF":                      "CS Fullerton",
    "Nevada":                    "Nevada",
    "New Mexico St.":            "New Mexico State",
    "McNeese":                   "McNeese State",
    "McNeese St.":               "McNeese State",
    "Lamar":                     "Lamar",
    "Nicholls":                  "Nicholls State",
    "Nicholls St.":              "Nicholls State",
    "SE Missouri St.":           "Southeast Missouri State",
    "SE Louisiana":              "Southeastern Louisiana",
    "Southeastern Louisiana":    "Southeastern Louisiana",
    "Tennessee St.":             "Tennessee State",
    "Tennessee Tech":            "Tennessee Technological",
    "Middle Tennessee":          "Middle Tennessee State",
    "Western Kentucky":          "Western Kentucky",
    "Western Michigan":          "Western Michigan",
    "Eastern Kentucky":          "Eastern Kentucky",
    "Eastern Michigan":          "Eastern Michigan",
    "Eastern Illinois":          "Eastern Illinois",
    "Eastern Washington":        "Eastern Washington",
    "Northern Iowa":             "Northern Iowa",
    "Northern Illinois":         "Northern Illinois",
    "Northern Kentucky":         "Northern Kentucky",
    "Northern Arizona":          "Northern Arizona",
    "Southern Utah":             "Southern Utah",
    "Southern Indiana":          "Southern Indiana",
    "Southern Miss":             "Southern Mississippi",
    "Southern":                  "Southern",
    "Grambling":                 "Grambling State",
    "Grambling St.":             "Grambling State",
    "Prairie View":              "Prairie View A&M",
    "Texas Southern":            "Texas Southern",
    "Jackson St.":               "Jackson State",
    "Alcorn St.":                "Alcorn State",
    "Bethune-Cookman":           "Bethune Cookman",
    "Fla. Atlantic":             "Florida Atlantic",
    "Fla. International":        "Florida International",
    "Kennesaw St.":              "Kennesaw State",
    "Sam Houston":               "Sam Houston State",
    "Sam Houston St.":           "Sam Houston State",
    "Abilene Christian":         "Abilene Christian",
    "Stephen F. Austin":         "Stephen F. Austin",
    "Grand Canyon":              "Grand Canyon",
    "Utah Valley":               "Utah Valley",
    "Utah Tech":                 "Utah Tech",
    "Southern Utah":             "Southern Utah",
    "Weber St.":                 "Weber State",
    "Montana St.":               "Montana State",
    "Portland St.":              "Portland State",
    "Sacramento St.":            "Sacramento State",
    "Idaho St.":                 "Idaho State",
    "Northern Colorado":         "Northern Colorado",
    "North Dakota":              "North Dakota",
    "North Dakota St.":          "North Dakota State",
    "South Dakota":              "South Dakota",
    "South Dakota St.":          "South Dakota State",
    "Oral Roberts":              "Oral Roberts",
    "Denver":                    "Denver",
    "Omaha":                     "Nebraska Omaha",
    "Kansas City":               "Missouri Kansas City",
    "Army":                      "Army West Point",
    "Lehigh":                    "Lehigh",
    "American":                  "American University",
    "Navy":                      "Navy",
    "Colgate":                   "Colgate",
    "Holy Cross":                "Holy Cross",
    "Lafayette":                 "Lafayette",
    "Bucknell":                  "Bucknell",
    "Fordham":                   "Fordham",
    "La Salle":                  "La Salle",
    "Saint Joseph's":            "Saint Joseph's",
    "Drexel":                    "Drexel",
    "George Mason":              "George Mason",
    "George Washington":         "George Washington",
    "Duquesne":                  "Duquesne",
    "Rhode Island":              "Rhode Island",
    "Davidson":                  "Davidson",
    "Samford":                   "Samford",
    "Furman":                    "Furman",
    "Wofford":                   "Wofford",
    "The Citadel":               "Citadel",
    "Chattanooga":               "Chattanooga",
    "VMI":                       "Virginia Military Institute",
    "Western Carolina":          "Western Carolina",
    "Mercer":                    "Mercer",
    "ETSU":                      "East Tennessee State",
    "Appalachian St.":           "Appalachian State",
    "Coastal Carolina":          "Coastal Carolina",
    "Georgia Southern":          "Georgia Southern",
    "Georgia State":             "Georgia State",
    "Troy":                      "Troy",
    "South Alabama":             "South Alabama",
    "Old Dominion":              "Old Dominion",
    "Charlotte":                 "Charlotte",
    "Marshall":                  "Marshall",
    "Rice":                      "Rice",
    "Middle Tennessee":          "Middle Tennessee State",
    "Florida International":     "Florida International",
    "Louisiana Tech":            "Louisiana Tech",
    "William & Mary":            "William & Mary",
    "James Madison":             "James Madison",
    "Towson":                    "Towson",
    "Delaware":                  "Delaware",
    "Hofstra":                   "Hofstra",
    "Northeastern":              "Northeastern",
    "Vermont":                   "Vermont",
    "Albany":                    "Albany (NY)",
    "Hartford":                  "Hartford",
    "Maine":                     "Maine",
    "New Hampshire":             "New Hampshire",
    "Stony Brook":               "Stony Brook",
    "UMBC":                      "Maryland Baltimore County",
    "Binghamton":                "Binghamton",
    "Bryant":                    "Bryant",
    "Sacred Heart":              "Sacred Heart",
    "Central Connecticut":       "Central Connecticut State",
    "Long Island":               "Long Island University",
    "Mount St. Mary's":          "Mount St. Mary's",
    "NJIT":                      "New Jersey Institute of Technology",
    "Wagner":                    "Wagner",
    "Merrimack":                 "Merrimack",
    "Queens":                    "Queens University",
    "Lindenwood":                "Lindenwood",
    "West Georgia":              "West Georgia",
    "Southern Indiana":          "Southern Indiana",
    "Tarleton St.":              "Tarleton State",
    "Houston Christian":         "Houston Christian",
    "SFA":                       "Stephen F. Austin",
}


ALL_NICKNAMES = [
    " Crimson Tide", " Bulldogs", " Wildcats", " Tigers", " Bears",
    " Trojans", " Cardinals", " Eagles", " Cougars", " Panthers",
    " Spartans", " Tar Heels", " Volunteers", " Longhorns", " Aggies",
    " Seminoles", " Gators", " Hurricanes", " Demon Deacons", " Mountaineers",
    " Blue Devils", " Hoyas", " Cavaliers", " Hoos", " Hokies",
    " Ducks", " Beavers", " Huskies", " Utes", " Bruins",
    " Ducks", " Nittany Lions", " Hawkeyes", " Cornhuskers", " Huskers",
    " Badgers", " Illini", " Fighting Illini", " Boilermakers", " Hoosiers",
    " Wolverines", " Buckeyes", " Gophers", " Golden Gophers", " Hawkeyes",
    " Razorbacks", " Gamecocks", " Bonnies", " Bonnies", " Friars",
    " Musketeers", " Golden Eagles", " Blue Demons", " Ramblers",
    " Green Wave", " Golden Hurricane", " Shockers", " Jays", " Bluejays",
    " Creighton Bluejays", " Flyers", " Pilots", " Pilots", " Zags",
    " Bulldogs", " Rainbow Warriors", " Warriors", " Rainbow Warriors",
    " Lobos", " Cowboys", " Pokes", " Sooners", " Longhorns",
    " Red Raiders", " Horned Frogs", " Bears", " Mustangs", " Mean Green",
    " Miners", " Roadrunners", " Lumberjacks", " Bearkats",
    " Anteaters", " Highlanders", " Tritons", " Gauchos", " Banana Slugs",
    " Matadors", " Roadrunners", " Aggies", " Broncos", " Dirtbags",
    " Hornet", " Hornets", " 49ers", " Spiders", " Rams", " Owls",
    " Flames", " Hawks", " Norse", " Penguins", " Flash", " Golden Flashes",
    " Zips", " Rockets", " Falcons", " Chippewas", " Cardinals",
    " Red Hawks", " Eagles", " Thunderbirds", " Lopes", " Skyhawks",
    " Running Eagles", " Lions", " Monarchs", " Pride", " Big Green",
    " Princes", " Leopards", " Mountain Hawks", " Raiders", " Bison",
    " Patriots", " Colonials", " Colonels", " Red Foxes", " Greyhounds",
    " Golden Rams", " Explorers", " A-Sun", " Sun Belt",
    " Terrapins", " Terps", " Scarlet Knights", " Golden Knights",
    " Rebels", " Wolf Pack", " Wolfpack", " Wolf Pack",
    " Retrievers", " Retrievers", " Fighting Hawks", " Jackrabbits",
    " Coyotes", " Bison", " Thunderbirds", " Lumberjacks",
    " Panthers", " Governors", " Eagles", " Red Wolves", " Lions",
    " Warhawks", " Red Storm", " Gaels", " Seawolves", " Great Danes",
    " America East", " Terriers", " Crusaders", " Holy Cross",
    " Paladins", " Southern Conf",
    " Fighting Irish", " Mids", " Black Knights", " Cadets",
    " Midshipmen", " Keydets", " Catamounts", " Penmen",
    " Minutemen", " Minutewomen", " Beacons", " River Hawks",
    " Green Terror", " Golden Bears", " Golden Bears",
    " Jaguars", " Royals", " Mavericks", " Storm", " Ospreys",
    " Stetson Hatters", " Hatters", " Dolphins", " Corsairs",
    " Blazers", " Beacons", " Phoenix", " Billikens", " Braves",
    " Redhawks", " Riverhawks", " Mean Green", " Texans",
    " Chanticleers", " Chanticleers", " Thundering Herd", " Herds",
    " Screaming Eagles", " Fighting Eagles",
]


def _sim(a: str, b: str) -> float:
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()


def strip_nickname(name: str) -> str:
    """Strip known team nicknames from the end of a name."""
    for nick in sorted(ALL_NICKNAMES, key=len, reverse=True):
        if name.endswith(nick):
            return name[: -len(nick)].strip()
    return name.strip()


def fetch_odds_team_names() -> list[str]:
    """Pull unique team names from today's Odds API response."""
    key = _get_secret("ODDS_API_KEY")
    if not key:
        print("  ODDS_API_KEY not set — skipping live Odds API fetch.")
        return []

    from datetime import datetime, timezone, timedelta
    now_utc = datetime.now(timezone.utc)
    day_start = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end   = now_utc.replace(hour=23, minute=59, second=59, microsecond=0)
    iso = lambda dt: dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    params = {
        "apiKey":           key,
        "regions":          "us",
        "markets":          "spreads",
        "oddsFormat":       "american",
        "commenceTimeFrom": iso(day_start),
        "commenceTimeTo":   iso(day_end),
        "dateFormat":       "iso",
    }
    try:
        resp = requests.get(
            "https://api.the-odds-api.com/v4/sports/basketball_ncaab/odds",
            params=params, timeout=10
        )
        resp.raise_for_status()
        games = resp.json()
        names = set()
        for g in games:
            names.add(g["home_team"])
            names.add(g["away_team"])
        print(f"  Odds API returned {len(games)} games, {len(names)} unique team names")
        return sorted(names)
    except Exception as e:
        print(f"  Odds API fetch failed: {e}")
        return []


def build_map(kenpom_teams: list[str], odds_teams: list[str]) -> pd.DataFrame:
    """
    Auto-match KenPom names to Odds API names.
    Priority: seed table → strip+exact → fuzzy.
    """
    rows = []
    unmatched = []

    # Pre-strip all odds names for comparison
    odds_stripped = {strip_nickname(o).lower(): o for o in odds_teams}

    for kp in sorted(kenpom_teams):
        # 1. Seed table (authoritative)
        if kp in SEED:
            rows.append({
                "kenpom_name":  kp,
                "odds_name":    SEED[kp],
                "auto_matched": True,
                "confidence":   1.0,
                "source":       "seed",
            })
            continue

        # 2. Exact match after stripping nicknames
        kp_stripped = strip_nickname(kp).lower()
        if kp_stripped in odds_stripped:
            rows.append({
                "kenpom_name":  kp,
                "odds_name":    odds_stripped[kp_stripped],
                "auto_matched": True,
                "confidence":   1.0,
                "source":       "exact_strip",
            })
            continue

        # 3. Fuzzy match against stripped odds names
        best_score  = 0.0
        best_odds   = None
        for stripped_lower, orig_odds in odds_stripped.items():
            s = _sim(kp_stripped, stripped_lower)
            if s > best_score:
                best_score = s
                best_odds  = orig_odds

        if best_score >= MATCH_CUTOFF and best_odds:
            rows.append({
                "kenpom_name":  kp,
                "odds_name":    best_odds,
                "auto_matched": True,
                "confidence":   round(best_score, 3),
                "source":       "fuzzy",
            })
        else:
            unmatched.append(kp)
            rows.append({
                "kenpom_name":  kp,
                "odds_name":    "",        # fill in manually
                "auto_matched": False,
                "confidence":   round(best_score, 3),
                "source":       "unmatched",
            })

    if unmatched:
        print(f"\n  ⚠  {len(unmatched)} teams NOT auto-matched — fill odds_name manually in {OUTPUT_PATH}:")
        for t in unmatched:
            print(f"     {t}")

    return pd.DataFrame(rows)


def run():
    os.makedirs("data", exist_ok=True)

    # Load KenPom teams
    print("Loading KenPom team names...")
    try:
        from kenpom_fetcher import fetch_teams
        kp_df = fetch_teams()
        kp_teams = kp_df["TeamName"].dropna().tolist()
        print(f"  {len(kp_teams)} KenPom teams loaded")
    except Exception as e:
        print(f"  KenPom fetch failed: {e}")
        # Fall back to existing CSV if available
        if os.path.exists("data/teams.csv"):
            kp_teams = pd.read_csv("data/teams.csv")["TeamName"].dropna().tolist()
            print(f"  Loaded {len(kp_teams)} teams from data/teams.csv")
        else:
            print("  No KenPom team data available — aborting.")
            return

    # Load Odds API team names
    print("Loading Odds API team names...")
    odds_teams = fetch_odds_team_names()

    if not odds_teams:
        print("  No Odds API teams — map will be seed-table only.")

    # Build map
    print("Building team map...")
    df = build_map(kp_teams, odds_teams)

    # Merge with existing map to preserve manual edits
    if os.path.exists(OUTPUT_PATH):
        existing = pd.read_csv(OUTPUT_PATH)
        # Keep manually corrected rows (auto_matched=False but odds_name filled in)
        manual = existing[(existing["auto_matched"] == False) & (existing["odds_name"] != "")]
        if not manual.empty:
            print(f"  Preserving {len(manual)} manual corrections from existing map.")
            for _, row in manual.iterrows():
                mask = df["kenpom_name"] == row["kenpom_name"]
                df.loc[mask, "odds_name"]    = row["odds_name"]
                df.loc[mask, "auto_matched"] = True
                df.loc[mask, "source"]       = "manual"

    df.to_csv(OUTPUT_PATH, index=False)
    matched = df[df["odds_name"] != ""]
    print(f"\n  ✓ Saved {len(df)} teams to {OUTPUT_PATH}")
    print(f"    {len(matched)} matched  |  {len(df) - len(matched)} still need manual odds_name")


if __name__ == "__main__":
    run()
