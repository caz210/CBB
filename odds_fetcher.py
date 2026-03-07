# -*- coding: utf-8 -*-
"""
odds_fetcher.py
Fetches pre-game Vegas lines from The Odds API.
- Skips games that have already started (no live lines)
- Covers 10pm CT games by extending window into next UTC day
- Uses exact Odds API team names as seed (ground truth from API team list)
- 5-layer matching: seed → exact-strip → normalize → token-overlap → fuzzy
"""

import os
import re
import requests
import pandas as pd
from difflib import SequenceMatcher
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

load_dotenv()

CENTRAL = ZoneInfo("America/Chicago")


def _get_secret(key: str) -> str:
    try:
        import streamlit as st
        return st.secrets.get(key) or os.getenv(key)
    except Exception:
        return os.getenv(key)


ODDS_API_KEY    = _get_secret("ODDS_API_KEY")
BASE_URL        = "https://api.the-odds-api.com/v4/sports/basketball_ncaab/odds"
BOOK_PRIORITY   = ["draftkings", "fanduel", "betmgm", "caesars", "bovada", "mybookieag"]
MATCH_THRESHOLD = 0.72   # lowered — better normalization compensates
TEAM_MAP_PATH   = "data/team_map.csv"

_odds_last_fetched: str = ""

# ─────────────────────────────────────────────────────────────────────────────
# MASTER SEED TABLE  —  KenPom name  →  exact Odds API full_name
# This is ground truth. Fuzzy matching is only a fallback for teams not here.
# Format: "KenPom TeamName": "Odds API full_name (with nickname)"
# ─────────────────────────────────────────────────────────────────────────────
_SEED: dict = {
    # ── Abbreviations / Acronyms ─────────────────────────────────────────────
    "BYU":                            "BYU Cougars",
    "TCU":                            "TCU Horned Frogs",
    "SMU":                            "SMU Mustangs",
    "UCF":                            "UCF Knights",
    "UNLV":                           "UNLV Rebels",
    "VCU":                            "VCU Rams",
    "VMI":                            "VMI Keydets",
    "UMBC":                           "UMBC Retrievers",
    "UMKC":                           "UMKC Kangaroos",
    "UAB":                            "UAB Blazers",
    "UTEP":                           "UTEP Miners",
    "UTSA":                           "UTSA Roadrunners",
    "NJIT":                           "NJIT Highlanders",
    "LIU":                            "LIU Sharks",
    "UIC":                            "UIC Flames",
    "FDU":                            "Fairleigh Dickinson Knights",
    "SFA":                            "Stephen F. Austin Lumberjacks",
    "UIW":                            "Incarnate Word Cardinals",
    "ETSU":                           "East Tennessee St Buccaneers",
    "FIU":                            "Florida Intl Panthers",
    "FAU":                            "Florida Atlantic Owls",
    "SIUE":                           "SIU Edwardsville Cougars",
    "SIU":                            "Southern Illinois Salukis",
    "LSU":                            "LSU Tigers",
    "USC":                            "USC Trojans",
    "UNC":                            "North Carolina Tar Heels",
    "UNCW":                           "UNC Wilmington Seahawks",
    "UConn":                          "Connecticut Huskies",

    # ── Common KenPom abbreviations that differ from Odds API ────────────────
    "Pitt":                           "Pittsburgh Panthers",
    "Ole Miss":                       "Mississippi Rebels",
    "UConn":                          "Connecticut Huskies",
    "UCSB":                           "UC Santa Barbara Gauchos",
    "Miami FL":                       "Miami (FL) Hurricanes",
    "Miami OH":                       "Miami (OH) RedHawks",
    "Detroit":                        "Detroit Mercy Titans",
    "N.C. State":                     "NC State Wolfpack",
    "NC State":                       "NC State Wolfpack",
    "Loyola Chicago":                 "Loyola (IL) Ramblers",
    "Loyola MD":                      "Loyola Maryland Greyhounds",
    "Loyola NO":                      "Loyola New Orleans Wolf Pack",
    "TAM C. Christi":                 "Texas A&M Corpus Christi Islanders",
    "Texas A&M Corpus Chris":         "Texas A&M Corpus Christi Islanders",
    "Texas A&M Corpus Christi":       "Texas A&M Corpus Christi Islanders",
    "Col. of Charleston":             "College of Charleston Cougars",
    "College of Charleston":          "College of Charleston Cougars",
    "St. Mary's":                     "Saint Mary's Gaels",
    "St. Mary's CA":                  "Saint Mary's Gaels",
    "St. John's":                     "St. John's Red Storm",
    "St. Bonaventure":                "St. Bonaventure Bonnies",
    "St. Francis PA":                 "Saint Francis Red Flash",
    "St. Francis NY":                 "St. Francis Brooklyn Terriers",
    "St. Peter's":                    "Saint Peter's Peacocks",
    "St. Thomas":                     "St. Thomas Tommies",
    "Saint Mary's":                   "Saint Mary's Gaels",
    "Seton Hall":                     "Seton Hall Pirates",
    "Prairie View":                   "Prairie View A&M Panthers",
    "Prairie View A&M":               "Prairie View A&M Panthers",
    "Grambling":                      "Grambling State Tigers",
    "Grambling St.":                  "Grambling State Tigers",
    "Jackson St.":                    "Jackson State Tigers",
    "Alcorn St.":                     "Alcorn State Braves",
    "Bethune-Cookman":                "Bethune-Cookman Wildcats",
    "Kennesaw St.":                   "Kennesaw State Owls",
    "Kennesaw State":                 "Kennesaw State Owls",
    "Sam Houston":                    "Sam Houston Bearkats",
    "Sam Houston St.":                "Sam Houston Bearkats",
    "Grand Canyon":                   "Grand Canyon Antelopes",
    "Utah Valley":                    "Utah Valley Wolverines",
    "Utah Tech":                      "Utah Tech Trailblazers",
    "Weber St.":                      "Weber State Wildcats",
    "Weber State":                    "Weber State Wildcats",
    "Montana St.":                    "Montana State Bobcats",
    "Montana State":                  "Montana State Bobcats",
    "Portland St.":                   "Portland State Vikings",
    "Portland State":                 "Portland State Vikings",
    "Sacramento St.":                 "Sacramento State Hornets",
    "Sacramento State":               "Sacramento State Hornets",
    "Idaho St.":                      "Idaho State Bengals",
    "Idaho State":                    "Idaho State Bengals",
    "North Dakota St.":               "North Dakota State Bison",
    "North Dakota State":             "North Dakota State Bison",
    "South Dakota St.":               "South Dakota State Jackrabbits",
    "South Dakota State":             "South Dakota State Jackrabbits",
    "Southern Miss":                  "Southern Miss Golden Eagles",
    "Southern Mississippi":           "Southern Miss Golden Eagles",
    "Miss. Valley St.":               "Mississippi Valley State Delta Devils",
    "Mississippi Valley St.":         "Mississippi Valley State Delta Devils",
    "Mississippi Valley State":       "Mississippi Valley State Delta Devils",
    "SE Missouri St.":                "Southeast Missouri State Redhawks",
    "Southeast Missouri St.":         "Southeast Missouri State Redhawks",
    "Southeast Missouri State":       "Southeast Missouri State Redhawks",
    "App. State":                     "Appalachian State Mountaineers",
    "Appalachian St.":                "Appalachian State Mountaineers",
    "Appalachian State":              "Appalachian State Mountaineers",
    "Georgia St.":                    "Georgia State Panthers",
    "Georgia State":                  "Georgia State Panthers",
    "Arkansas St.":                   "Arkansas State Red Wolves",
    "Arkansas State":                 "Arkansas State Red Wolves",
    "Wright St.":                     "Wright State Raiders",
    "Wright State":                   "Wright State Raiders",
    "Youngstown St.":                 "Youngstown State Penguins",
    "Youngstown State":               "Youngstown State Penguins",
    "Oklahoma St.":                   "Oklahoma State Cowboys",
    "Oklahoma State":                 "Oklahoma State Cowboys",
    "Washington St.":                 "Washington State Cougars",
    "Washington State":               "Washington State Cougars",
    "Jacksonville St.":               "Jacksonville State Gamecocks",
    "Jacksonville State":             "Jacksonville State Gamecocks",
    "Mississippi St.":                "Mississippi State Bulldogs",
    "Mississippi State":              "Mississippi State Bulldogs",
    "Cal St. Fullerton":              "Cal State Fullerton Titans",
    "CSU Fullerton":                  "Cal State Fullerton Titans",
    "Cal St. Bakersfield":            "Cal State Bakersfield Roadrunners",
    "CSU Bakersfield":                "Cal State Bakersfield Roadrunners",
    "Cal St. Northridge":             "Cal State Northridge Matadors",
    "CSU Northridge":                 "Cal State Northridge Matadors",
    "Cal St. Long Beach":             "Long Beach State Beach",
    "Long Beach St.":                 "Long Beach State Beach",
    "Long Beach State":               "Long Beach State Beach",
    "Florida St.":                    "Florida State Seminoles",
    "Florida State":                  "Florida State Seminoles",
    "Coppin St.":                     "Coppin State Eagles",
    "Coppin State":                   "Coppin State Eagles",
    "Norfolk St.":                    "Norfolk State Spartans",
    "Norfolk State":                  "Norfolk State Spartans",
    "Morgan St.":                     "Morgan State Bears",
    "Morgan State":                   "Morgan State Bears",
    "Chicago St.":                    "Chicago State Cougars",
    "Chicago State":                  "Chicago State Cougars",
    "Delaware St.":                   "Delaware State Hornets",
    "Delaware State":                 "Delaware State Hornets",
    "South Carolina St.":             "South Carolina State Bulldogs",
    "South Carolina State":           "South Carolina State Bulldogs",
    "N.C. A&T":                       "North Carolina A&T Aggies",
    "NC A&T":                         "North Carolina A&T Aggies",
    "North Carolina A&T":             "North Carolina A&T Aggies",
    "N.C. Central":                   "North Carolina Central Eagles",
    "NC Central":                     "North Carolina Central Eagles",
    "North Carolina Central":         "North Carolina Central Eagles",
    "Wis.-Milwaukee":                 "Milwaukee Panthers",
    "Milwaukee":                      "Milwaukee Panthers",
    "Green Bay":                      "Green Bay Phoenix",
    "Wis.-Green Bay":                 "Green Bay Phoenix",
    "N. Colorado":                    "Northern Colorado Bears",
    "Northern Colorado":              "Northern Colorado Bears",
    "Tarleton St.":                   "Tarleton State Texans",
    "Tarleton State":                 "Tarleton State Texans",
    "New Mexico St.":                 "New Mexico State Aggies",
    "New Mexico State":               "New Mexico State Aggies",
    "Texas St.":                      "Texas State Bobcats",
    "Texas State":                    "Texas State Bobcats",
    "Bowling Green":                  "Bowling Green Falcons",
    "Bowling Green St.":              "Bowling Green Falcons",
    "Miami (OH)":                     "Miami (OH) RedHawks",
    "Miami (FL)":                     "Miami (FL) Hurricanes",
    "Penn St.":                       "Penn State Nittany Lions",
    "Penn State":                     "Penn State Nittany Lions",
    "Ohio St.":                       "Ohio State Buckeyes",
    "Ohio State":                     "Ohio State Buckeyes",
    "Michigan St.":                   "Michigan State Spartans",
    "Michigan State":                 "Michigan State Spartans",
    "Arizona St.":                    "Arizona State Sun Devils",
    "Arizona State":                  "Arizona State Sun Devils",
    "Iowa St.":                       "Iowa State Cyclones",
    "Iowa State":                     "Iowa State Cyclones",
    "Kansas St.":                     "Kansas State Wildcats",
    "Kansas State":                   "Kansas State Wildcats",
    "Oregon St.":                     "Oregon State Beavers",
    "Oregon State":                   "Oregon State Beavers",
    "Colorado St.":                   "Colorado State Rams",
    "Colorado State":                 "Colorado State Rams",
    "Boise St.":                      "Boise State Broncos",
    "Boise State":                    "Boise State Broncos",
    "Fresno St.":                     "Fresno State Bulldogs",
    "Fresno State":                   "Fresno State Bulldogs",
    "San Diego St.":                  "San Diego State Aztecs",
    "San Diego State":                "San Diego State Aztecs",
    "Utah St.":                       "Utah State Aggies",
    "Utah State":                     "Utah State Aggies",
    "Nevada Las Vegas":               "UNLV Rebels",
    "American":                       "American Eagles",
    "Illinois St.":                   "Illinois State Redbirds",
    "Illinois State":                 "Illinois State Redbirds",
    "Indiana St.":                    "Indiana State Sycamores",
    "Indiana State":                  "Indiana State Sycamores",
    "Ball St.":                       "Ball State Cardinals",
    "Ball State":                     "Ball State Cardinals",
    "Kent St.":                       "Kent State Golden Flashes",
    "Kent State":                     "Kent State Golden Flashes",
    "Wichita St.":                    "Wichita State Shockers",
    "Wichita State":                  "Wichita State Shockers",
    "Murray St.":                     "Murray State Racers",
    "Murray State":                   "Murray State Racers",
    "Morehead St.":                   "Morehead State Eagles",
    "Morehead State":                 "Morehead State Eagles",
    "Eastern Kentucky":               "Eastern Kentucky Colonels",
    "Eastern Illinois":               "Eastern Illinois Panthers",
    "Eastern Michigan":               "Eastern Michigan Eagles",
    "Eastern Washington":             "Eastern Washington Eagles",
    "Northern Iowa":                  "Northern Iowa Panthers",
    "Northern Illinois":              "Northern Illinois Huskies",
    "Northern Kentucky":              "Northern Kentucky Norse",
    "Northern Arizona":               "Northern Arizona Lumberjacks",
    "Southern Utah":                  "Southern Utah Thunderbirds",
    "Southern Indiana":               "Southern Indiana Screaming Eagles",
    "Tennessee St.":                  "Tennessee State Tigers",
    "Tennessee State":                "Tennessee State Tigers",
    "Tennessee Tech":                 "Tennessee Tech Golden Eagles",
    "Austin Peay":                    "Austin Peay Governors",
    "Belmont":                        "Belmont Bruins",
    "Lipscomb":                       "Lipscomb Bisons",
    "IUPUI":                          "IUPUI Jaguars",
    "Purdue Fort Wayne":              "Purdue Fort Wayne Mastodons",
    "Fort Wayne":                     "Purdue Fort Wayne Mastodons",
    "Western Illinois":               "Western Illinois Leathernecks",
    "Western Kentucky":               "Western Kentucky Hilltoppers",
    "Western Michigan":               "Western Michigan Broncos",
    "Middle Tennessee":               "Middle Tennessee Blue Raiders",
    "Middle Tenn.":                   "Middle Tennessee Blue Raiders",
    "Fla. Atlantic":                  "Florida Atlantic Owls",
    "Fla. International":             "Florida Intl Panthers",
    "Coastal Carolina":               "Coastal Carolina Chanticleers",
    "High Point":                     "High Point Panthers",
    "Elon":                           "Elon Phoenix",
    "Radford":                        "Radford Highlanders",
    "Winthrop":                       "Winthrop Eagles",
    "UNC Asheville":                  "UNC Asheville Bulldogs",
    "UNC Greensboro":                 "UNC Greensboro Spartans",
    "Western Carolina":               "Western Carolina Catamounts",
    "The Citadel":                    "The Citadel Bulldogs",
    "Chattanooga":                    "Chattanooga Mocs",
    "Mercer":                         "Mercer Bears",
    "Samford":                        "Samford Bulldogs",
    "ETSU":                           "East Tennessee St Buccaneers",
    "Wofford":                        "Wofford Terriers",
    "VMI":                            "VMI Keydets",
    "Furman":                         "Furman Paladins",
    "La Salle":                       "La Salle Explorers",
    "La Salle":                       "La Salle Explorers",
    "Drexel":                         "Drexel Dragons",
    "Hofstra":                        "Hofstra Pride",
    "Northeastern":                   "Northeastern Huskies",
    "Towson":                         "Towson Tigers",
    "William & Mary":                 "William & Mary Tribe",
    "William and Mary":               "William & Mary Tribe",
    "James Madison":                  "James Madison Dukes",
    "UNCW":                           "UNC Wilmington Seahawks",
    "UNC Wilmington":                 "UNC Wilmington Seahawks",
    "Charleston So.":                 "Charleston Southern Buccaneers",
    "Charleston Southern":            "Charleston Southern Buccaneers",
    "Gardner-Webb":                   "Gardner-Webb Runnin' Bulldogs",
    "Campbell":                       "Campbell Camels",
    "Longwood":                       "Longwood Lancers",
    "Hampton":                        "Hampton Pirates",
    "Howard":                         "Howard Bison",
    "Iona":                           "Iona Gaels",
    "Manhattan":                      "Manhattan Jaspers",
    "Siena":                          "Siena Saints",
    "Niagara":                        "Niagara Purple Eagles",
    "Canisius":                       "Canisius Golden Griffins",
    "Rider":                          "Rider Broncs",
    "Quinnipiac":                     "Quinnipiac Bobcats",
    "Sacred Heart":                   "Sacred Heart Pioneers",
    "Bryant":                         "Bryant Bulldogs",
    "Central Conn. St.":              "Central Connecticut Blue Devils",
    "Central Connecticut":            "Central Connecticut Blue Devils",
    "Mount St. Mary's":               "Mount St. Mary's Mountaineers",
    "UMBC":                           "UMBC Retrievers",
    "UMKC":                           "UMKC Kangaroos",
    "Oral Roberts":                   "Oral Roberts Golden Eagles",
    "North Dakota":                   "North Dakota Fighting Hawks",
    "South Dakota":                   "South Dakota Coyotes",
    "Denver":                         "Denver Pioneers",
    "Omaha":                          "Omaha Mavericks",
    "Kansas City":                    "UMKC Kangaroos",
    "Incarnate Word":                 "Incarnate Word Cardinals",
    "Abilene Christian":              "Abilene Christian Wildcats",
    "Stephen F. Austin":              "Stephen F. Austin Lumberjacks",
    "Lamar":                          "Lamar Cardinals",
    "Houston Baptist":                "Houston Christian Huskies",
    "Houston Christian":              "Houston Christian Huskies",
    "SE Louisiana":                   "Southeastern Louisiana Lions",
    "Southeastern Louisiana":         "Southeastern Louisiana Lions",
    "McNeese St.":                    "McNeese Cowboys",
    "McNeese":                        "McNeese Cowboys",
    "Nicholls St.":                   "Nicholls Colonels",
    "Nicholls":                       "Nicholls Colonels",
    "Northwestern St.":               "Northwestern State Demons",
    "Northwestern State":             "Northwestern State Demons",
    "Louisiana":                      "Louisiana Ragin' Cajuns",
    "Louisiana-Lafayette":            "Louisiana Ragin' Cajuns",
    "UL Lafayette":                   "Louisiana Ragin' Cajuns",
    "Louisiana-Monroe":               "Louisiana Monroe Warhawks",
    "UL Monroe":                      "Louisiana Monroe Warhawks",
    "Ark.-Pine Bluff":                "Arkansas-Pine Bluff Golden Lions",
    "Arkansas-Pine Bluff":            "Arkansas-Pine Bluff Golden Lions",
    "UAPB":                           "Arkansas-Pine Bluff Golden Lions",
    "Mississippi Valley St.":         "Mississippi Valley State Delta Devils",
    "Texas Southern":                 "Texas Southern Tigers",
    "Southern":                       "Southern Jaguars",
    "Southern Univ.":                 "Southern Jaguars",
    "Grambling State":                "Grambling State Tigers",
    "Alabama A&M":                    "Alabama A&M Bulldogs",
    "Alabama St.":                    "Alabama State Hornets",
    "Alabama State":                  "Alabama State Hornets",
    "Alcorn State":                   "Alcorn State Braves",
    "Jackson State":                  "Jackson State Tigers",
    "Southern Univ.":                 "Southern Jaguars",
    "South Carolina St.":             "South Carolina State Bulldogs",
    "South Carolina State":           "South Carolina State Bulldogs",
    "Bethune-Cookman":                "Bethune-Cookman Wildcats",
    "Florida A&M":                    "Florida A&M Rattlers",
    "Howard":                         "Howard Bison",
    "Morgan State":                   "Morgan State Bears",
    "Coppin State":                   "Coppin State Eagles",
    "Norfolk State":                  "Norfolk State Spartans",
    "Delaware State":                 "Delaware State Hornets",
    "Maryland-Eastern Shore":         "Maryland-Eastern Shore Hawks",
    "UMES":                           "Maryland-Eastern Shore Hawks",
    "North Carolina A&T":             "North Carolina A&T Aggies",
    "North Carolina Central":         "North Carolina Central Eagles",
    "Winston-Salem St.":              "Winston-Salem State Rams",
    "Queens":                         "Queens Royals",
    "Queens NC":                      "Queens Royals",
    "Lindenwood":                     "Lindenwood Lions",
    "Southern Ind.":                  "Southern Indiana Screaming Eagles",
    "Bellarmine":                     "Bellarmine Knights",
    "Le Moyne":                       "Le Moyne Dolphins",
    "Stonehill":                      "Stonehill Skyhawks",
    "Queens (NC)":                    "Queens Royals",
}


# ─────────────────────────────────────────────────────────────────────────────
# NORMALIZATION  —  multi-step text normalization for robust fuzzy fallback
# ─────────────────────────────────────────────────────────────────────────────

# Expansion map for KenPom abbreviations → full words
_ABBREV_EXPAND = {
    r"\bst\.?\b":        "state",
    r"\bn\.c\.\b":       "north carolina",
    r"\bnc\b":           "north carolina",
    r"\bfla\.\b":        "florida",
    r"\bmiss\.\b":       "mississippi",
    r"\bapp\.\b":        "appalachian",
    r"\btenn\.\b":       "tennessee",
    r"\bconn\.\b":       "connecticut",
    r"\bwis\.\b":        "wisconsin",
    r"\bmich\.\b":       "michigan",
    r"\bindiana\b":      "indiana",
    r"\bkentucky\b":     "kentucky",
    r"\bgeo\.\b":        "george",
    r"\bjax\b":          "jacksonville",
    r"\bwku\b":          "western kentucky",
    r"\butah\b":         "utah",
    r"\butep\b":         "texas el paso",
    r"\butsa\b":         "texas san antonio",
    r"\bfau\b":          "florida atlantic",
    r"\bfiu\b":          "florida international",
    r"\betsu\b":         "east tennessee state",
    r"\bsiue\b":         "southern illinois edwardsville",
    r"\bsiu\b":          "southern illinois",
    r"\bvcu\b":          "virginia commonwealth",
    r"\bvmi\b":          "virginia military",
    r"\bumbc\b":         "maryland baltimore county",
    r"\bumkc\b":         "missouri kansas city",
    r"\buab\b":          "alabama birmingham",
    r"\buic\b":          "illinois chicago",
    r"\bnjit\b":         "new jersey technology",
    r"\bliu\b":          "long island",
    r"\bfdu\b":          "fairleigh dickinson",
    r"\bsfa\b":          "stephen austin",
    r"\buiw\b":          "incarnate word",
    r"\bbyu\b":          "brigham young",
    r"\btcu\b":          "texas christian",
    r"\bsmu\b":          "southern methodist",
    r"\bucf\b":          "central florida",
    r"\bunlv\b":         "nevada las vegas",
    r"\buscb\b":         "santa barbara",
    r"\bucsb\b":         "santa barbara",
    r"\bunc\b":          "north carolina",
    r"\buconn\b":        "connecticut",
    r"\busc\b":          "southern california",
    r"\blsu\b":          "louisiana state",
}

# All known Odds API team nicknames (stripped before fuzzy match)
_ALL_NICKNAMES = [
    " Tar Heels", " Blue Devils", " Wolfpack", " Wolf Pack", " Demon Deacons",
    " Cavaliers", " Hokies", " Orange", " Golden Eagles", " Panthers",
    " Terrapins", " Terps", " Scarlet Knights", " Nittany Lions", " Hawkeyes",
    " Boilermakers", " Wildcats", " Illini", " Fighting Illini", " Hoosiers",
    " Badgers", " Golden Gophers", " Buckeyes", " Spartans", " Wolverines",
    " Cornhuskers", " Huskers", " Hawkeyes", " Cyclones", " Jayhawks",
    " Tigers", " Bears", " Eagles", " Lions", " Bulldogs", " Aggies",
    " Longhorns", " Razorbacks", " Rebels", " Crimson Tide", " Gators",
    " Seminoles", " Hurricanes", " Knights", " Cougars", " Mustangs",
    " Horned Frogs", " Frogs", " Cowboys", " Sooners", " Pokes",
    " Red Raiders", " Miners", " Roadrunners", " Lumberjacks", " Bearkats",
    " Mavericks", " Monarchs", " Cardinals", " Rams", " Falcons",
    " Broncos", " Aztecs", " Lobos", " Aggies", " Rainbow Warriors",
    " Warriors", " Bows", " Gauchos", " Tritons", " Anteaters",
    " Highlanders", " Matadors", " Banana Slugs", " Dirtbags", " 49ers",
    " Hornets", " Spiders", " Owls", " Flames", " Hawks", " Norse",
    " Penguins", " Flash", " Golden Flashes", " Zips", " Rockets",
    " Chippewas", " RedHawks", " Redhawks", " Bobcats", " Grizzlies",
    " Vikings", " Purple Aces", " Sycamores", " Salukis",
    " Leathernecks", " Lancers", " Penmen", " Tommies", " Jaspers",
    " Red Foxes", " Peacocks", " Stags", " Waves", " Pilots",
    " Trailblazers", " Mastodons", " Blue Raiders", " Mocs", " Paladins",
    " Warhawks", " Thunderbirds", " Skyhawks", " Buccaneers", " Explorers",
    " Ramblers", " Cyclones", " Mountaineers", " Hilltoppers", " Camels",
    " Quakers", " Big Red", " Crimson", " Big Green", " Ephs", " Judges",
    " Bald Eagles", " Comets", " Sharks", " Kangaroos", " Retrievers",
    " Ospreys", " Gaels", " Seawolves", " Great Danes", " Terriers",
    " Crusaders", " Black Knights", " Cadets", " Midshipmen", " Keydets",
    " Catamounts", " Minutemen", " Beacons", " River Hawks", " Green Terror",
    " Golden Bears", " Jaguars", " Royals", " Storm", " Hatters", " Dolphins",
    " Corsairs", " Blazers", " Phoenix", " Billikens", " Braves",
    " Chanticleers", " Thundering Herd", " Screaming Eagles", " Fighting Eagles",
    " Bison", " Jackrabbits", " Coyotes", " Fighting Hawks", " Lopes",
    " Running Eagles", " Pride", " Red Storm", " Colonials", " Colonels",
    " Greyhounds", " Golden Rams", " Patriots", " Red Wolves", " Governors",
    " Warhawks", " Mean Green", " Texans", " Blue Hens", " Retrievers",
    " Flyers", " Pilots", " Ducks", " Beavers", " Sun Devils",
    " Huskies", " Utes", " Cougars", " Trojans", " Bruins", " Ducks",
    " Wildcats", " Cardinal", " Golden Bears", " Buffaloes", " Buffs",
    " Thunderwolves", " Runnin' Bulldogs", " Bonnies", " Friars",
    " Hoyas", " Orangemen", " Retrievers", " Pioneers", " Colonials",
    " Flyers", " Musketeers", " Bearcats", " Billikens", " Friars",
    " Bulldogs", " Bonnies", " Red Flash", " Terriers", " Jaspers",
    " Peacocks", " Redmen", " Gaels", " Seahawks", " Chanticleers",
    " Privateers", " Delta Devils", " Dukes", " Redbirds", " Racers",
    " Commodores", " Shockers", " Bearkats", " Purple Eagles", " Lancers",
    " Griffins", " Golden Griffins", " Broncs", " Highlanders",
    " Pride", " Colonials", " Mavericks", " Skyhawks",
    " Islanders", " Thunderbirds", " Lumberjacks", " Bearkats",
    " Bengals", " Colonels", " Hilltoppers", " Norse", " Penguins",
    " Purple Eagles", " Bisons", " Stags", " Waves", " Pilots",
    " Riverhawks", " Texans", " Chanticleers", " Herd",
    " Beach", " Matadors", " Dirtbags", " 49ers",
]


def _strip_nickname(name: str) -> str:
    """Strip ALL known team nicknames from end of name."""
    for suffix in sorted(_ALL_NICKNAMES, key=len, reverse=True):
        if name.endswith(suffix):
            return name[: -len(suffix)].strip()
    return name.strip()


def _normalize(name: str) -> str:
    """
    Deep normalization for fuzzy fallback.
    Handles: periods, parentheticals, St./Saint, abbreviations, case.
    """
    n = name.lower().strip()
    # Remove parentheticals like (FL), (OH), (IL), (NY)
    n = re.sub(r"\s*\([^)]{1,5}\)", "", n)
    # Remove periods
    n = re.sub(r"\.", "", n)
    # Normalize separators
    n = re.sub(r"[-–—&]", " ", n)
    # Apply abbreviation expansions
    for pattern, replacement in _ABBREV_EXPAND.items():
        n = re.sub(pattern, replacement, n)
    # Collapse whitespace
    n = re.sub(r"\s+", " ", n).strip()
    return n


def _token_overlap(a: str, b: str) -> float:
    """
    Token overlap score: |intersection| / |union| (Jaccard similarity on words).
    Great at catching 'North Carolina State' vs 'NC State' after normalization.
    """
    ta = set(a.split())
    tb = set(b.split())
    if not ta or not tb:
        return 0.0
    return len(ta & tb) / len(ta | tb)


def _sim(a: str, b: str) -> float:
    """SequenceMatcher ratio."""
    return SequenceMatcher(None, a, b).ratio()


def _best_score(a: str, b: str) -> float:
    """Combined score: max of sequence ratio and token overlap."""
    seq = _sim(a, b)
    tok = _token_overlap(a, b)
    return max(seq, tok * 1.05)   # token overlap gets slight boost


def _load_team_map() -> dict:
    """
    Start with hardcoded seed (ground truth), then layer data/team_map.csv on top.
    Tries multiple path resolutions for Streamlit Cloud compatibility.
    """
    mapping = dict(_SEED)

    paths_to_try = [
        TEAM_MAP_PATH,
        os.path.join(os.path.dirname(os.path.abspath(__file__)), TEAM_MAP_PATH),
        os.path.join(os.getcwd(), TEAM_MAP_PATH),
    ]
    for path in paths_to_try:
        if os.path.exists(path):
            try:
                df = pd.read_csv(path)
                df = df[df["odds_name"].notna() & (df["odds_name"].str.strip() != "")]
                csv_map = dict(zip(df["kenpom_name"], df["odds_name"]))
                mapping.update(csv_map)
                print(f"    team_map.csv loaded: {len(csv_map)} CSV + {len(_SEED)} seed = {len(mapping)} total")
                return mapping
            except Exception as e:
                print(f"    team_map.csv error ({path}): {e}")

    print(f"    team_map.csv not found — using hardcoded seed ({len(_SEED)} entries)")
    return mapping


KENPOM_TO_ODDS = _load_team_map()


def fetch_vegas_lines() -> pd.DataFrame:
    if not ODDS_API_KEY:
        print("     ODDS_API_KEY not set — skipping Vegas lines.")
        return pd.DataFrame()

    now_utc = datetime.now(timezone.utc)
    now_ct  = datetime.now(CENTRAL)

    day_start_ct = now_ct.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end_ct   = day_start_ct + timedelta(hours=32)
    to_iso = lambda dt: dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    params = {
        "apiKey":             ODDS_API_KEY,
        "regions":            "us",
        "markets":            "spreads,totals,h2h",
        "oddsFormat":         "american",
        "bookmakers":         ",".join(BOOK_PRIORITY),
        "commenceTimeFrom":   to_iso(day_start_ct),
        "commenceTimeTo":     to_iso(day_end_ct),
        "dateFormat":         "iso",
    }

    try:
        resp = requests.get(BASE_URL, params=params, timeout=10)
    except Exception as e:
        print(f"    Odds API request failed: {e}")
        return pd.DataFrame()

    if resp.status_code == 401:
        print("    Odds API: 401 Unauthorized — check ODDS_API_KEY.")
        return pd.DataFrame()
    if resp.status_code == 429:
        print("    Odds API: 429 Too Many Requests — rate limited.")
        return pd.DataFrame()
    resp.raise_for_status()

    games = resp.json()
    remaining = resp.headers.get("x-requests-remaining", "?")
    print(f"    Vegas lines fetched ({len(games)} games) | Credits remaining: {remaining}")

    rows = []
    skipped_live = 0
    for game in games:
        raw_time = game.get("commence_time", "")
        game_time_ct = ""
        if raw_time:
            try:
                utc_dt = datetime.strptime(raw_time, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
                if utc_dt <= now_utc:
                    skipped_live += 1
                    continue
                ct_dt = utc_dt.astimezone(CENTRAL)
                game_time_ct = ct_dt.strftime("%-I:%M %p CT")
            except Exception:
                pass

        home = game["home_team"]
        away = game["away_team"]
        vegas_spread  = None
        vegas_total   = None
        vegas_home_ml = None
        source_book   = None

        for book_key in BOOK_PRIORITY:
            book = next((b for b in game["bookmakers"] if b["key"] == book_key), None)
            if not book:
                continue
            markets = {m["key"]: m["outcomes"] for m in book["markets"]}

            if vegas_spread is None and "spreads" in markets:
                for outcome in markets["spreads"]:
                    if outcome["name"] == home:
                        vegas_spread = outcome["point"]
                        source_book  = book["title"]
                        break

            if vegas_total is None and "totals" in markets:
                for outcome in markets["totals"]:
                    if outcome["name"] == "Over":
                        vegas_total = outcome["point"]
                        break

            if vegas_home_ml is None and "h2h" in markets:
                for outcome in markets["h2h"]:
                    if outcome["name"] == home:
                        vegas_home_ml = outcome["price"]
                        break

            if vegas_spread is not None and vegas_total is not None:
                break

        rows.append({
            "vegas_home":     home,
            "vegas_away":     away,
            "vegas_spread":   vegas_spread,
            "vegas_total":    vegas_total,
            "vegas_home_ml":  vegas_home_ml,
            "source_book":    source_book,
            "odds_game_time": game_time_ct,
        })

    if skipped_live:
        print(f"    Skipped {skipped_live} live/started games")

    global _odds_last_fetched
    _odds_last_fetched = now_ct.strftime("%-I:%M %p CT")

    return pd.DataFrame(rows)


def get_odds_last_fetched() -> str:
    return _odds_last_fetched


def _score_pair(kp: str, odds_home: str, odds_away: str):
    """
    5-layer match scoring for a single KenPom name vs a single Odds API name.
    Returns (score, is_home_match) where score > 0 means a match was found.

    Layers (tried in order, returns immediately on seed/exact hit):
      1. Seed exact lookup (KENPOM_TO_ODDS)
      2. Exact match after nickname strip
      3. Normalize both sides, exact match
      4. Token overlap score
      5. SequenceMatcher fuzzy score
    """
    # Layer 1: Seed lookup → exact compare to both sides
    seeded = KENPOM_TO_ODDS.get(kp)
    if seeded:
        if seeded == odds_home:
            return 1.0, True
        if seeded == odds_away:
            return 1.0, False

    # Prepare normalized forms
    kp_stripped  = _strip_nickname(kp).lower()
    kp_norm      = _normalize(kp_stripped)

    h_stripped   = _strip_nickname(odds_home).lower()
    h_norm       = _normalize(h_stripped)
    a_stripped   = _strip_nickname(odds_away).lower()
    a_norm       = _normalize(a_stripped)

    # Layer 2: Exact after strip
    if kp_stripped == h_stripped:
        return 0.99, True
    if kp_stripped == a_stripped:
        return 0.99, False

    # Layer 3: Exact after full normalization
    if kp_norm == h_norm:
        return 0.98, True
    if kp_norm == a_norm:
        return 0.98, False

    # Layers 4 + 5: scored comparison
    score_h = _best_score(kp_norm, h_norm)
    score_a = _best_score(kp_norm, a_norm)

    if score_h >= score_a:
        return score_h, True
    else:
        return score_a, False


def match_vegas_to_game(result: dict, vegas_df: pd.DataFrame) -> dict:
    """
    Match KenPom game to Vegas line using 5-layer matching.
    Returns result dict enriched with Vegas data or nulled fields.
    """
    _null = lambda r: {**r,
        "vegas_spread": None, "vegas_total": None, "vegas_home_ml": None,
        "spread_edge": None, "total_edge": None, "edge_score": None,
        "vegas_fav": None, "my_fav": None, "sides_agree": None,
        "source_book": None, "odds_game_time": None}

    if vegas_df.empty:
        return _null(result)

    kp_t1 = result["team1"]  # team1 = home
    kp_t2 = result["team2"]  # team2 = away

    best_match   = None
    best_flipped = False
    best_score   = 0.0

    for _, row in vegas_df.iterrows():
        vh = row["vegas_home"]
        va = row["vegas_away"]

        # Score t1 vs home side and t2 vs away side
        s1, t1_is_home = _score_pair(kp_t1, vh, va)
        s2, t2_is_home = _score_pair(kp_t2, vh, va)

        # Normal orientation: t1=home, t2=away
        normal_score  = 0.0
        flipped_score = 0.0

        if t1_is_home and not t2_is_home:
            # t1 matched home, t2 matched away — perfect normal
            normal_score = (s1 + s2) / 2
        elif not t1_is_home and t2_is_home:
            # t1 matched away, t2 matched home — flipped
            flipped_score = (s1 + s2) / 2
        else:
            # Try all four combos for edge cases
            sh_t1 = _best_score(_normalize(_strip_nickname(kp_t1).lower()),
                                 _normalize(_strip_nickname(vh).lower()))
            sa_t2 = _best_score(_normalize(_strip_nickname(kp_t2).lower()),
                                 _normalize(_strip_nickname(va).lower()))
            sh_t2 = _best_score(_normalize(_strip_nickname(kp_t2).lower()),
                                 _normalize(_strip_nickname(vh).lower()))
            sa_t1 = _best_score(_normalize(_strip_nickname(kp_t1).lower()),
                                 _normalize(_strip_nickname(va).lower()))

            normal_score  = (sh_t1 + sa_t2) / 2
            flipped_score = (sh_t2 + sa_t1) / 2

        top = max(normal_score, flipped_score)
        if top > best_score:
            best_score   = top
            best_match   = row
            best_flipped = (flipped_score > normal_score)

    if best_match is None or best_score < MATCH_THRESHOLD:
        print(f"    NO MATCH: {kp_t1} vs {kp_t2}  (best_score={best_score:.3f})")
        return _null(result)

    # ── Apply matched Vegas line ──────────────────────────────────────────────
    v_total     = best_match["vegas_total"]
    raw_vspread = best_match["vegas_spread"]
    v_spread    = (-raw_vspread if raw_vspread is not None else None) if best_flipped else raw_vspread

    result["vegas_spread"]   = v_spread
    result["vegas_total"]    = v_total
    result["vegas_home_ml"]  = best_match["vegas_home_ml"]
    result["source_book"]    = best_match["source_book"]
    result["odds_game_time"] = best_match.get("odds_game_time", "")

    if v_spread is not None and v_total and v_total > 0:
        my_spread = result["spread"]
        my_vs     = -my_spread

        spread_diff = abs(v_spread - my_vs)
        vegas_fav   = result["team1"] if v_spread < 0 else (result["team2"] if v_spread > 0 else "Pick")
        my_fav      = result["team1"] if my_spread > 0 else (result["team2"] if my_spread < 0 else "Pick")

        result["spread_edge"] = round(spread_diff, 2)
        result["total_edge"]  = round(abs(result["total"] - v_total), 2)
        result["edge_score"]  = round(spread_diff / v_total, 4)
        result["vegas_fav"]   = vegas_fav
        result["my_fav"]      = my_fav
        result["sides_agree"] = (vegas_fav == my_fav)
    else:
        result["spread_edge"] = None
        result["total_edge"]  = None
        result["edge_score"]  = None
        result["vegas_fav"]   = None
        result["my_fav"]      = None
        result["sides_agree"] = None

    return result
