# 🏀 CBB Betting Model — Python MVP

Automates your Google Sheets CBB model using KenPom data + NCAA NET rankings.

## Setup

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Set up credentials (NEVER commit this file)
cp .env.example .env
# Edit .env and add your KenPom email, password, and API key

# 3. Run
python run.py
```

## Project Structure

```
cbb_model/
├── run.py              ← Main entry point. Edit GAMES list here daily.
├── model.py            ← All projection logic (pace, TO, REB, FT, unit score, HCA)
├── kenpom_fetcher.py   ← Logs into KenPom and downloads CSVs
├── net_fetcher.py      ← Scrapes NCAA NET rankings (public page)
├── requirements.txt
├── .env.example        ← Copy to .env and fill in credentials
├── data/               ← Auto-populated CSVs from KenPom
└── outputs/            ← projections.csv written here after each run
```

## Column Names Expected from KenPom CSVs

The model expects these columns (from standard KenPom exports):

| File           | Key Columns                                              |
|----------------|----------------------------------------------------------|
| summary.csv    | TeamName, Rk, AdjOE, AdjDE, AdjTempo                   |
| four_factors.csv | TeamName, TOPct, DTOPct, ORPct, DORPct, FTRate, DFTRate |
| height.csv     | TeamName, AvgHgt, Experience                            |
| hca.csv        | HCA (single value)                                      |
| net.csv        | TeamName, Rank                                          |

If column names differ after downloading, update the references in model.py.

## Scheduling (run automatically every morning)

**Mac/Linux — cron:**
```bash
crontab -e
# Add this line to run at 7am daily:
0 7 * * * cd /path/to/cbb_model && python run.py >> logs/run.log 2>&1
```

**GitHub Actions (free cloud scheduling):** Create `.github/workflows/daily.yml` — ask Claude for help setting this up.

## Next Steps (Nice to Have)
- Push outputs/projections.csv to Google Sheets automatically via `gspread`
- Build a Flask/FastAPI web dashboard
- Add backtesting against historical results
