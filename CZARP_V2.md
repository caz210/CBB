# CZarp v2: model changes

## What changed
| # | Change | Where |
|---|--------|-------|
| 1 | The season comes from the Central date (Nov 2026 is season 2027, rolling over July 1). Fixed the `czarz_ml_correct` typo in the app. | `czarp/season.py`, `kenpom_fetcher.py`, `run.py`, `app.py` |
| 2 | KenPom multiplicative efficiency: PPP = AdjOE × oppAdjDE / national avg / 100, and pace = T1×T2/avg. You can switch back to the v1 additive formula in config. | `model.py` |
| 3 | The four factors are no longer added on top of AdjOE/AdjDE, so they aren't double counted. They are replaced by **matchup cards**: four offense archetypes (Addicted to 3s, Don't Blink, Board Crashers, Free Throw Merchants) and four defense archetypes (Lock Down, No Rebounds for You, Arc Wall, Turnover Machine). Each archetype is the top 15% nationally. Counter pairs subtract points and exploits of the bottom 15% add points. Every coefficient is in config, and team tags appear in the output. | `czarp/teamstats.py`, `czarp/config.py` |
| 4 | The unit score (height/exp/bench) has configurable weights. Optional multipliers add weight for close games, neutral sites and late season. All multipliers are 1.0 (off) by default. | `model.py`, `UnitConfig` |
| 5 | Each team gets its own home-court advantage (HCA) from KenPom `hca.php`, which needs a subscriber login (KENPOM_EMAIL/PASSWORD). The values are cached to `data/hca.csv`. If they aren't available, the model uses the league-average fallback of 3.5. | `hca_fetcher.py` |
| 6 | Clutch is rebuilt from z-scores of eFG%, TO% and FT%. It only applies when \|margin\| ≤ 5 and is capped at ±1.5. v1 called the wrong endpoint (`misc`, which should be `misc-stats`), so its clutch value was always 0. | `model.py`, `czarp/teamstats.py` |
| 7 | Full precision internally. The `*_raw` fields hold full-precision values, and only the display fields are rounded to 0.5. | `model.py` |
| 8 | The projected margin becomes a probability through a Normal distribution with SD 11; totals use SD 15. Edge = model probability − break-even probability (vig or fees included). Tiers are 3/5/8 pp. Stakes are quarter-Kelly, capped at 2.5%. | `czarp/probability.py`, `czarp/betting.py` |
| 9 | Price source toggle: `--price-source vegas\|kalshi\|robinhood`. Vegas prices are now kept and de-vigged. The Kalshi option reads the KXNCAAMBGAME/SPREAD/TOTAL boards and applies Kalshi or Robinhood fees. | `odds_fetcher.py`, `czarp/kalshi.py`, `czarp/betting.py`, `run.py` |
| 10 | Backtest: free ESPN results with DraftKings open and close lines, plus KenPom point-in-time ratings from the archive endpoint. It reports ATS/totals results by tier, ROI, CLV, Brier and calibration. | `espn_history.py`, `backtest.py` |

## Early season
At the moment KenPom 2027 only has the preseason AdjOE/AdjDE/AdjTempo. Its four factors, misc and height tables hold placeholders (0/100/200/-99). Those placeholders are treated as missing. When `data/prior/` exists (written by `kenpom_fetcher.py`, `run.py` and the app), last season's values fill the gaps, and those columns are listed in `stats_from_prior_season`. Otherwise the related adjustments are set to 0.

## Config
Copy any subset of `czarp/config.py` into `czarp_config.json` (it is gitignored) or point `CZARP_CONFIG` at a file. For example:
```json
{"betting": {"margin_sd": 10.5, "kalshi_fee_profile": "robinhood_gold"},
 "matchup": {"counter": {"Addicted to 3s|Arc Wall": 1.0}}, "unit": {"close_game_mult": 1.5}}
```

## Commands
```bash
python run.py --price-source vegas          # or kalshi / robinhood; --no-refresh, --date YYYY-MM-DD
python hca_fetcher.py                       # needs KENPOM_EMAIL / KENPOM_PASSWORD
python espn_history.py 2026-01-05 2026-02-28
python backtest.py data/history/espn_20260105_20260228.csv --ratings archive --style <dir with 2026 four_factors/height/misc csvs>
python -m pytest -q tests
```
