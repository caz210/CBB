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

## Early season (`model.early`)
Efficiency and tempo always come from KenPom's current AdjOE/AdjDE/AdjTempo (its own preseason prior); there is no prior-season override. Early in the season KenPom 2027's four factors, misc and height tables hold placeholders (0/100/200/-99), which are treated as missing. Style stats (four factors, 3PA rate, etc.) are filled according to `early.style_fill`:

| mode | behaviour |
|------|-----------|
| `regressed` (default) | last season's value shrunk toward the national mean: `mean + keep*(prior-mean)`, `keep = prior_keep_max(0.5) * continuity` (0.25 if continuity unknown), then blended with this season's stats by games played, `w = min(1, games/full_weight_games(10))` |
| `prior` | last season's raw value (v2 phase-1 behaviour) |
| `off` | no fill; related adjustments are 0 until real stats exist |

Archetype tags need current-season evidence: a team with fewer than `tag_evidence_games` (5) games shows its tags as "X (tentative)" (`tags_tentative` in the output), and any matchup points involving a tentative team are multiplied by `tentative_tag_mult` (0.5).

## Fitted defaults (walk-forward backtest, Nov 2026)
`walkforward.py` fits everything on point-in-time data (KenPom archive ratings by date, to-date ESPN box stats + regressed prior, DraftKings open/close lines via ESPN), refitting monthly on all earlier games and testing on the next month. 9,356 OOS games, 2025-01-01..2026-04-06. Full report: `outputs/walkforward/report.txt`.

* calibration: margin = 1.137 × base margin; total = 27.04 + 0.822 × base total; KenPom per-team HCA × 1.109
* margin SD 11.4 (blend 11.2), total SD 16.9
* quality gap, matchup points and clutch are **off by default** — none added out-of-sample value (matchup points hurt: dMSE −0.21, CI excludes 0). Tags/cards are still computed and shown.
* unit score kept with fitted weights (height 208.7 per ft-equivalent unit ≈ 0.27 pt/in, experience 8.66 ≈ 0.48 pt/yr, bench −14.4); small OOS gain (dMSE +0.20, CI −0.08..+0.48) and a modest edge vs the market residual (slope +0.36, t 2.1).
* `betting.blend_with_market = True`: edges are computed from `0.1 × model + 0.9 × market` (spread/ML) and `0.15 × model + 0.85 × market` for totals. With Kalshi/Robinhood the market number is implied from the Kalshi spread/total ladders (mids between 10–90%), falling back to the ML. Set it to false to see raw model edges.
* No edge tier was profitable at −110 in both seasons; treat bets as experimental.
* Robinhood fees: `"robinhood"` now means Robinhood Gold (k = 5%); `"robinhood_standard"` is k = 10%.

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
python fetch_history.py 2025-11-03 2026-04-06     # ESPN results + DK open/close lines → data/history/
python fetch_kenpom_history.py                    # KenPom season tables + daily archive/fanmatch → data/history/
python walkforward.py build 2025 2026             # point-in-time features (needs KENPOM_API_KEY; cached archive pulls)
python walkforward.py fit                         # walk-forward fit → outputs/walkforward/{report.txt,fit.json,fits_by_month.csv}
python kenpom_scraper.py 2026-11-02               # FanMatch scrape incl. KenPom predicted score (kp_team1_margin / kp_total)
python -m pytest -q tests
```
