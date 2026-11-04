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

## Daily automation (`daily_run.py`)

    python daily_run.py [--date YYYY-MM-DD] [--no-refresh] [--no-odds-api] [--no-kalshi] [--dry-run] [--config FILE]

One command: refresh KenPom (ratings/prior/HCA) + FanMatch → ESPN slate + DraftKings open/current
lines (free) → Odds API fallback only if ESPN lacks lines for ≥25% of D-I games (≤1 call/day,
100-credit reserve, usage in `data/ledger/odds_api_usage.json`) → Kalshi reference prices →
v2 sides + original-v1 totals → plays (sides edge% ≥4% vs BOTH open and current line; totals edge% ≥3% vs the open, skipped if the total moved ≥3 against us) with quarter-Kelly
paper stakes (conservative 54.5% / 53.5% hit-rate estimates, 2.5% cap, plays under $1 skipped)
→ `data/ledger/ledger.csv` (one pick per game+market, first price kept; per-run snapshots in
`data/ledger/snapshots/`) → grades earlier plays (final score, DK closing line, CLV, P&L) →
`outputs/daily/YYYY-MM-DD.md`. Safe to re-run; missing games/APIs degrade gracefully.
Run it early (e.g. cron `0 7 * * *`): the backtested edge exists only against opening lines.

**Top 10 edges.** Every report has a "Top 10 edges" table: the 10 largest edge% candidates of the day
across sides (v2) and totals (v1), regardless of threshold, with Vegas open/current, CZarp line,
edge% vs open and vs current, whether it was played, and why not (below threshold, edge gone vs
current, no opening line, game started, price too juiced, line-move flag).

**Line-move ("likely news/injury") flag.** A side whose DK spread moved ≥2 pts open→current is flagged.
`python line_move_study.py data/history/v1_preds.pkl` → `outputs/walkforward/line_move_study.txt`:
22% of games move ≥2 pts (35% in November), and flagged side picks did *better* than unflagged
(sides ≥4% vs open: flagged 22-5, unflagged 26-22), so the flag is informational by default;
`--exclude-line-moves` turns the exclusion on. `--dry-run` writes the report to
`outputs/daily/<date>.dryrun.md` (gitignored) and never touches the ledger.

## Config-driven model (custom models)

`ModelConfig.model_version` + `ModelConfig.features` (list of `{name, kind, weight, enabled}`).
Built-in kinds (`efficiency, hca, quality_gap, matchup, unit, clutch`) multiply that component;
generic `stat_diff` / `stat_matchup` kinds add z-scored team-table stats with a points weight.
The default list reproduces v2 exactly (verified: max diff 0.0 on the 11/02 slate). Examples:
`configs/czarp_v2.json`, `configs/example_custom.json`; select with `--config` or `CZARP_CONFIG`.

## Supabase mirror (optional)

`db/schema.sql` (multi-sport: sports/leagues/teams/games/model_configs/projections/market_lines/
picks + `pick_record` view; RLS read-only for anon/authenticated, service-role writes; seeds CBB).
`daily_run.py` mirrors outputs via PostgREST only when `SUPABASE_URL` and `SUPABASE_SERVICE_KEY`
are set (`czarp/supabase_writer.py`); the CSV ledger stays the source of truth. No raw KenPom stats.

Front-end (czarp-web) contract: read-only views `slate_games`, `value_bets`, `ledger` (columns exactly as in
czarp-web `src/lib/supabase/README.md`), plus per-user tables `profiles` (own row, read-only to the user;
tier set server-side) and `user_models` (own rows, full CRUD). Picks (and so value_bets/ledger rows) are
visible to everyone after tip-off and immediately to pro/syndicate tiers (`public.is_pro()`). Units: 1u = 1%
of the paper bankroll at pick time. The writer deletes mirrored picks no longer in the CSV ledger.

## NEW COACH lock, line-move skip, pre-tip check

**NEW COACH ("new philosophy") lock** (`czarp/coach.py`). A team whose KenPom head coach this season
differs from last season's (`data/ratings.csv` / `data/teams.csv` `Coach` vs `data/prior/ratings.csv`,
or `data/coaches/teams_<prev season>.csv`) is tagged `NEW COACH`. Its games are not value plays until it has
played `COACH_LOCK_TOTALS` games for totals (default 4, `--coach-lock-totals N`) and `COACH_LOCK_SIDES`
games for sides (default 8, `--coach-lock-sides N`) this season (KenPom W+L; `--coach-lock-games N` sets
both; 0 = off). Split per the backtest below (approved 2026-11-03). `data/coach_changes.csv` (`season,team[,coach,prev_coach][,new_coach]`)
adds a team or, with `new_coach=0`, clears a false positive. The Top 10 table has a Tags column
(`NEW COACH Air Force 1/8 (7 left)`), and the report notes list every locked team on the slate.

**Per-team flag file: `config/coach_locks.csv`** (git-tracked, overrides auto-detection; audited 2026-11-04 against
the 2026 carousel trackers, every row has a source link). Columns: `season,team,new_coach,coach,prev_coach,lock_sides,lock_totals,note,source`.
`new_coach` Y = locked (added even if KenPom missed it), N = never locked. `lock_sides` / `lock_totals`: blank = default
length, `N`/`0` = no lock for that market, a number = custom games. Teams not in the file use KenPom auto-detection.
Edit by hand or with `python3 coach_locks.py on|off TEAM [--sides N --totals N --note .. --source ..]`, `list`, `audit`,
then commit. One-run overrides: `daily_run.py --coach-lock TEAM`, `--coach-unlock TEAM`, `--no-coach-lock`.

Backtest (`python coach_lock_study.py` → `outputs/walkforward/coach_lock_study.txt`; 2024-25 + 2025-26,
67 + 63 coaching changes, bets at the open, -110):

| lock N | sides ≥4% vs open: kept · excluded | totals ≥3% vs open: kept · excluded |
|---|---|---|
| 0 | 62-55 (53.0%) · — | 813-671 (54.8%) · — |
| 4 | 59-49 (54.6%) · 3-6 | 775-638 (54.8%) · 38-33 (53.5%) |
| 6 | 59-47 (55.7%) · 3-8 | 744-629 (54.2%) · 69-42 (62.2%) |
| 8 | 58-46 (55.8%) · 4-9 | 723-617 (54.0%) · 90-54 (62.5%) |
| 10 | 57-45 (55.9%) · 5-10 | 710-599 (54.2%) · 103-72 (58.9%) |
| 12 | 56-44 (56.0%) · 6-11 | 696-587 (54.2%) · 117-84 (58.2%) |

Sides: early new-coach side picks lost (4-9 under N=8), but the sample is tiny. Totals: only the first
~4 games were worse than the market (model-minus-market MAE +0.20 for new-coach teams in games 0-3, vs
-0.06 for other teams); games 4-7 were *better* than average, so a long totals lock gives up winners.

**Totals: edge vs open only (2026-11-03).** Totals no longer also need edge% ≥3% vs the *current*
line. In `coach_lock_study.py`, totals ≥3% vs open hit 54.8%; adding the vs-current check dropped it to
48.3% (it throws out lines that moved toward us, our best bets); ≥3% vs open + the move-against skip below
hit 55.6% (~+6% ROI, closing-line backtest). Edge vs current is still shown in the Top 10 table, and the
pre-tip check no longer flags totals for edge vs current (line moves only). Sides are unchanged.

**Totals line-move skip.** A total whose DK line moved ≥ `TOTAL_MOVE_SKIP_PTS` (3) from open to current
*away from our side* is skipped (backtest: 24-42, -30.6% ROI; 2-2.5 pts against was 26-41). A side that
moved ≥3 against us is flagged ⚠ only (`--skip-side-moves` to skip): there was 1 such side pick in two
seasons, so there is nothing to test. The Top 10 "Line move" column now says how far and whether it
moved with or against our side.

**Pre-tip check** (`pretip_check.py`). Re-pulls the DK line (ESPN; Odds API only for plays ESPN lacks,
within the daily budget) for today's open plays that have not tipped, and prints a short summary of any
play whose line moved ≥2 pts against us since the pick or (sides only) whose edge vs the current line fell
below the threshold. It prints nothing when there is nothing to flag. It only writes the ledger's `pretip_flag`
column (never prices, stakes or results); `--dry-run` writes nothing and spends no Odds API credits.

    python pretip_check.py [--dry-run] [--within MINUTES] [--move 2] [--date YYYY-MM-DD] [--no-odds-api] [-v]

## Track record: OFFICIAL vs TRACK tiers, immutable ledger
- **OFFICIAL** (unchanged): sides ≥4% vs open AND vs current; totals ≥3% vs open (+ move skip). Quarter-Kelly paper stakes.
- **TRACK** (shadow, paper only): sides ≥4% vs open with ≥2% still left vs current that are not official. Every other
  rule is identical (same side, not started, NEW COACH lock), so the two records differ only in the vs-current bar.
  Flat 1u, `stake` 0 (never touches the bankroll), `pick_id` = `<event>-spread-track`, never pre-tip alerted.
  `--track-ignore-coach-lock` (or `TRACK_IGNORE_COACH_LOCK`) also logs NEW COACH-locked sides.
- Ledger columns added: `tier`, `source` (live/backfill), `logged_ct`, `pnl_units` (flat 1u), `pick_hash`.
- **Immutability**: `write_ledger` refuses to change a logged pick's fields, drop a pick, or change a graded result;
  live picks must be logged before tip-off; each row is sealed with `pick_hash = sha256(prev_hash + pick fields)`
  and `verify_ledger()` runs every morning (result printed in the report). Supabase mirrors this with trigger
  `picks_immutable` (pick and grading fields are set-once, deletes refused, live picks must predate tip-off).
- Report: "Tracked (not played)" section + "Running record by tier" table (W-L-P, flat units, ROI, CLV, beat-close).
- Supabase: `picks.tier/source/logged_at/units_pnl/pick_hash`; view `tier_record`; `tier` appended to `pick_record`,
  `value_bets`, `ledger` (migration `db/migrations/2026-11-04_pick_tiers_and_immutability.sql`).
- Backfill past slates from the saved morning snapshot: `python backfill_track.py --date YYYY-MM-DD [--write]`.
