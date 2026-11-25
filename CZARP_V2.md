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
lines (free) → Odds API fallback whenever any D-I game lacks an ESPN/DK line (≤20 paid calls per CT day across all jobs, raised from 5 on 24 Nov 2026 for The Odds API 20K plan: daily 2 (the 7 AM run + one re-run), pretip 10 (pretip_check: missing market_lines fill + closing-line snapshot, one shared call per run), refresh 6 (the intraday `refresh_lines.py`), manual 2 (app.py, run.py, team_mapper.py, results_tracker.py);
every paid request is claimed first in ONE place, `czarp/odds_budget.py` (via `odds_fetcher.fetch_vegas_lines` / `odds_fetcher.odds_api_get`), key in the `x-api-key` header;
100-credit reserve, usage in `data/ledger/odds_api_usage.json` under the checkout — counted under the real CT date, never `--date`;
fails closed if that file is missing/corrupt; create it once with `python3 -m czarp.odds_usage --init`, check it with `--check`.
The 7 AM run and `pretip_check.py` MUST run on the same machine from the same checkout: the cap is a local file + flock and is not
shared across machines/runners. Exception: `refresh_lines.py` runs in GitHub Actions and does NOT use that file for its cap: its
paid calls are claimed in Supabase (`odds_api_refresh_claims`, primary key (CT day, n)); the job only inserts n=1..6
and the table's CHECK is n between 1 and 6 (migration `2026-11-25_odds_api_cap_20_closing_lines.sql`). A missing, stale or
unsaved Actions cache can never hide earlier calls, and --force or a duplicate firing cannot exceed 6. Each refresh run first reads the API's own
x-requests-used / x-requests-remaining from the free `/v4/sports` endpoint (key in a header); if that read fails, credits
are under the reserve, or the API shows 20+ paid calls since today's first refresh run, it makes no paid call (ESPN only).
Box daily (2) + pretip (10) + manual (2) + refresh (6) = at most 20 a day)

**Intraday line refresh (`refresh_lines.py`, GitHub Actions on main: "Intraday line refresh").** Normal days run at 10 AM, 1 PM,
4 PM and 7 PM CT; light days (fewer than `LIGHT_DAY_MAX_GAMES` = 20 D-I games on today's slate) at 12 PM and 5 PM CT. The workflow
fires at :05 of every UTC hour that can be one of those CT hours in CST or CDT; the script gates on America/Chicago time: a run
counts for slot H from H:00 to H+1:05 CT (a firing up to 60 min late still runs), deciding the day type from a HEAD count of today's games. Each run pulls
current lines (ESPN first, the Odds API 'refresh' slot only for games ESPN lacks), inserts a new vegas `current` market_lines row only
when a line/price changed. For a game whose line inputs changed since its newest projections row it writes a new projections row
per model: projections copied unchanged, and ONLY the changed market's edges, skip codes, skip reasons and cover/over prob
recomputed by `daily_run.attach_skip_reasons` from the full-precision model numbers (`projections.home_margin_full` /
`total_full`, written by the 7 AM run), so they equal what daily_run computes for the same line; the other market is copied as-is
(a total move also re-stamps the spread, whose edge divides by the total). edges_as_of = created_at. An unchanged line writes
nothing. Each (CT day, slot) runs once (`line_refresh_runs`). New-coach holds are carried over from the 7:15 row. It never writes
picks or the ledger, and never stamps a game within 10 minutes after a pick is logged on it. `pretip_check.py` routes the games it
fills through the same `refresh_lines.recompute_edges`. Needs migration `db/migrations/2026-11-21_intraday_line_refresh.sql`
applied first (checked before any write).
    python refresh_lines.py [--force] [--dry-run] [--no-odds-api]

**Odds API counter recovery** (`data/ledger/odds_api_usage.json`). If the daily report notes or pretip_check's stderr say
"Odds API call refused (fail closed)", Odds API calls stop until the file is fixed (ESPN lines and picks are unaffected):
1. `python3 -m czarp.odds_usage --check` — prints why the file is not usable.
2. If the cause is an I/O error (read-only folder, disk full), fix permissions/space; the file itself is fine.
3. If the file is corrupt or missing, do NOT just re-init it (that would read as "0 calls today" and allow 2 more):
   find today's real calls first — the 7 AM log/daily report ("Odds API: filled ..." / "Vegas lines fetched") and
   pretip_check stderr — and the latest "Credits remaining" figure.
4. Move the bad file aside (`mv odds_api_usage.json odds_api_usage.json.bad`), run `python3 -m czarp.odds_usage --init`,
   then write today's real counts back (CT date), e.g.
   `{"calls": {"2026-11-21": 1}, "calls_by_job": {"2026-11-21": {"daily": 1}}, "remaining": "19980"}`.
   When unsure, record today's daily and pretip slots as used, e.g. `"calls_by_job": {"<today>": {"daily": 1, "pretip": 1}}`
   with `"calls": {"<today>": 2}` (no more box calls today; it resets tomorrow).
5. `python3 -m czarp.odds_usage --check` must say ok. → Kalshi reference prices →
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

**Grading between 7 AM runs** (`grade_finals.py`). Grades open plays (official and track) whose games are final —
the same `daily_run.grade()` the 7 AM run uses (result, scores, P&L, DK closing line + CLV; void only when ESPN says
STATUS_POSTPONED / STATUS_CANCELED, since a void is permanent) — then fills a DK close that was not posted yet on plays
graded in the last 2 days (NULL fields only), writes the ledger, and PATCHes only the still-NULL grading columns of
those Supabase `picks` rows (what `picks_immutable` allows). Every ESPN call happens BEFORE it takes the ledger lock
(capped at 240 s); under the lock it re-reads the ledger, grades from the fetched data and writes (milliseconds);
the Supabase PATCHes run after the lock is released. Free ESPN only: no KenPom, Odds API, Kalshi, new picks, or
games/projections/market_lines writes. Re-runs are no-ops; a broken hash chain stops it before any write. It MUST
run on the same host and checkout as `daily_run.py` and `pretip_check.py` (the lock is a local flock; never in CI):

    python grade_finals.py [--dry-run] [--no-supabase] [--lock-wait SECONDS] [-v]

**Ledger lock.** `daily_run.py`, `pretip_check.py` and `grade_finals.py` all rewrite `data/ledger/ledger.csv`, so each
holds an exclusive flock on `data/ledger/ledger.csv.lock` (gitignored; `daily_run.ledger_lock`) from its ledger read to
its write, and nobody makes a network call while holding it except the 7 AM run itself.
- 7 AM run (any run that writes the ledger; `--dry-run`, `--resync` and `--chat-only` take no lock): holds it for the
  whole run. It waits up to 30 min (`DAILY_LOCK_WAIT_S`) for it — the other holders keep it for milliseconds — and
  it NEVER runs unlocked: if it still can't get it, it reads, grades, logs and writes nothing, prints `!!! ... NOTHING
  WAS WRITTEN`, exits 3, and is safe to re-run.
- `pretip_check.py`: writes `pretip_flag` on a fresh read under the lock (waits up to 2 min; if it can't, the alert
  is still printed and only the flag is skipped).
- `grade_finals.py`: skips the cycle when the lock is busy (e.g. during the 7 AM run).
- `check_immutable` makes every grading field set once, like the trigger: result, graded_ct, pnl, pnl_units, scores,
  close_line, close_price and clv_pts may go from empty to a value, never change or go back to empty; a stale copy
  written over a newer ledger is refused. The 7 AM grading stays as the backstop.

**Pre-tip check** (`pretip_check.py`). Re-pulls the DK line (ESPN; Odds API only for plays ESPN lacks,
within the daily budget) for today's open plays that have not tipped, and prints a short summary of any
play whose line moved ≥2 pts against us since the pick or (sides only) whose edge vs the current line fell
below the threshold. It prints nothing when there is nothing to flag. It only writes the ledger's `pretip_flag`
column (never prices, stakes or results); `--dry-run` writes nothing and spends no Odds API credits.
**Closing lines:** every run also snapshots the spread/total of games with an open official or lean play tipping within
`--close-within` minutes (default 30) into Supabase `closing_line_snapshots` (insert-only; ESPN/DK every run, The Odds
API once per game when the pretip slot allows). A game's closing line is its newest row before tipoff. Picks, the
picks_immutable trigger and the hash chain are never touched. All Odds API use in one run shares ONE claimed call.

    python pretip_check.py [--dry-run] [--within MINUTES] [--move 2] [--date YYYY-MM-DD] [--no-odds-api]
                           [--close-within 30] [-v]

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
