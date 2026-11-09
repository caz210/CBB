# Tag & Four Factors Review Proposal

**Date:** 2026-11-08 (America/Chicago)  
**Branch:** `czarp-v2`  
**Status:** Research only — no code, DB, or site changes in this pass.  
**Audience:** Christian (product) + World Wide (UI) + CZarp (model pipeline)

---

## 1. What we have today

### 1.1 Code inventory (`czarp/teamstats.py` + `MatchupConfig`)

Tags are **national top/bottom 15%** (`MatchupConfig.top_pct = 0.15`) over the canonical team table built from KenPom ratings / four-factors / misc / height.

| Tag (shown) | Rule | Data field | Group |
|---|---|---|---|
| **Addicted to 3s** | top 15% 3PA rate | `FG3A_Rate` (misc) | Shooting volume |
| **Don't Blink** | top 15% of tempo−possession composite, or AdjTempo alone if APL missing | `AdjTempo` + `APL_Off` | Pace |
| **Board Crashers** | top 15% OR% | `OR` | Glass |
| **Free Throw Merchants** | top 15% FT rate | `FTR` | Free throws |
| **Lock Down** | **best** 15% AdjDE (lowest AdjDE) | `AdjDE` | Outcome defense (not a Four Factor) |
| **No Rebounds for You** | best 15% (lowest) opp OR% | `Opp_OR` | Glass D |
| **Arc Wall** | best 15% (lowest) opp 3PA rate | `Opp_FG3A_Rate` | 3PT defense volume |
| **Turnover Machine** | top 15% defensive TO% | `D_TO` | Ball pressure |

**Hidden tags** (computed, used in matchup notes, **not** written into `home`/`away` arrays):

| Hidden tag | Rule | Pair |
|---|---|---|
| Sieve | worst 15% AdjDE | weakness of Lock Down |
| Glass Leak | worst 15% Opp_OR | weakness of No Rebounds for You |
| Open Arc | worst 15% Opp_FG3A_Rate | weakness of Arc Wall |
| Passive D | worst 15% D_TO (low force) | weakness of Turnover Machine |
| Careless | worst 15% offensive TO% | used vs Turnover Machine |

**Not emitted as tags at all (but data exists):** eFG%, D_eFG%, FT%, 3P%, Opp 3P%, slow pace, low FT rate, low OR%.

**Site mirror:** `web/src/lib/archetypes.ts` hard-codes the same 8 strength + 5 weakness names with tooltips. Weaknesses appear in example/demo data but almost never in live `archetype_tags` arrays.

**NEW COACH** is a separate per-team lock flag (`daily_run.tag_new_coaches` / `czarp/coach.py`), shown on reports, **not** stored in `projections.archetype_tags`.

**Matchup points:** counters/exploits exist in config, but `MatchupConfig.apply_to_score = False` (walk-forward said they hurt). Tags are effectively **display + notes only** for scoring.

### 1.2 Live Supabase frequencies (project `czarp-cbb`)

| Metric | Value |
|---|---|
| Total projection rows | 411 |
| Empty `{}` archetype_tags | **203 (49%)** — mostly v1 rows / early writes |
| Rows with structured tags | 208 |
| Of those, `tentative: true` | **208 / 208 (100%)** |
| Both home & away empty arrays | 46 |
| Distinct tag names in live data | **exactly the 8 strength tags** |
| Avg tags per game (latest v2) | 1.83 (home 1.20, away 0.63) |
| Max tags on one game | **9** (Virginia vs Arkansas 11/8) |
| Rows with matchup notes | 53 |

**Tag appearance counts** (latest projection per game, home+away flattened):

| Tag | Appearances | Games |
|---|---:|---:|
| Lock Down | 62 | 59 |
| Arc Wall | 54 | 50 |
| Addicted to 3s | 50 | 47 |
| Turnover Machine | 48 | 42 |
| Don't Blink | 46 | 39 |
| Board Crashers | 46 | 46 |
| No Rebounds for You | 38 | 36 |
| Free Throw Merchants | 37 | 34 |

Local recompute on current KenPom pull (365 D-I teams, all ≤3 games → **all tentative**): each strength tag hits ~53–55 teams (~15%), as designed. **124 teams (34%) have zero tags; 11 teams have 5–6 tags** (Duke, Virginia, Grand Canyon, etc.).

### 1.3 Problems (why it feels “all over the place and sloppy”)

1. **Badge soup on good teams.** Elite teams collect 5–6 unrelated chips (e.g. Virginia: Addicted to 3s + Don't Blink + Board Crashers + Lock Down + No Rebounds for You + Turnover Machine). Looking at the card does not feel like knowing the team — it feels like a keyword cloud.
2. **“Lock Down” is not a style.** It is top-15% AdjDE (overall defensive *outcome*). Only ~half of Lock Down teams are also top-15% D_eFG. Mixing outcome tags with style tags muddies the story.
3. **No shooting-efficiency identity.** eFG / D_eFG are the heart of Four Factors and are already in the pipeline, but no tag uses them. A team can be “Addicted to 3s” while shooting poorly.
4. **Asymmetric offense/defense vocabulary.** Offense gets cute names; defense gets different metaphors; weaknesses exist only in buried notes (“vs Sieve”) and on the site type list but not in live arrays.
5. **100% tentative right now.** With `tag_evidence_games = 5` and mean ~2 games played, every chip is “(tentative)”. That is correct statistically, but visually every badge looks provisional and noisy — especially when built from prior-season regression.
6. **Game-level stamp, not team-level record.** Tags live only inside `projections.archetype_tags` JSON per game. `teams` has no style/Four Factors columns. There is nothing durable to “look up a team and know them.”
7. **Empty / inconsistent payloads.** Half of projection rows are `{}`; v1 projections omit tags; notes sometimes name a team, sometimes don’t; weakness names leak into notes but not chips.
8. **Naming inconsistency risk.** Test fixtures mention “Track Meet” (not a real tag). Site legend lists weaknesses the API rarely sends. NEW COACH is a third tagging system on the report.

---

## 2. Four Factors & related data availability

KenPom **already provides** everything we need. No new credentials or paywall scrape is required for the core eight factors.

| Stat | Off / Def | In `data/four_factors.csv` / team table today? | KenPom rank column? | Used for tags today? | In Supabase? |
|---|---|---|---|---|---|
| eFG% | Off | **Yes** → `eFG` | `RankeFG_Pct` | No | No |
| eFG% allowed | Def | **Yes** → `D_eFG` | `RankDeFG_Pct` | No | No |
| TO% | Off | **Yes** → `TO` | `RankTO_Pct` | Hidden only (`Careless`) | No |
| TO% forced | Def | **Yes** → `D_TO` | `RankDTO_Pct` | Yes (`Turnover Machine`) | No (tags only) |
| OR% | Off | **Yes** → `OR` | `RankOR_Pct` | Yes (`Board Crashers`) | No |
| Opp OR% | Def | **Yes** → `Opp_OR` (from `DOR_Pct`) | `RankDOR_Pct` | Yes (`No Rebounds for You`) | No |
| FT rate | Off | **Yes** → `FTR` | `RankFT_Rate` | Yes (`Free Throw Merchants`) | No |
| FT rate allowed | Def | **Yes** → `D_FTR` | `RankDFT_Rate` | No | No |
| AdjTempo | — | **Yes** (ratings) | `RankAdjTempo` | Partial (`Don't Blink`) | No |
| 3PA rate | Off | **Yes** (misc `F3GRate`) | `RankF3GRate` | Yes | No |
| Opp 3PA rate | Def | **Yes** | `RankOppF3GRate` | Yes (`Arc Wall`) | No |
| 3P% / Opp 3P% | Off/Def | **Yes in misc CSV** (`FG3Pct`, `OppFG3Pct`) | ranks present | **No** — not even mapped into `teamstats` aliases | No |
| FT% | Off | **Yes** → `FTPct` | `RankFTPct` | Clutch composite only | No |

**Fetcher note:** `kenpom_fetcher.fetch_four_factors` docstring undersells the payload (mentions TO/OR/FT only); the live CSV also includes eFG, DeFG, OE/DE, tempo, and all rank columns. `fetch_misc` already pulls 3P% and 3PA rates.

**Early-season caveat:** raw KenPom four factors after 1–2 games are noisy (Duke raw eFG 61.6 / KP rank 26 vs regressed model eFG 54.2). The model’s `EarlySeasonConfig.style_fill = "regressed"` blend is the right source for *tags and profiles*; KenPom’s official ranks should still be shown as “KenPom rank” for transparency.

**Gap vs Christian’s ask:** the numbers exist locally every daily run; they are **not stamped** into Supabase on a per-team record. That is the main product hole.

---

## 3. Proposed clean tag taxonomy

Design rules:

- Small set (≤12 display tags).
- Consistent Title Case, 1–2 words where possible.
- **Percentile vs D-I**, same 15% bar (configurable).
- Each tag = one Four Factor / tempo / 3PA concept — no AdjOE/AdjDE outcome tags in the style set.
- Cap **3 style tags shown per team** on cards (highest |z| / most extreme percentiles win). Full set available on the profile.
- Tentative only when `games < 5`; after that, drop the suffix.
- Weaknesses are first-class chips with a distinct color (red/amber), not only buried notes.

### 3.1 Display tags

#### Pace
| Tag | Plain English | Exact rule |
|---|---|---|
| **Fast** | Plays up-tempo | AdjTempo ≥ 85th pct nationally |
| **Slow** | Plays deliberately | AdjTempo ≤ 15th pct |

*(Replaces “Don't Blink”; drops APL composite for clarity. APL can stay as an advanced profile field.)*

#### Shooting
| Tag | Plain English | Exact rule |
|---|---|---|
| **Sharp** | Elite shot-making (eFG) | eFG ≥ 85th pct |
| **Cold** | Poor shot-making | eFG ≤ 15th pct |
| **Volume 3s** | Lives beyond the arc | FG3A_Rate ≥ 85th pct |
| **Paint First** | Rarely shoots threes | FG3A_Rate ≤ 15th pct |

*(“Volume 3s” replaces “Addicted to 3s”. Adds the missing eFG identity.)*

#### Ball security
| Tag | Plain English | Exact rule |
|---|---|---|
| **Secure** | Protects the ball | TO% ≤ 15th pct (low turnovers) |
| **Careless** | Gives it away | TO% ≥ 85th pct |
| **Disrupt** | Forces turnovers | D_TO ≥ 85th pct |

*(“Disrupt” replaces “Turnover Machine”.)*

#### Glass
| Tag | Plain English | Exact rule |
|---|---|---|
| **Crash** | Crashes the offensive glass | OR% ≥ 85th pct |
| **Control Glass** | Denies opponent offensive boards | Opp OR% ≤ 15th pct |

*(Replaces “Board Crashers” / “No Rebounds for You”.)*

#### Free throws
| Tag | Plain English | Exact rule |
|---|---|---|
| **At the Line** | Gets to the stripe | FTR ≥ 85th pct |
| **Foul Prone** | Sends opponents to the line | D_FTR ≥ 85th pct |

*(Replaces “Free Throw Merchants”; adds the missing defensive FT-rate tag.)*

#### Defense (shot quality) — new, Four Factor–true
| Tag | Plain English | Exact rule |
|---|---|---|
| **Wall** | Forces tough shots (low eFG allowed) | D_eFG ≤ 15th pct |
| **Soft D** | Allows efficient shooting | D_eFG ≥ 85th pct |
| **Crowd 3s** | Keeps opponents off the three | Opp FG3A_Rate ≤ 15th pct |

*(“Crowd 3s” replaces “Arc Wall”. “Wall”/“Soft D” replace outcome-based Lock Down/Sieve for *style*.)*

### 3.2 Outcome badges (separate strip — not style)

Keep **Elite D** (AdjDE ≤ 15th) and **Elite O** (AdjOE ≥ 85th) as optional *efficiency* badges in a different color/shape so they are not confused with Four Factor style. Do **not** feed them into matchup counter cards unless walk-forward re-proves value.

**NEW COACH** stays its own lock badge (existing rules: 8 games sides / 4 totals).

### 3.3 Model vs display-only

| Piece | Role |
|---|---|
| Style tags above | **Display** on slate chips + profile; optional inputs to matchup notes |
| Matchup counter/exploit points | **Model (off by default)** — keep `apply_to_score=False` until a fresh walk-forward on the new names |
| Four Factors profile numbers | **Display + stamped record**; clutch composite already uses eFG/TO/FT% under the hood |
| Elite O/D badges | Display-only unless explicitly wired later |
| NEW COACH | Rule engine (skips official plays), not a style tag |

Rename map for a one-time migration of historical JSON (optional; or just start clean from next run):

| Old | New |
|---|---|
| Addicted to 3s | Volume 3s |
| Don't Blink | Fast |
| Board Crashers | Crash |
| Free Throw Merchants | At the Line |
| No Rebounds for You | Control Glass |
| Arc Wall | Crowd 3s |
| Turnover Machine | Disrupt |
| Lock Down | *(retire from style; optional Elite D)* |
| Sieve / Glass Leak / Open Arc / Passive D | Soft D / (drop or map to inverse of Control Glass / Crowd 3s / Disrupt) |
| Careless | Careless (keep) |

---

## 4. Per-team profile card (the “stamp”)

Christian’s ask: *when I look at a team, I want to feel like I know them.* Tags alone cannot do that. Proposal: a durable **team profile** stamped each daily run.

### 4.1 Suggested fields (new `team_profiles` table or `teams.profile` jsonb)

Per team × season × `as_of_date`:

- Identity: `team_name`, `season`, `games_played`, `tentative` bool  
- Efficiency header: AdjOE, AdjDE, AdjEM, AdjTempo + KenPom ranks  
- **Four Factors table (8 rows):** value, D-I percentile (from *regressed* series), KenPom official rank  
  - Off: eFG%, TO%, OR%, FTR  
  - Def: DeFG%, DTO%, Opp OR%, DFT_Rate  
- Extras: 3PA rate, 3P%, FT%, Opp 3PA, Opp 3P% (+ ranks)  
- `style_tags` text[] (capped list from §3)  
- `source`: `kenpom_regressed` | `kenpom_raw`  
- `created_at`

### 4.2 UI sketch (World Wide)

```
┌─────────────────────────────────────────────┐
│ VIRGINIA                    AdjEM #16       │
│ 2 GP · tags tentative                       │
│ [Volume 3s] [Crash] [Control Glass] [Wall]  │
├─────────────────────────────────────────────┤
│          OFFENSE          DEFENSE           │
│ eFG%    53.6  (93%ile)   49.8  (81%ile)     │
│         KP #46           KP #155            │
│ TO%     16.9  (51%ile)   18.4  (90%ile) ★   │
│ OR%     32.5  (85%ile) ★ 27.8  (98%ile) ★   │
│ FTR     33.6  (26%ile)   33.0  (85%ile)     │
├─────────────────────────────────────────────┤
│ Tempo 71.9 (67%ile) · 3PA rate 42% (89%ile) │
└─────────────────────────────────────────────┘
```

Stars = factors that earned a style tag. Clicking a slate chip opens this card.

### 4.3 Real examples (data as of Sat Nov 7 KenPom pull, regressed)

**Duke** — today shows 6 noisy chips. Under the new system (top-3 by extremity): roughly **Crash** (OR 98%ile), **Wall** (D_eFG 99%ile), **Disrupt** (D_TO 97%ile), with optional Elite O/D. Profile tells the story: elite shot defense + glass + pressure; average tempo; not a 3-point volume team.

**Alabama** — **Fast** (AdjTempo ~100%ile / KP #1), **Volume 3s** (96%ile), **Disrupt**. Profile: track-meet offense that sprays threes and pressures the ball; middling eFG so far.

**Houston** — **Slow** (tempo 13%ile), **Wall** (D_eFG 92%ile), maybe **At the Line**. Profile: deliberate, elite shot defense — the classic Houston identity, which today’s single “Lock Down (tentative)” chip completely undersells.

**Purdue** — **Slow** (tempo 3%ile), **Secure** (TO 91%ile). No fake “Lock Down” just because AdjDE is decent; profile shows average eFG and quiet glass.

---

## 5. Implementation plan (when approved — not done in this pass)

### 5.1 Pipeline / scraper (CZarp)
1. **No new KenPom scrape** for the eight factors — already fetched.  
2. Map `FG3Pct` / `OppFG3Pct` into `teamstats.ALIASES` (one-line addition).  
3. Rewrite `tag_teams` to the taxonomy in §3; keep a `TAG_RENAME` dict; cap display list at 3.  
4. Emit `team_profiles` rows in `czarp/supabase_writer.py` each daily run (alongside projections).  
5. Slim `projections.archetype_tags` to `{home, away, home_tentative, away_tentative, notes}` using new names; stop stuffing 6 chips.  
6. Leave `apply_to_score=False`; update counter keys only after a dedicated walk-forward.

### 5.2 Supabase migration
- `team_profiles` table (or jsonb on `teams`) with RLS read for anon like projections.  
- Optional view `team_profile_latest`.  
- No change required to picks/ledger immutability.  
- Backfill: optional one-shot from latest local CSVs; or just stamp going forward.

### 5.3 Site (World Wide)
- Update `web/src/lib/archetypes.ts` + `ArchetypeChip` colors (strength / weakness / pace / outcome).  
- Cap chips on slate cards at 3; link/open **Team Profile** sheet with the Four Factors grid.  
- Legend copy in plain English from §3.  
- Handle old tag names gracefully for a week if historical projections remain.  
- Do **not** surface KenPom overall ranks beyond what we already agreed to keep off the public marketing surface — factor ranks on the profile card are fine as “national rank among D-I.”

### 5.4 Suggested sequence
1. Christian approves taxonomy names + “max 3 chips” + profile card.  
2. CZarp ships migration + writer + new `tag_teams` on `czarp-v2` (PR for review — no auto-merge to main).  
3. Ping World Wide with the new JSON shape + chip list.  
4. World Wide PR for chips + profile sheet.  
5. After ~2 weeks of stamped profiles, revisit whether matchup points should turn back on.

---

## 6. Open questions for Christian

1. Prefer the shorter names (**Fast / Crash / Wall**) or keep some of the old flavor (**Don't Blink**, **Board Crashers**)?  
2. Max chips on a slate card: **3** (recommended) or 4?  
3. Show KenPom official factor ranks on the public profile, or only our percentiles?  
4. Should weakness tags (Cold, Careless, Soft D, Foul Prone) appear on the slate, or only on the profile?  
5. OK to start stamping profiles from the next daily run without backfilling Nov 3–8?

---

*Generated from read-only inspection of `czarp/teamstats.py`, `czarp/config.py`, `kenpom_fetcher.py`, local `data/*.csv`, and Supabase `czarp-cbb` projections/teams. No code or DB writes were made.*
