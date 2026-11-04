"""Side line-move study (analysis only; no model rules changed).

Question: do SIDES with edge >= 3% vs the OPEN line win when the line then moved TOWARD our number?
  edge%   = |CZarp home margin + Vegas home spread| / Vegas total   (same convention as daily_run / edge_slices)
  pick    = sign(model + open_home_spread)   (+1 home, -1 away), fixed at the open
  move    = pick * (open_home_spread - close_home_spread)   (+pts = line moved toward our pick)
Graded ATS (pushes excluded) against the OPEN line (bet early) and against the CLOSE line
(bet late, after the move is confirmed).  ROI assumes flat -110 (win +0.909, loss -1; break-even 52.38%).
CAVEAT: the close is hindsight. A real play needs an intraday line; see the intraday section at the bottom.

Run:  python3 analysis/side_move_study.py   -> writes outputs/walkforward/side_move_study.txt
"""
import math, os, sys
import numpy as np, pandas as pd

os.chdir(os.path.join(os.path.dirname(__file__), ".."))
OUT = "outputs/walkforward/side_move_study.txt"
BE = 110 / 210
lines = []
def p(s=""):
    lines.append(s); print(s)

def wilson(k, n, z=1.96):
    if n == 0: return (float("nan"),) * 2
    ph = k / n; den = 1 + z * z / n
    c = (ph + z * z / (2 * n)) / den; h = z * math.sqrt(ph * (1 - ph) / n + z * z / (4 * n * n)) / den
    return c - h, c + h

def binom_sf(k, n, q=BE):  # one-sided P(X>=k) under break-even
    return sum(math.comb(n, i) * q**i * (1 - q)**(n - i) for i in range(k, n + 1)) if n else float("nan")

def stat(w):
    """w: Series of 1/0 (pushes already NaN) -> dict"""
    w = w.dropna(); n = len(w); k = int(w.sum())
    if n == 0: return dict(n=0, k=0, hit=np.nan, lo=np.nan, hi=np.nan, roi=np.nan, p=np.nan)
    lo, hi = wilson(k, n)
    return dict(n=n, k=k, hit=k / n, lo=lo, hi=hi, roi=(k * 100 / 110 - (n - k)) / n, p=binom_sf(k, n))

def fmt(s, pval=False):
    if s["n"] == 0: return f"{'-':>5} {'':>6} {'':>13} {'':>7}"
    out = f"{s['n']:>5} {s['hit']*100:5.1f}% [{s['lo']*100:4.1f}-{s['hi']*100:4.1f}] {s['roi']*100:+6.1f}%"
    return out + (f"  p={s['p']:.3f}" if pval else "")

# ------------------------------------------------------------------ data
d = pd.read_pickle("outputs/walkforward/oos.pkl")
d = d[d.model.notna() & d.open_home_spread.notna() & d.close_home_spread.notna()
      & d.open_total.notna() & (d.open_total > 0) & d.margin.notna()].copy()
gap = d.model + d.open_home_spread
d = d[gap != 0].copy(); gap = gap[gap != 0]
d["pick"] = np.sign(gap)
d["edge"] = gap.abs() / d.open_total
ct = d.close_total.fillna(d.open_total)
d["edge_close_signed"] = d.pick * (d.model + d.close_home_spread) / ct   # <0 = line moved past our number
d["move"] = d.pick * (d.open_home_spread - d.close_home_spread)
a_open = d.pick * (d.margin + d.open_home_spread)
a_close = d.pick * (d.margin + d.close_home_spread)
d["w_open"] = np.where(a_open > 0, 1.0, np.where(a_open < 0, 0.0, np.nan))
d["w_close"] = np.where(a_close > 0, 1.0, np.where(a_close < 0, 0.0, np.nan))
# big favourite context (yesterday: LSU/Mizzou/Tennessee were all 20+ home favourites and we laid the points)
fav_spread = d.open_home_spread.abs()
pick_is_fav = np.sign(-d.open_home_spread) == d.pick
d["ctx"] = np.where(fav_spread >= 20, np.where(pick_is_fav, "lay 20+ fav", "take 20+ dog"), "spread <20")

EB = [("2-3%", .02, .03), ("3-4%", .03, .04), ("4-5%", .04, .05), ("5%+", .05, 9), ("3%+", .03, 9)]
MV = [("against (<=-0.5)", lambda m: m <= -0.5), ("flat (0)", lambda m: m.abs() < 0.5),
      ("toward >=0.5", lambda m: m >= 0.5), ("toward >=1", lambda m: m >= 1),
      ("toward >=2", lambda m: m >= 2), ("toward >=3", lambda m: m >= 3), ("ALL", lambda m: m > -999)]
HDR = f"{'n':>5} {'hit':>6} {'95% CI':>13} {'ROI':>7}"

p("SIDE LINE-MOVE STUDY  (walk-forward OOS, seasons " + ", ".join(map(str, sorted(d.season.unique())))
  + f"; {len(d):,} games with open+close spread)")
p("edge% vs OPEN picks the side; move = open->close pts toward our pick (+) / against (-).")
p("Grades ATS, pushes excluded, ROI at flat -110 (break-even 52.4%). Wilson 95% CI.  p = one-sided binomial vs 52.4%.")
p("Toward-buckets are cumulative (>=1 includes >=2).")
p()
p(f"Move distribution by edge band (share of games):")
for lab, lo, hi in EB:
    g = d[(d.edge >= lo) & (d.edge < hi)]
    p(f"  {lab:5} n={len(g):5}  against {(g.move<=-.5).mean()*100:4.1f}%  flat {(g.move.abs()<.5).mean()*100:4.1f}%  "
      f"toward>=0.5 {(g.move>=.5).mean()*100:4.1f}%  >=1 {(g.move>=1).mean()*100:4.1f}%  >=2 {(g.move>=2).mean()*100:4.1f}%")
p()

def table(df, title, pval=False):
    p(title)
    p(f"  {'edge':5} {'move':17} | {'ATS vs OPEN':^{len(HDR)}} | {'ATS vs CLOSE':^{len(HDR)}}")
    p(f"  {'':5} {'':17} | {HDR} | {HDR}")
    for lab, lo, hi in EB:
        g = df[(df.edge >= lo) & (df.edge < hi)]
        for ml, f in MV:
            h = g[f(g.move)]
            p(f"  {lab:5} {ml:17} | {fmt(stat(h.w_open))} | {fmt(stat(h.w_close))}"
              + (f"  p_open={stat(h.w_open)['p']:.3f}" if pval and stat(h.w_open)['n'] else ""))
        p()

table(d, "=== 1. ALL SEASONS: edge vs open x move direction ===", pval=True)

p("=== 2. EDGE LEFT vs CLOSE on toward-moves (same pick; negative = line moved past our number) ===")
p(f"  {'edge':5} {'move':13} {'n':>5} {'med edge@open':>13} {'med edge@close':>14} {'still>=2%':>9} {'still>=3%':>9} {'flipped':>8}")
for lab, lo, hi in EB:
    g = d[(d.edge >= lo) & (d.edge < hi)]
    for ml, thr in (("toward >=0.5", .5), ("toward >=1", 1), ("toward >=2", 2)):
        h = g[g.move >= thr]
        if not len(h): continue
        ec = h.edge_close_signed
        p(f"  {lab:5} {ml:13} {len(h):5} {h.edge.median()*100:12.1f}% {ec.median()*100:13.1f}% "
          f"{(ec>=.02).mean()*100:8.0f}% {(ec>=.03).mean()*100:8.0f}% {(ec<0).mean()*100:7.0f}%")
p()
p("  ATS vs CLOSE for 3%+-vs-open toward>=1 games, split by edge still left at the close:")
g = d[(d.edge >= .03) & (d.move >= 1)]
for lab, f in (("close edge <0 (moved past us)", lambda e: e < 0), ("0-1%", lambda e: (e >= 0) & (e < .01)),
               ("1-2%", lambda e: (e >= .01) & (e < .02)), ("2-3%", lambda e: (e >= .02) & (e < .03)),
               (">=3%", lambda e: e >= .03)):
    h = g[f(g.edge_close_signed)]
    p(f"    {lab:30} open {fmt(stat(h.w_open))} | close {fmt(stat(h.w_close))}")
p()

p("=== 3. SEASON SPLIT (stability) ===")
for s in sorted(d.season.unique()):
    table(d[d.season == s], f"--- season {s} ---")

p("=== 4. BIG FAVOURITES vs THE REST (3%+ vs open) ===")
p(f"  {'context':13} {'move':17} | {'ATS vs OPEN':^{len(HDR)}} | {'ATS vs CLOSE':^{len(HDR)}}")
g3 = d[d.edge >= .03]
for c in ("lay 20+ fav", "take 20+ dog", "spread <20"):
    gc = g3[g3.ctx == c]
    for ml, f in MV:
        if ml in ("toward >=3",): continue
        h = gc[f(gc.move)]
        p(f"  {c:13} {ml:17} | {fmt(stat(h.w_open))} | {fmt(stat(h.w_close))}")
    p()
p("  Home favourite laying 20+ at 3%+ vs open, by season (move toward >=1):")
for s in sorted(d.season.unique()):
    h = g3[(g3.season == s) & (g3.ctx == "lay 20+ fav") & (g3.move >= 1)]
    p(f"    {s}: open {fmt(stat(h.w_open))} | close {fmt(stat(h.w_close))}")
p()

p("=== 5. CANDIDATE 'WATCH' TAGS (3%+ vs open AND toward-move >= X), by season ===")
p(f"  {'rule':26} {'season':6} | {'ATS vs OPEN':^{len(HDR)}} | {'ATS vs CLOSE':^{len(HDR)}}")
for lo in (.03, .02):
    for thr in (.5, 1, 1.5, 2):
        for s in list(sorted(d.season.unique())) + ["all"]:
            h = d[(d.edge >= lo) & (d.move >= thr)]
            if s != "all": h = h[h.season == s]
            p(f"  edge>={lo*100:.0f}% & move>={thr:<4} {str(s):6} | {fmt(stat(h.w_open))} | {fmt(stat(h.w_close))}")
        p()
p("  Reference - current side rule (edge >=4% vs open AND >=4% vs close/current):")
h = d[(d.edge >= .04) & (d.edge_close_signed >= .04)]
p(f"  {'4%/4% rule':26} {'all':6} | {fmt(stat(h.w_open))} | {fmt(stat(h.w_close))}")
p()

# ------------------------------------------------------------------ intraday availability
p("=== 6. INTRADAY LINES: how much of the open->close move is visible mid-day? ===")
p("  ESPN history (data/history/espn_cache, espn_*.csv) stores only open / current / close - no intraday ticks.")
p("  data/ledger/snapshots/*.csv are the ~7 AM CT daily_run snapshots (since 2026-11-02): open + current at 7 AM only.")
p("  The pre-tip check (pretip_check.py) only writes a pretip_flag on ledger PLAYS, it does not archive lines.")
snap_path = "/tmp/daily_snapshots.csv"
if os.path.exists(snap_path):
    s = pd.read_csv(snap_path)
    s = s[s.vegas_spread.notna()].copy()
    s["t"] = pd.to_datetime(s.snapshot_time, utc=True, format="ISO8601").dt.tz_convert("America/Chicago")
    # match on KenPom names via the OOS frame (same naming as the v1 app), tip time from ESPN by event_id
    o = pd.read_pickle("outputs/walkforward/oos.pkl")
    o = o[o.open_home_spread.notna() & o.close_home_spread.notna()]
    e = pd.read_csv("data/history/espn_20251103_20260406.csv", usecols=["event_id", "tip_ct", "home_full", "away_full"])
    o = o.merge(e, on="event_id", how="left")
    o["tip"] = pd.to_datetime(o.tip_ct).dt.tz_localize("America/Chicago", ambiguous="NaT", nonexistent="NaT")
    def is_team(fav, kp, full):
        f = str(fav).strip().lower()
        return f in (str(kp).lower(), str(full).lower()) or (str(full).lower().startswith(f) and len(f) > 3)
    rows = []
    for _, r in s.iterrows():
        m = o[(o.date == r.snapshot_date) & (((o.home == r.team1) & (o.away == r.team2)) | ((o.home == r.team2) & (o.away == r.team1)))]
        if len(m) != 1: continue
        m = m.iloc[0]
        fav_home = is_team(r.vegas_fav, m.home, m.home_full)
        fav_away = is_team(r.vegas_fav, m.away, m.away_full)
        if fav_home == fav_away: continue
        snap_home = -abs(r.vegas_spread) if fav_home else abs(r.vegas_spread)
        hrs = (m.tip - r.t).total_seconds() / 3600 if pd.notna(m.tip) else np.nan
        rows.append(dict(open=m.open_home_spread, snap=snap_home, close=m.close_home_spread, hrs=hrs))
    x = pd.DataFrame(rows)
    x = x[x.hrs > 0]  # snapshot taken before tip
    p(f"  Legacy Supabase daily_snapshots (v1 app, Odds API consensus line, Mar 2026 only): {len(x)} pre-tip games matched to ESPN open/close.")
    if len(x):
        x["tot"] = x.close - x.open; x["seen"] = x.snap - x.open
        mv = x[x.tot.abs() >= 1]
        same_dir = (np.sign(mv.seen) == np.sign(mv.tot)) & (mv.seen.abs() >= .5)
        frac = (mv.seen / mv.tot).clip(-1, 2)
        p(f"  median hours snapshot->tip: {x.hrs.median():.1f}h (range {x.hrs.min():.1f}-{x.hrs.max():.1f})")
        p(f"  games whose open->close move was >=1 pt: {len(mv)}")
        p(f"    share where the mid-day line had already moved >=0.5 the same way: {same_dir.mean()*100:.0f}%")
        p(f"    median share of the full move already visible: {frac.median()*100:.0f}%  (mean {frac.mean()*100:.0f}%)")
        big = mv[mv.tot.abs() >= 2]
        if len(big):
            p(f"    moves >=2 pts (n={len(big)}): mid-day line already >=1 pt the same way in "
              f"{((np.sign(big.seen)==np.sign(big.tot))&(big.seen.abs()>=1)).mean()*100:.0f}%; median share visible "
              f"{(big.seen/big.tot).clip(-1,2).median()*100:.0f}%")
        p("  (Different books - Odds API consensus vs ESPN BET/DraftKings - add ~0.5 pt noise; sample is small and March-only.)")
else:
    p("  (legacy Supabase daily_snapshots export not found at /tmp/daily_snapshots.csv - section skipped)")
p("  Not checked: The Odds API historical endpoint (/v4/historical/...odds) can return a line at any timestamp,")
p("  but costs credits per call; not spent here.")

os.makedirs(os.path.dirname(OUT), exist_ok=True)
open(OUT, "w").write("\n".join(lines) + "\n")
print(f"\nwrote {OUT}", file=sys.stderr)
