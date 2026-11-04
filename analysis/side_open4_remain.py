"""Sides: edge >=4% vs OPEN AND >=X% still left vs CLOSE (same side). Analysis only.
Conventions identical to side_move_study.py (close = proxy for current; pushes excluded; ROI at -110).
Run: python3 analysis/side_open4_remain.py -> outputs/walkforward/side_open4_remain.txt
"""
import math, os
import numpy as np, pandas as pd
os.chdir(os.path.join(os.path.dirname(__file__), ".."))
OUT = "outputs/walkforward/side_open4_remain.txt"; L = []
def p(s=""): L.append(s); print(s)
def wilson(k, n, z=1.96):
    ph = k/n; den = 1+z*z/n; c = (ph+z*z/(2*n))/den; h = z*math.sqrt(ph*(1-ph)/n+z*z/(4*n*n))/den; return c-h, c+h
def fmt(w):
    w = w.dropna(); n = len(w)
    if not n: return f"{'0':>4} {'-':>8} {'':>6} {'':>13} {'':>7}"
    k = int(w.sum()); lo, hi = wilson(k, n)
    return f"{n:>4} {f'{k}-{n-k}':>8} {k/n*100:5.1f}% [{lo*100:4.1f}-{hi*100:4.1f}] {(k*100/110-(n-k))/n*100:+6.1f}%"
d = pd.read_pickle("outputs/walkforward/oos.pkl")
d = d[d.model.notna() & d.open_home_spread.notna() & d.close_home_spread.notna() & (d.open_total > 0) & d.margin.notna()].copy()
gap = d.model + d.open_home_spread; d = d[gap != 0].copy(); gap = gap[gap != 0]
d["pick"] = np.sign(gap); d["edge"] = gap.abs()/d.open_total
ct = d.close_total.fillna(d.open_total)
d["ec"] = d.pick*(d.model+d.close_home_spread)/ct
d["move"] = d.pick*(d.open_home_spread-d.close_home_spread)
for k, col in (("open", "open_home_spread"), ("close", "close_home_spread")):
    a = d.pick*(d.margin+d[col]); d["w_"+k] = np.where(a > 0, 1.0, np.where(a < 0, 0.0, np.nan))
d["fav20"] = (d.open_home_spread.abs() >= 20) & (np.sign(-d.open_home_spread) == d.pick)
H = f"{'n':>4} {'W-L':>8} {'hit':>6} {'95% CI':>13} {'ROI':>7}"
p(f"SIDES: edge>=4% vs OPEN and >=X% left vs CLOSE (proxy for current). {len(d):,} OOS games, seasons {sorted(d.season.unique())}.")
p("Pushes excluded; ROI flat -110 (break-even 52.4%); Wilson 95% CI. CLOSE grade = the price you'd actually get.")
rules = [(f"open>=4% & left>={x}%", (d.edge >= .04) & (d.ec >= x/100)) for x in (2, 2.5, 3, 3.5, 4)]
rules[-1] = ("open>=4% & left>=4% (CURRENT)", rules[-1][1])
rules += [("open 3-4% & left>=3%", (d.edge >= .03) & (d.edge < .04) & (d.ec >= .03)),
          ("open>=4% (any close)", d.edge >= .04)]
def block(title, sub):
    p(); p(f"=== {title} ==="); p(f"  {'rule':32} | {'graded at CLOSE':^{len(H)}} | {'graded at OPEN':^{len(H)}}"); p(f"  {'':32} | {H} | {H}")
    for name, m in rules:
        h = d[m & sub]; p(f"  {name:32} | {fmt(h.w_close)} | {fmt(h.w_open)}")
block("ALL", d.edge > -1)
for s in sorted(d.season.unique()): block(f"SEASON {s}", d.season == s)
for lab, m in (("move toward us >=1", d.move >= 1), ("flat (<1 either way)", d.move.abs() < 1), ("move against (<=-1)", d.move <= -1)):
    block(f"MOVE: {lab}", m)
block("LAYING 20+ FAVORITES", d.fav20)
block("EVERYTHING ELSE (not laying 20+)", ~d.fav20)
open(OUT, "w").write("\n".join(L)+"\n")
