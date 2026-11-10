"""BIG MOVE flag + tracked record (info only — never a pick or a bet).

A game/market is a BIG MOVE when the DraftKings line moved from the open to the current line by
  * total  >= TOTAL_PTS (4) points, or
  * spread >= SPREAD_PTS (3) points (home-spread notation), in either direction.

Each one is reported (daily report, pre-tip check) with CZarp's number and lean, whether it was
skipped as a play and why, and whether the move went against CZarp. The daily run stores one row
per game/market/date in Supabase `big_moves` (first report wins) and grades it once the game is
final, against the CURRENT line at report time:
  landed_with_move  — the final went past that line in the direction the line moved
  landed_with_czarp — CZarp's lean beat that line
`big_moves_record` (view) summarises W-L. The immutable picks ledger is never touched.
"""
from __future__ import annotations

from datetime import datetime

TOTAL_PTS = 4.0
SPREAD_PTS = 3.0
LEAGUE = "ncaam"


def _f(x):
    try:
        if x is None:
            return None
        v = float(x)
        return None if v != v else v
    except (TypeError, ValueError):
        return None


def threshold(market: str) -> float:
    return TOTAL_PTS if market == "total" else SPREAD_PTS


def move_of(market: str, open_line, current_line) -> float | None:
    """Signed current - open (home spread for spreads); None if either line is missing."""
    o, c = _f(open_line), _f(current_line)
    if o is None or c is None:
        return None
    return round(c - o, 2)


def is_big(market: str, open_line, current_line) -> bool:
    mv = move_of(market, open_line, current_line)
    return mv is not None and abs(mv) >= threshold(market) - 1e-9


def move_side(market: str, move: float, home: str, away: str) -> str:
    """'Over'/'Under' for totals; the team the spread moved toward (more favored / fewer points)."""
    if market == "total":
        return "Over" if move > 0 else "Under"
    return home if move < 0 else away          # home spread went down = market likes home more


def direction(market: str, move: float, home: str, away: str) -> str:
    if market == "total":
        return "up" if move > 0 else "down"
    return f"toward {move_side(market, move, home, away)}"


def czarp_side_for(market: str, czarp_line, current_line, home: str, away: str) -> str | None:
    """CZarp's lean vs the current line. czarp_line = model total, or model HOME spread (-margin)."""
    m, c = _f(czarp_line), _f(current_line)
    if m is None or c is None or m == c:
        return None
    if market == "total":
        return "Over" if m > c else "Under"
    return home if m < c else away              # model home spread below the market = home covers


def describe(market: str, line) -> str:
    v = _f(line)
    if v is None:
        return "—"
    return f"{v:+g}" if market == "spread" else f"{v:g}"


def build_row(g: dict, market: str, cand: dict | None = None, official: bool = False) -> dict | None:
    """Big-move row for one game/market from a daily_run game dict (+ its candidates() row)."""
    if market == "total":
        o, c = g.get("open_total"), g.get("current_total")
    else:
        o, c = g.get("open_home_spread"), g.get("current_home_spread")
    if not is_big(market, o, c):
        return None
    home, away = g.get("kp_home") or g.get("home"), g.get("kp_away") or g.get("away")
    mv = move_of(market, o, c)
    if cand is not None:
        if market == "total":
            cz = _f(cand.get("model_value"))
        else:
            sgn = 1 if cand.get("home") else -1
            cz = -sgn * _f(cand.get("model_value")) if _f(cand.get("model_value")) is not None else None
    else:
        cz = _f(g.get("v1_total") if g.get("v1_total") is not None else g.get("v2_total")) if market == "total" \
            else (-_f(g["v2_home_margin"]) if _f(g.get("v2_home_margin")) is not None else None)
    cz_side = czarp_side_for(market, cz, c, home, away)
    ms = move_side(market, mv, home, away)
    against = None if cz_side is None else (cz_side != ms)
    reasons = list((cand or {}).get("reasons") or [])
    skip_reason = None if official else ("; ".join(reasons) if reasons else
                                         ("no CZarp number" if cz is None else "not an official play"))
    return dict(
        event_id=str(g.get("event_id")), game_date=g.get("game_date"), tip_ct=g.get("tip_ct"),
        home=home, away=away, neutral=bool(g.get("neutral")), market=market,
        open_line=_f(o), current_line=_f(c), move=mv, move_size=abs(mv),
        direction=direction(market, mv, home, away), move_side=ms,
        czarp_line=round(cz, 1) if cz is not None else None, czarp_side=cz_side, against_czarp=against,
        official_pick=bool(official), skipped=not official, skip_reason=skip_reason,
        note=note_for(against, official, market, mv))


def note_for(against, official, market, mv) -> str:
    n = f"{'total' if market == 'total' else 'spread'} moved {abs(mv):g} pts"
    if against is True:
        n += " AGAINST CZarp's lean"
    elif against is False:
        n += " with CZarp's lean"
    if official:
        n += "; official play"
    return n


def detect(games: list[dict], cands: list[dict] | None = None, played_ids=()) -> list[dict]:
    """Every big move on the slate (D-I games with DK open + current), sorted by tip then size."""
    by = {(str(c["g"].get("event_id")), c["market"]): c for c in (cands or [])}
    played = {str(x) for x in played_ids}
    out = []
    for g in games:
        if g.get("skip"):
            continue
        for m in ("total", "spread"):
            pid = f"{g.get('event_id')}-{m}"
            r = build_row(g, m, by.get((str(g.get("event_id")), m)), official=pid in played)
            if r:
                out.append(r)
    return sorted(out, key=lambda r: (str(r.get("tip_ct") or ""), -r["move_size"]))


def top10_note(g: dict, market: str) -> str | None:
    """Short 'Big move' note for a Top 10 entry, or None."""
    r = build_row(g, market)
    if not r:
        return None
    return f"Big move: {r['direction']} {r['move_size']:g} pts ({describe(market, r['open_line'])} → {describe(market, r['current_line'])})"


def report_md(rows: list[dict], record: dict | None = None) -> list[str]:
    L = ["## Big moves",
         f"_DK line moved ≥{TOTAL_PTS:g} pts (totals) or ≥{SPREAD_PTS:g} pts (spreads) from open to now, either direction. "
         "Info only, never a play by itself. CZarp lean = our side vs the current line; ⚠ = the move went against our lean._", ""]
    if record:
        L.append("**Big-move record (info only, graded vs the line at report time):** " + record_line(record))
        L.append("")
    if not rows:
        return L + ["No big moves on today's slate.", ""]
    L.append("| Tip (CT) | Game | Market | Open | Current | Move | CZarp | CZarp lean | Play? | Why skipped | Note |")
    L.append("|---|---|---|---|---|---|---|---|---|---|---|")
    for r in rows:
        m = r["market"]
        L.append(f"| {str(r.get('tip_ct') or '')[11:]} | {r['away']} @ {r['home']}{' (N)' if r.get('neutral') else ''} | "
                 f"{'Total' if m == 'total' else 'Spread (home)'} | {describe(m, r['open_line'])} | {describe(m, r['current_line'])} | "
                 f"{r['move_size']:g} {r['direction']} | {describe(m, r['czarp_line'])} | {r['czarp_side'] or '—'} | "
                 f"{'official' if r['official_pick'] else 'skipped'} | {r['skip_reason'] or ''} | "
                 f"{'⚠ ' if r['against_czarp'] else ''}{r['note']} |")
    L.append("")
    return L


def record_line(rec: dict) -> str:
    a = rec.get("all") or {}
    if not a or not a.get("graded"):
        return "no graded big moves yet."
    def wl(w, l, p=0):
        return f"{w}-{l}" + (f"-{p}" if p else "")
    s = (f"followed the move {wl(a['with_move_w'], a['with_move_l'], a.get('with_move_p', 0))}, "
         f"CZarp's lean {wl(a['with_czarp_w'], a['with_czarp_l'])}, "
         f"CZarp when the move was against it {wl(a['against_czarp_w'], a['against_czarp_l'])} "
         f"({a['graded']} graded")
    parts = [f"{k} {wl(rec[k]['with_move_w'], rec[k]['with_move_l'])} with move / "
             f"{wl(rec[k]['with_czarp_w'], rec[k]['with_czarp_l'])} CZarp" for k in ("total", "spread") if rec.get(k)]
    return s + ("; " + ", ".join(parts) if parts else "") + ")."


# ── grading ──────────────────────────────────────────────────────────────────
def grade_row(row: dict, home_score, away_score, home: str | None = None) -> dict:
    """landed_with_move / landed_with_czarp vs current_line (push -> None)."""
    hs, as_ = _f(home_score), _f(away_score)
    m, cur = row["market"], _f(row["current_line"])
    if hs is None or as_ is None or cur is None:
        return {}
    home = home or row.get("home")
    if m == "total":
        final = hs + as_
        diff = final - cur                       # > 0 = over hit
        winner = None if diff == 0 else ("Over" if diff > 0 else "Under")
        text = "push" if winner is None else f"final {final:g} vs {cur:g} → {winner}"
    else:
        final = hs - as_                         # home margin
        diff = final + cur                       # > 0 = home covered
        winner = None if diff == 0 else (home if diff > 0 else row.get("away"))
        text = "push" if winner is None else f"home margin {final:+g} vs {cur:+g} → {winner} covered"
    out = dict(final_home=int(hs), final_away=int(as_), final_value=final, result_text=text,
               landed_with_move=None if winner is None else (winner == row.get("move_side")),
               landed_with_czarp=None if (winner is None or not row.get("czarp_side")) else (winner == row.get("czarp_side")))
    return out


def db_row(r: dict, game_id: int, now: datetime, source: str = "live") -> dict:
    keys = ("game_date", "market", "open_line", "current_line", "move", "move_size", "direction", "move_side",
            "czarp_line", "czarp_side", "against_czarp", "official_pick", "skipped", "skip_reason", "note")
    out = {k: r.get(k) for k in keys}
    out.update(league=LEAGUE, game_id=game_id, source=source, reported_at=now.isoformat(timespec="seconds"))
    if source == "backfill":
        out["close_available"] = False
    return out


def write_rows(c, rows: list[dict], gid: dict, now: datetime, log=print) -> int:
    """Insert today's big moves; an existing (game, market, date) row is kept (first report wins)."""
    out = [db_row(r, gid[r["event_id"]], now) for r in rows if r["event_id"] in gid]
    c.upsert("big_moves", out, on_conflict="game_id,market,game_date", returning=False, ignore_duplicates=True)
    return len(out)


def grade_pending(c, now: datetime, lines_for=None, log=print) -> int:
    """Grade every ungraded big move whose game is final (or void it if postponed/canceled)."""
    rows = c.get("big_moves", select="*,games(home,away,status,home_score,away_score,external_id)",
                 graded_at="is.null", league=f"eq.{LEAGUE}")
    n = 0
    ts = now.isoformat(timespec="seconds")
    for r in rows:
        gm = r.get("games") or {}
        st = gm.get("status")
        upd = {}
        if st in ("postponed", "canceled", "cancelled"):
            upd = dict(result_text="void", graded_at=ts)
        elif st == "final" and gm.get("home_score") is not None and gm.get("away_score") is not None:
            row = dict(r, home=gm.get("home"), away=gm.get("away"))
            upd = grade_row(row, gm["home_score"], gm["away_score"])
            if not upd:
                continue
            upd["graded_at"] = ts
            if r.get("source") != "backfill" and lines_for is not None and r.get("close_line") is None:
                try:
                    ln = lines_for(str(gm.get("external_id"))) or {}
                except Exception:
                    ln = {}
                close = ln.get("close_total" if r["market"] == "total" else "close_home_spread")
                upd["close_line"] = close
                upd["close_available"] = close is not None
        if upd:
            c.patch("big_moves", upd, id=f"eq.{r['id']}", graded_at="is.null")
            n += 1
    if n:
        log(f"  big moves: graded {n}")
    return n


def fetch_record(c) -> dict:
    return {r["scope"]: r for r in c.get("big_moves_record", select="*")}


# ── slate context for the pre-tip check ─────────────────────────────────────
import json as _json
import os as _os

CTX_DIR = _os.path.join("data", "ledger", "big_moves")


def save_context(today: str, cands: list[dict], big: list[dict], d: str = CTX_DIR) -> str:
    """Morning skip reasons per game-market + today's big moves, for pretip_check (not the ledger)."""
    _os.makedirs(d, exist_ok=True)
    reasons = {f"{c['g'].get('event_id')}-{c['market']}": "; ".join(c.get("reasons") or []) for c in cands}
    p = _os.path.join(d, f"{today}.json")
    with open(p + ".tmp", "w") as fh:
        _json.dump({"reasons": reasons, "big": big}, fh, default=str)
    _os.replace(p + ".tmp", p)
    return p


def load_context(today: str, d: str = CTX_DIR) -> dict:
    try:
        with open(_os.path.join(d, f"{today}.json")) as fh:
            return _json.load(fh)
    except Exception:
        return {}


def pretip_lines(rows: list[dict]) -> list[str]:
    """Plain-text lines for the pre-tip check (empty list = nothing to say)."""
    out = []
    for r in rows:
        m = r["market"]
        cz = "" if r.get("czarp_line") is None else f" CZarp {describe(m, r['czarp_line'])}" + (
            f" ({r['czarp_side']})" if r.get("czarp_side") else "")
        warn = " ⚠ against our lean." if r.get("against_czarp") else (" — with our lean." if r.get("against_czarp") is False else ".")
        why = r.get("skip_reason")
        play = "Official play." if r.get("official_pick") else (
            f"Not a play: {why}." if why and why != "not an official play" else "Not a play.")
        out.append(f"- {str(r.get('tip_ct') or '')[11:]} CT {r['away']} @ {r['home']}: "
                   f"{'Total' if m == 'total' else 'Spread (home)'} {describe(m, r['open_line'])} → {describe(m, r['current_line'])} "
                   f"({r['move_size']:g} {r['direction']}).{cz}{warn} {play}")
    return out
