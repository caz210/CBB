"""grade_finals.py — grade open plays (official and track) soon after their games go final.

The 7 AM daily_run grades everything once a day; this job does the same grading in between, so the public record
updates within minutes of a final instead of the next morning. Same box, same checkout, same CSV ledger (the
source of truth) as daily_run.py and pretip_check.py, under the same ledger lock.

    python grade_finals.py                  # grade finals, fill late closing lines, mirror changes to Supabase
    python grade_finals.py --dry-run        # report what would change; no ledger or Supabase write
    python grade_finals.py --no-supabase    # ledger only
    options: --lock-wait SECONDS (default 0: if another job holds the ledger, skip this cycle)  -v

One cycle:
  1. lock the ledger, seal(read_ledger()), verify_ledger() — a broken hash chain stops the cycle (nothing written)
  2. daily_run.grade(): open plays whose game is final -> result, scores, P&L, closing line + CLV;
     postponed / canceled -> void (identical to the 7 AM grading)
  3. fill_closes(): graded plays from the last CLOSE_FILL_DAYS days still missing a DK closing line get it now
     (NULL fields only: close_line / close_price / clv_pts are not hashed and check_immutable allows the fill)
  4. write_ledger() (check_immutable: no pick field, hash or graded result can change) — skipped when nothing changed
  5. Supabase picks: for recent graded ledger rows, PATCH only the grading columns that are still NULL in the
     database (result / graded_at / pnl / units_pnl together, guarded by result IS NULL; closing_line and clv one
     at a time, guarded by col IS NULL) — exactly what the picks_immutable trigger allows. A missed write
     self-heals next cycle. No other table is written.
It never reads KenPom, never calls The Odds API or Kalshi, never logs picks and never touches games /
projections / market_lines. ESPN (free) is the only feed: one scoreboard call per open game date, one odds call per
game being graded or missing a close. Re-running is a no-op. Exit 0 = done or skipped (lock busy); 1 = error.
"""
from __future__ import annotations

import argparse
import sys
from datetime import date, timedelta

import pandas as pd

import daily_run as dr
from czarp.supabase_writer import LEAGUE

CLOSE_FILL_DAYS = 2           # look back this many game days for graded plays still missing a DK close
SYNC_DAYS = 3                 # mirror grading for plays from the last N game days (self-heals a missed write)
GRADE_COLS = ("status", "graded_ct", "away_score", "home_score", "close_line", "close_price", "clv_pts",
              "result", "pnl", "pnl_units")


def _missing(v) -> bool:
    return v is None or (not isinstance(v, str) and pd.isna(v))


def fill_closes(ledger: pd.DataFrame, now, days: int = CLOSE_FILL_DAYS, log=dr.log) -> list[int]:
    """Fill a missing DK close (close_line, close_price, clv_pts) on recently graded, non-void plays.
    NULL fields only; result / P&L / scores are never touched. Returns the filled row indexes."""
    if ledger.empty:
        return []
    for c in ("close_line", "close_price", "clv_pts"):
        ledger[c] = ledger[c].astype(object)
    since = (now.date() - timedelta(days=days)).isoformat()
    todo = ledger[(ledger.status == "graded") & ledger.close_line.apply(_missing)
                  & (ledger.game_date.astype(str) >= since)]
    if todo.empty:
        return []
    from backtest import NameMatcher
    from czarp.sync_finals import align_espn_box
    done = []
    for d in sorted(todo.game_date.astype(str).unique()):
        try:
            sb = {g["event_id"]: g for g in dr.espn_scoreboard(date.fromisoformat(d))}
        except Exception as e:
            log(f"  close fill {d}: ESPN scoreboard failed ({type(e).__name__}) — will retry next run")
            continue
        rows = todo[todo.game_date.astype(str) == d]
        names = (set(rows["home"].dropna().astype(str)) | set(rows["away"].dropna().astype(str))) - {""}
        matcher = NameMatcher(names) if names else None
        for i in rows.index:
            r = ledger.loc[i]
            g = sb.get(str(r.event_id))
            if g is None:
                continue
            aligned = align_espn_box(r.home, r.away, g.get("home"), g.get("away"),
                                     g.get("home_score"), g.get("away_score"), match=matcher)
            if aligned is None:
                continue
            close, cpx, clv = dr.close_fields(r, dr.espn_dk_lines(str(r.event_id)), aligned[2])
            if close is None:
                continue
            for col, val in (("close_line", close), ("close_price", cpx), ("clv_pts", clv)):
                if _missing(ledger.at[i, col]) and val is not None:
                    ledger.at[i, col] = val
            done.append(i)
    return done


def _clean(v):
    from czarp.supabase_writer import _clean as c
    if _missing(v):
        return None
    return c(pd.to_numeric(v, errors="coerce")) if not isinstance(v, str) else v


def picks_patches(ledger: pd.DataFrame, remote: dict) -> list[tuple[str, dict, str]]:
    """(pick_key, fields, null-guard column) PATCHes that bring Supabase picks up to the ledger's grading.
    Only columns that are NULL in the database and set in the ledger; the same values the 7 AM full sync writes."""
    out = []
    for r in ledger.to_dict("records"):
        key = str(r["pick_id"])
        p = remote.get(key)
        if p is None or _missing(r.get("result")) or not isinstance(r.get("result"), str):
            continue
        if p.get("result") is None:
            out.append((key, {"result": r["result"],
                              "graded_at": r["graded_ct"] if isinstance(r.get("graded_ct"), str) else None,
                              "pnl": _clean(r.get("pnl")), "units_pnl": _clean(r.get("pnl_units"))}, "result"))
        for col, src in (("closing_line", "close_line"), ("clv", "clv_pts")):
            if p.get(col) is None and _clean(r.get(src)) is not None:
                out.append((key, {col: _clean(r.get(src))}, col))
    return out


def sync_picks(ledger: pd.DataFrame, now, client=None, days: int = SYNC_DAYS, log=dr.log) -> int:
    """Mirror recent grading to Supabase picks (NULL columns only; see picks_patches). Returns PATCHes sent."""
    since = (now.date() - timedelta(days=days)).isoformat()
    recent = ledger[ledger.result.apply(lambda v: isinstance(v, str)) & (ledger.game_date.astype(str) >= since)]
    if recent.empty:
        return 0
    if client is None:
        from czarp.supabase_writer import Client
        client = Client()
    keys = ",".join(sorted(f'"{k}"' for k in recent.pick_id.astype(str)))
    remote = {p["pick_key"]: p for p in client.get("picks", select="pick_key,result,closing_line,clv",
                                                     league=f"eq.{LEAGUE}", pick_key=f"in.({keys})")}
    missing = sorted(set(recent.pick_id.astype(str)) - set(remote))
    if missing:
        log(f"  Supabase: {len(missing)} graded plays not mirrored yet (the 7 AM sync adds them): {', '.join(missing[:5])}")
    patches = picks_patches(recent, remote)
    for key, fields, guard in patches:
        client.patch("picks", fields, pick_key=f"eq.{key}", **{guard: "is.null"})
    return len(patches)


def run(now=None, dry_run: bool = False, supabase: bool = True, lock_wait: float = 0.0, client=None,
        log=dr.log) -> dict:
    """One grading cycle. Returns counts: graded, voided, closes (late closing lines filled), written (ledger
    rewritten), patched (Supabase PATCHes), skipped (lock busy message) and sync_error."""
    import os
    now = now or dr.now_ct()
    out = {"graded": 0, "voided": 0, "closes": 0, "written": False, "patched": 0, "skipped": None, "sync_error": None}
    mirror = supabase and not dry_run and (client is not None or bool(
        os.getenv("SUPABASE_URL") and os.getenv("SUPABASE_SERVICE_KEY")))
    try:
        with dr.ledger_lock(wait=lock_wait):
            ledger = dr.seal(dr.read_ledger())
            bad = dr.verify_ledger(ledger)
            if bad:
                raise dr.LedgerImmutableError(f"ledger integrity check failed: {'; '.join(bad[:5])}")
            gi = dr.grade(ledger, now)
            ci = fill_closes(ledger, now, log=log)
            out["voided"] = sum(ledger.at[i, "result"] == "void" for i in gi)
            out["graded"] = len(gi) - out["voided"]
            out["closes"] = len(ci)
            for i in gi:
                r = ledger.loc[i]
                log(f"  graded {r.pick_id} ({r.tier}): {r.side} {r.line} -> {r.result}"
                    + ("" if r.result == "void" else
                       (f", CLV {float(r.clv_pts):+g}" if not _missing(r.clv_pts) else ", close pending")))
            if (gi or ci) and not dry_run:
                dr.write_ledger(ledger)
                out["written"] = True
            if mirror:
                try:
                    out["patched"] = sync_picks(ledger, now, client=client, log=log)
                except Exception as e:      # the CSV ledger is graded; the mirror retries next cycle
                    log(f"  Supabase picks sync failed ({type(e).__name__}: {e}) — ledger graded; retry next cycle")
                    out["sync_error"] = type(e).__name__
    except dr.LedgerBusy as e:
        out["skipped"] = str(e)
        log(f"  skipped: {e} — next cycle will grade")
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dry-run", action="store_true", help="report only: no ledger or Supabase write")
    ap.add_argument("--no-supabase", action="store_true", help="grade the ledger only")
    ap.add_argument("--lock-wait", type=float, default=0.0,
                    help="seconds to wait for the ledger lock (default 0: skip this cycle if busy)")
    ap.add_argument("-v", "--verbose", action="store_true")
    a = ap.parse_args(argv)
    now = dr.now_ct()
    log = dr.log if a.verbose else (lambda m: None)
    try:
        s = run(now, dry_run=a.dry_run, supabase=not a.no_supabase, lock_wait=a.lock_wait, log=log)
    except Exception as e:
        print(f"grade_finals {now:%Y-%m-%d %H:%M} CT: FAILED ({type(e).__name__}: {e})", file=sys.stderr)
        return 1
    if s["skipped"]:
        print(f"grade_finals {now:%Y-%m-%d %H:%M} CT: skipped ({s['skipped']})")
    elif s["graded"] or s["voided"] or s["closes"] or s["patched"] or s["sync_error"] or a.verbose:
        print(f"grade_finals {now:%Y-%m-%d %H:%M} CT: graded {s['graded']}, voided {s['voided']}, "
              f"closing lines filled {s['closes']}, Supabase picks patched {s['patched']}"
              + (f", Supabase sync FAILED ({s['sync_error']}; retries next cycle)" if s["sync_error"] else "")
              + (" (dry run)" if a.dry_run else ""))
    return 0


if __name__ == "__main__":
    sys.exit(main())
