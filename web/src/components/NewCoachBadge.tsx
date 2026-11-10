import { isCoachLocked, newCoachLines, newCoachUnlocked, type NewCoachLock } from "@/lib/coach";

export function NewCoachBadge({ lock, name = false, className = "" }: { lock: NewCoachLock; name?: boolean; className?: string }) {
  const locked = isCoachLocked(lock);
  const lines = locked ? newCoachLines(lock, name) : [newCoachUnlocked(lock)];
  return (
    <div className={`rounded-md border border-amber-400/40 bg-amber-400/10 px-2 py-1 text-[11px] leading-snug text-amber-100 ${className}`}>
      {lines.map((l) => <div key={l}>{l}</div>)}
    </div>
  );
}

export function NewCoachBadges({ locks, name }: { locks?: NewCoachLock[] | null; name?: boolean }) {
  if (!locks?.length) return null;
  const showName = name ?? locks.length > 1;
  return (
    <div className="space-y-1">
      {locks.map((l) => <NewCoachBadge key={l.team} lock={l} name={showName} />)}
    </div>
  );
}

/** Machine-readable skip codes → short labels when human text is missing. */
export const SKIP_LABELS: Record<string, string> = {
  below_open_threshold: "below open threshold",
  below_current_threshold: "below current threshold",
  line_moved_against: "line moved against",
  new_coach_lock: "new coach lock",
  kelly_too_small: "Kelly stake too small",
  game_started: "game started",
  no_current_line: "no current line",
  no_opening_line: "no opening line",
};

type WhyNotSource = {
  coachLocks?: NewCoachLock[] | null;
  skip_reason_spread?: string | null;
  skip_reason_total?: string | null;
  skip_reasons_spread?: string[] | null;
  skip_reasons_total?: string[] | null;
};

function formatSkip(human?: string | null, codes?: string[] | null): string | null {
  if (human && human.trim()) return human.trim();
  if (codes && codes.length) return codes.map((c) => SKIP_LABELS[c] ?? c.replace(/_/g, " ")).join("; ");
  return null;
}

/** Prefer slate_games human skip text; else map codes; else new-coach countdown. Empty is fine. */
export function whyNotPlayCopy(g: WhyNotSource | undefined, officialMarkets: Iterable<string>): string[] {
  if (!g) return [];
  const mk = new Set(officialMarkets);
  const out: string[] = [];
  if (!mk.has("spread") && !mk.has("moneyline")) {
    const s = formatSkip(g.skip_reason_spread, g.skip_reasons_spread);
    if (s) out.push(`Spread: ${s}`);
  }
  if (!mk.has("total")) {
    const s = formatSkip(g.skip_reason_total, g.skip_reasons_total);
    if (s) out.push(`Total: ${s}`);
  }
  if (out.length) return out;
  const locks = g.coachLocks ?? [];
  const named = locks.length > 1;
  for (const l of locks) {
    const lines = newCoachLines(l, named);
    if (l.sidesLocked && !mk.has("spread") && !mk.has("moneyline")) out.push(...lines.filter((x) => x.includes("sides")));
    if (l.totalsLocked && !mk.has("total")) out.push(...lines.filter((x) => x.includes("totals")));
  }
  return out;
}
