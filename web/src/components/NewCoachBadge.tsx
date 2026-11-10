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

/** "Why not a play" lines for markets still locked out of official. */
export function whyNotPlayCopy(locks: NewCoachLock[] | undefined, officialMarkets: Iterable<string>): string[] {
  if (!locks?.length) return [];
  const mk = new Set(officialMarkets);
  const named = locks.length > 1;
  const out: string[] = [];
  for (const l of locks) {
    const lines = newCoachLines(l, named);
    if (l.sidesLocked && !mk.has("spread") && !mk.has("moneyline")) out.push(...lines.filter((x) => x.includes("sides")));
    if (l.totalsLocked && !mk.has("total")) out.push(...lines.filter((x) => x.includes("totals")));
  }
  return out;
}
