/** New-coach lock copy. Defaults match daily_run.py (COACH_LOCK_SIDES=8, COACH_LOCK_TOTALS=4).
 *  team_profiles has new_coach / new_coach_games only — no per-team lock_sides/lock_totals columns. */
export const COACH_LOCK_SIDES = 8;
export const COACH_LOCK_TOTALS = 4;

export type NewCoachLock = {
  team: string;
  gamesPlayed: number;
  lockSides: number;
  lockTotals: number;
  sidesLocked: boolean;
  totalsLocked: boolean;
};

export function newCoachLock(
  team: string,
  isNew: boolean,
  games: number | null | undefined,
): NewCoachLock | null {
  if (!isNew) return null;
  const gamesPlayed = games ?? 0;
  return {
    team,
    gamesPlayed,
    lockSides: COACH_LOCK_SIDES,
    lockTotals: COACH_LOCK_TOTALS,
    sidesLocked: gamesPlayed < COACH_LOCK_SIDES,
    totalsLocked: gamesPlayed < COACH_LOCK_TOTALS,
  };
}

/** Countdown lines, e.g. "New coach: no official sides until game 8 (2 of 8 played)". */
export function newCoachLines(lock: NewCoachLock, name = false): string[] {
  const who = name && lock.team ? ` (${lock.team})` : "";
  const lines: string[] = [];
  if (lock.sidesLocked) {
    lines.push(`New coach${who}: no official sides until game ${lock.lockSides} (${lock.gamesPlayed} of ${lock.lockSides} played)`);
  }
  if (lock.totalsLocked) {
    lines.push(`New coach${who}: no official totals until game ${lock.lockTotals} (${lock.gamesPlayed} of ${lock.lockTotals} played)`);
  }
  return lines;
}

export function newCoachUnlocked(lock: NewCoachLock): string {
  return `New coach · official sides and totals unlocked (${lock.gamesPlayed} games played)`;
}

export function isCoachLocked(lock: NewCoachLock): boolean {
  return lock.sidesLocked || lock.totalsLocked;
}
