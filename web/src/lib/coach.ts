/** New-coach lock copy from team_profiles / team_profile_latest.
 *  Lengths and remaining come from the DB (lock_sides_games, lock_totals_games,
 *  lock_sides_remaining, lock_totals_remaining). Remaining is null when the
 *  team is not a new-coach team. 8 / 4 are last-resort defaults only if a
 *  length column is missing. */
export const COACH_LOCK_SIDES = 8;
export const COACH_LOCK_TOTALS = 4;

export type NewCoachLock = {
  team: string;
  gamesPlayed: number;
  lockSides: number;
  lockTotals: number;
  sidesRemaining: number | null;
  totalsRemaining: number | null;
  sidesLocked: boolean;
  totalsLocked: boolean;
};

export type CoachProfileFields = {
  new_coach?: boolean;
  new_coach_games?: number | null;
  games_played?: number | null;
  lock_sides_games?: number | null;
  lock_totals_games?: number | null;
  lock_sides_remaining?: number | null;
  lock_totals_remaining?: number | null;
};

export function newCoachLock(team: string, p: CoachProfileFields): NewCoachLock | null {
  if (!p.new_coach) return null;
  const lockSides = p.lock_sides_games ?? COACH_LOCK_SIDES;
  const lockTotals = p.lock_totals_games ?? COACH_LOCK_TOTALS;
  const gamesPlayed = p.new_coach_games ?? p.games_played ?? 0;
  const sidesRemaining = p.lock_sides_remaining ?? null;
  const totalsRemaining = p.lock_totals_remaining ?? null;
  return {
    team,
    gamesPlayed,
    lockSides,
    lockTotals,
    sidesRemaining,
    totalsRemaining,
    sidesLocked: sidesRemaining != null ? sidesRemaining > 0 : gamesPlayed < lockSides,
    totalsLocked: totalsRemaining != null ? totalsRemaining > 0 : gamesPlayed < lockTotals,
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
