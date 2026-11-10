/** New-coach hold copy from team_profiles / team_profile_latest.
 *  Lengths and remaining come from the DB (lock_sides_games, lock_totals_games,
 *  lock_sides_remaining, lock_totals_remaining). Remaining is null when the
 *  team is not a new-coach team. 8 / 4 are last-resort defaults only if a
 *  length column is missing.
 *
 *  User-facing wording is "paused" / "back on" — never lock, unlock, or locked.
 */
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

/** Countdown lines for the team-page badge. */
export function newCoachLines(lock: NewCoachLock, name = false): string[] {
  const who = name && lock.team ? `${lock.team}: ` : "";
  const lines: string[] = [];
  if (lock.sidesLocked) {
    const played = sidesPlayed(lock);
    lines.push(`${who}Sides are paused until game ${lock.lockSides} (${played} of ${lock.lockSides} played).`);
  }
  if (lock.totalsLocked) {
    const played = totalsPlayed(lock);
    lines.push(`${who}Totals are paused until game ${lock.lockTotals} (${played} of ${lock.lockTotals} played).`);
  }
  return lines;
}

export function newCoachUnlocked(lock: NewCoachLock): string {
  return `New coach · official sides and totals are back on (${lock.gamesPlayed} games played)`;
}

export function isCoachLocked(lock: NewCoachLock): boolean {
  return lock.sidesLocked || lock.totalsLocked;
}

/** Played count from lock length minus remaining; fall back to gamesPlayed. */
export function sidesPlayed(lock: NewCoachLock): number {
  if (lock.sidesRemaining != null) return Math.max(0, lock.lockSides - lock.sidesRemaining);
  return lock.gamesPlayed;
}

export function totalsPlayed(lock: NewCoachLock): number {
  if (lock.totalsRemaining != null) return Math.max(0, lock.lockTotals - lock.totalsRemaining);
  return lock.gamesPlayed;
}

/** Teams that still have at least one market paused. Fully clear new-coach teams stay off the slate chip. */
export function lockedCoachLocks(locks?: NewCoachLock[] | null): NewCoachLock[] {
  return (locks ?? []).filter(isCoachLocked);
}

/** Bottom-sheet copy. A market that is already clear says e.g. "Totals are back on." */
export function newCoachSheetCopy(lock: NewCoachLock): { title: string; body: string } {
  const parts: string[] = [
    `${lock.team} has a new coach, so we've paused plays while the model learns the new system.`,
  ];
  if (lock.sidesLocked) {
    const played = sidesPlayed(lock);
    parts.push(`Sides are paused until game ${lock.lockSides} (${played} of ${lock.lockSides} played).`);
  } else {
    parts.push("Sides are back on.");
  }
  if (lock.totalsLocked) {
    const played = totalsPlayed(lock);
    parts.push(`Totals are paused until game ${lock.lockTotals} (${played} of ${lock.lockTotals} played).`);
  } else {
    parts.push("Totals are back on.");
  }
  return { title: "New coach", body: parts.join(" ") };
}
