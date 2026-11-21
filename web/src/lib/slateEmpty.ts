/** Copy and date rules for an empty slate. Dates are YYYY-MM-DD in America/Chicago. */

export const SLATE_PAST_EMPTY_BODY = "We don't have a stored slate for this date.";
/** Today before the slate posts, and future dates. */
export const SLATE_UNPOSTED_EMPTY_BODY = "This slate posts by 7:15 AM CT on game day.";
export const SLATE_FUTURE_EMPTY_BODY = SLATE_UNPOSTED_EMPTY_BODY;
export const SLATE_TODAY_EMPTY_BODY = SLATE_UNPOSTED_EMPTY_BODY;
export const SLATE_MATCHUP_CTA_LABEL = "Get an early line → Matchup maker";
/** Today, after we've confirmed nothing is on the schedule. */
export const SLATE_NO_GAMES_TODAY_BODY = "No games are scheduled today.";
export const SLATE_NO_GAMES_CTA_LABEL = "Run a matchup → Matchup maker";

/** The morning run loads today's games and slate together around 7:15 AM CT, so before then an empty games
 *  table only means "not loaded yet". */
export const SLATE_POST_MINUTES_CT = 7 * 60 + 15;

/** Minutes past midnight in America/Chicago. */
export function minutesCT(now = new Date()): number {
  const parts = new Intl.DateTimeFormat("en-US", { timeZone: "America/Chicago", hour: "2-digit", minute: "2-digit", hourCycle: "h23" }).formatToParts(now);
  const get = (t: string) => Number(parts.find((p) => p.type === t)?.value ?? 0);
  return get("hour") * 60 + get("minute");
}

/**
 * What the empty today page knows about the schedule: false = confirmed no games (games table empty AND it's past
 * the 7:15 AM CT post time), otherwise the raw flag (true / unknown). Before 7:15 the games table hasn't loaded
 * yet, so an empty read proves nothing.
 */
export function confirmedSchedule(hasScheduledGames: boolean | null | undefined, now = new Date()): boolean | null {
  if (hasScheduledGames === false && minutesCT(now) < SLATE_POST_MINUTES_CT) return null;
  return hasScheduledGames ?? null;
}

export type SlateDateRelation = "past" | "today" | "future";

/** Compare two calendar dates already expressed in America/Chicago (e.g. from todayCT). */
export function slateDateRelation(date: string, today: string): SlateDateRelation {
  if (date < today) return "past";
  if (date > today) return "future";
  return "today";
}

/**
 * hasScheduledGames: false only when the games table confirms nothing is on today's schedule.
 * Unknown (null/undefined) keeps the "posts by 7:15" line, since the slate may just be late.
 */
export function slateEmptyBody(relation: SlateDateRelation, hasScheduledGames?: boolean | null): string {
  if (relation === "past") return SLATE_PAST_EMPTY_BODY;
  if (relation === "today" && hasScheduledGames === false) return SLATE_NO_GAMES_TODAY_BODY;
  return SLATE_UNPOSTED_EMPTY_BODY;
}

export function slateMatchupCtaLabel(relation: SlateDateRelation, hasScheduledGames?: boolean | null): string {
  return relation === "today" && hasScheduledGames === false ? SLATE_NO_GAMES_CTA_LABEL : SLATE_MATCHUP_CTA_LABEL;
}

/** Early-line CTA on dates that have not posted yet (future, and today before the slate lands). */
export function showSlateMatchupCta(relation: SlateDateRelation): boolean {
  return relation === "future" || relation === "today";
}

export function slateMatchupHref(sportKey: string): string {
  return `/${sportKey}/matchup`;
}

/** Green Live chip (or Final/Updated) only when this date has games and is not in the future. */
export function showSlateLiveBadge(relation: SlateDateRelation, gameCount: number): boolean {
  return gameCount > 0 && relation !== "future";
}
