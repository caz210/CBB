/** Copy and date rules for an empty slate. Dates are YYYY-MM-DD in America/Chicago. */

export const SLATE_PAST_EMPTY_BODY = "We don't have a stored slate for this date.";
export const SLATE_TODAY_EMPTY_BODY =
  "Nothing has been posted for this date yet. The model publishes each morning, so check back later.";
export const SLATE_FUTURE_EMPTY_BODY = "This slate posts by 7:15 AM CT on game day.";
export const SLATE_MATCHUP_CTA_LABEL = "Get an early line → Matchup maker";

export type SlateDateRelation = "past" | "today" | "future";

/** Compare two calendar dates already expressed in America/Chicago (e.g. from todayCT). */
export function slateDateRelation(date: string, today: string): SlateDateRelation {
  if (date < today) return "past";
  if (date > today) return "future";
  return "today";
}

export function slateEmptyBody(relation: SlateDateRelation): string {
  if (relation === "past") return SLATE_PAST_EMPTY_BODY;
  if (relation === "future") return SLATE_FUTURE_EMPTY_BODY;
  return SLATE_TODAY_EMPTY_BODY;
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
