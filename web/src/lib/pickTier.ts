/** Portfolio (results) pick-tier resolution.
 *
 *  Tracked picks and their queries stay in the codebase. Flip
 *  SHOW_TRACKED_TIER to true to bring the Official/Tracked toggle back.
 *  While it is false, the portfolio always shows official picks, and
 *  ?tier=track (or any other non-official value) is not left on the URL.
 */
import type { RangeKey } from "./data";
import type { PickTier } from "./labels";

export const SHOW_TRACKED_TIER = false;

/** Map a ?tier= query to the book we actually show. */
export function resolvePortfolioTier(
  raw: string | null | undefined,
  showTracked: boolean = SHOW_TRACKED_TIER,
): PickTier {
  if (showTracked && raw === "track") return "track";
  return "official";
}

/** True when the URL names a tier we will not display (so the page should replace-redirect). */
export function shouldStripPortfolioTierParam(
  raw: string | null | undefined,
  showTracked: boolean = SHOW_TRACKED_TIER,
): boolean {
  if (raw == null || raw === "") return false;
  return resolvePortfolioTier(raw, showTracked) !== raw;
}

/** Portfolio href. Never writes ?tier=track while SHOW_TRACKED_TIER is false. */
export function resultsHref(
  sportKey: string,
  range: RangeKey = "season",
  tier: string | null | undefined = "official",
  showTracked: boolean = SHOW_TRACKED_TIER,
): string {
  const q = new URLSearchParams();
  const t = resolvePortfolioTier(tier, showTracked);
  if (showTracked && t !== "official") q.set("tier", t);
  if (range !== "season") q.set("range", range);
  const qs = q.toString();
  return `/${sportKey}/results${qs ? `?${qs}` : ""}`;
}
