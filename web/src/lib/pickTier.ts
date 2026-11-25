/** Portfolio (results) pick-tier resolution.
 *
 *  Tracked picks and their queries stay in the codebase. Flip
 *  SHOW_TRACKED_TIER to true to bring the Official/Tracked toggle back.
 *  While it is false, ?tier=track (and any unknown value) is not left on the URL.
 *  CZarp Leans (?tier=lean) stay visible even when Tracked is hidden.
 */
import type { RangeKey } from "./data";
import type { PickTier } from "./labels";

export const SHOW_TRACKED_TIER = false;

/** Map a ?tier= query to the book we actually show. */
export function resolvePortfolioTier(
  raw: string | null | undefined,
  showTracked: boolean = SHOW_TRACKED_TIER,
): PickTier {
  if (raw === "lean") return "lean";
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
  if (t === "lean") q.set("tier", "lean");
  else if (showTracked && t !== "official") q.set("tier", t);
  if (range !== "season") q.set("range", range);
  const qs = q.toString();
  return `/${sportKey}/results${qs ? `?${qs}` : ""}`;
}

export type QueryValue = string | string[] | undefined;

/** Copy every query param except `tier`. The path is always /{sport}/results (no open redirect). */
export function resultsUrlStrippingTier(
  sportKey: string,
  sp: URLSearchParams | Record<string, QueryValue>,
): string {
  const q = sp instanceof URLSearchParams ? new URLSearchParams(sp) : recordToSearchParams(sp);
  q.delete("tier");
  const qs = q.toString();
  return `/${sportKey}/results${qs ? `?${qs}` : ""}`;
}

function recordToSearchParams(sp: Record<string, QueryValue>): URLSearchParams {
  const q = new URLSearchParams();
  for (const [k, v] of Object.entries(sp)) {
    if (v == null) continue;
    if (Array.isArray(v)) {
      for (const item of v) q.append(k, item);
    } else {
      q.append(k, v);
    }
  }
  return q;
}
