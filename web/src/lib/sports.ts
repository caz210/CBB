// Sport registry. Every data table (slate, ledger, models) carries a `sport`
// key so new leagues plug in by flipping `enabled` (and `status` to "live").
export type SportKey = "cbb" | "nba" | "wnba";

export type Sport = {
  key: SportKey;
  label: string;
  long: string;
  /** leagues.code in Supabase (pick_record is keyed by league, not sport) */
  league: string;
  status: "live" | "coming_soon";
  /** When false the sport is hidden and its routes redirect to the CBB slate. Flip to re-add. */
  enabled: boolean;
};

const ALL_SPORTS: Sport[] = [
  { key: "cbb", label: "CBB", long: "NCAA Men's Basketball", league: "ncaam", status: "live", enabled: true },
  { key: "nba", label: "NBA", long: "NBA", league: "nba", status: "coming_soon", enabled: false },
  { key: "wnba", label: "WNBA", long: "WNBA", league: "wnba", status: "coming_soon", enabled: false },
];

/** The live catalog. Switcher, sitemap, nav and chips all read this. */
export const SPORTS: Sport[] = ALL_SPORTS.filter((s) => s.enabled);

export const DEFAULT_SPORT: SportKey = "cbb";

export function getSport(key: string): Sport | undefined {
  return SPORTS.find((s) => s.key === key);
}

/** Temporary 307s for sports that aren't in SPORTS yet, so old /nba and /wnba links don't 404. */
export function sportRedirects(): { source: string; destination: string; statusCode: 307 }[] {
  // statusCode 307 (not `permanent`) is a temporary redirect, so the URLs can come back.
  return ALL_SPORTS.filter((s) => !s.enabled).flatMap((s) => [
    { source: `/${s.key}`, destination: `/${DEFAULT_SPORT}/slate`, statusCode: 307 as const },
    { source: `/${s.key}/:path*`, destination: `/${DEFAULT_SPORT}/slate`, statusCode: 307 as const },
  ]);
}
