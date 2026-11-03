// Sport registry. Every data table (slate, ledger, models) carries a `sport`
// key so new leagues plug in by flipping `status` to "live".
export type SportKey = "cbb" | "nba" | "wnba";

export type Sport = {
  key: SportKey;
  label: string;
  long: string;
  status: "live" | "coming_soon";
};

export const SPORTS: Sport[] = [
  { key: "cbb", label: "CBB", long: "NCAA Men's Basketball", status: "live" },
  { key: "nba", label: "NBA", long: "NBA", status: "coming_soon" },
  { key: "wnba", label: "WNBA", long: "WNBA", status: "coming_soon" },
];

export const DEFAULT_SPORT: SportKey = "cbb";

export function getSport(key: string): Sport | undefined {
  return SPORTS.find((s) => s.key === key);
}
