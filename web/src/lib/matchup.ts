// Matchup maker helpers. The numbers all come from the `project_matchup` RPC (CZarp's pipeline); nothing here
// computes a projection. This file only validates the RPC reply, formats it, parses/builds the share URL and
// maps RPC errors to friendly text. Pure functions, unit-tested in tests/matchup.test.ts.

export type Venue = "neutral" | "a_home" | "b_home";
export const VENUES: Venue[] = ["neutral", "a_home", "b_home"];

export type Matchup = {
  team_a: number;
  team_b: number;
  venue: Venue;
  /** ISO timestamp of the ratings behind the projection */
  as_of: string;
  score_a: number;
  score_b: number;
  total: number;
  /** negative = team A favored */
  spread_a: number;
  /** 0-1 */
  win_pct_a: number;
  win_pct_b: number;
};

/** A stored projection older than this is flagged. The pipeline runs daily. */
export const STALE_HOURS = 30;

const isVenue = (v: unknown): v is Venue => v === "neutral" || v === "a_home" || v === "b_home";
const n = (v: unknown): number | null => {
  if (v == null || v === "") return null;
  const x = Number(v);
  return Number.isFinite(x) ? x : null;
};

/** Validate an RPC reply (numeric columns can arrive as numbers or strings). Null when anything is missing. */
export function parseMatchup(raw: unknown): Matchup | null {
  if (!raw || typeof raw !== "object") return null;
  const r = (Array.isArray(raw) ? raw[0] : raw) as Record<string, unknown> | undefined;
  if (!r) return null;
  const f = {
    team_a: n(r.team_a), team_b: n(r.team_b), score_a: n(r.score_a), score_b: n(r.score_b), total: n(r.total),
    spread_a: n(r.spread_a), win_pct_a: n(r.win_pct_a), win_pct_b: n(r.win_pct_b),
  };
  if (Object.values(f).some((v) => v == null) || !isVenue(r.venue) || typeof r.as_of !== "string") return null;
  return { ...(f as Record<keyof typeof f, number>), venue: r.venue, as_of: r.as_of };
}

// ---- URL state: ?a=ID&b=ID&venue=neutral|a_home|b_home ---------------------------------------------------

export type MatchupParams = { a: number | null; b: number | null; venue: Venue };

const parseId = (v: string | string[] | undefined): number | null => {
  const s = Array.isArray(v) ? v[0] : v;
  if (!s || !/^\d{1,12}$/.test(s)) return null;
  const x = Number(s);
  return x > 0 ? x : null;
};

/** Never throws: bad ids become null and a bad or missing venue becomes "neutral". */
export function parseMatchupParams(sp: Record<string, string | string[] | undefined> | URLSearchParams): MatchupParams {
  const get = (k: string) => (sp instanceof URLSearchParams ? sp.get(k) ?? undefined : sp[k]);
  const v = get("venue");
  const venue = Array.isArray(v) ? v[0] : v;
  return { a: parseId(get("a")), b: parseId(get("b")), venue: isVenue(venue) ? venue : "neutral" };
}

export function matchupHref(sportKey: string, p: Partial<MatchupParams> = {}): string {
  const q = new URLSearchParams();
  if (p.a) q.set("a", String(p.a));
  if (p.b) q.set("b", String(p.b));
  if ((p.a || p.b) && p.venue) q.set("venue", p.venue);
  const qs = q.toString();
  return `/${sportKey}/matchup${qs ? `?${qs}` : ""}`;
}

// ---- RPC errors ---------------------------------------------------------------------------------------------

export type RpcError = { code?: string | null; message?: string | null } | null | undefined;
export type MatchupError = { kind: "invalid" | "bad_id" | "updating" | "other"; message: string };

/** Friendly text for the RPC's HTTP 400s. Raw database messages are never shown to customers. */
export function mapRpcError(e: RpcError): MatchupError {
  switch (e?.code) {
    case "22023":
      return { kind: "invalid", message: "We couldn't build that matchup. Pick two different teams and a court, then try again." };
    case "22P02":
      return { kind: "bad_id", message: "That team isn't recognized. Pick teams from the list." };
    case "55000":
      return { kind: "updating", message: "Projections are updating, try again shortly." };
    default:
      return { kind: "other", message: "Something went wrong loading that matchup. Try again in a moment." };
  }
}

type RpcClient = {
  rpc: (fn: string, args: Record<string, unknown>) => PromiseLike<{ data: unknown; error: RpcError }>;
};
export type MatchupResult = { ok: true; data: Matchup } | { ok: false; error: MatchupError };

/** Call project_matchup with the anon client. Errors, malformed replies and network failures all map to a friendly error. */
export async function fetchMatchup(sb: RpcClient, a: number, b: number, venue: Venue): Promise<MatchupResult> {
  try {
    const { data, error } = await sb.rpc("project_matchup", { team_a: a, team_b: b, venue });
    if (error) return { ok: false, error: mapRpcError(error) };
    const m = parseMatchup(data);
    return m ? { ok: true, data: m } : { ok: false, error: mapRpcError(null) };
  } catch {
    return { ok: false, error: mapRpcError(null) };
  }
}

// ---- display formatting -------------------------------------------------------------------------------------

/** Scores and totals to one decimal. A displayed pair can be 0.1 off the displayed total; that's fine. */
export const fmtScore = (x: number) => x.toFixed(1);

/** Win % to one decimal; the RPC's 0.000 and 1.000 become "<0.1%" and ">99.9%". */
export function fmtWinPct(p: number): string {
  if (p < 0.001) return "<0.1%";
  if (p > 0.999) return ">99.9%";
  return `${(p * 100).toFixed(1)}%`;
}

/** Width of the team-A side of the win bar, kept visible at the extremes. */
export const winBarPct = (pa: number) => Math.min(99, Math.max(1, Math.round(pa * 1000) / 10));

export function favorite(m: Pick<Matchup, "spread_a">): "a" | "b" | null {
  return m.spread_a < 0 ? "a" : m.spread_a > 0 ? "b" : null;
}

const MINUS = "\u2212";
/** Betting notation for the favorite: "Duke −3.5", or "Pick'em" at 0. */
export function fmtSpread(m: Pick<Matchup, "spread_a">, nameA: string, nameB: string): string {
  const fav = favorite(m);
  if (!fav) return "Pick'em";
  return `${fav === "a" ? nameA : nameB} ${MINUS}${Math.abs(m.spread_a).toFixed(1)}`;
}

/** Hours since as_of; null when unparseable. */
export function ageHours(asOf: string, now = new Date()): number | null {
  const t = Date.parse(asOf);
  return Number.isFinite(t) ? (now.getTime() - t) / 3_600_000 : null;
}

/** Stale when older than ~30h. An unparseable timestamp counts as stale. */
export function isStale(asOf: string, now = new Date()): boolean {
  const h = ageHours(asOf, now);
  return h == null || h > STALE_HOURS;
}

/** "Nov 18, 7:31 PM CT" in Central time, whatever the viewer's timezone. */
export function fmtAsOf(asOf: string): string | null {
  const d = new Date(asOf);
  if (Number.isNaN(d.getTime())) return null;
  const date = new Intl.DateTimeFormat("en-US", { timeZone: "America/Chicago", month: "short", day: "numeric" }).format(d);
  const time = new Intl.DateTimeFormat("en-US", { timeZone: "America/Chicago", hour: "numeric", minute: "2-digit" }).format(d);
  return `${date}, ${time} CT`;
}

export function venueLabel(venue: Venue, nameA: string, nameB: string): string {
  return venue === "neutral" ? "Neutral court" : `At ${venue === "a_home" ? nameA : nameB}`;
}

/** Swapping teams keeps the same real-world home team. */
export const swapVenue = (v: Venue): Venue => (v === "a_home" ? "b_home" : v === "b_home" ? "a_home" : v);

// ---- "vs neutral" delta: difference between two RPC replies, never a projection of our own ---------------------

export type VenueDelta = { home: "a" | "b"; spread: number; total: number };
const r1 = (x: number) => Math.round(x * 10) / 10;

/** How much the home team's margin and the total moved compared with the neutral-court reply. */
export function venueDelta(neutral: Matchup, at: Matchup): VenueDelta | null {
  if (at.venue === "neutral") return null;
  const home = at.venue === "a_home" ? "a" : "b";
  // spread_a is team A's line, so a lower spread_a is a better margin for A
  const shiftA = r1(neutral.spread_a - at.spread_a);
  return { home, spread: home === "a" ? shiftA : -shiftA, total: r1(at.total - neutral.total) };
}

const signed = (x: number) => (x > 0 ? `+${x.toFixed(1)}` : x < 0 ? `${MINUS}${Math.abs(x).toFixed(1)}` : "0.0");

/** "Texas St. +2.9 pts vs neutral · total +0.5" */
export function fmtVenueDelta(d: VenueDelta, nameA: string, nameB: string): string {
  const home = d.home === "a" ? nameA : nameB;
  const spread = d.spread === 0 ? `${home} no change` : `${home} ${signed(d.spread)} pts`;
  return `${spread} vs neutral${d.total !== 0 ? ` · total ${signed(d.total)}` : ""}`;
}
