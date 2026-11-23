/** Map an ESPN scoreboard/summary status to Live Line period + whole minutes left.
 *  Pure: no I/O. A final or unreadable status returns null so the UI can fall back. */
import { OT_MIN, HALF_MIN, type LivePeriod } from "./liveLine";

export type EspnLiveClock = { period: Exclude<LivePeriod, "TIP">; minutesLeft: number };

const finite = (n: unknown): n is number => typeof n === "number" && Number.isFinite(n);

function asRecord(v: unknown): Record<string, unknown> | null {
  return v && typeof v === "object" && !Array.isArray(v) ? (v as Record<string, unknown>) : null;
}

function typeBlob(status: Record<string, unknown>): Record<string, unknown> {
  return asRecord(status.type) ?? {};
}

function text(...xs: unknown[]): string {
  return xs.map((x) => (x == null ? "" : String(x))).join(" ");
}

/** True when ESPN says the game is over — not a live clock we should prefill. */
export function isEspnFinal(status: unknown): boolean {
  const s = asRecord(status);
  if (!s) return false;
  const t = typeBlob(s);
  if (t.state === "post" || t.completed === true) return true;
  const blob = text(t.name, t.description, t.detail, t.shortDetail).toLowerCase();
  return t.name === "STATUS_FINAL" || /\bfinal\b/.test(blob);
}

/** Halftime from status.type name or description (STATUS_HALFTIME, "Halftime"), not "2nd Half". */
export function isEspnHalftime(status: unknown): boolean {
  const s = asRecord(status);
  if (!s) return false;
  const t = typeBlob(s);
  const blob = text(t.name, t.description, t.detail, t.shortDetail, t.altDetail);
  return /STATUS_HALFTIME/i.test(String(t.name ?? "")) || /halftime/i.test(blob);
}

/** Parse ESPN displayClock ('12:34', '0:45.2', '20:00') into minutes, including fractional seconds. */
export function parseDisplayClock(raw: unknown): number | null {
  if (raw == null) return null;
  if (finite(raw)) return raw >= 0 ? raw : null;
  const s = String(raw).trim();
  if (!s) return null;
  const m = s.match(/^(\d{1,2})(?::(\d{1,2}(?:\.\d+)?))?$/);
  if (!m) return null;
  const mm = Number(m[1]);
  const ss = m[2] != null ? Number(m[2]) : 0;
  if (!Number.isFinite(mm) || !Number.isFinite(ss) || ss >= 60) return null;
  return mm + ss / 60;
}

/** status.period: 1 → 1H, 2 → 2H, 3+ → OT. Null if missing or not a positive integer. */
export function periodFromEspnPeriod(period: unknown): "1H" | "2H" | "OT" | null {
  const n = typeof period === "string" && /^\d+$/.test(period.trim()) ? Number(period) : period;
  if (!finite(n) || n < 1 || !Number.isInteger(n)) return null;
  if (n === 1) return "1H";
  if (n === 2) return "2H";
  return "OT";
}

/** Round minutes to the nearest whole minute and clamp to the slider (20–1, or 5–1 in OT). 0:00 and 0:xx become 1. */
export function clampRoundedMinutes(period: Exclude<LivePeriod, "TIP">, minutes: number): number {
  if (period === "FINAL" || period === "HALFTIME") return period === "FINAL" ? 0 : HALF_MIN;
  const max = period === "OT" ? OT_MIN : HALF_MIN;
  const rounded = Math.round(minutes);
  return Math.min(max, Math.max(1, rounded));
}

/** Map a single ESPN competition status object to a Live Line clock, or null if it can't be used. */
export function clockFromEspnStatus(status: unknown): EspnLiveClock | null {
  const s = asRecord(status);
  if (!s || isEspnFinal(s)) return null;
  if (isEspnHalftime(s)) return { period: "HALFTIME", minutesLeft: HALF_MIN };
  const period = periodFromEspnPeriod(s.period);
  if (!period) return null;
  const parsed = parseDisplayClock(s.displayClock);
  if (parsed == null) return null;
  return { period, minutesLeft: clampRoundedMinutes(period, parsed) };
}

/** Pull the competition status object out of an ESPN summary or scoreboard payload. */
export function statusFromEspnPayload(payload: unknown): unknown {
  const root = asRecord(payload);
  if (!root) return null;
  const fromComp = (comp: unknown) => asRecord(comp)?.status ?? null;
  const header = asRecord(root.header);
  const headerComps = header && Array.isArray(header.competitions) ? header.competitions : [];
  if (headerComps.length) return fromComp(headerComps[0]) ?? header?.status ?? null;
  const events = Array.isArray(root.events) ? root.events : [];
  for (const ev of events) {
    const comps = asRecord(ev) && Array.isArray(asRecord(ev)!.competitions) ? (asRecord(ev)!.competitions as unknown[]) : [];
    if (comps.length && fromComp(comps[0])) return fromComp(comps[0]);
    if (asRecord(ev)?.status) return asRecord(ev)!.status;
  }
  return header?.status ?? root.status ?? null;
}

export function clockFromEspnPayload(payload: unknown): EspnLiveClock | null {
  return clockFromEspnStatus(statusFromEspnPayload(payload));
}

/** Scores from the same payload, if present (home/away). Nulls omitted. */
export function scoresFromEspnPayload(payload: unknown): { home: number; away: number } | null {
  const root = asRecord(payload);
  if (!root) return null;
  const comps: unknown[] = [];
  const header = asRecord(root.header);
  if (header && Array.isArray(header.competitions)) comps.push(...header.competitions);
  if (Array.isArray(root.events)) {
    for (const ev of root.events) {
      const c = asRecord(ev);
      if (c && Array.isArray(c.competitions)) comps.push(...c.competitions);
    }
  }
  for (const comp of comps) {
    const competitors = asRecord(comp) && Array.isArray(asRecord(comp)!.competitors) ? (asRecord(comp)!.competitors as unknown[]) : [];
    let home: number | null = null, away: number | null = null;
    for (const raw of competitors) {
      const c = asRecord(raw);
      if (!c) continue;
      const score = c.score == null || c.score === "" ? NaN : Number(c.score);
      if (!Number.isFinite(score)) continue;
      const side = String(c.homeAway ?? "").toLowerCase();
      if (side === "home") home = score;
      if (side === "away") away = score;
    }
    if (home != null && away != null) return { home, away };
  }
  return null;
}

export const ESPN_MEN_CBB_SUMMARY = (eventId: string) =>
  `https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/summary?event=${encodeURIComponent(eventId)}`;

export const isEspnEventId = (id: string) => /^\d{4,12}$/.test(id);

export type EspnGamePhase = "pre" | "in" | "post";

/** ESPN type.state (pre / in / post), with name fallbacks. Postponed/canceled return null. */
export function phaseFromEspnStatus(status: unknown): EspnGamePhase | null {
  const s = asRecord(status);
  if (!s) return null;
  const t = typeBlob(s);
  const name = String(t.name ?? "");
  const blob = text(t.name, t.description, t.detail, t.shortDetail).toLowerCase();
  if (/STATUS_POSTPONED|STATUS_CANCELED|STATUS_CANCELLED/i.test(name) || /\b(postponed|canceled|cancelled)\b/.test(blob)) {
    return null;
  }
  const state = String(t.state ?? "").toLowerCase();
  if (state === "pre") return "pre";
  if (state === "in") return "in";
  if (state === "post" || t.completed === true || name === "STATUS_FINAL" || /\bfinal\b/.test(blob)) return "post";
  if (/STATUS_SCHEDULED|STATUS_PRE/i.test(name)) return "pre";
  if (/STATUS_IN_PROGRESS|STATUS_HALFTIME|STATUS_END_PERIOD/i.test(name)) return "in";
  return null;
}

export type LiveFeedSnapshot = {
  phase: EspnGamePhase;
  clock: EspnLiveClock | null;
  scores: { home: number; away: number } | null;
};

export function feedFromEspnStatus(
  status: unknown,
  scores: { home: number; away: number } | null,
): LiveFeedSnapshot | null {
  const phase = phaseFromEspnStatus(status);
  if (!phase) return null;
  return { phase, clock: clockFromEspnStatus(status), scores };
}

export function feedFromEspnPayload(payload: unknown): LiveFeedSnapshot | null {
  return feedFromEspnStatus(statusFromEspnPayload(payload), scoresFromEspnPayload(payload));
}

export type LiveLineUiState = {
  period: Exclude<LivePeriod, "TIP">;
  minutesLeft: number;
  home: number;
  away: number;
  fromFeed: boolean;
};

/** Apply an ESPN snapshot to the panel. pre keeps the current defaults; in prefills clock and ESPN scores;
 *  post is Final with ESPN scores (wins over a stale DB in_progress). Dirty state is left alone. */
export function applyEspnFeed(current: LiveLineUiState, feed: LiveFeedSnapshot, dirty: boolean): LiveLineUiState {
  if (dirty) return current;
  if (feed.phase === "pre") return { ...current, fromFeed: false };
  if (feed.phase === "post") {
    return {
      period: "FINAL",
      minutesLeft: 0,
      home: feed.scores != null ? Math.max(0, Math.round(feed.scores.home)) : current.home,
      away: feed.scores != null ? Math.max(0, Math.round(feed.scores.away)) : current.away,
      fromFeed: true,
    };
  }
  if (!feed.clock) return current;
  return {
    period: feed.clock.period,
    minutesLeft: feed.clock.minutesLeft,
    home: feed.scores != null ? Math.max(0, Math.round(feed.scores.home)) : current.home,
    away: feed.scores != null ? Math.max(0, Math.round(feed.scores.away)) : current.away,
    fromFeed: true,
  };
}

/** How early to start asking ESPN, relative to tipoff. */
export const LIVE_FEED_LEAD_MS = 5 * 60 * 1000;

export function isClosedDbStatus(status: string | null | undefined): boolean {
  const s = (status ?? "").toLowerCase().trim();
  return s === "final" || s === "completed" || s === "postponed" || s === "canceled" || s === "cancelled";
}

/** Fetch ESPN when we have an event id, the DB row is not closed, and we are within 5 minutes of tip (or past it).
 *  Do not wait for games.status === "in_progress" — that column often stays "scheduled" mid-game. */
export function shouldFetchLiveFeed(input: {
  externalId?: string | null;
  status?: string | null;
  tipoff?: string | null;
  now?: number | Date;
}): boolean {
  if (!input.externalId || !isEspnEventId(String(input.externalId))) return false;
  if (isClosedDbStatus(input.status)) return false;
  const now = input.now instanceof Date ? input.now.getTime() : (input.now ?? Date.now());
  if (input.tipoff == null || String(input.tipoff).trim() === "") return true;
  const tip = Date.parse(String(input.tipoff));
  if (!Number.isFinite(tip)) return true;
  return now >= tip - LIVE_FEED_LEAD_MS;
}
