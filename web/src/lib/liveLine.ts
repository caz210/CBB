/** CZarp Live Line: live spread, total, and win % from stored pregame numbers plus a score and time.
 *  The UI computes no new pregame projections — it only applies this remaining-share split to
 *  stored czarp_total / proj_home / proj_away (or proj_spread as a fallback). No book line, no
 *  Edge %, no in-game pace blend, and nothing is written to the official record. */

export const SHARE_1H = 0.475;
export const SHARE_2H = 0.525;
/** Margin SD for a full game, in points. Win % uses 12.5 * sqrt(time_left / 40). */
export const MARGIN_SD = 12.5;
export const REG_MIN = 40;
export const HALF_MIN = 20;
export const OT_MIN = 5;

/** Periods the util accepts. HALFTIME is 2H with 20 minutes left; TIP is 1H with 20 minutes left. */
export type LivePeriod = "1H" | "2H" | "OT" | "HALFTIME" | "TIP";

export type LiveClock = { period: Exclude<LivePeriod, "HALFTIME" | "TIP">; minutesLeft: number };

const finite = (n: unknown): n is number => typeof n === "number" && Number.isFinite(n);

export function clampMinutes(period: LivePeriod, minutesLeft: number): number {
  if (!finite(minutesLeft)) return period === "OT" ? OT_MIN : HALF_MIN;
  const max = period === "OT" ? OT_MIN : HALF_MIN;
  return Math.min(max, Math.max(0, minutesLeft));
}

/** Resolve HALFTIME / TIP aliases and clamp minutes. The slider hides for HALFTIME; the util still accepts it. */
export function normalizeClock(period: LivePeriod, minutesLeft: number): LiveClock {
  if (period === "TIP") return { period: "1H", minutesLeft: HALF_MIN };
  if (period === "HALFTIME") return { period: "2H", minutesLeft: HALF_MIN };
  return { period, minutesLeft: clampMinutes(period, minutesLeft) };
}

/** Slider range in whole minutes, or null when the control should be hidden (Halftime / Tip). */
export function sliderBounds(period: LivePeriod): { min: number; max: number } | null {
  if (period === "HALFTIME" || period === "TIP") return null;
  if (period === "OT") return { min: 1, max: OT_MIN };
  return { min: 1, max: HALF_MIN };
}

/** Remaining scoring share of the official (pregame) total. */
export function remainingShare(period: LivePeriod, minutesLeft: number): number {
  const c = normalizeClock(period, minutesLeft);
  if (c.period === "1H") return SHARE_1H * (c.minutesLeft / HALF_MIN) + SHARE_2H;
  if (c.period === "2H") return SHARE_2H * (c.minutesLeft / HALF_MIN);
  return Math.max(0, c.minutesLeft) / REG_MIN;
}

/** Minutes still to be played, used in the win-% SD. OT counts only its remaining minutes. */
export function timeLeftMin(period: LivePeriod, minutesLeft: number): number {
  const c = normalizeClock(period, minutesLeft);
  if (c.period === "1H") return c.minutesLeft + HALF_MIN;
  return c.minutesLeft;
}

/** Nearest half-point (0, 0.5, 1, …). */
export function roundToHalf(n: number): number {
  return Math.round(n * 2) / 2;
}

/** Abramowitz & Stegun erf, good to ~1e-7. */
export function erf(x: number): number {
  const sign = x < 0 ? -1 : 1;
  const a = Math.abs(x);
  const a1 = 0.254829592, a2 = -0.284496736, a3 = 1.421413741, a4 = -1.453152027, a5 = 1.061405429, p = 0.3275911;
  const t = 1 / (1 + p * a);
  return sign * (1 - ((((a5 * t + a4) * t + a3) * t + a2) * t + a1) * t * Math.exp(-a * a));
}

/** Standard normal CDF Φ(x). */
export function phi(x: number): number {
  return 0.5 * (1 + erf(x / Math.SQRT2));
}

export type LiveLineInput = {
  czarpTotal?: number | null;
  projHome?: number | null;
  projAway?: number | null;
  /** Home-line notation (away − home). Used only when a team projection is missing. */
  projSpread?: number | null;
  homeScore: number;
  awayScore: number;
  period: LivePeriod;
  minutesLeft: number;
};

export type LiveLineResult = {
  remShare: number;
  remTotal: number;
  remHome: number;
  remAway: number;
  liveHome: number;
  liveAway: number;
  /** Rounded to 0.5. */
  liveTotal: number;
  /** Home-line notation (live_away − live_home), rounded to 0.5. */
  liveSpread: number;
  /** Unrounded live_home − live_away, used for win %. */
  liveMargin: number;
  homeWinProb: number;
  awayWinProb: number;
  timeLeftMin: number;
};

/** Stored pregame team scores used to split remaining points. Prefer proj_home / proj_away;
 *  if one is missing, reconstruct from czarp_total and the stored proj_spread. */
export function pregameSplit(input: Pick<LiveLineInput, "czarpTotal" | "projHome" | "projAway" | "projSpread">): {
  total: number;
  projHome: number;
  projAway: number;
} | null {
  const ph = finite(input.projHome) ? input.projHome : null;
  const pa = finite(input.projAway) ? input.projAway : null;
  const teamSum = ph != null && pa != null ? ph + pa : null;
  const total = finite(input.czarpTotal) ? input.czarpTotal : teamSum;
  if (total == null || !(total > 0)) return null;
  if (ph != null && pa != null && ph + pa > 0) return { total, projHome: ph, projAway: pa };
  if (finite(input.projSpread)) {
    // proj_spread is home-line: away − home. home + away = total ⇒ home = (total − spread) / 2.
    const home = (total - input.projSpread) / 2;
    const away = (total + input.projSpread) / 2;
    if (home >= 0 && away >= 0) return { total, projHome: home, projAway: away };
  }
  if (ph != null && ph >= 0 && ph <= total) return { total, projHome: ph, projAway: total - ph };
  if (pa != null && pa >= 0 && pa <= total) return { total, projHome: total - pa, projAway: pa };
  return { total, projHome: total / 2, projAway: total / 2 };
}

export function liveLine(input: LiveLineInput): LiveLineResult | null {
  const pre = pregameSplit(input);
  if (!pre) return null;
  const homeScore = finite(input.homeScore) ? Math.max(0, input.homeScore) : 0;
  const awayScore = finite(input.awayScore) ? Math.max(0, input.awayScore) : 0;
  const remShare = remainingShare(input.period, input.minutesLeft);
  const remTotal = pre.total * remShare;
  const teamSum = pre.projHome + pre.projAway;
  const remHome = teamSum > 0 ? remTotal * (pre.projHome / teamSum) : remTotal / 2;
  const remAway = teamSum > 0 ? remTotal * (pre.projAway / teamSum) : remTotal / 2;
  const liveHome = homeScore + remHome;
  const liveAway = awayScore + remAway;
  const liveMargin = liveHome - liveAway;
  const remMin = timeLeftMin(input.period, input.minutesLeft);
  let homeWinProb: number;
  if (remMin <= 0) {
    homeWinProb = liveMargin > 0 ? 1 : liveMargin < 0 ? 0 : 0.5;
  } else {
    const sd = MARGIN_SD * Math.sqrt(remMin / REG_MIN);
    homeWinProb = sd < 1e-12 ? (liveMargin > 0 ? 1 : liveMargin < 0 ? 0 : 0.5) : phi(liveMargin / sd);
  }
  return {
    remShare,
    remTotal,
    remHome,
    remAway,
    liveHome,
    liveAway,
    liveTotal: roundToHalf(liveHome + liveAway),
    liveSpread: roundToHalf(liveAway - liveHome),
    liveMargin,
    homeWinProb,
    awayWinProb: 1 - homeWinProb,
    timeLeftMin: remMin,
  };
}

/** Opening defaults: pregame/scheduled is 1H at 20; a live game starts at 2H at 20 with the stored score. */
export function defaultLiveState(status: string | null | undefined): { period: Exclude<LivePeriod, "TIP">; minutesLeft: number } {
  if (status === "in_progress") return { period: "2H", minutesLeft: HALF_MIN };
  return { period: "1H", minutesLeft: HALF_MIN };
}

export function defaultLiveScores(
  status: string | null | undefined,
  scores: { home_score?: number | null; away_score?: number | null },
): { home: number; away: number } {
  if (status === "in_progress") {
    return {
      home: finite(scores.home_score) ? Math.max(0, Math.round(scores.home_score)) : 0,
      away: finite(scores.away_score) ? Math.max(0, Math.round(scores.away_score)) : 0,
    };
  }
  return { home: 0, away: 0 };
}

export const LIVE_LINE_DISCLAIMER =
  "This is an estimate, not an official CZarp play. It never goes on the official record. Home or neutral is already in the pregame numbers. Second halves typically run a little higher than first halves, and that is built in.";
