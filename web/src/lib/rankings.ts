/** CZarp weekly rankings (Top 25 + full poll). Pure helpers live here so tests
 *  don't need a Supabase client. The public record is the D-I vs D-I W-L only. */

export const TOP_N = 25;

/** Columns read from ranking_snapshots. Keep this list tight — never request unused W-L fields. */
export const RANKING_SNAPSHOT_COLUMNS =
  "team_id,rank,prev_rank,rating,di_wins,di_losses,as_of,week_of,season";

export type MovementKind = "up" | "down" | "same" | "new" | "none";

export type Movement = {
  kind: MovementKind;
  /** Spots moved when kind is up or down; 0 when same. */
  delta: number;
};

export type RankingRow = {
  team_id: number;
  team: string;
  rank: number;
  /** Rank in the previous official week, when known. */
  prev_rank: number | null;
  rating: number;
  di_wins: number | null;
  di_losses: number | null;
  movement: Movement;
};

export type RankingsData = {
  week_of: string;
  as_of: string;
  season: number | null;
  rows: RankingRow[];
};

export type PrevRank = { team_id: number; rank: number };

const isFiniteNum = (n: unknown): n is number => typeof n === "number" && Number.isFinite(n);

/** One decimal, half away from zero, so negatives don't use JavaScript's half-toward-+inf. */
export function round1(n: number): number {
  if (!Number.isFinite(n)) return n;
  const s = n < 0 ? -1 : 1;
  return (s * Math.round(Math.abs(n) * 10)) / 10;
}

/** Projected margin vs an average D-I team, with a sign and one decimal, e.g. +28.8. */
export function fmtRating(n: number | null | undefined): string {
  if (!isFiniteNum(n)) return "—";
  const r = round1(n);
  if (Object.is(r, -0) || r === 0) return "0.0";
  return `${r > 0 ? "+" : "−"}${Math.abs(r).toFixed(1)}`;
}

/** D-I vs D-I record. Missing either side is unknown, not 0-0. */
export function fmtRecord(wins: number | null | undefined, losses: number | null | undefined): string {
  if (!isFiniteNum(wins) || !isFiniteNum(losses) || wins < 0 || losses < 0) return "—";
  return `${wins}–${losses}`;
}

/** Compare this week's rank to a previous official-week rank. */
export function rankMovement(rank: number, prevRank: number | null | undefined): Movement {
  if (!isFiniteNum(rank) || rank < 1) return { kind: "none", delta: 0 };
  if (!isFiniteNum(prevRank) || prevRank < 1) return { kind: "new", delta: 0 };
  const delta = prevRank - rank;
  if (delta > 0) return { kind: "up", delta };
  if (delta < 0) return { kind: "down", delta: -delta };
  return { kind: "same", delta: 0 };
}

/**
 * Attach movement vs a previous official week.
 * - `previous` is an array (possibly empty) when we know whether a prior poll exists.
 *   An empty list means there is no prior poll: every team gets a dash (`none`), never "New"
 *   or a fabricated arrow.
 * - `previous` is null/undefined when the prior poll could not be loaded. Fall back to each
 *   row's stored `prev_rank` when any is present; otherwise dash.
 */
export function withMovement<T extends { team_id: number; rank: number; prev_rank?: number | null }>(
  current: T[],
  previous: PrevRank[] | null | undefined,
): (T & { prev_rank: number | null; movement: Movement })[] {
  if (previous == null) {
    const anyStored = current.some((r) => isFiniteNum(r.prev_rank) && (r.prev_rank as number) >= 1);
    return current.map((r) => {
      const prev = isFiniteNum(r.prev_rank) && r.prev_rank >= 1 ? r.prev_rank : null;
      return {
        ...r,
        prev_rank: prev,
        movement: anyStored ? rankMovement(r.rank, prev) : { kind: "none" as const, delta: 0 },
      };
    });
  }
  if (previous.length === 0) {
    return current.map((r) => ({ ...r, prev_rank: null, movement: { kind: "none" as const, delta: 0 } }));
  }
  const map = new Map<number, number>();
  for (const p of previous) {
    if (isFiniteNum(p.team_id) && isFiniteNum(p.rank) && p.rank >= 1) map.set(p.team_id, p.rank);
  }
  return current.map((r) => {
    const prev = map.get(r.team_id) ?? null;
    return { ...r, prev_rank: prev, movement: rankMovement(r.rank, prev) };
  });
}

/** Rank ascending, then team name. Does not mutate the input. */
export function sortRankings<T extends { rank: number; team: string }>(rows: T[]): T[] {
  return rows.slice().sort((a, b) => a.rank - b.rank || a.team.localeCompare(b.team));
}

/** Calendar date (YYYY-MM-DD) as "Nov 21" or "Nov 16, 2026". Invalid → null. */
export function fmtPollDate(d: string | null | undefined, opts: { year?: boolean } = {}): string | null {
  if (!d || !/^\d{4}-\d{2}-\d{2}$/.test(d)) return null;
  const t = new Date(d + "T00:00:00Z");
  if (Number.isNaN(t.getTime())) return null;
  return new Intl.DateTimeFormat("en-US", {
    timeZone: "UTC",
    month: "short",
    day: "numeric",
    ...(opts.year ? { year: "numeric" as const } : {}),
  }).format(t);
}

export function weekLabel(weekOf: string): string {
  const d = fmtPollDate(weekOf, { year: true });
  return d ? `Week of ${d}` : "Latest poll";
}

export function updatedLabel(asOf: string): string | null {
  const d = fmtPollDate(asOf);
  return d ? `Updated ${d} CT` : null;
}

/** Early-season copy through January, when the poll still moves a lot. */
export function isEarlySeason(weekOf: string): boolean {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(weekOf)) return false;
  const m = Number(weekOf.slice(5, 7));
  return m === 11 || m === 12 || m === 1;
}

export function movementLabel(m: Movement): string {
  switch (m.kind) {
    case "up":
      return `Up ${m.delta}`;
    case "down":
      return `Down ${m.delta}`;
    case "same":
      return "No change";
    case "new":
      return "New";
    default:
      return "No previous poll";
  }
}

/** Visible mark. '=' means no change; '—' is reserved for no previous poll. */
export function movementMark(m: Movement): string {
  switch (m.kind) {
    case "up":
      return `▲${m.delta}`;
    case "down":
      return `▼${m.delta}`;
    case "same":
      return "=";
    case "new":
      return "New";
    default:
      return "—";
  }
}

export type RankingsErrorReason = "unreachable" | "unavailable";

/** Map a loader failure to a customer-safe reason. Never put codes or schema text in the UI. */
export function rankingsErrorKind(error: { message?: string; code?: string } | null | undefined): RankingsErrorReason {
  const code = String(error?.code ?? "").trim();
  const msg = String(error?.message ?? "").toLowerCase();
  if (/fetch failed|failed to fetch|networkerror|network request|econnrefused|econnreset|enotfound|etimedout|timeout|aborted|abort/.test(msg)) {
    return "unreachable";
  }
  if (/^(ECONNREFUSED|ECONNRESET|ENOTFOUND|ETIMEDOUT|ABORT_ERR|UND_ERR)$/i.test(code)) {
    return "unreachable";
  }
  if (code || msg) return "unavailable";
  return "unreachable";
}

export function rankingsErrorCopy(reason: RankingsErrorReason = "unreachable"): { title: string; body: string } {
  if (reason === "unavailable") {
    return {
      title: "Rankings are temporarily unavailable",
      body: "Please refresh in a minute.",
    };
  }
  return {
    title: "Couldn't load the rankings",
    body: "The request didn't get a response. Please refresh in a minute.",
  };
}

/** Fixture used only when RANKINGS_QA=1 for screenshots of movement arrows. */
export function movementDemoRows(): RankingRow[] {
  const base: Array<{ team_id: number; team: string; rank: number; prev: number | null; rating: number; w: number; l: number }> = [
    { team_id: 1, team: "Purdue", rank: 1, prev: 3, rating: 28.8, w: 4, l: 0 },
    { team_id: 2, team: "Duke", rank: 2, prev: 1, rating: 27.4, w: 5, l: 0 },
    { team_id: 3, team: "Houston", rank: 3, prev: 3, rating: 25.1, w: 4, l: 0 },
    { team_id: 4, team: "UConn", rank: 4, prev: null, rating: 23.6, w: 4, l: 1 },
    { team_id: 5, team: "Florida", rank: 5, prev: 8, rating: 22.2, w: 3, l: 1 },
  ];
  return withMovement(
    base.map((x) => ({
      team_id: x.team_id,
      team: x.team,
      rank: x.rank,
      prev_rank: x.prev,
      rating: x.rating,
      di_wins: x.w,
      di_losses: x.l,
      movement: { kind: "none" as const, delta: 0 },
    })),
    base
      .filter((x) => x.prev != null)
      .map((x) => ({ team_id: x.team_id, rank: x.prev as number })),
  );
}

/** QA fixture: long names in the Top 25 so truncate can be checked at 320px. */
export function longNameDemoRows(): RankingRow[] {
  const names = [
    "Texas A&M-Corpus Christi",
    "Cal State Northridge",
    "Southern Illinois Edwardsville",
    "Maryland Eastern Shore",
    "St. Thomas Aquinas Saints",
  ];
  const current = names.map((team, i) => ({
    team_id: i + 1,
    team,
    rank: i + 1,
    prev_rank: null as number | null,
    rating: 20 - i,
    di_wins: 4,
    di_losses: 1,
  }));
  return withMovement(current, []);
}
