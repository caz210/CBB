// TV Rating: how watchable a game looks, 0–100 (slate_games.tv_rating).
// Null means unknown, not 0 — no chip, and the game sorts after every rated one.

export type SlateSort = "time" | "value" | "tv";

export function parseSlateSort(v: string | null | undefined): SlateSort {
  return v === "value" || v === "tv" ? v : "time";
}

/** Display the rating as an integer (77.3 → 77, 67.6 → 68). Null/non-finite → no chip. */
export function fmtTvRating(x: number | null | undefined): number | null {
  if (x == null || !Number.isFinite(Number(x))) return null;
  return Math.round(Number(x));
}

export type TvSortable = { game_id: number; tv_rating?: number | null; tipoff?: string | null };

function tipMs(g: TvSortable): number {
  return g.tipoff ? Date.parse(g.tipoff) : Infinity;
}

function byTime(a: TvSortable, z: TvSortable): number {
  return tipMs(a) - tipMs(z) || a.game_id - z.game_id;
}

function isRated(x: number | null | undefined): x is number {
  return x != null && Number.isFinite(x);
}

/** Highest first, nulls last, then earlier tip, then game_id. */
export function compareTvRating(a: TvSortable, z: TvSortable): number {
  const ar = isRated(a.tv_rating), zr = isRated(z.tv_rating);
  if (ar && zr && a.tv_rating !== z.tv_rating) return (z.tv_rating as number) - (a.tv_rating as number);
  if (ar !== zr) return ar ? -1 : 1;
  return byTime(a, z);
}

/**
 * Highlight rule: a game is highlighted when its rating is >= 50, OR it is in the
 * day's top 5 by rating (nulls ignored). That's the union, so we always show at
 * least the top 5 when five or more ratings exist, and every 50+ game even when
 * that's more than five. Ties at the 5th-place rating are included, so a sixth
 * game with the same rating isn't dropped at random.
 */
export const TV_HIGHLIGHT_MIN = 50;
export const TV_HIGHLIGHT_TOP_N = 5;

export function tvHighlightIds(games: TvSortable[]): Set<number> {
  const rated = games.filter((g) => isRated(g.tv_rating)).sort(compareTvRating);
  const out = new Set<number>();
  for (const g of rated) {
    if ((g.tv_rating as number) >= TV_HIGHLIGHT_MIN) out.add(g.game_id);
  }
  const nth = rated[Math.min(TV_HIGHLIGHT_TOP_N, rated.length) - 1];
  if (nth && isRated(nth.tv_rating)) {
    const cutoff = nth.tv_rating;
    for (const g of rated) {
      if ((g.tv_rating as number) >= cutoff) out.add(g.game_id);
    }
  }
  return out;
}
