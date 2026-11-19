// One place that decides how a game is titled. Neutral-site games read "Away vs Home" (no "@", since nobody
// is on the road) and carry a small "Neutral" chip. A missing or null flag means a normal home game.
export type GameTitle = {
  away: string;
  home: string;
  /** "@" for a home game, "vs" for a neutral site */
  sep: "@" | "vs";
  neutral: boolean;
  /** "Away @ Home" or "Away vs Home" */
  text: string;
};

export const NEUTRAL_CHIP_LABEL = "Neutral";

export function isNeutral(neutral: boolean | null | undefined): boolean {
  return neutral === true;
}

export function gameTitle(away: string, home: string, neutral?: boolean | null): GameTitle {
  const n = isNeutral(neutral);
  const sep = n ? "vs" : "@";
  return { away, home, sep, neutral: n, text: `${away} ${sep} ${home}` };
}

/** Retitle a stored "Away @ Home" string (slate_games.game) when the game is neutral. Anything else is returned as is. */
export function retitle(game: string, neutral?: boolean | null): string {
  if (!isNeutral(neutral)) return game;
  const i = game.indexOf(" @ ");
  return i < 0 ? game : `${game.slice(0, i)} vs ${game.slice(i + 3)}`;
}

/** Row label for a team line on a card. A neutral game has no home team to call "Home". */
export function sideLabel(side: "Away" | "Home", neutral?: boolean | null): string {
  return isNeutral(neutral) ? "Team" : side;
}

/** Graded ATS side ("home" / "away" / "push"). On a neutral site there is no home team, so name the team instead. */
export function atsSideLabel(side: string, away: string, home: string, neutral?: boolean | null): string {
  if (!isNeutral(neutral)) return side;
  if (side === "home") return home;
  if (side === "away") return away;
  return side;
}
