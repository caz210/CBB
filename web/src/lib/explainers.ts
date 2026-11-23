/** Short tap-to-learn copy. No promises of winning — an edge is not a guarantee, and small samples prove nothing. */
import { OFFICIAL_POST_TIME } from "./officialPost";
import { NO_LINE_TIP, verdictDef } from "./verdicts";

/** Verdict ladder tips come from the shared ladder definitions (lib/verdicts.ts). */
const ladderTip = (level: Parameters<typeof verdictDef>[0]) => ({ title: verdictDef(level).label, body: verdictDef(level).short });

export const EXPLAINERS = {
  tossUp: ladderTip("toss_up"),
  lean: ladderTip("lean"),
  value: ladderTip("value"),
  lateValue: ladderTip("late_value"),
  noLine: NO_LINE_TIP,
  edge: {
    title: "Edge",
    body: "The gap between our number and the market price, as a share of the expected total score. Example: we like a team by 4.5 more points than the market, and the total is 154.5 → edge ≈ 2.9%. Bigger edge means a better price — not a guaranteed win. One bet, or a few dozen, proves nothing.",
  },
  fairValue: {
    title: "Fair value",
    body: "Our model's number for the spread or total. Think of it as the price we'd pay — if the market is offering something better, that's value.",
  },
  price: {
    title: "Price",
    body: "The market line (consensus price) — what you can take right now.",
  },
  variance: {
    title: "Variance",
    body: "Even a +EV book has losing nights and drawdowns. Swings are normal; one card proves nothing, and an early sample proves little.",
  },
  clv: {
    title: "Closing line value (CLV)",
    body: "Did you beat the closing price? Positive CLV means you bought cheaper than the market settled. It's the cleanest long-run signal that you're getting value.",
  },
  percentile: {
    title: "Percentile",
    body: "Percentiles vs all D-I teams, from 0 (worst) to 100 (best). Use them to compare style, not as a leaderboard.",
  },
  /** Read by the slate chips and the methodology glossary; derived from lib/verdicts.ts (one source). */
  czarpPlay: ladderTip("official"),
  ladderRule: {
    title: "How each market is rated",
    body: `The spread and the total each get their own rating, judged on the edge vs the current line: Toss Up, Lean, Value, or Late Value as the edge grows (sides 2%, 3%, 4%; totals 1.5%, 2.25%, 3%). CZarp Plays are set once, at the ${OFFICIAL_POST_TIME} run, and are never added later.`,
  },
  tvRating: {
    title: "TV Rating",
    body: "How watchable the game looks, 0–100: higher when both teams are good, CZarp expects a close game, and the pace is quick. Not a pick.",
  },
  rating: {
    title: "Rating",
    body: "Projected margin vs an average D-I team on a neutral court.",
  },
  teamRating: {
    title: "Rating",
    body: "Projected margin vs an average D-I team on a neutral floor.",
  },
  diRecord: {
    title: "D-I record",
    body: "Records count only games between two D-I teams.",
  },
} as const;

export type ExplainerKey = keyof typeof EXPLAINERS;
