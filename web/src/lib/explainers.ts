/** Short tap-to-learn copy. No promises of winning — buy at value, small samples prove nothing. */
export const EXPLAINERS = {
  edge: {
    title: "Edge",
    body: "The gap between our number and the Vegas price, as a share of the expected total score. Example: we like a team by 4.5 more points than Vegas, and the total is 154.5 → edge ≈ 2.9%. Bigger edge means a better price — not a guaranteed win. One bet, or a few dozen, proves nothing.",
  },
  fairValue: {
    title: "Fair value",
    body: "Our model's number for the spread or total. Think of it as the price we'd pay — if the market is offering something better, that's value.",
  },
  price: {
    title: "Price",
    body: "The market line (Vegas consensus). Same idea as a stock's ask: what you can buy right now.",
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
    body: "Where this team ranks among D1 teams this season, from 0 (worst) to 100 (best). We show percentiles — not KenPom ranks — so you can compare style without obsessing over a leaderboard.",
  },
  totalsModel: {
    title: "Totals models",
    body: "Official totals use czarp-v1-totals. The default model, czarp-v2, is used for sides and as a second look at the total. A small “v1” tag on a card means the fair value shown is from the official totals model, not the default.",
  },
  lean: {
    title: "Lean",
    body: "The edge at today's line clears our bar, but the edge vs the opening line doesn't, so it's not an official play. Bars: 4% sides, 3% totals.",
  },
  openLineRule: {
    title: "Why we use the opening line",
    body: "We only play edges that were there at the open, so we're never chasing line moves. A number that looks good vs the current price can still be low value if it wasn't there when the market opened.",
  },
} as const;

export type ExplainerKey = keyof typeof EXPLAINERS;
