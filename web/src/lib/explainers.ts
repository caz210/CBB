/** Short tap-to-learn copy. No promises of winning — buy at value, judge over hundreds of bets. */
export const EXPLAINERS = {
  edge: {
    title: "Edge",
    body: "How far the market price is from our fair value, as a percent of the line. Bigger edge means a better price — not a guaranteed win. Judge it over hundreds of bets.",
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
    body: "Even a +EV book has losing nights and drawdowns. Swings are normal; the portfolio is judged over hundreds of bets, not one card.",
  },
  clv: {
    title: "Closing line value (CLV)",
    body: "Did you beat the closing price? Positive CLV means you bought cheaper than the market settled. It's the cleanest long-run signal that you're getting value.",
  },
} as const;

export type ExplainerKey = keyof typeof EXPLAINERS;
