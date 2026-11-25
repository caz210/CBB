/** Methodology page sections. Placeholder boxes and the Official-vs-tracked
 *  copy are gated so flipping the flags restores them. No data or queries here. */
import { SHOW_TRACKED_TIER } from "./pickTier";

export const SHOW_METHODOLOGY_PLACEHOLDERS = false;

export type MethodologySection = {
  title: string;
  body: string;
  placeholder?: boolean;
  /** Hidden while SHOW_TRACKED_TIER is false. */
  tracked?: boolean;
};

export const METHODOLOGY_SECTIONS: MethodologySection[] = [
  {
    title: "The models",
    placeholder: true,
    body: "CZarp projects a spread and a total for every game. Describe features, inputs, and what is not used (third-party ranking lists are never shown).",
  },
  {
    title: "Fair value, price, and edge",
    placeholder: true,
    body: "Edge is the gap between our number and the market price, as a share of the expected total score: |fair − price| ÷ market total, for both sides and totals. Play bar: sides 4%, totals 3%. Each game gets two ratings, one for the spread and one for the total, judged on the edge vs the current line: Toss Up (under 2% sides / 1.5% totals), Small edge (2–3% / 1.5–2.25%), Value (3–4% / 2.25–3%, or past the bar with a hold rule), Late Value (past the bar with no hold, after the morning run; not part of the official record), and CZarp Play (posted at the morning run).",
  },
  {
    title: "Official vs tracked",
    placeholder: true,
    tracked: true,
    body: "Official picks are the ones CZarp actually stakes (¼-Kelly units) and go on the public record. Tracked picks are flat 1u, tracked for testing, not part of the official record.",
  },
  {
    title: "Stake sizing",
    placeholder: true,
    body: "Stakes are quarter-Kelly in units (1u = 1% of bankroll, capped at 2.5u). Describe any bankroll or cap rules here.",
  },
  {
    title: "Grading and closing line value",
    placeholder: true,
    body: "Picks are time-stamped once posted and can't be edited. They are graded from final scores. CLV compares the posted line to the close.",
  },
  {
    title: "New coaches and early season",
    placeholder: true,
    body: "Sides are paused until a new coach has 8 games; totals are paused until 4. Early-season tags can be tentative.",
  },
  {
    title: "Limitations",
    placeholder: true,
    body: "A plus-EV process still has losing nights and drawdowns. Edges are not a promise of winning. The record is an early sample; read it as evidence, not proof.",
  },
];

/** Drop placeholder sections and tracked-only copy according to the flags, so headings are not left orphaned. */
export function visibleMethodologySections(
  showPlaceholders: boolean = SHOW_METHODOLOGY_PLACEHOLDERS,
  showTracked: boolean = SHOW_TRACKED_TIER,
): MethodologySection[] {
  return METHODOLOGY_SECTIONS.filter((s) => {
    if (s.tracked && !showTracked) return false;
    if (s.placeholder && !showPlaceholders) return false;
    return true;
  });
}
