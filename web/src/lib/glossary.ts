/** Customer-facing definitions for verdicts, game chips and style tags that are live on the site.
 *
 *  Verdict entries are built from VERDICT_LADDER (lib/verdicts.ts), the single source for
 *  their labels and copy; nothing is restated here.
 *
 *  Style-tag wording is adapted from ARCHETYPE_HINT until CZarp sends final copy.
 *  Soft Glass and Open Arc are hidden signals and are not listed here.
 */
import { VERDICT_LADDER, type VerdictLevel } from "./verdicts";

export type GlossaryGroup = "verdict" | "chip" | "style";

export type GlossaryEntry = {
  /** Stable id used by InfoTip and tests (kebab-case, or the style-tag name). */
  id: string;
  title: string;
  body: string;
  group: GlossaryGroup;
};

export const GLOSSARY_ANCHOR = "glossary";
export const GLOSSARY_HREF = `/methodology#${GLOSSARY_ANCHOR}`;

const VERDICT_ID: Record<VerdictLevel, string> = { toss_up: "toss-up", lean: "lean", favor: "favor", official: "official-play" };

/** Verdict ladder, weakest to strongest, straight from VERDICT_LADDER. */
const VERDICT_ENTRIES: readonly GlossaryEntry[] = VERDICT_LADDER.map((d) => ({
  id: VERDICT_ID[d.level],
  title: d.label,
  body: d.short,
  group: "verdict" as const,
}));

export const GLOSSARY_ENTRIES: readonly GlossaryEntry[] = [
  ...VERDICT_ENTRIES,

  // --- Game chips ---
  {
    id: "neutral",
    title: "Neutral",
    body: "This game is at a neutral site, so neither team gets a home-court edge in CZarp's number. The matchup is written as “vs,” not “@.”",
    group: "chip",
  },
  {
    id: "tv-rating",
    title: "TV Rating",
    body: "How watchable the game looks, from 0 to 100: higher when both teams are good, CZarp expects a close game, and the pace is quick. Not a pick.",
    group: "chip",
  },
  {
    id: "upset-alert",
    title: "Upset alert",
    body: "CZarp flagged a style matchup that could help the underdog. It is a watchability note, not a pick and not a prediction that the favorite will lose.",
    group: "chip",
  },
  {
    id: "new-coach-pause",
    title: "New-coach pause",
    body: "Sides are paused until a new coach has 8 games; totals are paused until 4. The chip names the team, or reads “Both teams paused” when each side has a hold.",
    group: "chip",
  },
  {
    id: "tentative",
    title: "Tentative",
    body: "This team's style tags are early-season readings: there haven't been enough games yet for a full sample. A dashed chip and a “· tent.” marker call that out.",
    group: "chip",
  },

  // --- 16 public style tags (TAG_DEFS in czarp/teamstats.py) ---
  {
    id: "Fast",
    title: "Fast",
    body: "This team plays at a high tempo — more possessions than most D-I teams.",
    group: "style",
  },
  {
    id: "Slow",
    title: "Slow",
    body: "This team plays at a low tempo — fewer possessions than most D-I teams.",
    group: "style",
  },
  {
    id: "Sharp",
    title: "Sharp",
    body: "This team shoots a high effective field-goal percentage.",
    group: "style",
  },
  {
    id: "Cold",
    title: "Cold",
    body: "This team shoots a low effective field-goal percentage.",
    group: "style",
  },
  {
    id: "Volume 3s",
    title: "Volume 3s",
    body: "This team takes a high share of its shots from three.",
    group: "style",
  },
  {
    id: "Paint First",
    title: "Paint First",
    body: "This team takes a low share of its shots from three and works more inside.",
    group: "style",
  },
  {
    id: "Secure",
    title: "Secure",
    body: "This team takes care of the ball — a low turnover rate.",
    group: "style",
  },
  {
    id: "Careless",
    title: "Careless",
    body: "This team turns the ball over at a high rate.",
    group: "style",
  },
  {
    id: "Disrupt",
    title: "Disrupt",
    body: "This team's defense forces turnovers at a high rate.",
    group: "style",
  },
  {
    id: "Crash",
    title: "Crash",
    body: "This team is an elite offensive rebounding team.",
    group: "style",
  },
  {
    id: "Control Glass",
    title: "Control Glass",
    body: "This team limits opponents' offensive rebounds.",
    group: "style",
  },
  {
    id: "At the Line",
    title: "At the Line",
    body: "This team gets to the free-throw line a lot.",
    group: "style",
  },
  {
    id: "Foul Prone",
    title: "Foul Prone",
    body: "This team's defense sends opponents to the free-throw line often.",
    group: "style",
  },
  {
    id: "Wall",
    title: "Wall",
    body: "This team's defense holds opponents to a low effective field-goal percentage.",
    group: "style",
  },
  {
    id: "Soft D",
    title: "Soft D",
    body: "This team's defense allows a high effective field-goal percentage.",
    group: "style",
  },
  {
    id: "Crowd 3s",
    title: "Crowd 3s",
    body: "This team's defense limits how often opponents shoot threes.",
    group: "style",
  },
];

export type GlossaryId = (typeof GLOSSARY_ENTRIES)[number]["id"];

const BY_ID = new Map<string, GlossaryEntry>(GLOSSARY_ENTRIES.map((e) => [e.id, e]));
const BY_TITLE = new Map<string, GlossaryEntry>(GLOSSARY_ENTRIES.map((e) => [e.title.toLowerCase(), e]));

/** Look up a glossary entry by id or visible title (e.g. "TV Rating"). */
export function glossaryEntry(idOrTitle: string): GlossaryEntry | undefined {
  return BY_ID.get(idOrTitle) ?? BY_TITLE.get(idOrTitle.toLowerCase());
}

export function glossaryEntriesIn(group: GlossaryGroup): GlossaryEntry[] {
  return GLOSSARY_ENTRIES.filter((e) => e.group === group);
}

/** Every public style-tag name must have a glossary entry of the same id. */
export function styleGlossaryIds(): string[] {
  return glossaryEntriesIn("style").map((e) => e.id);
}
