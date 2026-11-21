// Archetype tags from CZarp (czarp/teamstats.py TAG_DEFS).
// New shape: { name, kind: strength|weakness|style, tentative?, extremity? }
// Old shape (still accepted): plain string arrays — kind inferred from known lists.
//
// TAG_DEFS lives in the pipeline at czarp/teamstats.py (not shipped on the
// main web tree). The 16 public names below are a snapshot of that list.
// Soft Glass and Open Arc are hidden signals, not in TAG_DEFS, and must
// never render as chips — filter them with visibleTags().

export type TagKind = "strength" | "weakness" | "style";

export type ArchetypeTag = {
  name: string;
  kind: TagKind;
  tentative?: boolean;
  /** Pipeline tail extremity (0–1) or a comparable strength score, when stored. */
  extremity?: number;
};

/** Current public chip names (CZarp TAG_DEFS). */
export const STRENGTH_TAGS = [
  "Fast", "Sharp", "Volume 3s", "Secure", "Disrupt", "Crash", "Control Glass",
  "At the Line", "Wall", "Crowd 3s",
] as const;
export const WEAKNESS_TAGS = ["Cold", "Careless", "Foul Prone", "Soft D"] as const;
export const STYLE_TAGS = ["Slow", "Paint First"] as const;

/** Snapshot of TAG_DEFS names in czarp/teamstats.py, in definition order. */
export const PUBLIC_TAG_NAMES = [
  "Fast", "Slow", "Sharp", "Cold", "Volume 3s", "Paint First", "Secure", "Careless",
  "Disrupt", "Crash", "Control Glass", "At the Line", "Foul Prone", "Wall", "Soft D",
  "Crowd 3s",
] as const;

export type PublicTagName = (typeof PUBLIC_TAG_NAMES)[number];

/** Hidden matchup/upset signals — never shown as chips. Filter only via visibleTags(). */
export const HIDDEN_TAGS = ["Soft Glass", "Open Arc"] as const;
export type HiddenTagName = (typeof HIDDEN_TAGS)[number];

const HIDDEN_SET = new Set<string>(HIDDEN_TAGS.map((n) => n.toLowerCase()));

/** Legacy names from the previous taxonomy — still accepted if they appear in old rows. */
const LEGACY_WEAKNESS = ["Sieve", "Glass Leak", "Open Arc", "Passive D", "Careless"] as const;

export type ArchetypeName = string;

export const ARCHETYPE_HINT: Record<string, string> = {
  Fast: "Top tempo — plays fast",
  Slow: "Bottom tempo — plays slow",
  Sharp: "Top effective FG%",
  Cold: "Bottom effective FG%",
  "Volume 3s": "High three-point attempt rate",
  "Paint First": "Low three-point attempt rate",
  Secure: "Low turnover rate",
  Careless: "High turnover rate",
  Disrupt: "Forces turnovers",
  Crash: "Elite offensive rebounding",
  "Control Glass": "Limits opponent offensive boards",
  "At the Line": "Gets to the free-throw line",
  "Foul Prone": "Sends opponents to the line",
  Wall: "Limits opponent effective FG%",
  "Soft D": "Allows high effective FG%",
  "Crowd 3s": "Limits opponent three-point volume",
  // legacy
  "Addicted to 3s": "Top-15% three-point attempt rate",
  "Don't Blink": "Fast tempo, short possessions",
  "Board Crashers": "Elite offensive rebounding",
  "Free Throw Merchants": "Gets to the line a lot",
  "Lock Down": "Top-15% adjusted defense",
  "No Rebounds for You": "Limits opponent offensive boards",
  "Arc Wall": "Limits opponent threes",
  "Turnover Machine": "Forces turnovers",
  Sieve: "Bottom-15% adjusted defense",
  "Glass Leak": "Gives up offensive boards",
  "Open Arc": "Allows lots of threes",
  "Passive D": "Rarely forces turnovers",
};

const WEAK_SET = new Set<string>([...WEAKNESS_TAGS, ...LEGACY_WEAKNESS]);
const STYLE_SET = new Set<string>([...STYLE_TAGS]);

export function isWeakness(t: string) {
  return WEAK_SET.has(t);
}
export function isStyle(t: string) {
  return STYLE_SET.has(t);
}
export function isHiddenTag(name: string) {
  return HIDDEN_SET.has(name.trim().toLowerCase());
}

/** The one place hidden signals are dropped from anything customers can see. */
export function visibleTags(tags: ArchetypeTag[]): ArchetypeTag[] {
  return tags.filter((t) => !isHiddenTag(t.name));
}

function inferKind(name: string): TagKind {
  if (WEAK_SET.has(name)) return "weakness";
  if (STYLE_SET.has(name)) return "style";
  return "strength";
}

function finiteNum(v: unknown): number | undefined {
  const n = typeof v === "number" ? v : typeof v === "string" && v.trim() ? Number(v) : NaN;
  return Number.isFinite(n) ? n : undefined;
}

/** Normalize home/away tag lists from either string[] or {name,kind}[]. */
export function parseTagList(v: unknown, sideTentative = false): ArchetypeTag[] {
  if (!Array.isArray(v)) return [];
  const out: ArchetypeTag[] = [];
  for (const item of v) {
    if (typeof item === "string" && item.trim()) {
      out.push({ name: item.trim(), kind: inferKind(item.trim()), tentative: sideTentative || undefined });
      continue;
    }
    if (item && typeof item === "object") {
      const o = item as Record<string, unknown>;
      const name = typeof o.name === "string" ? o.name.trim() : "";
      if (!name) continue;
      const raw = typeof o.kind === "string" ? o.kind.toLowerCase() : "";
      const kind: TagKind = raw === "weakness" || raw === "style" || raw === "strength" ? raw : inferKind(name);
      const tentative = o.tentative === true || sideTentative || undefined;
      const extremity = finiteNum(o.extremity) ?? finiteNum(o.strength);
      out.push({ name, kind, tentative: tentative || undefined, extremity });
    }
  }
  return out;
}

/** How many tags a 320px slate card shows before the +N overflow chip. */
export const SLATE_COMPACT_TAG_CAP = 2;

/**
 * Rank a fired tag for the compact slate card.
 *
 * 1) A stored `extremity` (or `strength`) score, when present — this is the
 *    pipeline's tail extremity in czarp/teamstats.py (how far past the 15th /
 *    85th percentile the stat sits).
 * 2) Otherwise, membership and order in the stored top-2 `tags` list, which
 *    the pipeline already ranked by that same extremity.
 * 3) Otherwise 0, with a name tie-break.
 */
export function tagExtremity(tag: ArchetypeTag, topNames: readonly string[] = []): number {
  if (typeof tag.extremity === "number" && Number.isFinite(tag.extremity)) return tag.extremity;
  const i = topNames.indexOf(tag.name);
  if (i >= 0) return 1 + (topNames.length - i) / (topNames.length + 1);
  return 0;
}

export function mostExtremeTags(tags: ArchetypeTag[], n = SLATE_COMPACT_TAG_CAP, top: ArchetypeTag[] = []): ArchetypeTag[] {
  const vis = visibleTags(tags);
  const topNames = visibleTags(top).map((t) => t.name);
  return [...vis].sort((a, b) => {
    const d = tagExtremity(b, topNames) - tagExtremity(a, topNames);
    return d !== 0 ? d : a.name.localeCompare(b.name);
  }).slice(0, n);
}

export type CompactSlateTags = { shown: ArchetypeTag[]; more: number };

/** Tags to render on a tight (320px) slate card: 2 most extreme, plus a leftover count. */
export function compactSlateTags(tags: ArchetypeTag[], top: ArchetypeTag[] = [], n = SLATE_COMPACT_TAG_CAP): CompactSlateTags {
  const vis = visibleTags(tags);
  const shown = mostExtremeTags(vis, n, top);
  const keep = new Set(shown.map((t) => t.name));
  return { shown, more: vis.filter((t) => !keep.has(t.name)).length };
}


const HIDDEN_MENTION = new RegExp(`\\b(?:${HIDDEN_TAGS.map((n) => n.replace(/ /g, "\\s+")).join("|")})\\b`, "i");

/** Drop clauses that name a hidden signal so they never appear in notes or reason copy. */
export function visibleCopy(text: string | null | undefined): string {
  if (!text) return "";
  const kept = text.split(/\s*[;·]\s*/).filter((c) => c.trim() && !HIDDEN_MENTION.test(c));
  return kept.join(" · ").replace(/\s{2,}/g, " ").trim();
}

export function visibleNotes(notes: string[]): string[] {
  return notes.map(visibleCopy).filter(Boolean);
}

/** @deprecated Slate cards now show every kind of tag. Prefer visibleTags / compactSlateTags. */
export function weaknessesOnly(tags: ArchetypeTag[], n = 2): ArchetypeTag[] {
  return visibleTags(tags).filter((t) => t.kind === "weakness").slice(0, n);
}
