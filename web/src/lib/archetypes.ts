// Archetype tags from CZarp (czarp/teamstats.py TAG_DEFS).
// New shape: { name, kind: strength|weakness|style, tentative? }
// Old shape (still accepted): plain string arrays — kind inferred from known lists.

export type TagKind = "strength" | "weakness" | "style";

export type ArchetypeTag = {
  name: string;
  kind: TagKind;
  tentative?: boolean;
};

/** Current public chip names (CZarp TAG_DEFS). */
export const STRENGTH_TAGS = [
  "Fast", "Sharp", "Volume 3s", "Secure", "Disrupt", "Crash", "Control Glass",
  "At the Line", "Wall", "Crowd 3s",
] as const;
export const WEAKNESS_TAGS = ["Cold", "Careless", "Foul Prone", "Soft D"] as const;
export const STYLE_TAGS = ["Slow", "Paint First"] as const;

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

function inferKind(name: string): TagKind {
  if (WEAK_SET.has(name)) return "weakness";
  if (STYLE_SET.has(name)) return "style";
  return "strength";
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
      out.push({ name, kind, tentative: tentative || undefined });
    }
  }
  return out;
}

/** Weakness chips for slate cards — max `n` per team. */
export function weaknessesOnly(tags: ArchetypeTag[], n = 2): ArchetypeTag[] {
  return tags.filter((t) => t.kind === "weakness").slice(0, n);
}
