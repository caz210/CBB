// Archetype tag names exactly as emitted by CZarp (czarp/teamstats.py tag_teams).
export const STRENGTH_TAGS = [
  "Addicted to 3s", "Don't Blink", "Board Crashers", "Free Throw Merchants",
  "Lock Down", "No Rebounds for You", "Arc Wall", "Turnover Machine",
] as const;
export const WEAKNESS_TAGS = ["Sieve", "Glass Leak", "Open Arc", "Passive D", "Careless"] as const;

export type ArchetypeName = (typeof STRENGTH_TAGS)[number] | (typeof WEAKNESS_TAGS)[number];

export const ARCHETYPE_HINT: Record<ArchetypeName, string> = {
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
  Careless: "Turns it over a lot",
};

export function isWeakness(t: string) {
  return (WEAKNESS_TAGS as readonly string[]).includes(t);
}
