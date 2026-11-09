import { ARCHETYPE_HINT, type ArchetypeTag, type TagKind } from "@/lib/archetypes";

const KIND_CLASS: Record<TagKind, string> = {
  strength: "border-sky-400/40 bg-sky-400/10 text-sky-300",
  weakness: "border-rose-400/40 bg-rose-400/10 text-rose-300",
  style: "border-violet-400/40 bg-violet-400/10 text-violet-300",
};

export function ArchetypeChip({
  name,
  kind,
  tentative,
  tag,
}: {
  name?: string;
  kind?: TagKind;
  tentative?: boolean;
  /** Prefer passing a full tag object (new shape). */
  tag?: ArchetypeTag;
}) {
  const n = tag?.name ?? name ?? "";
  const k: TagKind = tag?.kind ?? kind ?? "strength";
  const tent = tag?.tentative ?? tentative;
  const base = KIND_CLASS[k] ?? KIND_CLASS.strength;
  const tentCls = tent ? "border-dashed bg-transparent opacity-75" : "";
  return (
    <span
      title={`${ARCHETYPE_HINT[n] ?? n}${tent ? " · tentative: too few games this season" : ""}`}
      className={`inline-flex items-center gap-1 whitespace-nowrap rounded-md border px-1.5 py-0.5 text-[11px] font-medium ${base} ${tentCls}`}
    >
      {n}
      {tent && <span className="text-[9px] font-bold uppercase tracking-wide opacity-80">· tent.</span>}
    </span>
  );
}

export function ArchetypeLegend() {
  return (
    <div className="flex flex-wrap items-center gap-2 text-[11px] text-zinc-500">
      <span className="rounded-md border border-sky-400/40 bg-sky-400/10 px-1.5 py-0.5 text-sky-300">Strength</span>
      <span className="rounded-md border border-rose-400/40 bg-rose-400/10 px-1.5 py-0.5 text-rose-300">Weakness</span>
      <span className="rounded-md border border-violet-400/40 bg-violet-400/10 px-1.5 py-0.5 text-violet-300">Style</span>
      <span className="rounded-md border border-dashed border-zinc-500 px-1.5 py-0.5 text-zinc-400">Tentative: early season</span>
    </div>
  );
}
