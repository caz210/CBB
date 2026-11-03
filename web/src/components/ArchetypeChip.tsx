import { ARCHETYPE_HINT, isWeakness, type ArchetypeName } from "@/lib/archetypes";

export function ArchetypeChip({ name, tentative }: { name: ArchetypeName; tentative?: boolean }) {
  const weak = isWeakness(name);
  const base = weak
    ? "border-rose-400/40 bg-rose-400/10 text-rose-300"
    : "border-sky-400/40 bg-sky-400/10 text-sky-300";
  const tent = tentative ? "border-dashed bg-transparent opacity-75" : "";
  return (
    <span
      title={`${ARCHETYPE_HINT[name]}${tentative ? " · tentative: too few games this season" : ""}`}
      className={`inline-flex items-center gap-1 whitespace-nowrap rounded-md border px-1.5 py-0.5 text-[11px] font-medium ${base} ${tent}`}
    >
      {name}
      {tentative && <span className="text-[9px] font-bold uppercase tracking-wide opacity-80">· tent.</span>}
    </span>
  );
}

export function ArchetypeLegend() {
  return (
    <div className="flex flex-wrap items-center gap-2 text-[11px] text-zinc-500">
      <span className="rounded-md border border-sky-400/40 bg-sky-400/10 px-1.5 py-0.5 text-sky-300">Strength</span>
      <span className="rounded-md border border-rose-400/40 bg-rose-400/10 px-1.5 py-0.5 text-rose-300">Weakness</span>
      <span className="rounded-md border border-dashed border-zinc-500 px-1.5 py-0.5 text-zinc-400">Tentative: early season</span>
    </div>
  );
}
