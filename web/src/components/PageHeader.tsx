import { ExampleBadge } from "./ExampleBadge";

export function PageHeader({ eyebrow, title, sub, right }: { eyebrow: string; title: string; sub?: React.ReactNode; right?: React.ReactNode }) {
  return (
    <div className="flex flex-col gap-3 border-b border-ink-700/60 pb-4 md:flex-row md:items-end md:justify-between md:gap-4 md:pb-6">
      <div>
        <div className="flex flex-wrap items-center gap-2 text-[11px] font-bold uppercase tracking-wider text-court-400">
          {eyebrow} <ExampleBadge />
        </div>
        <h1 className="mt-1.5 text-2xl font-black tracking-tight md:mt-2 md:text-3xl">{title}</h1>
        {sub && <p className="mt-1 text-sm text-zinc-400">{sub}</p>}
      </div>
      {right}
    </div>
  );
}
