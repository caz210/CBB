/** Simple document layout for /terms, /privacy, /methodology, /about. */
export function LegalPage({
  eyebrow, title, banner, children,
}: {
  eyebrow: string;
  title: string;
  banner?: string;
  children: React.ReactNode;
}) {
  return (
    <article className="mx-auto max-w-3xl px-4 py-8 md:px-6 md:py-14">
      <div className="text-[11px] font-bold uppercase tracking-wider text-court-400">{eyebrow}</div>
      <h1 className="mt-1.5 text-3xl font-black tracking-tight md:text-4xl">{title}</h1>
      {banner && (
        <div className="mt-4 rounded-xl border border-amber-400/40 bg-amber-400/10 px-4 py-3 text-sm text-amber-100">
          {banner}
        </div>
      )}
      <div className="prose-legal mt-6 space-y-5 text-[15px] leading-relaxed text-zinc-300">
        {children}
      </div>
    </article>
  );
}

export function H({ children }: { children: React.ReactNode }) {
  return <h2 className="pt-2 text-lg font-bold text-white">{children}</h2>;
}
