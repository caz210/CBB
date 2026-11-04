export function ExampleBadge({ className = "" }: { className?: string }) {
  return (
    <span
      className={`inline-flex items-center rounded-md border border-amber-400/40 bg-amber-400/10 px-1.5 py-0.5 text-[10px] font-bold uppercase tracking-wider text-amber-300 ${className}`}
    >
      Example
    </span>
  );
}

export function ExampleBanner() {
  return (
    <div className="border-b border-amber-400/20 bg-amber-400/5 px-4 py-1.5 text-center text-[11px] text-amber-200/90 md:text-xs">
      Preview: the teams, numbers and picks on this page are fictional <b>example data</b>. Slate and Results show live model output.
    </div>
  );
}
