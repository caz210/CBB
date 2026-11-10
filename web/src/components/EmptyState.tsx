import Link from "next/link";
import { fmtFinalAt, fmtUpdated, isStale } from "@/lib/labels";

/** Clear, mobile-friendly empty / not-connected state for live data pages. */
export function EmptyState({
  title, body, actions = [], tone = "neutral",
}: {
  title: string;
  body: React.ReactNode;
  actions?: { href: string; label: string }[];
  tone?: "neutral" | "warn";
}) {
  const ring = tone === "warn" ? "border-amber-400/30 bg-amber-400/5" : "border-ink-700 bg-ink-900";
  return (
    <div className={`rounded-2xl border border-dashed p-6 text-center md:p-10 ${ring}`}>
      <div className="mx-auto mb-3 grid h-11 w-11 place-items-center rounded-full bg-ink-800 text-xl text-zinc-400" aria-hidden>
        {tone === "warn" ? "!" : "○"}
      </div>
      <h2 className="text-lg font-bold">{title}</h2>
      <p className="mx-auto mt-1.5 max-w-md text-sm leading-relaxed text-zinc-400">{body}</p>
      {actions.length > 0 && (
        <div className="mt-4 flex flex-col gap-2 sm:flex-row sm:justify-center">
          {actions.map((a) => (
            <Link key={a.href} href={a.href} className="flex min-h-11 items-center justify-center rounded-xl border border-ink-600 px-4 text-sm font-semibold hover:bg-ink-800">
              {a.label}
            </Link>
          ))}
        </div>
      )}
    </div>
  );
}

/** Freshness chip. "Live" only when the slate is current (<2h) and not finished.
 *  Finished slates use max(final_at), not updated_at. Staleness applies only while live. */
export function LiveBadge({
  updatedAt, finished, finalAt, finalAtEstimated, slateDate,
}: {
  updatedAt?: string | null; finished?: boolean;
  finalAt?: string | null; finalAtEstimated?: boolean;
  slateDate?: string | null;
} = {}) {
  if (finished) {
    const when = fmtFinalAt(finalAt, { estimated: finalAtEstimated, relativeTo: slateDate });
    return (
      <span className="inline-flex items-center gap-1 rounded-md border border-ink-600 bg-ink-850 px-1.5 py-0.5 text-[10px] font-bold uppercase tracking-wider text-zinc-400">
        Final{when ? ` · ${when}` : ""}
      </span>
    );
  }
  const stale = isStale(updatedAt ?? null);
  const when = updatedAt ? fmtUpdated(updatedAt) : null;
  if (stale) {
    if (!when) return null;
    return (
      <span className="inline-flex items-center gap-1 rounded-md border border-ink-600 bg-ink-850 px-1.5 py-0.5 text-[10px] font-bold uppercase tracking-wider text-zinc-400">
        Updated · {when}
      </span>
    );
  }
  return (
    <span className="inline-flex items-center gap-1 rounded-md border border-edge-400/40 bg-edge-400/10 px-1.5 py-0.5 text-[10px] font-bold uppercase tracking-wider text-edge-400">
      <span className="h-1.5 w-1.5 rounded-full bg-edge-400" /> Live{when ? ` · ${when}` : ""}
    </span>
  );
}
