"use client";
// Date links must be built on the client from the live ?q=/sort/hide, because typing
// updates the URL with replaceState and never re-renders the server page.
import Link from "next/link";
import { useSearchParams } from "next/navigation";
import { slateDateHref } from "@/lib/slateSearch";

export function SlateDateNav({
  sportKey, date, today, prevDate, nextDate, className,
}: {
  sportKey: string; date: string; today: string; prevDate: string; nextDate: string; className: string;
}) {
  const search = useSearchParams();
  const href = (d: string) => slateDateHref({ sportKey, date: d, today, search });
  return (
    <nav className="flex gap-2 text-sm" aria-label="Slate date">
      <Link href={href(prevDate)} className={className}>‹ Prev</Link>
      {date !== today && <Link href={href(today)} className={className}>Today</Link>}
      <Link href={href(nextDate)} className={className}>Next ›</Link>
    </nav>
  );
}

/** Prev/next (or Try again) links in empty states, also built from the live URL. */
export function SlateDateLinks({
  sportKey, today, items,
}: {
  sportKey: string; today: string; items: { date: string; label: string }[];
}) {
  const search = useSearchParams();
  if (!items.length) return null;
  return (
    <div className="mt-4 flex flex-col gap-2 sm:flex-row sm:justify-center">
      {items.map((a) => (
        <Link
          key={`${a.date}-${a.label}`}
          href={slateDateHref({ sportKey, date: a.date, today, search })}
          className="flex min-h-11 items-center justify-center rounded-xl border border-ink-600 px-4 text-sm font-semibold hover:bg-ink-800"
        >
          {a.label}
        </Link>
      ))}
    </div>
  );
}
