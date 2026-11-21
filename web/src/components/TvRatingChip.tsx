import { fmtTvRating } from "@/lib/tvRating";

function TvIcon({ className = "h-3 w-3" }: { className?: string }) {
  return (
    <svg viewBox="0 0 24 24" className={className} fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
      <path d="M7 3l5 5 5-5" />
      <rect x="3" y="8" width="18" height="11" rx="2" />
      <path d="M8 22h8" />
    </svg>
  );
}

function FlameIcon({ className = "h-3 w-3" }: { className?: string }) {
  return (
    <svg viewBox="0 0 24 24" className={className} fill="currentColor" aria-hidden="true">
      <path d="M12 2s2.5 4 2.5 7.2c0 1.3-.5 2.4-1.3 3.2.9-.3 1.8-1 2.4-2 1.4 2 2.4 4 2.4 6.1A6 6 0 0 1 12 22a6 6 0 0 1-6-5.5c0-3.2 1.8-5.6 3.2-7.6.4 1.6 1.3 2.8 2.3 3.6C11 9.2 12 6.2 12 2z" />
    </svg>
  );
}

/**
 * Informational venue-style chip (not a verdict tag — doesn't count toward the 2-tag max).
 * Hidden when the rating is null. Highlighted = flame accent when the rounded
 * displayed rating is 50+, or it is in the day's top 5 by rating with a
 * displayed rating of at least 25 (see tvHighlightIds).
 */
export function TvRatingChip({ rating, highlight, className = "" }: { rating: number | null | undefined; highlight: boolean; className?: string }) {
  const n = fmtTvRating(rating);
  if (n == null) return null;
  return (
    <span
      className={`inline-flex shrink-0 items-center gap-0.5 whitespace-nowrap rounded-md border px-1.5 py-px align-middle text-[10px] font-semibold leading-4 ${className} ${
        highlight
          ? "border-amber-400/50 bg-amber-400/10 text-amber-200"
          : "border-ink-600 text-zinc-400"
      }`}
      title={highlight ? `TV Rating ${n} · one of the day's most watchable games` : `TV Rating ${n}`}
      aria-label={`TV Rating ${n}${highlight ? ", one of the day's most watchable games" : ""}`}
    >
      {highlight && <FlameIcon />}
      <TvIcon />
      {n}
    </span>
  );
}
