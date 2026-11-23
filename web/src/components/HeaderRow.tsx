import type { CardHeaderRow, EdgeTone } from "@/lib/cardHeader";

const EDGE_TONE: Record<EdgeTone, string> = { green: "text-edge-400", neutral: "text-zinc-300", dim: "text-zinc-500" };

/** The one-row headline used by the slate card, the game sheet and the team page's next game:
 *  "SPREAD  Texas St. -2.5  2.5%". Data comes from lib/cardHeader (cardHeaderRow). The tier lives in the
 *  status chip next to it; the % color follows the tier. Official plays add a compact posted / now /
 *  vs-open line (nowrap numbers) under the headline — never a downgrade sentence. */
export function HeaderRow({ row }: { row: CardHeaderRow }) {
  return (
    <div>
      <div className="flex items-baseline gap-2">
        {row.marketLabel && <span className="shrink-0 text-[10px] font-semibold uppercase tracking-wider text-zinc-500">{row.marketLabel}</span>}
        <span className={`min-w-0 flex-1 break-words font-sans text-[15px] font-bold leading-tight ${row.pick ? "text-white" : "font-semibold text-zinc-400"}`}>{row.pick ?? row.fallback}</span>
        <span className={`shrink-0 font-mono text-2xl font-black leading-none ${EDGE_TONE[row.tone]}`}>{row.edgeLabel ?? "—"}</span>
      </div>
      {row.postedNowParts.length > 0 ? (
        <div className="mt-0.5 flex flex-wrap items-baseline gap-x-1 text-[11px] leading-snug text-zinc-500">
          {row.postedNowParts.map((part, i) => (
            <span key={`${part}-${i}`} className="font-mono whitespace-nowrap">
              {i > 0 ? <span className="text-zinc-600"> · </span> : null}
              {part}
            </span>
          ))}
        </div>
      ) : row.now ? (
        <div className="mt-0.5 font-mono text-[11px] text-zinc-500">{row.now}</div>
      ) : null}
    </div>
  );
}
