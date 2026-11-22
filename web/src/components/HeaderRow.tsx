import type { CardHeaderRow, EdgeTone } from "@/lib/cardHeader";
import { LINE_MOVE_NOTE } from "@/lib/verdicts";

/** Under the headline of a CZarp Play whose edge now is below the bar. The chip stays CZarp Play. */
export function LineMoveNote({ className = "" }: { className?: string }) {
  return <div className={`text-[11px] font-medium leading-snug text-amber-300/90 ${className}`}>{LINE_MOVE_NOTE}</div>;
}

const EDGE_TONE: Record<EdgeTone, string> = { green: "text-edge-400", neutral: "text-zinc-300", dim: "text-zinc-500" };

/** The one-row headline used by the slate card, the game sheet and the team page's next game:
 *  "SPREAD  Texas St. -2.5  2.5%". Data comes from lib/cardHeader (cardHeaderRow). The tier lives in the
 *  status chip next to it; the % color follows the tier. */
export function HeaderRow({ row }: { row: CardHeaderRow }) {
  return (
    <div>
      <div className="flex items-baseline gap-2">
        {row.marketLabel && <span className="shrink-0 text-[10px] font-semibold uppercase tracking-wider text-zinc-500">{row.marketLabel}</span>}
        <span className={`min-w-0 flex-1 break-words font-sans text-[15px] font-bold leading-tight ${row.pick ? "text-white" : "font-semibold text-zinc-400"}`}>{row.pick ?? row.fallback}</span>
        <span className={`shrink-0 font-mono text-2xl font-black leading-none ${EDGE_TONE[row.tone]}`}>{row.edgeLabel ?? "—"}</span>
      </div>
      {row.now && <div className="mt-0.5 font-mono text-[11px] text-zinc-500">{row.now}</div>}
      {row.lineMoveNote && <LineMoveNote className="mt-1" />}
    </div>
  );
}
