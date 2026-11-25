/** Official Pick banner at the top of GameSheet. One block per official bet. */
import type { OfficialPickView } from "@/lib/officialPick";
import { LineMoveNote, OpenNowNote } from "./HeaderRow";

export function OfficialPickSection({ views }: { views: OfficialPickView[] }) {
  if (!views.length) return null;
  return (
    <div className="space-y-2">
      {views.map((v) => (
        <div
          key={`${v.market}-${v.side}`}
          className="rounded-2xl border border-edge-400/30 bg-edge-400/[0.04] px-4 py-3"
        >
          <div className="flex items-start justify-between gap-2">
            <div className="min-w-0">
              <div className="mb-1 flex flex-wrap items-center gap-1.5">
                <span className="text-[10px] font-semibold uppercase tracking-wider text-edge-400">Official Pick</span>
                <span className="text-[10px] font-semibold uppercase tracking-wider text-zinc-500">{v.marketLabel}</span>
              </div>
              <div className="text-[15px] font-bold leading-tight">{v.side}</div>
            </div>
            <div className={`shrink-0 font-mono text-2xl font-black leading-none ${v.belowBar ? "text-zinc-300" : "text-edge-400"}`}>
              {v.edgeLabel}
            </div>
          </div>
          <div className="mt-2 space-y-0.5 text-[12px] text-zinc-300">
            {v.stakeLabel && (
              <div><span className="whitespace-nowrap font-mono font-semibold">{v.stakeLabel}</span></div>
            )}
            {(v.fairLabel || v.priceLabel) && (
              <div className="font-mono text-zinc-400">
                {v.fairLabel && <span className="whitespace-nowrap">FV {v.fairLabel}</span>}
                {v.fairLabel && v.priceLabel ? " · " : null}
                {v.priceLabel && <span className="whitespace-nowrap">Price {v.priceLabel}</span>}
              </div>
            )}
            {(v.postedLineLabel || v.postedAtLabel) && (
              <div className="text-zinc-400">
                {v.postedLineLabel && <span className="whitespace-nowrap">Posted line {v.postedLineLabel}</span>}
                {v.postedLineLabel && v.postedAtLabel ? " · " : null}
                {v.postedAtLabel && <span className="whitespace-nowrap">{v.postedAtLabel}</span>}
              </div>
            )}
            {v.lineMoveNote && <LineMoveNote className="mt-1" />}
            {!v.lineMoveNote && v.openNowContext && <OpenNowNote text={v.openNowContext} className="mt-1" />}
            {v.resultLabel && (
              <div className={`font-semibold ${v.result === "W" ? "text-edge-400" : v.result === "L" ? "text-red-400" : "text-zinc-300"}`}>
                <span className="whitespace-nowrap">{v.resultLabel}{v.profitLabel ? ` · ${v.profitLabel}` : ""}</span>
                {v.clvLabel ? <> · <span className="whitespace-nowrap">CLV {v.clvLabel}</span></> : null}
              </div>
            )}
            {v.pendingNote && <p className="text-[11px] leading-snug text-zinc-500">{v.pendingNote}</p>}
          </div>
        </div>
      ))}
    </div>
  );
}
