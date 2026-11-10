import type { Metadata } from "next";
import { H, LegalPage } from "@/components/LegalPage";

export const metadata: Metadata = { title: "Methodology · CZarp Analytics" };

function Placeholder({ children }: { children: React.ReactNode }) {
  return (
    <div className="rounded-xl border border-dashed border-ink-600 bg-ink-850 px-4 py-3 text-sm text-zinc-500">
      <div className="mb-1 text-[10px] font-bold uppercase tracking-wider text-zinc-500">Placeholder — CZarp writes this</div>
      {children}
    </div>
  );
}

export default function MethodologyPage() {
  return (
    <LegalPage
      eyebrow="How it works"
      title="Methodology"
      banner="Layout and section stubs only. CZarp, the product owner, will write the final copy."
    >
      <p>This page will explain how CZarp builds projections, measures edge, and grades the public book. Until that copy lands, the sections below are an outline.</p>
      <H>The models</H>
      <Placeholder>czarp-v2 is the default model for sides (spreads). Official totals use czarp-v1-totals. Describe features, inputs, and what is not used (for example KenPom ranks are never shown).</Placeholder>
      <H>Fair value, price, and edge</H>
      <Placeholder>Edge is the gap between our number and the Vegas price, as a share of the expected total score: |fair − price| ÷ Vegas total, for both sides and totals. Play thresholds: sides 4%, totals 3%; EDGE at 5%+, HOT at 7%+.</Placeholder>
      <H>Official vs tracked</H>
      <Placeholder>Official picks are the ones CZarp actually stakes (¼-Kelly units) and go on the public portfolio. Tracked picks are paper-only, flat 1u, never bet.</Placeholder>
      <H>Stake sizing</H>
      <Placeholder>Stakes are quarter-Kelly in units (1u = 1% of bankroll, capped at 2.5u). Describe any bankroll or cap rules here.</Placeholder>
      <H>Grading and closing line value</H>
      <Placeholder>Picks are time-stamped once posted and can&apos;t be edited. They are graded from final scores. CLV compares the posted line to the close.</Placeholder>
      <H>New coaches and early season</H>
      <Placeholder>Sides are paused until a new coach has 8 games; totals are paused until 4. Early-season tags can be tentative.</Placeholder>
      <H>Limitations</H>
      <Placeholder>A plus-EV process still has losing nights and drawdowns. Edges are not a promise of winning. Judge the portfolio over hundreds of bets.</Placeholder>
    </LegalPage>
  );
}
