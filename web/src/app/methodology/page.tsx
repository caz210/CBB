import type { Metadata } from "next";
import { H, LegalPage } from "@/components/LegalPage";
import { SHOW_METHODOLOGY_PLACEHOLDERS, visibleMethodologySections } from "@/lib/methodology";

export const metadata: Metadata = { title: "Methodology" };

function Placeholder({ children }: { children: React.ReactNode }) {
  return (
    <div className="rounded-xl border border-dashed border-ink-600 bg-ink-850 px-4 py-3 text-sm text-zinc-500">
      <div className="mb-1 text-[10px] font-bold uppercase tracking-wider text-zinc-500">Placeholder — CZarp writes this</div>
      {children}
    </div>
  );
}

export default function MethodologyPage() {
  const sections = visibleMethodologySections();
  return (
    <LegalPage
      eyebrow="How it works"
      title="Methodology"
    >
      <p>This page will explain how CZarp builds projections, measures edge, and grades the public book.{sections.length ? " Until that copy lands, the sections below are an outline." : " The section copy is on the way."}</p>
      {sections.map((s) => (
        <div key={s.title}>
          <H>{s.title}</H>
          {s.placeholder && SHOW_METHODOLOGY_PLACEHOLDERS ? <Placeholder>{s.body}</Placeholder> : !s.placeholder ? <p>{s.body}</p> : null}
        </div>
      ))}
    </LegalPage>
  );
}
