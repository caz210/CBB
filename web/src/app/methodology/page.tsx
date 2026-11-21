import type { Metadata } from "next";
import { H, LegalPage } from "@/components/LegalPage";
import { SHOW_METHODOLOGY_PLACEHOLDERS, visibleMethodologySections } from "@/lib/methodology";
import { GLOSSARY_ANCHOR, glossaryEntriesIn } from "@/lib/glossary";

export const metadata: Metadata = { title: "Methodology" };

function Placeholder({ children }: { children: React.ReactNode }) {
  return (
    <div className="rounded-xl border border-dashed border-ink-600 bg-ink-850 px-4 py-3 text-sm text-zinc-500">
      <div className="mb-1 text-[10px] font-bold uppercase tracking-wider text-zinc-500">Outline — CZarp writes the final copy</div>
      {children}
    </div>
  );
}

function GlossaryBlock({ title, group }: { title: string; group: "chip" | "style" }) {
  const entries = glossaryEntriesIn(group);
  return (
    <div className="mt-4">
      <h3 className="text-base font-bold text-white">{title}</h3>
      <dl className="mt-2 space-y-3">
        {entries.map((e) => (
          <div key={e.id} id={e.id}>
            <dt className="font-semibold text-white">{e.title}</dt>
            <dd className="mt-0.5 text-zinc-300">{e.body}</dd>
          </div>
        ))}
      </dl>
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
      <p>This page will explain how CZarp builds projections, measures edge, and grades the public book.</p>
      {sections.map((s) => (
        <div key={s.title}>
          <H>{s.title}</H>
          {s.placeholder && SHOW_METHODOLOGY_PLACEHOLDERS ? <Placeholder>{s.body}</Placeholder> : !s.placeholder ? <p>{s.body}</p> : null}
        </div>
      ))}
      <section id={GLOSSARY_ANCHOR} className="scroll-mt-24">
        <H>Glossary</H>
        <p>Tap any chip on the site for the same definition. Style tags describe how a team plays relative to the rest of D-I; a dashed chip marked “· tent.” means the sample is still early.</p>
        <GlossaryBlock title="Game chips" group="chip" />
        <GlossaryBlock title="Style tags" group="style" />
      </section>
    </LegalPage>
  );
}
