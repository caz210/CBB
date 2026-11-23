import { VERDICT_LADDER } from "@/lib/verdicts";
import type { Metadata } from "next";
import { H, LegalPage } from "@/components/LegalPage";
import { pageMetadata } from "@/lib/seo";
import { SHOW_METHODOLOGY_PLACEHOLDERS, visibleMethodologySections } from "@/lib/methodology";
import { GLOSSARY_ANCHOR, glossaryEntriesIn, type GlossaryGroup } from "@/lib/glossary";

export const metadata: Metadata = pageMetadata({
  title: "Methodology",
  description: "How CZarp grades a game: Edge % versus the market price, the play bar official picks must clear, and a time-stamped public record that cannot be edited. For entertainment, not betting advice.",
  path: "/methodology",
});

function Placeholder({ children }: { children: React.ReactNode }) {
  return (
    <div className="rounded-xl border border-dashed border-ink-600 bg-ink-850 px-4 py-3 text-sm text-zinc-500">
      <div className="mb-1 text-[10px] font-bold uppercase tracking-wider text-zinc-500">Outline — CZarp writes the final copy</div>
      {children}
    </div>
  );
}

function GlossaryBlock({ title, group }: { title: string; group: GlossaryGroup }) {
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
      <p>Edge % compares our fair number to the market price. Official picks have to clear the play bar at the morning run: 4% edge on sides, 3% on totals, a stake of at least 0.1u, and on totals, no 3-point-or-more move against us since the open. A new-coach pause can also keep a pick off the record. Every official pick is time-stamped and can&apos;t be edited.</p>
      {sections.map((s) => (
        <div key={s.title}>
          <H>{s.title}</H>
          {s.placeholder && SHOW_METHODOLOGY_PLACEHOLDERS ? <Placeholder>{s.body}</Placeholder> : !s.placeholder ? <p>{s.body}</p> : null}
        </div>
      ))}
      <section id={GLOSSARY_ANCHOR} className="scroll-mt-24">
        <H>Glossary</H>
        <p>Tap the ⓘ on any verdict, game chip or style tag on the site for its definition. Each game gets two ratings, one for the spread and one for the total, judged on the edge vs the current line, from weakest to strongest: {VERDICT_LADDER.map((d) => d.label).join(", ")}. Style tags describe how a team plays relative to the rest of D-I; a dashed chip marked “· tent.” means the sample is still early.</p>
        <GlossaryBlock title="Verdicts" group="verdict" />
        <GlossaryBlock title="Game chips" group="chip" />
        <GlossaryBlock title="Style tags" group="style" />
      </section>
    </LegalPage>
  );
}
