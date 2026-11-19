import type { Metadata } from "next";
import { notFound } from "next/navigation";
import { getSport } from "@/lib/sports";
import { parseMatchupParams } from "@/lib/matchup";
import { ComingSoon } from "@/components/ComingSoon";
import { PageHeader } from "@/components/PageHeader";
import { MatchupMaker } from "@/components/MatchupMaker";

export const dynamic = "force-dynamic";

export const metadata: Metadata = {
  title: "Matchup maker",
  description: "Pick any two teams and a court. CZarp projects the score, spread, total and win chance.",
};

export default async function MatchupPage({ params, searchParams }: {
  params: Promise<{ sport: string }>;
  searchParams: Promise<Record<string, string | string[] | undefined>>;
}) {
  const { sport: key } = await params;
  const sport = getSport(key);
  if (!sport) notFound();
  if (sport.status !== "live") return <ComingSoon sport={sport} />;
  const initial = parseMatchupParams(await searchParams);

  return (
    <div className="mx-auto max-w-7xl px-4 py-5 md:px-6 md:py-10">
      <PageHeader
        badge="none"
        eyebrow={`${sport.long} · Matchup`}
        title="Matchup maker"
        sub="Pick two teams and a court. CZarp projects the score, spread, total and win chance."
      />
      <div className="mt-4 md:mt-6">
        <MatchupMaker sportKey={sport.key} initial={initial} />
      </div>
    </div>
  );
}
