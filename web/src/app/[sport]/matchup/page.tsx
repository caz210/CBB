import type { Metadata } from "next";
import { Suspense } from "react";
import { notFound } from "next/navigation";
import { getSport } from "@/lib/sports";
import { ComingSoon } from "@/components/ComingSoon";
import { PageHeader } from "@/components/PageHeader";
import { MatchupMaker } from "@/components/MatchupMaker";

export const dynamic = "force-dynamic";

export const metadata: Metadata = {
  title: "Matchup maker",
  description: "Pick any two teams and a court. CZarp projects the score, spread, total and win chance.",
};

export default async function MatchupPage({ params }: { params: Promise<{ sport: string }> }) {
  const { sport: key } = await params;
  const sport = getSport(key);
  if (!sport) notFound();
  if (sport.status !== "live") return <ComingSoon sport={sport} />;

  return (
    <div className="mx-auto max-w-7xl px-4 py-5 md:px-6 md:py-10">
      <PageHeader
        badge="none"
        eyebrow={`${sport.long} · Matchup`}
        title="Matchup maker"
        sub="Pick two teams and a court. CZarp projects the score, spread, total and win chance."
      />
      <div className="mt-4 md:mt-6">
        {/* The maker reads the URL itself (useSearchParams), so Back/Forward and shared links stay in sync. */}
        <Suspense fallback={<div className="mx-auto max-w-2xl text-sm text-zinc-500">Loading…</div>}>
          <MatchupMaker sportKey={sport.key} />
        </Suspense>
      </div>
    </div>
  );
}
