import type { Metadata } from "next";
import { notFound } from "next/navigation";
import { getSport } from "@/lib/sports";
import { loadRankings } from "@/lib/data";
import { movementDemoRows, type RankingsData } from "@/lib/rankings";
import { ComingSoon } from "@/components/ComingSoon";
import { EmptyState } from "@/components/EmptyState";
import { PageHeader } from "@/components/PageHeader";
import { RankingsBoard } from "@/components/RankingsBoard";

export const dynamic = "force-dynamic";

export async function generateMetadata({ params }: { params: Promise<{ sport: string }> }): Promise<Metadata> {
  const { sport: key } = await params;
  const sport = getSport(key);
  const who = sport?.long ?? "College basketball";
  return {
    title: "CZarp Top 25",
    description: `${who} rankings: CZarp Top 25 by projected margin vs an average D-I team on a neutral court, with D-I-only records.`,
  };
}

export default async function RankingsPage({
  params,
  searchParams,
}: {
  params: Promise<{ sport: string }>;
  searchParams: Promise<{ demo?: string }>;
}) {
  const { sport: key } = await params;
  const sport = getSport(key);
  if (!sport) notFound();
  if (sport.status !== "live") return <ComingSoon sport={sport} />;

  const qa = process.env.RANKINGS_QA === "1";
  const sp = qa ? await searchParams : { demo: undefined };
  let res: Awaited<ReturnType<typeof loadRankings>>;
  if (qa && sp.demo === "empty") {
    res = { state: "live", data: { week_of: "", as_of: "", season: null, rows: [] } };
  } else if (qa && sp.demo === "error") {
    res = { state: "error" };
  } else if (qa && sp.demo === "movement") {
    const rows = movementDemoRows();
    const data: RankingsData = {
      week_of: "2026-11-23",
      as_of: "2026-11-23",
      season: 2027,
      rows,
    };
    res = { state: "live", data };
  } else {
    res = await loadRankings(sport);
  }

  return (
    <div className="mx-auto max-w-7xl px-4 py-5 md:px-6 md:py-10">
      <PageHeader
        badge="none"
        eyebrow={`${sport.long} · Rankings`}
        title="CZarp Top 25"
        sub="Projected margin vs an average D-I team on a neutral court."
      />
      <div className="mt-4 md:mt-6">
        {res.state === "unconfigured" ? (
          <EmptyState
            tone="warn"
            title="Live data isn't connected yet"
            body="This deployment is missing its Supabase settings, so there's no poll to show. Once they're added, the CZarp Top 25 appears here automatically."
          />
        ) : res.state === "error" ? (
          <EmptyState
            tone="warn"
            title="Couldn't load the rankings"
            body="The data service didn't respond. Please refresh in a minute."
            actions={[{ href: `/${sport.key}/rankings`, label: "Try again" }]}
          />
        ) : res.data.rows.length === 0 ? (
          <EmptyState
            title="No rankings yet"
            body="The first CZarp poll hasn't been published for this league, or this week's snapshot isn't available yet. New polls go out Monday mornings."
            actions={[{ href: `/${sport.key}/slate`, label: "Back to the slate" }]}
          />
        ) : (
          <RankingsBoard data={res.data} sportKey={sport.key} />
        )}
      </div>
    </div>
  );
}
