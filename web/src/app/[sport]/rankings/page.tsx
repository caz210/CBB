import type { Metadata } from "next";
import { notFound } from "next/navigation";
import { getSport } from "@/lib/sports";
import { loadRankings } from "@/lib/data";
import { rankingsErrorCopy } from "@/lib/rankings";
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

  // NODE_ENV is inlined at `next build`, so this whole branch is stripped from
  // a production build. `next start` cannot turn fixtures on, even if RANKINGS_QA=1.
  let res: Awaited<ReturnType<typeof loadRankings>> | undefined;
  if (process.env.NODE_ENV !== "production") {
    const { tryRankingsDemo } = await import("@/lib/rankingsQa");
    const demo = await tryRankingsDemo(searchParams);
    if (demo) res = demo;
  }
  if (!res) res = await loadRankings(sport);

  const err = res.state === "error" ? rankingsErrorCopy(res.reason ?? "unreachable") : null;

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
        ) : res.state === "error" && err ? (
          <EmptyState
            tone="warn"
            title={err.title}
            body={err.body}
            actions={[{ href: `/${sport.key}/rankings`, label: "Try again" }]}
          />
        ) : res.state === "live" && res.data.rows.length === 0 ? (
          <EmptyState
            title="No rankings yet"
            body="The first CZarp poll hasn't been published for this league, or this week's snapshot isn't available yet. New polls go out Monday mornings."
            actions={[{ href: `/${sport.key}/slate`, label: "Back to the slate" }]}
          />
        ) : res.state === "live" ? (
          <RankingsBoard data={res.data} sportKey={sport.key} />
        ) : null}
      </div>
    </div>
  );
}
