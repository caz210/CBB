import Link from "next/link";
import { notFound } from "next/navigation";
import { getSport } from "@/lib/sports";
import { isIsoDate, loadSlate, shiftDate, todayCT } from "@/lib/data";
import { fmtGameDate } from "@/lib/labels";
import { ComingSoon } from "@/components/ComingSoon";
import { EmptyState } from "@/components/EmptyState";
import { PageHeader } from "@/components/PageHeader";
import { SlateBoard } from "@/components/SlateBoard";

// Live data: render per request (RLS depends on the visitor's session).
export const dynamic = "force-dynamic";

export default async function SlatePage({
  params, searchParams,
}: {
  params: Promise<{ sport: string }>;
  searchParams: Promise<{ date?: string }>;
}) {
  const { sport: key } = await params;
  const sport = getSport(key);
  if (!sport) notFound();
  if (sport.status !== "live") return <ComingSoon sport={sport} />;

  const sp = await searchParams;
  const today = todayCT();
  const date = isIsoDate(sp.date) ? sp.date : today;
  const res = await loadSlate(sport, date);
  const href = (d: string) => (d === today ? `/${sport.key}/slate` : `/${sport.key}/slate?date=${d}`);
  const navBtn = "flex min-h-10 flex-1 items-center justify-center rounded-lg border border-ink-600 px-3 text-zinc-300 hover:bg-ink-800 md:flex-none";

  return (
    <div className="mx-auto max-w-7xl px-4 py-5 md:px-6 md:py-10">
      <PageHeader
        badge={res.state === "live" ? "live" : "none"}
        eyebrow={`${sport.long} · Slate`}
        title={`${fmtGameDate(date)}${date === today ? " · Today" : ""}`}
        sub="Model projections update each morning. Tap a value bet for the edge and stake."
        right={
          <nav className="flex gap-2 text-sm" aria-label="Slate date">
            <Link href={href(shiftDate(date, -1))} className={navBtn}>‹ Prev</Link>
            {date !== today && <Link href={href(today)} className={navBtn}>Today</Link>}
            <Link href={href(shiftDate(date, 1))} className={navBtn}>Next ›</Link>
          </nav>
        }
      />
      <div className="mt-4 md:mt-6">
        {res.state === "unconfigured" ? (
          <EmptyState tone="warn" title="Live data isn't connected yet"
            body="This deployment is missing its Supabase settings, so there's no slate to show. Once they're added, today's games and value bets appear here automatically." />
        ) : res.state === "error" ? (
          <EmptyState tone="warn" title="Couldn't load the slate"
            body="The data service didn't respond. Please refresh in a minute."
            actions={[{ href: href(date), label: "Try again" }]} />
        ) : res.data.games.length === 0 ? (
          <EmptyState
            title={`No ${sport.label} games on ${fmtGameDate(date)}`}
            body={date >= today
              ? "Nothing has been posted for this date yet. The model publishes each morning, so check back later."
              : "There are no projected games for this date."}
            actions={[
              ...(res.data.prevDate ? [{ href: href(res.data.prevDate), label: `‹ ${fmtGameDate(res.data.prevDate)}` }] : []),
              ...(res.data.nextDate ? [{ href: href(res.data.nextDate), label: `${fmtGameDate(res.data.nextDate)} ›` }] : []),
            ]}
          />
        ) : (
          <>
            {res.data.bets.length === 0 && (
              <div className="mb-4 rounded-xl border border-ink-700 bg-ink-900 px-3.5 py-3 text-sm text-zinc-400">
                No value bets are showing for this slate. Picks unlock at tip-off on the Free plan;{" "}
                <Link href="/pricing" className="font-semibold text-court-400">Pro</Link> sees them as soon as they post.
              </div>
            )}
            <SlateBoard games={res.data.games} bets={res.data.bets} tracked={res.data.tracked} />
          </>
        )}
      </div>
    </div>
  );
}
