import { notFound } from "next/navigation";
import { SPORTS, getSport } from "@/lib/sports";
import { EXAMPLE_SLATE, EXAMPLE_VALUE_BETS, SLATE_DATE } from "@/lib/example-data";
import { fmtGameDate } from "@/lib/labels";
import { ComingSoon } from "@/components/ComingSoon";
import { PageHeader } from "@/components/PageHeader";
import { SlateBoard } from "@/components/SlateBoard";

export function generateStaticParams() {
  return SPORTS.map((s) => ({ sport: s.key }));
}

export default async function SlatePage({ params }: { params: Promise<{ sport: string }> }) {
  const { sport: key } = await params;
  const sport = getSport(key);
  if (!sport) notFound();
  if (sport.status !== "live") return <ComingSoon sport={sport} />;

  // TODO(supabase): from("slate_games").select("*").eq("sport", key).eq("game_date", today).order("tipoff")
  //                 from("value_bets").select("*").eq("sport", key)   (RLS hides these until tip-off for Free)
  const games = EXAMPLE_SLATE.filter((g) => g.sport === sport.key);
  const bets = EXAMPLE_VALUE_BETS.filter((b) => b.sport === sport.key);

  return (
    <div className="mx-auto max-w-7xl px-4 py-5 md:px-6 md:py-10">
      <PageHeader
        eyebrow={`${sport.long} · Slate`}
        title={fmtGameDate(SLATE_DATE)}
        sub="Updated daily at 9:00 AM CT. Tap a value bet for the edge and stake."
        right={
          <div className="flex gap-2 text-sm">
            <button className="min-h-10 flex-1 rounded-lg border border-ink-600 px-3 text-zinc-400 md:flex-none">‹ Prev</button>
            <button className="min-h-10 flex-1 rounded-lg border border-ink-600 px-3 text-zinc-400 md:flex-none">Next ›</button>
          </div>
        }
      />
      <div className="mt-4 md:mt-6">
        <SlateBoard games={games} bets={bets} />
      </div>
    </div>
  );
}
