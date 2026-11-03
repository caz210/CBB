import Link from "next/link";
import type { Sport } from "@/lib/sports";

export function ComingSoon({ sport }: { sport: Sport }) {
  return (
    <div className="mx-auto max-w-2xl px-4 py-14 md:px-6 md:py-28 text-center">
      <div className="mx-auto mb-6 grid h-16 w-16 place-items-center rounded-2xl border border-ink-600 bg-ink-850 text-2xl font-black text-court-400">
        {sport.label}
      </div>
      <h1 className="text-2xl font-bold md:text-3xl">{sport.long} is coming soon</h1>
      <p className="mt-3 text-zinc-400">
        CZarp starts with NCAA men&apos;s basketball. {sport.long} projections, value flags and a public ledger will
        land here once the model has a track record for this league.
      </p>
      <div className="mt-8 grid gap-3 sm:flex sm:justify-center">
        <Link href="/cbb/slate" className="flex min-h-12 items-center justify-center rounded-xl bg-court-500 px-5 font-semibold text-ink-950 hover:bg-court-400">
          Go to CBB slate
        </Link>
        <Link href="/signup" className="flex min-h-12 items-center justify-center rounded-xl border border-ink-600 px-5 font-semibold hover:bg-ink-800">
          Notify me
        </Link>
      </div>
    </div>
  );
}
