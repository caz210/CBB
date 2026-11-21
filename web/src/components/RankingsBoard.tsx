import Link from "next/link";
import { TermLabel } from "./InfoTip";
import { teamHref } from "@/lib/teams";
import {
  TOP_N,
  fmtRating,
  fmtRecord,
  isEarlySeason,
  movementLabel,
  updatedLabel,
  weekLabel,
  type Movement,
  type RankingRow,
  type RankingsData,
} from "@/lib/rankings";

export function RankingsBoard({
  data,
  sportKey,
}: {
  data: RankingsData;
  sportKey: string;
}) {
  const top = data.rows.filter((r) => r.rank <= TOP_N);
  const rest = data.rows.filter((r) => r.rank > TOP_N);
  const week = weekLabel(data.week_of);
  const updated = data.as_of ? updatedLabel(data.as_of) : null;

  return (
    <div className="max-w-3xl">
      <div className="flex flex-wrap items-baseline gap-x-2 gap-y-1 text-sm text-zinc-400">
        <span className="font-medium text-zinc-200">{week}</span>
        {updated && (
          <>
            <span aria-hidden className="text-zinc-600">
              ·
            </span>
            <span>{updated}</span>
          </>
        )}
      </div>

      <div className="mt-3 space-y-1.5 text-sm leading-relaxed text-zinc-400">
        <p>Records count only games between two D-I teams.</p>
        {isEarlySeason(data.week_of) && (
          <p className="text-zinc-500">Early season: ratings move a lot until teams have played more games.</p>
        )}
      </div>

      <div className="card mt-4 max-w-full overflow-hidden md:mt-6">
        <RankTable rows={top} sportKey={sportKey} caption="CZarp Top 25" />
      </div>

      {rest.length > 0 && (
        <details className="group mt-4 md:mt-6">
          <summary className="flex min-h-11 cursor-pointer list-none items-center justify-center rounded-xl border border-ink-600 px-4 text-sm font-semibold text-zinc-200 hover:bg-ink-800 [&::-webkit-details-marker]:hidden">
            <span className="group-open:hidden">Show all {data.rows.length}</span>
            <span className="hidden group-open:inline">Show Top {TOP_N}</span>
          </summary>
          <div className="card mt-3 max-w-full overflow-hidden">
            <RankTable rows={rest} sportKey={sportKey} caption={`Ranks ${TOP_N + 1}–${data.rows.length}`} />
          </div>
        </details>
      )}
    </div>
  );
}

function RankTable({
  rows,
  sportKey,
  caption,
}: {
  rows: RankingRow[];
  sportKey: string;
  caption: string;
}) {
  return (
    <table className="w-full table-fixed text-sm">
      <caption className="sr-only">{caption}</caption>
      <colgroup>
        <col className="w-9 sm:w-10" />
        <col className="w-9 sm:w-12" />
        <col />
        <col className="w-14 sm:w-20" />
        <col className="w-12 sm:w-16" />
      </colgroup>
      <thead className="bg-ink-850 text-left text-[11px] font-bold uppercase tracking-normal text-zinc-500 sm:tracking-wider">
        <tr>
          <th scope="col" className="py-2.5 pl-2 pr-1 sm:pl-4">
            <span className="sr-only">Rank</span>#
          </th>
          <th scope="col" className="py-2.5 pr-1">
            <span className="sr-only">Movement</span>
          </th>
          <th scope="col" className="min-w-0 max-w-0 py-2.5 pr-2">
            Team
          </th>
          <th scope="col" className="whitespace-nowrap py-2.5 pr-2 text-right">
            <TermLabel term="rating" className="justify-end">
              Rating
            </TermLabel>
          </th>
          <th scope="col" className="whitespace-nowrap py-2.5 pl-1 pr-2 text-right sm:pr-4">
            Record
          </th>
        </tr>
      </thead>
      <tbody>
        {rows.map((r) => (
          <tr key={r.team_id} className="border-t border-ink-700/60 hover:bg-ink-850">
            <td className="whitespace-nowrap py-2.5 pl-2 pr-1.5 font-mono text-sm tabular-nums text-zinc-300 sm:pl-4 sm:py-3">
              {r.rank}
            </td>
            <td className="whitespace-nowrap py-2.5 pr-1 sm:py-3">
              <MoveBadge m={r.movement} />
            </td>
            <td className="min-w-0 max-w-0 w-full py-2.5 pr-2 sm:py-3">
              <Link
                href={teamHref(sportKey, r.team)}
                title={r.team}
                aria-label={r.team}
                className="block truncate font-semibold text-white hover:text-court-400"
              >
                {r.team}
              </Link>
            </td>
            <td className="whitespace-nowrap py-2.5 pr-2 text-right font-mono text-sm tabular-nums text-zinc-200 sm:py-3">
              {fmtRating(r.rating)}
            </td>
            <td className="whitespace-nowrap py-2.5 pl-1 pr-2 text-right font-mono text-sm tabular-nums text-zinc-400 sm:py-3 sm:pr-4">
              {fmtRecord(r.di_wins, r.di_losses)}
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function MoveBadge({ m }: { m: Movement }) {
  const label = movementLabel(m);
  if (m.kind === "none") {
    return (
      <span className="inline-block w-full text-center text-zinc-600" title={label} aria-label={label}>
        —
      </span>
    );
  }
  if (m.kind === "new") {
    return (
      <span
        className="inline-block rounded px-0.5 text-[10px] font-bold uppercase tracking-wide text-court-400"
        title={label}
        aria-label={label}
      >
        New
      </span>
    );
  }
  if (m.kind === "same") {
    return (
      <span className="inline-block w-full text-center font-medium text-zinc-400" title={label} aria-label={label}>
        =
      </span>
    );
  }
  const up = m.kind === "up";
  return (
    <span
      className={`inline-flex items-center gap-0.5 font-mono text-[11px] tabular-nums ${up ? "text-edge-400" : "text-fade-400"}`}
      title={label}
      aria-label={label}
    >
      <span aria-hidden>{up ? "▲" : "▼"}</span>
      {m.delta}
    </span>
  );
}
