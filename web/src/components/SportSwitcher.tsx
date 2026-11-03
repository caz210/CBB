"use client";
import Link from "next/link";
import { usePathname } from "next/navigation";
import { SPORTS, DEFAULT_SPORT } from "@/lib/sports";

export function SportSwitcher() {
  const pathname = usePathname();
  const seg = pathname.split("/")[1];
  const current = SPORTS.some((s) => s.key === seg) ? seg : DEFAULT_SPORT;
  const sub = pathname.split("/")[2] ?? "slate";
  const page = sub === "results" ? "results" : "slate";

  return (
    <div className="inline-flex items-center gap-1 rounded-xl border border-ink-700 bg-ink-850 p-1">
      {SPORTS.map((s) => {
        const active = s.key === current;
        return (
          <Link
            key={s.key}
            href={`/${s.key}/${page}`}
            className={`relative whitespace-nowrap rounded-lg px-3 py-1.5 text-xs font-semibold transition ${
              active
                ? "bg-court-500 text-ink-950"
                : s.status === "live"
                  ? "text-zinc-300 hover:bg-ink-700"
                  : "text-zinc-500 hover:bg-ink-700"
            }`}
            title={s.status === "coming_soon" ? `${s.long}: coming soon` : s.long}
          >
            {s.label}
            {s.status === "coming_soon" && (
              <span className="ml-1 rounded bg-ink-600 px-1 text-[9px] font-bold uppercase text-zinc-400">soon</span>
            )}
          </Link>
        );
      })}
    </div>
  );
}
