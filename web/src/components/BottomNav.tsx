"use client";
import Link from "next/link";
import { usePathname } from "next/navigation";
import { SPORTS, DEFAULT_SPORT } from "@/lib/sports";
import { bottomNavLinks } from "@/lib/accounts";

const Icon = {
  slate: <path d="M4 5h16M4 12h16M4 19h10" />,
  rankings: <path d="M7 20V10m5 10V4m5 16v-6" />,
  matchup: <path d="M8 4v16M16 4v16M4 8h4m8 8h4M4 16h4m8-8h4" />,
  results: <path d="M4 19V9m6 10V5m6 14v-7m4 7H3" />,
  models: <path d="M5 7h14M5 12h9M5 17h5M17 15l2 2 3-4" />,
};

export function BottomNav() {
  const pathname = usePathname();
  const seg = pathname.split("/")[1];
  const sport = SPORTS.some((s) => s.key === seg) ? seg : DEFAULT_SPORT;
  // Rankings replaced Plans (Pricing stays in the footer and the desktop nav when accounts are on).
  const iconByMatch: Record<string, typeof Icon.slate> = {
    "/slate": Icon.slate,
    "/rankings": Icon.rankings,
    "/matchup": Icon.matchup,
    "/results": Icon.results,
    "/models": Icon.models,
  };
  const tabs = bottomNavLinks(sport);
  const cols = tabs.length === 5 ? "grid-cols-5" : "grid-cols-4";
  return (
    <nav className="fixed inset-x-0 bottom-0 z-40 border-t border-ink-700 bg-ink-950/95 pb-[env(safe-area-inset-bottom)] backdrop-blur lg:hidden">
      <div className={`grid ${cols}`}>
        {tabs.map((t) => {
          const active = pathname.includes(t.match);
          return (
            <Link
              key={t.label}
              href={t.href}
              className={`flex min-h-14 flex-col items-center justify-center gap-1 text-[11px] font-medium ${active ? "text-court-400" : "text-zinc-500"}`}
            >
              <svg viewBox="0 0 24 24" className="h-5 w-5" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                {iconByMatch[t.match]}
              </svg>
              {t.label}
            </Link>
          );
        })}
      </div>
    </nav>
  );
}
