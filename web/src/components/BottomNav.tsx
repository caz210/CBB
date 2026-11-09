"use client";
import Link from "next/link";
import { usePathname } from "next/navigation";
import { SPORTS, DEFAULT_SPORT } from "@/lib/sports";

const Icon = {
  slate: <path d="M4 5h16M4 12h16M4 19h10" />,
  results: <path d="M4 19V9m6 10V5m6 14v-7m4 7H3" />,
  models: <path d="M5 7h14M5 12h9M5 17h5M17 15l2 2 3-4" />,
  pricing: <path d="M12 3v18M16 7H10a3 3 0 0 0 0 6h4a3 3 0 0 1 0 6H7" />,
};

export function BottomNav() {
  const pathname = usePathname();
  const seg = pathname.split("/")[1];
  const sport = SPORTS.some((s) => s.key === seg) ? seg : DEFAULT_SPORT;
  const tabs = [
    { href: `/${sport}/slate`, label: "Slate", icon: Icon.slate, match: "/slate" },
    { href: `/${sport}/results`, label: "Portfolio", icon: Icon.results, match: "/results" },
    { href: "/models", label: "My Models", icon: Icon.models, match: "/models" },
    { href: "/pricing", label: "Plans", icon: Icon.pricing, match: "/pricing" },
  ];
  return (
    <nav className="fixed inset-x-0 bottom-0 z-40 border-t border-ink-700 bg-ink-950/95 pb-[env(safe-area-inset-bottom)] backdrop-blur md:hidden">
      <div className="grid grid-cols-4">
        {tabs.map((t) => {
          const active = pathname.includes(t.match);
          return (
            <Link
              key={t.label}
              href={t.href}
              className={`flex min-h-14 flex-col items-center justify-center gap-1 text-[11px] font-medium ${active ? "text-court-400" : "text-zinc-500"}`}
            >
              <svg viewBox="0 0 24 24" className="h-5 w-5" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                {t.icon}
              </svg>
              {t.label}
            </Link>
          );
        })}
      </div>
    </nav>
  );
}
