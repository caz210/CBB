"use client";
import Link from "next/link";
import { usePathname } from "next/navigation";
import { SPORTS, DEFAULT_SPORT } from "@/lib/sports";

export function NavLinks() {
  const pathname = usePathname();
  const seg = pathname.split("/")[1];
  const sport = SPORTS.some((s) => s.key === seg) ? seg : DEFAULT_SPORT;
  const links: { href: string; label: string; match: string; hideBelow?: "xl" }[] = [
    { href: `/${sport}/slate`, label: "Daily Slate", match: "/slate" },
    { href: `/${sport}/rankings`, label: "Rankings", match: "/rankings" },
    { href: `/${sport}/matchup`, label: "Matchup", match: "/matchup" },
    { href: `/${sport}/results`, label: "Portfolio", match: "/results" },
    { href: "/models", label: "My Models", match: "/models" },
    // Pricing stays at xl+ so the one-row header does not overflow at 1024–1279.
    { href: "/pricing", label: "Pricing", match: "/pricing", hideBelow: "xl" },
  ];
  return (
    <nav className="hidden shrink-0 items-center gap-1 lg:flex">
      {links.map((l) => {
        const active = pathname.includes(l.match);
        const hide = l.hideBelow === "xl" ? "hidden xl:inline-flex" : "";
        return (
          <Link
            key={l.label}
            href={l.href}
            className={`${hide} whitespace-nowrap rounded-lg px-2 py-2 text-sm font-medium transition lg:px-2.5 ${
              active ? "bg-ink-800 text-white" : "text-zinc-400 hover:text-white"
            }`}
          >
            {l.label}
          </Link>
        );
      })}
    </nav>
  );
}
