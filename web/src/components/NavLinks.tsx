"use client";
import Link from "next/link";
import { usePathname } from "next/navigation";
import { SPORTS, DEFAULT_SPORT } from "@/lib/sports";

export function NavLinks() {
  const pathname = usePathname();
  const seg = pathname.split("/")[1];
  const sport = SPORTS.some((s) => s.key === seg) ? seg : DEFAULT_SPORT;
  const links = [
    { href: `/${sport}/slate`, label: "Daily Slate", match: "/slate" },
    { href: `/${sport}/results`, label: "Portfolio", match: "/results" },
    { href: "/models", label: "My Models", match: "/models" },
    { href: "/pricing", label: "Pricing", match: "/pricing" },
  ];
  return (
    <nav className="hidden shrink-0 items-center gap-1 lg:flex">
      {links.map((l) => {
        const active = pathname.includes(l.match);
        return (
          <Link
            key={l.label}
            href={l.href}
            className={`whitespace-nowrap rounded-lg px-3 py-2 text-sm font-medium transition ${
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
