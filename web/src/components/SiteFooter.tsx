import Link from "next/link";

const links = [
  { href: "/about", label: "About" },
  { href: "/methodology", label: "Methodology" },
  { href: "/terms", label: "Terms" },
  { href: "/privacy", label: "Privacy" },
];

export function SiteFooter() {
  return (
    <footer className="mt-16 border-t border-ink-700/70 md:mt-24">
      <div className="mx-auto max-w-7xl px-4 py-6 md:px-6 md:py-8">
        <div className="flex flex-col gap-3 text-xs text-zinc-400 md:flex-row md:items-start md:justify-between">
          <div className="max-w-xl space-y-1.5">
            <p>© 2026 CZarpAnalytics.com · 21+ only.</p>
            <p>Not a sportsbook. For information/entertainment.</p>
            <p>
              If gambling is a problem, call{" "}
              <a href="tel:18004262537" className="font-semibold text-zinc-200 hover:text-white">1-800-GAMBLER</a>
              {" "}or visit{" "}
              <a href="https://www.ncpgambling.org" target="_blank" rel="noopener noreferrer" className="font-semibold text-zinc-200 underline-offset-2 hover:text-white hover:underline">ncpgambling.org</a>.
            </p>
          </div>
          <nav className="flex flex-wrap gap-x-4 gap-y-2" aria-label="Legal">
            {links.map((l) => (
              <Link key={l.href} href={l.href} className="hover:text-white">{l.label}</Link>
            ))}
          </nav>
        </div>
        <p className="mt-3 text-[11px] text-zinc-600">Projections are CZarp model outputs. Third-party raw data is not redistributed. Informational only, not betting advice.</p>
      </div>
    </footer>
  );
}
