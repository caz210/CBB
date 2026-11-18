import Link from "next/link";
import { Logo } from "./Logo";
import { NavLinks } from "./NavLinks";
import { SportSwitcher } from "./SportSwitcher";
import { TeamSearch } from "./TeamSearch";

export function SiteHeader() {
  return (
    <header className="sticky top-0 z-30 border-b border-ink-700/70 bg-ink-950/90 backdrop-blur">
      <div className="mx-auto flex h-14 max-w-7xl items-center gap-3 px-4 md:h-16 md:gap-6 md:px-6">
        <Logo />
        <NavLinks />
        <div className="ml-auto hidden xl:block">
          <SportSwitcher />
        </div>
        <div className="ml-auto flex shrink-0 items-center gap-1 md:gap-2 xl:ml-0 xl:gap-3">
          {/* Team search: icon + full-screen sheet below xl, inline box on xl+ (1280px).
              Below xl the sport switcher is its own row; below lg the tab bar replaces the top nav.
              MATCHUP MAKER: later, pass <TeamSearch footerSlot={<Link href="/matchup">Matchup maker</Link>} /> */}
          <TeamSearch />
          <Link href="/login" className="whitespace-nowrap rounded-lg px-2 py-2 text-sm font-medium text-zinc-300 hover:text-white">
            Log in
          </Link>
          <Link href="/signup" className="whitespace-nowrap rounded-lg bg-court-500 px-3 py-2 text-sm font-semibold text-ink-950 hover:bg-court-400">
            Sign up
          </Link>
        </div>
      </div>
      {/* below xl: sport switcher as its own scrollable row */}
      <div className="no-scrollbar overflow-x-auto border-t border-ink-700/50 px-4 py-2 md:px-6 xl:hidden">
        <SportSwitcher />
      </div>
    </header>
  );
}
