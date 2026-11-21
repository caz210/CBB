import Link from "next/link";
import { Logo } from "./Logo";
import { NavLinks } from "./NavLinks";
import { SportSwitcher } from "./SportSwitcher";
import { TeamSearch } from "./TeamSearch";
import { DEFAULT_SPORT, SPORTS } from "@/lib/sports";
import { matchupHref } from "@/lib/matchup";

export function SiteHeader() {
  // The switcher (and the row it used below xl) only appear when a second sport is enabled in SPORTS.
  const many = SPORTS.length > 1;
  return (
    <header className="sticky top-0 z-30 border-b border-ink-700/70 bg-ink-950/90 backdrop-blur">
      <div className="mx-auto flex h-14 max-w-7xl items-center gap-1.5 px-3 min-[360px]:gap-2 min-[360px]:px-4 sm:gap-3 md:h-16 md:gap-6 md:px-6">
        <Logo />
        <NavLinks />
        {many && (
          <div className="ml-auto hidden xl:block">
            <SportSwitcher />
          </div>
        )}
        <div className={`flex shrink-0 items-center gap-0.5 sm:gap-1 md:gap-2 ${many ? "ml-auto xl:ml-0 xl:gap-3" : "ml-auto xl:gap-3"}`}>
          {/* Team search: icon + full-screen sheet below xl, inline box on xl+ (1280px).
              Below lg the tab bar replaces the top nav. */}
          <TeamSearch
            footerSlot={
              <Link href={matchupHref(DEFAULT_SPORT)} className="flex min-h-11 items-center justify-between rounded-xl border border-ink-600 px-4 text-sm font-semibold text-white hover:bg-ink-800">
                <span>Build a matchup</span>
                <span className="text-xs font-medium text-zinc-400">Any two teams · pick the court</span>
              </Link>
            }
          />
          <Link href="/login" className="whitespace-nowrap rounded-lg px-1.5 py-2 sm:px-2 text-sm font-medium text-zinc-300 hover:text-white">
            Log in
          </Link>
          <Link href="/signup" className="whitespace-nowrap rounded-lg bg-court-500 px-2 py-2 text-sm font-semibold text-ink-950 hover:bg-court-400 min-[360px]:px-3">
            Sign up
          </Link>
        </div>
      </div>
      {many && (
        <div className="no-scrollbar overflow-x-auto border-t border-ink-700/50 px-4 py-2 md:px-6 xl:hidden">
          <SportSwitcher />
        </div>
      )}
    </header>
  );
}
