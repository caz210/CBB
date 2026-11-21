/** Account CTAs and hidden-auth routing, gated by SHOW_ACCOUNTS. */
import { SHOW_ACCOUNTS } from "./flags";
import { DEFAULT_SPORT } from "./sports";

export { SHOW_ACCOUNTS };

export const SLATE_HREF = `/${DEFAULT_SPORT}/slate`;

export type AccountLink = {
  href: string;
  label: string;
  /** Visual treatment for the control. */
  tone: "primary" | "secondary";
};

export type NavItem = {
  href: string;
  label: string;
  match: string;
  hideBelow?: "xl";
};

/** Header Log in / Sign up. Empty while accounts are hidden. */
export function headerAccountLinks(show: boolean = SHOW_ACCOUNTS): AccountLink[] {
  if (!show) return [];
  return [
    { href: "/login", label: "Log in", tone: "secondary" },
    { href: "/signup", label: "Sign up", tone: "primary" },
  ];
}

/** Desktop header nav. My Models and Pricing are omitted while accounts are hidden. */
export function headerNavLinks(sport: string, show: boolean = SHOW_ACCOUNTS): NavItem[] {
  const links: NavItem[] = [
    { href: `/${sport}/slate`, label: "Daily Slate", match: "/slate" },
    { href: `/${sport}/rankings`, label: "Rankings", match: "/rankings" },
    { href: `/${sport}/matchup`, label: "Matchup", match: "/matchup" },
    { href: `/${sport}/results`, label: "Portfolio", match: "/results" },
  ];
  if (show) {
    links.push({ href: "/models", label: "My Models", match: "/models" });
    // Pricing stays at xl+ so the one-row header does not overflow at 1024–1279.
    links.push({ href: "/pricing", label: "Pricing", match: "/pricing", hideBelow: "xl" });
  }
  return links;
}

/** Mobile tab bar. My Models is omitted while accounts are hidden. */
export function bottomNavLinks(sport: string, show: boolean = SHOW_ACCOUNTS): NavItem[] {
  const links: NavItem[] = [
    { href: `/${sport}/slate`, label: "Slate", match: "/slate" },
    { href: `/${sport}/rankings`, label: "Rankings", match: "/rankings" },
    { href: `/${sport}/matchup`, label: "Matchup", match: "/matchup" },
    { href: `/${sport}/results`, label: "Portfolio", match: "/results" },
  ];
  if (show) links.push({ href: "/models", label: "My Models", match: "/models" });
  return links;
}

/** Footer legal nav. Pricing is omitted while accounts are hidden. */
export function footerLinks(show: boolean = SHOW_ACCOUNTS): { href: string; label: string }[] {
  const rest = [
    { href: "/about", label: "About" },
    { href: "/methodology", label: "Methodology" },
    { href: "/terms", label: "Terms" },
    { href: "/privacy", label: "Privacy" },
  ];
  return show ? [{ href: "/pricing", label: "Pricing" }, ...rest] : rest;
}

/** Landing-page hero CTAs. */
export function homeCtas(show: boolean = SHOW_ACCOUNTS): AccountLink[] {
  if (!show) {
    return [{ href: SLATE_HREF, label: "See today's slate", tone: "primary" }];
  }
  return [
    { href: "/signup", label: "Create free account", tone: "primary" },
    { href: SLATE_HREF, label: "See today's slate →", tone: "secondary" },
  ];
}

/** Home "Try My Models" promo. Empty while accounts are hidden. */
export function homePromoCtas(show: boolean = SHOW_ACCOUNTS): AccountLink[] {
  if (!show) return [];
  return [{ href: "/models", label: "Try My Models", tone: "primary" }];
}

export type PricingTierKey = "free" | "pro" | "syndicate";

/** CTA on a pricing card. No account or waitlist language while the flag is off. */
export function pricingCta(tier: PricingTierKey, show: boolean = SHOW_ACCOUNTS): AccountLink {
  if (!show) {
    return { href: SLATE_HREF, label: "Free during beta", tone: tier === "pro" ? "primary" : "secondary" };
  }
  if (tier === "free") return { href: "/signup", label: "Create free account", tone: "secondary" };
  if (tier === "pro") return { href: "/signup", label: "Join the Pro waitlist", tone: "primary" };
  return { href: "/signup", label: "Contact us", tone: "secondary" };
}

export function pricingCtas(show: boolean = SHOW_ACCOUNTS): AccountLink[] {
  return (["free", "pro", "syndicate"] as const).map((tier) => pricingCta(tier, show));
}

/** Coming-soon sport page. The signup "Notify me" control is omitted while accounts are hidden. */
export function comingSoonCtas(show: boolean = SHOW_ACCOUNTS): AccountLink[] {
  const slate: AccountLink = { href: SLATE_HREF, label: "Go to CBB slate", tone: "primary" };
  if (!show) return [slate];
  return [slate, { href: "/signup", label: "Notify me", tone: "secondary" }];
}

type HiddenRoute = { prefixes: readonly string[]; to: string };

/** Paths that must not render while SHOW_ACCOUNTS is off, and where they bounce. */
const HIDDEN_ROUTES: readonly HiddenRoute[] = [
  { prefixes: ["/login", "/signup", "/auth", "/account", "/callback", "/pricing"], to: "/" },
  { prefixes: ["/models"], to: SLATE_HREF },
];

function normalizePath(pathname: string): string {
  return pathname.split("?")[0].replace(/\/+$/, "") || "/";
}

function matchesPrefix(path: string, prefix: string): boolean {
  return path === prefix || path.startsWith(`${prefix}/`);
}

/** Destination for a hidden account route, or null when the page may render. */
export function hiddenAccountRedirect(pathname: string, show: boolean = SHOW_ACCOUNTS): string | null {
  if (show) return null;
  const path = normalizePath(pathname);
  for (const route of HIDDEN_ROUTES) {
    if (route.prefixes.some((p) => matchesPrefix(path, p))) return route.to;
  }
  return null;
}

/** @deprecated use hiddenAccountRedirect — kept as the name middleware already imported. */
export const authRouteRedirect = hiddenAccountRedirect;

/** True when this request should bounce away from an account surface. */
export function isHiddenAuthPath(pathname: string, show: boolean = SHOW_ACCOUNTS): boolean {
  return hiddenAccountRedirect(pathname, show) != null;
}

const DEFAULT_NAV_SPORT = DEFAULT_SPORT;

/** Every customer-visible CTA/nav href from the gated chrome and landing surfaces. */
export function visibleAccountHrefs(show: boolean = SHOW_ACCOUNTS, sport: string = DEFAULT_NAV_SPORT): string[] {
  return [
    ...headerAccountLinks(show),
    ...headerNavLinks(sport, show),
    ...bottomNavLinks(sport, show),
    ...footerLinks(show),
    ...homeCtas(show),
    ...homePromoCtas(show),
    ...comingSoonCtas(show),
  ].map((l) => l.href);
}
