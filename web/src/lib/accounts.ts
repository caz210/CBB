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

/** Header Log in / Sign up. Empty while accounts are hidden. */
export function headerAccountLinks(show: boolean = SHOW_ACCOUNTS): AccountLink[] {
  if (!show) return [];
  return [
    { href: "/login", label: "Log in", tone: "secondary" },
    { href: "/signup", label: "Sign up", tone: "primary" },
  ];
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

/** Paths that must not render an auth form while SHOW_ACCOUNTS is off. */
const HIDDEN_AUTH_PREFIXES = ["/login", "/signup", "/auth", "/account", "/callback"] as const;

/** True when this request should bounce to the home page. */
export function isHiddenAuthPath(pathname: string, show: boolean = SHOW_ACCOUNTS): boolean {
  if (show) return false;
  const path = pathname.split("?")[0].replace(/\/+$/, "") || "/";
  return HIDDEN_AUTH_PREFIXES.some((p) => path === p || path.startsWith(`${p}/`));
}

/** Destination for a hidden auth route, or null when the page may render. */
export function authRouteRedirect(pathname: string, show: boolean = SHOW_ACCOUNTS): string | null {
  return isHiddenAuthPath(pathname, show) ? "/" : null;
}

/** Every customer-visible CTA href from the gated surfaces (header, home, pricing, ComingSoon). */
export function visibleAccountHrefs(show: boolean = SHOW_ACCOUNTS): string[] {
  return [
    ...headerAccountLinks(show),
    ...homeCtas(show),
    ...pricingCtas(show),
    ...comingSoonCtas(show),
  ].map((l) => l.href);
}
