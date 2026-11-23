import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import {
  SHOW_ACCOUNTS,
  SLATE_HREF,
  authRouteRedirect,
  bottomNavLinks,
  comingSoonCtas,
  footerLinks,
  headerAccountLinks,
  headerNavLinks,
  hiddenAccountRedirect,
  homeCtas,
  homePromoCtas,
  isHiddenAuthPath,
  pricingCtas,
  visibleAccountHrefs,
} from "../src/lib/accounts";

const FORBIDDEN_HREFS = new Set(["/login", "/signup", "/models", "/pricing"]);
const SPORT = "cbb";

type Link = { href: string; label: string };

const SURFACES: { name: string; files: string[]; links: (show?: boolean) => Link[] }[] = [
  {
    name: "header",
    files: ["src/components/SiteHeader.tsx", "src/components/NavLinks.tsx", "src/components/BottomNav.tsx"],
    links: (show?: boolean) => [
      ...headerAccountLinks(show),
      ...headerNavLinks(SPORT, show),
      ...bottomNavLinks(SPORT, show),
    ],
  },
  {
    name: "footer",
    files: ["src/components/SiteFooter.tsx"],
    links: (show?: boolean) => footerLinks(show),
  },
  {
    name: "home",
    files: ["src/app/page.tsx"],
    links: (show?: boolean) => [...homeCtas(show), ...homePromoCtas(show)],
  },
  {
    name: "slate",
    files: ["src/app/[sport]/slate/page.tsx", "src/components/SlateBoard.tsx"],
    links: () => [],
  },
  {
    name: "team page",
    files: ["src/app/[sport]/team/[slug]/page.tsx"],
    links: () => [],
  },
  {
    name: "matchup",
    files: ["src/app/[sport]/matchup/page.tsx", "src/components/MatchupMaker.tsx"],
    links: () => [],
  },
  {
    name: "ComingSoon",
    files: ["src/components/ComingSoon.tsx"],
    links: (show?: boolean) => comingSoonCtas(show),
  },
];

/** Same markup the gated surfaces render: one <a> per CTA or nav item. */
function renderLinks(links: Link[]): string {
  return links.map((l) => `<a href="${l.href}">${l.label}</a>`).join("");
}

function visibleForbiddenHrefs(html: string): string[] {
  return [...html.matchAll(/href="([^"]+)"/g)]
    .map((m) => m[1])
    .filter((href) => FORBIDDEN_HREFS.has(href) || [...FORBIDDEN_HREFS].some((p) => href.startsWith(`${p}/`)));
}

const HARDCODED_HREF = /href=["']\/(login|signup|models|pricing)(?:\/[^"']*)?["']/;
const HARDCODED_DATA = /href:\s*["']\/(login|signup|models|pricing)(?:\/[^"']*)?["']/;

test("SHOW_ACCOUNTS is the single flag that hides account UI, and it is off", () => {
  assert.equal(SHOW_ACCOUNTS, false);
});

test("no visible link to /login, /signup, /models or /pricing renders on the header, footer, home, slate, team page, matchup and ComingSoon", () => {
  assert.equal(SHOW_ACCOUNTS, false);
  for (const surface of SURFACES) {
    const html = renderLinks(surface.links());
    assert.deepEqual(
      visibleForbiddenHrefs(html),
      [],
      `${surface.name} rendered a forbidden link: ${html}`,
    );
  }
  // Every page also gets header, footer and bottom-nav chrome from the root layout.
  const chrome = renderLinks([
    ...headerAccountLinks(),
    ...headerNavLinks(SPORT),
    ...bottomNavLinks(SPORT),
    ...footerLinks(),
  ]);
  assert.deepEqual(visibleForbiddenHrefs(chrome), [], `layout chrome rendered a forbidden link: ${chrome}`);
  for (const href of visibleAccountHrefs()) {
    assert.ok(!FORBIDDEN_HREFS.has(href), `unexpected href ${href}`);
  }
});

test("header, footer, home and ComingSoon source use the shared helpers and do not hardcode account hrefs", () => {
  const root = process.cwd();
  const helperSurfaces = SURFACES.filter((s) => ["header", "footer", "home", "ComingSoon"].includes(s.name));
  for (const surface of helperSurfaces) {
    for (const file of surface.files) {
      const src = readFileSync(join(root, file), "utf8");
      assert.match(src, /from "@\/lib\/accounts"/, `${file} should import @/lib/accounts`);
      assert.doesNotMatch(src, HARDCODED_HREF, `${file} hardcodes an account href`);
      assert.doesNotMatch(src, HARDCODED_DATA, `${file} hardcodes an account href in data`);
    }
  }
});

test("slate, team page and matchup source have no /login, /signup, /models or /pricing links", () => {
  const root = process.cwd();
  const contentSurfaces = SURFACES.filter((s) => ["slate", "team page", "matchup"].includes(s.name));
  for (const surface of contentSurfaces) {
    for (const file of surface.files) {
      const src = readFileSync(join(root, file), "utf8");
      assert.doesNotMatch(src, HARDCODED_HREF, `${file} hardcodes an account href`);
      assert.doesNotMatch(src, HARDCODED_DATA, `${file} hardcodes an account href in data`);
    }
  }
});

test("when the flag is on, login, signup, models and pricing links come back", () => {
  const html = [
    renderLinks(headerAccountLinks(true)),
    renderLinks(headerNavLinks(SPORT, true)),
    renderLinks(bottomNavLinks(SPORT, true)),
    renderLinks(footerLinks(true)),
    renderLinks(homeCtas(true)),
    renderLinks(homePromoCtas(true)),
    renderLinks(pricingCtas(true)),
    renderLinks(comingSoonCtas(true)),
  ].join("");
  assert.ok(html.includes('href="/login"'));
  assert.ok(html.includes('href="/signup"'));
  assert.ok(html.includes('href="/models"'));
  assert.ok(html.includes('href="/pricing"'));
  assert.equal(headerAccountLinks(true)[0]?.label, "Log in");
  assert.equal(headerAccountLinks(true)[1]?.label, "Sign up");
  assert.ok(headerNavLinks(SPORT, true).some((l) => l.href === "/models" && l.label === "My Models"));
  assert.ok(headerNavLinks(SPORT, true).some((l) => l.href === "/pricing" && l.label === "Pricing"));
  assert.ok(bottomNavLinks(SPORT, true).some((l) => l.href === "/models"));
  assert.ok(footerLinks(true).some((l) => l.href === "/pricing"));
  assert.equal(homeCtas(true)[0]?.href, "/signup");
  assert.deepEqual(homePromoCtas(true).map((l) => l.href), ["/models"]);
  assert.ok(pricingCtas(true).every((l) => l.href === "/signup"));
  assert.ok(comingSoonCtas(true).some((l) => l.href === "/signup" && l.label === "Notify me"));
});

test("hidden account CTAs point at the slate, with no account or waitlist language", () => {
  assert.deepEqual(
    homeCtas().map((l) => ({ href: l.href, label: l.label })),
    [
      { href: SLATE_HREF, label: "Today's slate" },
      { href: "/cbb/results", label: "See the record" },
    ],
  );
  assert.equal(homePromoCtas().length, 0);
  assert.ok(pricingCtas().every((l) => l.href === SLATE_HREF && l.label === "Free during beta"));
  assert.deepEqual(
    comingSoonCtas().map((l) => l.label),
    ["Go to CBB slate"],
  );
  assert.equal(headerAccountLinks().length, 0);
  assert.ok(headerNavLinks(SPORT).every((l) => !FORBIDDEN_HREFS.has(l.href)));
  assert.equal(bottomNavLinks(SPORT).length, 4);
  assert.ok(!footerLinks().some((l) => l.href === "/pricing"));
});

test("/login, /signup and /pricing redirect home; /models redirects to the slate, while the flag is off", () => {
  for (const path of ["/login", "/signup", "/login/", "/signup/extra", "/auth/callback", "/account", "/callback", "/pricing", "/pricing/team"]) {
    assert.equal(isHiddenAuthPath(path), true, path);
    assert.equal(hiddenAccountRedirect(path), "/");
    assert.equal(authRouteRedirect(path), "/");
  }
  for (const path of ["/models", "/models/", "/models/preview"]) {
    assert.equal(isHiddenAuthPath(path), true, path);
    assert.equal(hiddenAccountRedirect(path), SLATE_HREF);
  }
  assert.equal(hiddenAccountRedirect("/"), null);
  assert.equal(hiddenAccountRedirect("/cbb/slate"), null);
  assert.equal(hiddenAccountRedirect("/about"), null);
  assert.equal(hiddenAccountRedirect("/methodology"), null);
  assert.equal(isHiddenAuthPath("/accounting"), false);
  assert.equal(hiddenAccountRedirect("/login", true), null);
  assert.equal(hiddenAccountRedirect("/models", true), null);
  assert.equal(hiddenAccountRedirect("/pricing", true), null);
});
