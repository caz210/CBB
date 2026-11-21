import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import {
  SHOW_ACCOUNTS,
  SLATE_HREF,
  authRouteRedirect,
  comingSoonCtas,
  headerAccountLinks,
  homeCtas,
  isHiddenAuthPath,
  pricingCtas,
  visibleAccountHrefs,
} from "../src/lib/accounts";

const AUTH_HREFS = new Set(["/login", "/signup"]);

const SURFACES: { name: string; file: string; links: (show?: boolean) => { href: string; label: string }[] }[] = [
  { name: "header", file: "src/components/SiteHeader.tsx", links: headerAccountLinks },
  { name: "home", file: "src/app/page.tsx", links: homeCtas },
  { name: "pricing", file: "src/app/pricing/page.tsx", links: pricingCtas },
  { name: "ComingSoon", file: "src/components/ComingSoon.tsx", links: comingSoonCtas },
];

/** Same markup the gated surfaces render: one <a> per CTA. */
function renderLinks(links: { href: string; label: string }[]): string {
  return links.map((l) => `<a href="${l.href}">${l.label}</a>`).join("");
}

function visibleAuthHrefs(html: string): string[] {
  return [...html.matchAll(/href="([^"]+)"/g)].map((m) => m[1]).filter((href) => AUTH_HREFS.has(href));
}

test("SHOW_ACCOUNTS is the single flag that hides account UI, and it is off", () => {
  assert.equal(SHOW_ACCOUNTS, false);
});

test("no visible link to /login or /signup renders when the flag is off (header, home, pricing, ComingSoon)", () => {
  assert.equal(SHOW_ACCOUNTS, false);
  for (const surface of SURFACES) {
    const html = renderLinks(surface.links());
    assert.deepEqual(
      visibleAuthHrefs(html),
      [],
      `${surface.name} rendered a login/signup link: ${html}`,
    );
  }
  for (const href of visibleAccountHrefs()) {
    assert.ok(!AUTH_HREFS.has(href), `unexpected auth href ${href}`);
  }
});

test("header, home, pricing and ComingSoon source use the shared helpers and do not hardcode auth hrefs", () => {
  const root = process.cwd();
  for (const surface of SURFACES) {
    const src = readFileSync(join(root, surface.file), "utf8");
    assert.match(src, /from "@\/lib\/accounts"/, `${surface.name} should import @/lib/accounts`);
    assert.doesNotMatch(src, /href=["']\/(login|signup)["']/, `${surface.name} hardcodes an auth href`);
    assert.doesNotMatch(src, /href:\s*["']\/(login|signup)["']/, `${surface.name} hardcodes an auth href in data`);
  }
});

test("when the flag is on, the same surfaces render login and signup links again", () => {
  const html = [
    renderLinks(headerAccountLinks(true)),
    renderLinks(homeCtas(true)),
    renderLinks(pricingCtas(true)),
    renderLinks(comingSoonCtas(true)),
  ].join("");
  assert.ok(html.includes('href="/login"'));
  assert.ok(html.includes('href="/signup"'));
  assert.equal(headerAccountLinks(true)[0]?.label, "Log in");
  assert.equal(headerAccountLinks(true)[1]?.label, "Sign up");
  assert.equal(homeCtas(true)[0]?.href, "/signup");
  assert.ok(pricingCtas(true).every((l) => l.href === "/signup"));
  assert.ok(comingSoonCtas(true).some((l) => l.href === "/signup" && l.label === "Notify me"));
});

test("hidden account CTAs point at the slate, with no account or waitlist language", () => {
  assert.deepEqual(
    homeCtas().map((l) => l.label),
    ["See today's slate"],
  );
  assert.ok(homeCtas().every((l) => l.href === SLATE_HREF));
  assert.ok(pricingCtas().every((l) => l.href === SLATE_HREF && l.label === "Free during beta"));
  assert.deepEqual(
    comingSoonCtas().map((l) => l.label),
    ["Go to CBB slate"],
  );
  assert.equal(headerAccountLinks().length, 0);
});

test("/login, /signup and future auth callback/account routes redirect home while the flag is off", () => {
  for (const path of ["/login", "/signup", "/login/", "/signup/extra", "/auth/callback", "/account", "/callback"]) {
    assert.equal(isHiddenAuthPath(path), true, path);
    assert.equal(authRouteRedirect(path), "/");
  }
  assert.equal(authRouteRedirect("/"), null);
  assert.equal(authRouteRedirect("/pricing"), null);
  assert.equal(authRouteRedirect("/cbb/slate"), null);
  assert.equal(isHiddenAuthPath("/accounting"), false);
  assert.equal(authRouteRedirect("/login", true), null);
});
