// PWA manifest constants, iOS Home Screen hint rules, no service worker. Run: npm test
import { test } from "node:test";
import assert from "node:assert/strict";
import { existsSync, readFileSync, readdirSync, statSync } from "node:fs";
import { join, resolve } from "node:path";
import {
  IOS_HOME_HINT_COPY,
  IOS_HOME_HINT_KEY,
  PWA_BACKGROUND_COLOR,
  PWA_DISPLAY,
  PWA_NAME,
  PWA_SCOPE,
  PWA_START_URL,
  PWA_THEME_COLOR,
  isIosSafari,
  shouldShowIosHomeHint,
} from "../src/lib/pwa";

const root = [resolve(__dirname, ".."), resolve(__dirname, "../..")].find((d) => {
  try { return statSync(join(d, "src/app/globals.css")).isFile(); } catch { return false; }
})!;
const read = (rel: string) => readFileSync(join(root, rel), "utf8");

const IPHONE_SAFARI =
  "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1";
const IPHONE_CHROME =
  "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/120.0.0.0 Mobile/15E148 Safari/604.1";
const IPHONE_FIREFOX =
  "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) FxiOS/120.0 Mobile/15E148 Safari/604.1";
const IPAD_DESKTOP =
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15";
const ANDROID_CHROME =
  "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36";
const DESKTOP_SAFARI =
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15";

test("theme and background colors are the site ink-950 token, not invented", () => {
  const css = read("src/app/globals.css");
  assert.match(css, /--color-ink-950:\s*#07090d/);
  assert.equal(PWA_THEME_COLOR, "#07090d");
  assert.equal(PWA_BACKGROUND_COLOR, "#07090d");
  assert.equal(PWA_NAME, "CZarp");
  assert.equal(PWA_START_URL, "/cbb/slate");
  assert.equal(PWA_SCOPE, "/");
  assert.equal(PWA_DISPLAY, "standalone");
});

test("manifest.ts uses the shared PWA constants and the three icon files", () => {
  const src = read("src/app/manifest.ts");
  assert.match(src, /name:\s*PWA_NAME/);
  assert.match(src, /short_name:\s*PWA_NAME/);
  assert.match(src, /start_url:\s*PWA_START_URL/);
  assert.match(src, /scope:\s*PWA_SCOPE/);
  assert.match(src, /display:\s*PWA_DISPLAY/);
  assert.match(src, /theme_color:\s*PWA_THEME_COLOR/);
  assert.match(src, /background_color:\s*PWA_BACKGROUND_COLOR/);
  assert.match(src, /icon-192\.png/);
  assert.match(src, /icon-512\.png/);
  assert.match(src, /icon-512-maskable\.png/);
  assert.match(src, /purpose:\s*"maskable"/);
  for (const f of ["public/icons/icon-192.png", "public/icons/icon-512.png", "public/icons/icon-512-maskable.png", "public/icons/apple-touch-icon.png"]) {
    assert.ok(existsSync(join(root, f)), f);
  }
});

test("root layout sets iOS / theme-color metadata and the apple-touch icon", () => {
  const src = read("src/app/layout.tsx");
  assert.match(src, /themeColor:\s*PWA_THEME_COLOR/);
  assert.match(src, /appleWebApp:/);
  assert.match(src, /title:\s*PWA_NAME/);
  assert.match(src, /capable:\s*true/);
  assert.match(src, /statusBarStyle:\s*"black"/);
  assert.match(src, /apple-mobile-web-app-capable/);
  assert.match(src, /apple-touch-icon\.png/);
  assert.match(src, /<IosHomeHint/);
});

test("the iOS hint is shown once, only in iOS Safari, and never when already installed", () => {
  const base = { userAgent: IPHONE_SAFARI, standalone: false, dismissed: false };
  assert.equal(shouldShowIosHomeHint(base), true);
  assert.equal(shouldShowIosHomeHint({ ...base, dismissed: true }), false);
  assert.equal(shouldShowIosHomeHint({ ...base, standalone: true }), false);
  assert.equal(isIosSafari(IPHONE_CHROME), false);
  assert.equal(shouldShowIosHomeHint({ ...base, userAgent: IPHONE_CHROME }), false);
  assert.equal(shouldShowIosHomeHint({ ...base, userAgent: IPHONE_FIREFOX }), false);
  assert.equal(shouldShowIosHomeHint({ ...base, userAgent: ANDROID_CHROME }), false);
  assert.equal(shouldShowIosHomeHint({ ...base, userAgent: DESKTOP_SAFARI, maxTouchPoints: 0 }), false);
  assert.equal(shouldShowIosHomeHint({ ...base, userAgent: IPAD_DESKTOP, maxTouchPoints: 5 }), true);
  assert.equal(shouldShowIosHomeHint({ ...base, userAgent: IPAD_DESKTOP, maxTouchPoints: 0 }), false);
  assert.match(IOS_HOME_HINT_COPY, /Add CZarp to your Home Screen: tap Share, then Add to Home Screen\./);
  assert.equal(IOS_HOME_HINT_KEY, "czarp.ios-homescreen-hint.dismissed");
  const hint = read("src/components/IosHomeHint.tsx");
  assert.match(hint, /useEffect/);
  assert.match(hint, /useState\(false\)/);
  assert.match(hint, /localStorage/);
  assert.doesNotMatch(hint, /serviceWorker|navigator\.serviceWorker/);
});

test("there is no service worker in the app source", () => {
  const hits: string[] = [];
  const skip = new Set(["node_modules", ".next", ".test-build", "coverage"]);
  const walk = (dir: string) => {
    for (const f of readdirSync(dir)) {
      if (skip.has(f)) continue;
      const p = join(dir, f);
      if (statSync(p).isDirectory()) walk(p);
      else if (/(^sw\.js$|service-worker|workbox)/i.test(f)) hits.push(p);
    }
  };
  walk(root);
  assert.deepEqual(hits, []);
  const srcFiles = [
    "src/app/layout.tsx",
    "src/app/manifest.ts",
    "src/lib/pwa.ts",
    "src/components/IosHomeHint.tsx",
    "scripts/generate-pwa-icons.py",
  ];
  for (const rel of srcFiles) {
    const src = read(rel);
    assert.doesNotMatch(src, /KenPom|kenpom|\bpaper\b|v1\/v2/i, rel);
  }
});
