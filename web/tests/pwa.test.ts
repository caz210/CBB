// PWA manifest constants, Home Screen hint rules (UA / platform), no service worker. Run: npm test
import { test } from "node:test";
import assert from "node:assert/strict";
import { existsSync, readFileSync, readdirSync, statSync } from "node:fs";
import { join, resolve } from "node:path";
import {
  HOME_HINT_KEY,
  HOME_HINT_KEY_LEGACY,
  INSTALL_HOME_HINT_COPY,
  IOS_SHARE_HOME_HINT_COPY,
  PWA_BACKGROUND_COLOR,
  PWA_DISPLAY,
  PWA_NAME,
  PWA_SCOPE,
  PWA_SHORT_NAME,
  PWA_START_URL,
  PWA_THEME_COLOR,
  SAFARI_HOME_HINT_COPY,
  homeHintCopy,
  homeHintDismissed,
  homeHintKind,
  isInAppBrowser,
  isIosSafari,
  isIosShareBrowser,
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
const IPHONE_EDGE =
  "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 EdgiOS/120.0.0.0 Mobile/15E148 Safari/605.1.15";
const IPAD_DESKTOP =
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15";
const ANDROID_CHROME =
  "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36";
const DESKTOP_CHROME =
  "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";
const DESKTOP_SAFARI =
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15";
const INSTAGRAM_IOS =
  "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 Instagram 10.0.0.0.0";
const FACEBOOK_IOS =
  "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 [FBAN/FBIOS;FBAV/10.0.0.0.0;]";
const FACEBOOK_ANDROID =
  "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/120.0.0.0 Mobile Safari/537.36 [FBAN/FB4A;FBAV/10.0.0.0.0;]";
const LINE_ANDROID =
  "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/120.0.0.0 Mobile Safari/537.36 Line/13.0.0";
const TIKTOK_IOS =
  "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 musical_ly_30.0.0 BytedanceWebview";

const open = { standalone: false, dismissed: false, hasInstallPrompt: false };

test("theme and background colors are the site ink-950 token, not invented", () => {
  const css = read("src/app/globals.css");
  assert.match(css, /--color-ink-950:\s*#07090d/);
  assert.equal(PWA_THEME_COLOR, "#07090d");
  assert.equal(PWA_BACKGROUND_COLOR, "#07090d");
  assert.equal(PWA_NAME, "CZarp Analytics");
  assert.equal(PWA_SHORT_NAME, "CZarp");
  assert.equal(PWA_START_URL, "/cbb/slate");
  assert.equal(PWA_SCOPE, "/");
  assert.equal(PWA_DISPLAY, "standalone");
});

test("manifest.ts uses the shared PWA constants and the three icon files", () => {
  const src = read("src/app/manifest.ts");
  assert.match(src, /name:\s*PWA_NAME/);
  assert.match(src, /short_name:\s*PWA_SHORT_NAME/);
  assert.match(src, /start_url:\s*PWA_START_URL/);
  assert.match(src, /scope:\s*PWA_SCOPE/);
  assert.match(src, /display:\s*PWA_DISPLAY/);
  assert.match(src, /theme_color:\s*PWA_THEME_COLOR/);
  assert.match(src, /background_color:\s*PWA_BACKGROUND_COLOR/);
  assert.match(src, /icon-192\.png/);
  assert.match(src, /icon-512\.png/);
  assert.match(src, /icon-512-maskable\.png/);
  assert.match(src, /purpose:\s*"maskable"/);
  for (const f of [
    "public/icons/icon-192.png",
    "public/icons/icon-512.png",
    "public/icons/icon-512-maskable.png",
    "public/icons/apple-touch-icon.png",
    "public/apple-touch-icon.png",
    "public/apple-touch-icon-precomposed.png",
  ]) {
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
  assert.match(src, /<HomeScreenHint/);
  assert.doesNotMatch(src, /IosHomeHint/);
  assert.match(src, /beforeInteractive/);
  assert.match(src, /window\.__bip/);
  assert.match(src, /beforeinstallprompt/);
  assert.match(src, /preventDefault/);
  assert.match(src, /metadataBase:\s*new URL\(SITE_URL\)/);
  assert.match(src, /openGraph:/);
  assert.match(src, /twitter:/);
  assert.match(src, /summary_large_image/);
  assert.match(src, /\/og\.png/);
  assert.match(src, /alt:\s*"CZarp Analytics"/);
});

test("iOS Safari gets the Safari Share copy, and not the Chrome address-bar copy", () => {
  assert.equal(isIosSafari(IPHONE_SAFARI), true);
  assert.equal(isIosShareBrowser(IPHONE_SAFARI), false);
  assert.equal(homeHintKind({ ...open, userAgent: IPHONE_SAFARI }), "safari");
  assert.equal(homeHintCopy("safari"), SAFARI_HOME_HINT_COPY);
  assert.match(SAFARI_HOME_HINT_COPY, /tap Share, then Add to Home Screen/);
  assert.doesNotMatch(SAFARI_HOME_HINT_COPY, /address bar/);
  assert.equal(homeHintKind({ ...open, userAgent: IPHONE_SAFARI, dismissed: true }), null);
  assert.equal(homeHintKind({ ...open, userAgent: IPHONE_SAFARI, standalone: true }), null);
  assert.equal(homeHintKind({ ...open, userAgent: IPAD_DESKTOP, maxTouchPoints: 5 }), "safari");
  assert.equal(homeHintKind({ ...open, userAgent: IPAD_DESKTOP, maxTouchPoints: 0 }), null);
  assert.equal(homeHintKind({ ...open, userAgent: DESKTOP_SAFARI, maxTouchPoints: 0 }), null);
});

test("iOS Chrome, Edge, and Firefox get the Share-in-the-address-bar copy, not Safari's", () => {
  for (const ua of [IPHONE_CHROME, IPHONE_EDGE, IPHONE_FIREFOX]) {
    assert.equal(isIosSafari(ua), false, ua);
    assert.equal(isIosShareBrowser(ua), true, ua);
    assert.equal(homeHintKind({ ...open, userAgent: ua }), "ios-share", ua);
    // A beforeinstallprompt event must not flip iOS Chrome over to the Install button / Safari copy.
    assert.equal(homeHintKind({ ...open, userAgent: ua, hasInstallPrompt: true }), "ios-share", ua);
  }
  assert.equal(homeHintCopy("ios-share"), IOS_SHARE_HOME_HINT_COPY);
  assert.match(IOS_SHARE_HOME_HINT_COPY, /Share icon in the address bar/);
  assert.notEqual(IOS_SHARE_HOME_HINT_COPY, SAFARI_HOME_HINT_COPY);
  assert.equal(homeHintKind({ ...open, userAgent: IPHONE_CHROME, dismissed: true }), null);
  assert.equal(homeHintKind({ ...open, userAgent: IPHONE_CHROME, standalone: true }), null);
});

test("Android and desktop Chrome show the Install hint only after beforeinstallprompt is stashed", () => {
  for (const ua of [ANDROID_CHROME, DESKTOP_CHROME]) {
    assert.equal(homeHintKind({ ...open, userAgent: ua, hasInstallPrompt: false }), null, ua);
    assert.equal(homeHintKind({ ...open, userAgent: ua, hasInstallPrompt: true }), "install", ua);
    assert.equal(homeHintKind({ ...open, userAgent: ua, hasInstallPrompt: true, dismissed: true }), null, ua);
    assert.equal(homeHintKind({ ...open, userAgent: ua, hasInstallPrompt: true, standalone: true }), null, ua);
  }
  assert.equal(homeHintCopy("install"), INSTALL_HOME_HINT_COPY);
  assert.equal(INSTALL_HOME_HINT_COPY, "Add CZarp Analytics to your Home Screen.");
});

test("in-app browsers (Instagram, Facebook, Line, TikTok) never get the hint", () => {
  for (const ua of [INSTAGRAM_IOS, FACEBOOK_IOS, FACEBOOK_ANDROID, LINE_ANDROID, TIKTOK_IOS]) {
    assert.equal(isInAppBrowser(ua), true, ua);
    assert.equal(homeHintKind({ ...open, userAgent: ua }), null, ua);
    assert.equal(homeHintKind({ ...open, userAgent: ua, hasInstallPrompt: true }), null, ua);
  }
  assert.equal(isInAppBrowser(IPHONE_SAFARI), false);
  assert.equal(isInAppBrowser(IPHONE_CHROME), false);
  assert.equal(isInAppBrowser(ANDROID_CHROME), false);
});

test("a stored dismiss (current or legacy key) hides the hint", () => {
  assert.equal(homeHintDismissed(null, null), false);
  assert.equal(homeHintDismissed("1", null), true);
  assert.equal(homeHintDismissed(null, "1"), true);
  assert.equal(homeHintDismissed("0", "0"), false);
  assert.equal(HOME_HINT_KEY, "czarp.homescreen-hint.dismissed");
  assert.equal(HOME_HINT_KEY_LEGACY, "czarp.ios-homescreen-hint.dismissed");
  const hint = read("src/components/HomeScreenHint.tsx");
  assert.match(hint, /useEffect/);
  assert.match(hint, /useState<HomeHintKind>\(null\)/);
  assert.match(hint, /beforeinstallprompt/);
  assert.match(hint, /__bip/);
  assert.match(hint, /appinstalled/);
  assert.match(hint, /localStorage/);
  assert.match(hint, />\s*Install\s*</);
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
    "src/components/HomeScreenHint.tsx",
    "scripts/generate-pwa-icons.py",
  ];
  for (const rel of srcFiles) {
    const src = read(rel);
    assert.doesNotMatch(src, /KenPom|kenpom|\bpaper\b|v1\/v2/i, rel);
  }
});
