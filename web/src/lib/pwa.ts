/** Installable-app constants and Home Screen hint helpers.
 *  Colors come from src/app/globals.css @theme — do not invent new ones. */

/** Site name as it should appear under a home-screen icon. */
export const PWA_NAME = "CZarp";
export const PWA_START_URL = "/cbb/slate";
export const PWA_SCOPE = "/";
export const PWA_DISPLAY = "standalone" as const;
/** --color-ink-950, the page canvas and existing theme-color. */
export const PWA_THEME_COLOR = "#07090d";
export const PWA_BACKGROUND_COLOR = PWA_THEME_COLOR;

export const HOME_HINT_KEY = "czarp.homescreen-hint.dismissed";
/** Previous key from the first installable-app commit; still honored when reading. */
export const HOME_HINT_KEY_LEGACY = "czarp.ios-homescreen-hint.dismissed";

export const SAFARI_HOME_HINT_COPY =
  "Add CZarp to your Home Screen: tap Share, then Add to Home Screen.";
/** iOS Chrome (16.4+), and the same Share-based path on iOS Edge / Firefox. */
export const IOS_SHARE_HOME_HINT_COPY =
  "Add CZarp to your Home Screen: tap the Share icon in the address bar, then Add to Home Screen.";
export const INSTALL_HOME_HINT_COPY = "Add CZarp to your Home Screen.";

export type HomeHintKind = "safari" | "ios-share" | "install" | null;

export function isIosDevice(ua: string, maxTouchPoints = 0): boolean {
  const u = ua.toLowerCase();
  return /iphone|ipod|ipad/.test(u) || (/macintosh/.test(u) && maxTouchPoints > 1);
}

/** Safari on iPhone / iPod / iPad (including iPadOS's Macintosh UA). Not Chrome, Firefox, or Edge on iOS. */
export function isIosSafari(ua: string, maxTouchPoints = 0): boolean {
  if (!isIosDevice(ua, maxTouchPoints)) return false;
  const u = ua.toLowerCase();
  if (/crios|fxios|edgios|opt\/|opios|android/.test(u)) return false;
  return /safari/.test(u) || /iphone|ipod|ipad/.test(u);
}

/** Chrome, Edge, or Firefox on iOS — they install via the Share sheet, not beforeinstallprompt. */
export function isIosShareBrowser(ua: string, maxTouchPoints = 0): boolean {
  if (!isIosDevice(ua, maxTouchPoints)) return false;
  return /crios|fxios|edgios/.test(ua.toLowerCase());
}

/** In-app browsers (Instagram, Facebook, Line, TikTok, and similar) cannot reliably add to the Home Screen. */
export function isInAppBrowser(ua: string): boolean {
  const u = ua.toLowerCase();
  return /instagram|fbav|fban|fb_iab|fbios|fb4a|messenger|line\/|bytedancewebview|tiktok|musical_ly|ttwebview|snapchat|whatsapp|micromessenger|linkedinapp/.test(u);
}

/**
 * Which Home Screen hint to show, if any.
 * iOS Safari and iOS Chrome/Edge/Firefox get platform copy (Chrome never gets the Safari wording).
 * Android / desktop Chromium only show once beforeinstallprompt has been stashed.
 * Hidden when already installed, previously dismissed, or inside an in-app browser.
 */
export function homeHintKind(opts: {
  userAgent: string;
  maxTouchPoints?: number;
  standalone: boolean;
  dismissed: boolean;
  hasInstallPrompt: boolean;
}): HomeHintKind {
  if (opts.dismissed || opts.standalone) return null;
  if (isInAppBrowser(opts.userAgent)) return null;
  const touch = opts.maxTouchPoints ?? 0;
  if (isIosSafari(opts.userAgent, touch)) return "safari";
  if (isIosShareBrowser(opts.userAgent, touch)) return "ios-share";
  if (opts.hasInstallPrompt) return "install";
  return null;
}

export function homeHintCopy(kind: Exclude<HomeHintKind, null>): string {
  if (kind === "safari") return SAFARI_HOME_HINT_COPY;
  if (kind === "ios-share") return IOS_SHARE_HOME_HINT_COPY;
  return INSTALL_HOME_HINT_COPY;
}

export function homeHintDismissed(...values: Array<string | null | undefined>): boolean {
  return values.some((v) => v === "1");
}
