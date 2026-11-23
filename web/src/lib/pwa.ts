/** Installable-app constants and the iOS Home Screen hint helpers.
 *  Colors come from src/app/globals.css @theme — do not invent new ones. */

/** Site name as it should appear under a home-screen icon. */
export const PWA_NAME = "CZarp";
export const PWA_START_URL = "/cbb/slate";
export const PWA_SCOPE = "/";
export const PWA_DISPLAY = "standalone" as const;
/** --color-ink-950, the page canvas and existing theme-color. */
export const PWA_THEME_COLOR = "#07090d";
export const PWA_BACKGROUND_COLOR = PWA_THEME_COLOR;

export const IOS_HOME_HINT_KEY = "czarp.ios-homescreen-hint.dismissed";
export const IOS_HOME_HINT_COPY =
  "Add CZarp to your Home Screen: tap Share, then Add to Home Screen.";

/** Safari on iPhone / iPod / iPad (including iPadOS's Macintosh UA). Not Chrome, Firefox, or Edge on iOS. */
export function isIosSafari(ua: string, maxTouchPoints = 0): boolean {
  const u = ua.toLowerCase();
  const ios = /iphone|ipod|ipad/.test(u) || (/macintosh/.test(u) && maxTouchPoints > 1);
  if (!ios) return false;
  if (/crios|fxios|edgios|opt\/|opios|android/.test(u)) return false;
  return /safari/.test(u) || /iphone|ipod|ipad/.test(u);
}

export function shouldShowIosHomeHint(opts: {
  userAgent: string;
  maxTouchPoints?: number;
  standalone: boolean;
  dismissed: boolean;
}): boolean {
  if (opts.dismissed || opts.standalone) return false;
  return isIosSafari(opts.userAgent, opts.maxTouchPoints ?? 0);
}
