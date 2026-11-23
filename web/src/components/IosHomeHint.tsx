"use client";
import { useEffect, useState } from "react";
import { IOS_HOME_HINT_COPY, IOS_HOME_HINT_KEY, shouldShowIosHomeHint } from "@/lib/pwa";

/** Dismissible, once-only Home Screen hint. iOS Safari only, and never in standalone (already installed).
 *  Renders nothing until after mount so server HTML and the first client paint match. */
export function IosHomeHint() {
  const [visible, setVisible] = useState(false);

  useEffect(() => {
    try {
      const dismissed = window.localStorage.getItem(IOS_HOME_HINT_KEY) === "1";
      const standalone =
        window.matchMedia("(display-mode: standalone)").matches
        || window.matchMedia("(display-mode: fullscreen)").matches
        || ("standalone" in window.navigator && (window.navigator as Navigator & { standalone?: boolean }).standalone === true);
      setVisible(shouldShowIosHomeHint({
        userAgent: window.navigator.userAgent,
        maxTouchPoints: window.navigator.maxTouchPoints ?? 0,
        standalone,
        dismissed,
      }));
    } catch {
      setVisible(false);
    }
  }, []);

  if (!visible) return null;

  const dismiss = () => {
    try { window.localStorage.setItem(IOS_HOME_HINT_KEY, "1"); } catch { /* private mode */ }
    setVisible(false);
  };

  return (
    <div role="status" className="border-b border-ink-700 bg-ink-900">
      <div className="mx-auto flex max-w-7xl items-start gap-2 px-3 py-2 min-[360px]:px-4 md:px-6">
        <p className="min-w-0 flex-1 text-[13px] leading-snug text-zinc-300">{IOS_HOME_HINT_COPY}</p>
        <button
          type="button"
          onClick={dismiss}
          className="min-h-11 shrink-0 rounded-lg px-3 text-sm font-semibold text-court-400 hover:text-court-500"
        >
          Got it
        </button>
      </div>
    </div>
  );
}
