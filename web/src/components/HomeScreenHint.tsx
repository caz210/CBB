"use client";
import { useEffect, useRef, useState } from "react";
import {
  HOME_HINT_KEY,
  HOME_HINT_KEY_LEGACY,
  homeHintCopy,
  homeHintDismissed,
  homeHintKind,
  type HomeHintKind,
} from "@/lib/pwa";

type InstallPrompt = {
  prompt: () => Promise<void>;
  userChoice?: Promise<{ outcome: "accepted" | "dismissed" }>;
};

function asInstallPrompt(value: unknown): InstallPrompt | null {
  if (!value || typeof value !== "object") return null;
  const ev = value as Partial<InstallPrompt>;
  return typeof ev.prompt === "function" ? ev as InstallPrompt : null;
}

/** Event stashed by the beforeInteractive listener in the root layout, if it fired before mount. */
function readStashedPrompt(): InstallPrompt | null {
  return asInstallPrompt((window as Window & { __bip?: unknown }).__bip);
}

function readDismissed(): boolean {
  try {
    return homeHintDismissed(window.localStorage.getItem(HOME_HINT_KEY), window.localStorage.getItem(HOME_HINT_KEY_LEGACY));
  } catch {
    return false;
  }
}

function readStandalone(): boolean {
  return (
    window.matchMedia("(display-mode: standalone)").matches
    || window.matchMedia("(display-mode: fullscreen)").matches
    || ("standalone" in window.navigator && (window.navigator as Navigator & { standalone?: boolean }).standalone === true)
  );
}

/** Dismissible, once-only Home Screen hint. Renders nothing until after mount so
 *  server HTML and the first client paint match (no hydration warning). */
export function HomeScreenHint() {
  const [kind, setKind] = useState<HomeHintKind>(null);
  const promptRef = useRef<InstallPrompt | null>(null);

  useEffect(() => {
    let cancelled = false;
    const refresh = () => {
      if (cancelled) return;
      setKind(homeHintKind({
        userAgent: window.navigator.userAgent,
        maxTouchPoints: window.navigator.maxTouchPoints ?? 0,
        standalone: readStandalone(),
        dismissed: readDismissed(),
        hasInstallPrompt: promptRef.current != null,
      }));
    };
    const stashed = readStashedPrompt();
    if (stashed) promptRef.current = stashed;
    refresh();

    const onPrompt = (event: Event) => {
      event.preventDefault();
      const ev = asInstallPrompt(event);
      if (!ev) return;
      promptRef.current = ev;
      try { (window as Window & { __bip?: unknown }).__bip = ev; } catch { /* ignore */ }
      refresh();
    };
    const onInstalled = () => {
      promptRef.current = null;
      try { window.localStorage.setItem(HOME_HINT_KEY, "1"); } catch { /* private mode */ }
      refresh();
    };
    window.addEventListener("beforeinstallprompt", onPrompt);
    window.addEventListener("appinstalled", onInstalled);
    return () => {
      cancelled = true;
      window.removeEventListener("beforeinstallprompt", onPrompt);
      window.removeEventListener("appinstalled", onInstalled);
    };
  }, []);

  if (!kind) return null;

  const dismiss = () => {
    try { window.localStorage.setItem(HOME_HINT_KEY, "1"); } catch { /* private mode */ }
    promptRef.current = null;
    setKind(null);
  };

  const install = async () => {
    const ev = promptRef.current;
    if (!ev) return;
    try {
      await ev.prompt();
      const choice = ev.userChoice ? await ev.userChoice : null;
      if (choice?.outcome === "accepted") {
        dismiss();
        return;
      }
    } catch { /* user closed the dialog or the event was already consumed */ }
  };

  return (
    <div role="status" className="border-b border-ink-700 bg-ink-900">
      <div className="mx-auto flex max-w-7xl flex-wrap items-start gap-2 px-3 py-2 min-[360px]:px-4 md:px-6">
        <p className="min-w-0 flex-1 text-[13px] leading-snug text-zinc-300">{homeHintCopy(kind)}</p>
        <div className="flex shrink-0 items-center gap-1">
          {kind === "install" && (
            <button
              type="button"
              onClick={install}
              className="min-h-11 shrink-0 rounded-lg bg-court-500 px-3 text-sm font-semibold text-ink-950 hover:bg-court-400"
            >
              Install
            </button>
          )}
          <button
            type="button"
            onClick={dismiss}
            className="min-h-11 shrink-0 rounded-lg px-3 text-sm font-semibold text-court-400 hover:text-court-500"
          >
            Got it
          </button>
        </div>
      </div>
    </div>
  );
}
