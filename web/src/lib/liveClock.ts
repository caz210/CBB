/** Read-only ESPN summary fetch used by /api/live-clock. No keys, no writes.
 *  Do not send a custom CZarp User-Agent — ESPN 403s it. */
import { ESPN_MEN_CBB_SUMMARY, competitorsFromEspnPayload, isEspnEventId, statusFromEspnPayload } from "./espnClock";

export const ESPN_TIMEOUT_MS = 5_000;

export type LiveClockBody = { status: unknown; competitors: ReturnType<typeof competitorsFromEspnPayload> };
export type LiveClockResult =
  | { http: 200; body: LiveClockBody }
  | { http: 400 | 502 | 504; body: { error: string } };

/** Init for the ESPN summary request: no User-Agent header (runtime default is fine). */
export function espnSummaryRequestInit(signal: AbortSignal): RequestInit {
  return { cache: "no-store", signal };
}

export async function loadLiveClockEvent(
  event: string,
  opts?: { fetch?: typeof fetch; timeoutMs?: number },
): Promise<LiveClockResult> {
  if (!isEspnEventId(event)) return { http: 400, body: { error: "event" } };
  const timeoutMs = opts?.timeoutMs ?? ESPN_TIMEOUT_MS;
  const fetcher = opts?.fetch ?? fetch;
  const ac = new AbortController();
  const t = setTimeout(() => ac.abort(), timeoutMs);
  try {
    const res = await fetcher(ESPN_MEN_CBB_SUMMARY(event), espnSummaryRequestInit(ac.signal));
    if (!res.ok) return { http: 502, body: { error: "espn" } };
    const payload: unknown = await res.json();
    return { http: 200, body: { status: statusFromEspnPayload(payload), competitors: competitorsFromEspnPayload(payload) } };
  } catch (err) {
    const aborted = ac.signal.aborted || (err instanceof Error && err.name === "AbortError");
    return { http: aborted ? 504 : 502, body: { error: aborted ? "timeout" : "espn" } };
  } finally {
    clearTimeout(t);
  }
}

/** 30s live-feed poller. stop() clears the interval so it does not keep firing after the first edit. */
export function createLiveFeedPoller(pull: () => void, intervalMs: number) {
  let timer: ReturnType<typeof setInterval> | null = null;
  const stop = () => {
    if (timer != null) {
      clearInterval(timer);
      timer = null;
    }
  };
  const start = () => {
    stop();
    void pull();
    timer = setInterval(() => { void pull(); }, intervalMs);
  };
  const isRunning = () => timer != null;
  return { start, stop, isRunning };
}
