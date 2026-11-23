/** Read-only proxy for one ESPN men's college basketball event status.
 *  Used when a browser-side fetch of site.api.espn.com is blocked. No keys, no writes.
 *  Short in-memory cache (~15s). Event id must be the numeric ESPN id (games.external_id). */
import { NextResponse } from "next/server";
import { ESPN_MEN_CBB_SUMMARY, isEspnEventId, statusFromEspnPayload, scoresFromEspnPayload } from "@/lib/espnClock";

export const dynamic = "force-dynamic";

const CACHE_MS = 15_000;
const cache = new Map<string, { at: number; body: unknown }>();

export async function GET(req: Request) {
  const event = new URL(req.url).searchParams.get("event")?.trim() ?? "";
  if (!isEspnEventId(event)) return NextResponse.json({ error: "event" }, { status: 400 });
  const now = Date.now();
  const hit = cache.get(event);
  if (hit && now - hit.at < CACHE_MS) {
    return NextResponse.json(hit.body, { headers: { "Cache-Control": "public, max-age=15" } });
  }
  let payload: unknown;
  try {
    const res = await fetch(ESPN_MEN_CBB_SUMMARY(event), {
      headers: { "User-Agent": "CZarpAnalytics/1.0 (live-line; +https://czarpanalytics.com)" },
      cache: "no-store",
    });
    if (!res.ok) return NextResponse.json({ error: "espn" }, { status: 502 });
    payload = await res.json();
  } catch {
    return NextResponse.json({ error: "espn" }, { status: 502 });
  }
  const body = { status: statusFromEspnPayload(payload), scores: scoresFromEspnPayload(payload) };
  cache.set(event, { at: now, body });
  if (cache.size > 200) {
    for (const [k, v] of cache) {
      if (now - v.at > CACHE_MS) cache.delete(k);
    }
  }
  return NextResponse.json(body, { headers: { "Cache-Control": "public, max-age=15" } });
}
