/** Read-only proxy for one ESPN men's college basketball event status.
 *  Used when a browser-side fetch of site.api.espn.com is blocked. No keys, no writes.
 *  Short in-memory cache (~15s). Event id must be the numeric ESPN id (games.external_id). */
import { NextResponse } from "next/server";
import { loadLiveClockEvent } from "@/lib/liveClock";

export const dynamic = "force-dynamic";

const CACHE_MS = 15_000;
const cache = new Map<string, { at: number; body: unknown }>();

export async function GET(req: Request) {
  const event = new URL(req.url).searchParams.get("event")?.trim() ?? "";
  const now = Date.now();
  const hit = cache.get(event);
  if (hit && now - hit.at < CACHE_MS) {
    return NextResponse.json(hit.body, { headers: { "Cache-Control": "public, max-age=15" } });
  }
  const result = await loadLiveClockEvent(event);
  if (result.http === 200) {
    cache.set(event, { at: now, body: result.body });
    if (cache.size > 200) {
      for (const [k, v] of cache) {
        if (now - v.at > CACHE_MS) cache.delete(k);
      }
    }
    return NextResponse.json(result.body, { status: 200, headers: { "Cache-Control": "public, max-age=15" } });
  }
  return NextResponse.json(result.body, { status: result.http });
}
