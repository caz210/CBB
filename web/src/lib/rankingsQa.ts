/** Dev/QA fixtures for /cbb/rankings. Imported only from a
 *  `process.env.NODE_ENV !== "production"` branch so a production build
 *  strips this module at compile time, even if RANKINGS_QA is set at runtime. */
import { longNameDemoRows, movementDemoRows, type RankingsData } from "./rankings";
import type { Loaded } from "./data";

export async function tryRankingsDemo(
  searchParams: Promise<{ demo?: string }>,
): Promise<Loaded<RankingsData> | null> {
  if (process.env.RANKINGS_QA !== "1") return null;
  const { demo } = await searchParams;
  if (demo === "empty") {
    return { state: "live", data: { week_of: "", as_of: "", season: null, rows: [] } };
  }
  if (demo === "error") {
    return { state: "error", reason: "unreachable" };
  }
  if (demo === "unavailable") {
    return { state: "error", reason: "unavailable" };
  }
  if (demo === "movement") {
    return {
      state: "live",
      data: {
        week_of: "2026-11-23",
        as_of: "2026-11-23",
        season: 2027,
        rows: movementDemoRows(),
      },
    };
  }
  if (demo === "longnames") {
    return {
      state: "live",
      data: {
        week_of: "2026-11-16",
        as_of: "2026-11-21",
        season: 2027,
        rows: longNameDemoRows(),
      },
    };
  }
  return null;
}
