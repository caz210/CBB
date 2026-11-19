// Shared team index for the header search and the matchup pickers: the team list loads once per session
// (module cache + sessionStorage) from /api/teams; every keystroke after that filters locally.
import { useEffect, useState } from "react";
import { buildIndex, chicagoDate, withFreshNext, type SearchTeam, type TeamIndexEntry } from "./teamSearch";

export type Load = { state: "loading" } | { state: "ready"; index: TeamIndexEntry[] } | { state: "error" };

// Next-game data goes stale, so the in-memory copy expires after TTL_MS, the sessionStorage key carries the
// CT date, and every render also drops games that have tipped (withFreshNext).
const TTL_MS = 5 * 60 * 1000;
const memo = new Map<string, { at: number; p: Promise<SearchTeam[]> }>();

function loadTeams(sport: string): Promise<SearchTeam[]> {
  const hit = memo.get(sport);
  if (hit && Date.now() - hit.at < TTL_MS) return hit.p;
  const key = `czarp:teams:v2:${sport}:${chicagoDate()}`;
  const keep = (p: Promise<SearchTeam[]>, at = Date.now()) => { memo.set(sport, { at, p }); return p; };
  try {
    const raw = sessionStorage.getItem(key);
    if (raw) {
      const { at, teams } = JSON.parse(raw) as { at: number; teams: SearchTeam[] };
      if (Date.now() - at < TTL_MS && Array.isArray(teams) && teams.length) return keep(Promise.resolve(teams), at);
    }
  } catch { /* storage blocked or corrupt: just refetch */ }
  const p = fetch(`/api/teams?sport=${encodeURIComponent(sport)}`)
    .then((r) => (r.ok ? r.json() : Promise.reject(new Error(String(r.status)))))
    .then((j: { teams?: SearchTeam[] }) => {
      const teams = j.teams ?? [];
      try { sessionStorage.setItem(key, JSON.stringify({ at: Date.now(), teams })); } catch { /* ignore */ }
      return teams;
    })
    .catch((e) => { memo.delete(sport); throw e; }); // don't cache failures
  return keep(p);
}

export function useTeamIndex(sport: string, enabled: boolean): Load {
  const [st, setSt] = useState<Load>({ state: "loading" });
  const [attempt, setAttempt] = useState(0);
  useEffect(() => {
    if (!enabled) return;
    let live = true;
    loadTeams(sport).then(
      (t) => live && setSt({ state: "ready", index: buildIndex(withFreshNext(t)) }),
      () => live && setSt({ state: "error" }),
    );
    return () => { live = false; };
  }, [sport, enabled, attempt]);
  // after an error, reopening retries
  useEffect(() => { if (enabled && st.state === "error") setAttempt((a) => a + 1); }, [enabled]); // eslint-disable-line react-hooks/exhaustive-deps
  return st;
}

