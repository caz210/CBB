import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { createLiveFeedPoller, espnSummaryRequestInit, loadLiveClockEvent } from "../src/lib/liveClock";

const SAMPLE_SUMMARY = {
  header: {
    competitions: [{
      status: { period: 1, displayClock: "12:00", type: { name: "STATUS_IN_PROGRESS" } },
      competitors: [
        { homeAway: "home", score: "40" },
        { homeAway: "away", score: "38" },
      ],
    }],
  },
};

test("outgoing ESPN request does not use the custom CZarp User-Agent", async () => {
  const calls: RequestInit[] = [];
  const fetcher: typeof fetch = async (_url, init) => {
    calls.push(init ?? {});
    return new Response(JSON.stringify(SAMPLE_SUMMARY), { status: 200, headers: { "content-type": "application/json" } });
  };
  const result = await loadLiveClockEvent("401911233", { fetch: fetcher });
  assert.equal(result.http, 200);
  assert.equal(calls.length, 1);
  const init = calls[0];
  const headers = init.headers;
  let ua = "";
  if (headers instanceof Headers) {
    ua = headers.get("User-Agent") ?? headers.get("user-agent") ?? "";
  } else if (Array.isArray(headers)) {
    const hit = headers.find(([k]) => k.toLowerCase() === "user-agent");
    ua = hit?.[1] ?? "";
  } else if (headers && typeof headers === "object") {
    const rec = headers as Record<string, string>;
    ua = rec["User-Agent"] ?? rec["user-agent"] ?? "";
  }
  assert.equal(ua, "", "User-Agent must be omitted so the runtime default is used");
  assert.equal("headers" in init && init.headers != null, false);
  assert.doesNotMatch(JSON.stringify(init), /CZarpAnalytics/i);
  const probe = espnSummaryRequestInit(new AbortController().signal);
  assert.equal(probe.headers, undefined);
  assert.doesNotMatch(JSON.stringify(probe), /CZarpAnalytics|User-Agent/i);
});

test("the live-clock route and helper do not send a custom CZarp User-Agent", () => {
  const root = [resolve(__dirname, "../src"), resolve(__dirname, "../../src")].find((d) => {
    try { readFileSync(resolve(d, "lib/liveClock.ts")); return true; } catch { return false; }
  });
  assert.ok(root, "src/lib/liveClock.ts");
  const route = readFileSync(resolve(root, "app/api/live-clock/route.ts"), "utf8");
  const helper = readFileSync(resolve(root, "lib/liveClock.ts"), "utf8");
  assert.doesNotMatch(route, /CZarpAnalytics|["']User-Agent["']/);
  assert.doesNotMatch(helper, /CZarpAnalytics\/1\.0/);
  assert.doesNotMatch(helper, /["']User-Agent["']/);
});

test("proxy timeout (AbortController) returns a clean 504", async () => {
  const fetcher: typeof fetch = (_url, init) => new Promise((_, reject) => {
    const signal = init?.signal;
    const fail = () => {
      const err = new Error("The operation was aborted");
      err.name = "AbortError";
      reject(err);
    };
    if (!signal) return;
    if (signal.aborted) fail();
    else signal.addEventListener("abort", fail, { once: true });
  });
  const t0 = Date.now();
  const result = await loadLiveClockEvent("401911233", { fetch: fetcher, timeoutMs: 40 });
  const elapsed = Date.now() - t0;
  assert.equal(result.http, 504);
  assert.deepEqual(result.body, { error: "timeout" });
  assert.ok(elapsed >= 30 && elapsed < 1000, `timeout should fire near 40ms, got ${elapsed}ms`);
});

test("bad event and ESPN HTTP errors return 400 / 502", async () => {
  const bad = await loadLiveClockEvent("not-an-id", { fetch: async () => { throw new Error("should not fetch"); } });
  assert.equal(bad.http, 400);
  assert.deepEqual(bad.body, { error: "event" });
  const fetcher: typeof fetch = async () => new Response("nope", { status: 403 });
  const espn = await loadLiveClockEvent("401911233", { fetch: fetcher });
  assert.equal(espn.http, 502);
  assert.deepEqual(espn.body, { error: "espn" });
});

test("createLiveFeedPoller clears the interval after stop() and does not fire again", async () => {
  let n = 0;
  const p = createLiveFeedPoller(() => { n += 1; }, 25);
  p.start();
  assert.equal(n, 1);
  assert.equal(p.isRunning(), true);
  await new Promise((r) => setTimeout(r, 70));
  const mid = n;
  assert.ok(mid >= 2, `expected at least one interval tick, got ${mid}`);
  p.stop();
  assert.equal(p.isRunning(), false);
  await new Promise((r) => setTimeout(r, 80));
  assert.equal(n, mid, "poller must not fire after stop()");
});
