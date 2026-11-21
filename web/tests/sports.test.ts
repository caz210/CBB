import { test } from "node:test";
import assert from "node:assert/strict";
import { DEFAULT_SPORT, SPORTS, getSport, sportRedirects } from "../src/lib/sports";

test("SPORTS only lists enabled leagues; today that is live CBB", () => {
  assert.deepEqual(SPORTS.map((s) => s.key), ["cbb"]);
  assert.ok(SPORTS.every((s) => s.enabled && s.status === "live"));
  assert.equal(DEFAULT_SPORT, "cbb");
  assert.equal(getSport("cbb")?.label, "CBB");
  // disabled leagues are not in the catalog, so pages 404 them if a redirect is missed
  assert.equal(getSport("nba"), undefined);
  assert.equal(getSport("wnba"), undefined);
  assert.equal(getSport("nfl"), undefined);
});

test("disabled sports 307 to the CBB slate (temporary, so they can come back)", () => {
  const r = sportRedirects();
  const src = r.map((x) => x.source).sort();
  assert.deepEqual(src, ["/nba", "/nba/:path*", "/wnba", "/wnba/:path*"]);
  for (const x of r) {
    assert.equal(x.destination, "/cbb/slate");
    assert.equal(x.statusCode, 307);
  }
});
