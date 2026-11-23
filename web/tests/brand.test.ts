import { test } from "node:test";
import assert from "node:assert/strict";
import { existsSync, readFileSync, readdirSync, statSync } from "node:fs";
import { join, resolve } from "node:path";

const root = [resolve(__dirname, ".."), resolve(__dirname, "../..")].find((d) => {
  try { return statSync(join(d, "src/app/globals.css")).isFile(); } catch { return false; }
})!;
const read = (rel: string) => readFileSync(join(root, rel), "utf8");

function pngSize(rel: string): { w: number; h: number } {
  const b = readFileSync(join(root, rel));
  assert.equal(b.toString("ascii", 1, 4), "PNG");
  return { w: b.readUInt32BE(16), h: b.readUInt32BE(20) };
}

function walkFiles(dir: string, skip = new Set(["node_modules", ".next", ".test-build", "coverage"])): string[] {
  const out: string[] = [];
  for (const f of readdirSync(dir)) {
    if (skip.has(f)) continue;
    const p = join(dir, f);
    if (statSync(p).isDirectory()) out.push(...walkFiles(p, skip));
    else out.push(p);
  }
  return out;
}

test("the header logo is a next/image wordmark with alt 'CZarp Analytics'", () => {
  const src = read("src/components/Logo.tsx");
  assert.match(src, /from "next\/image"/);
  assert.match(src, /alt="CZarp Analytics"/);
  assert.match(src, /wordmark@2x\.webp/);
  assert.match(src, /h-7/);
  assert.doesNotMatch(src, /Analytics Club/);
});

test("the homepage hero includes the CZarp Analytics wordmark", () => {
  const src = read("src/app/page.tsx");
  assert.match(src, /from "next\/image"/);
  assert.match(src, /wordmark-hero@2x\.webp/);
  assert.match(src, /alt="CZarp Analytics"/);
});

test("wordmark, icons, favicon, and OG image files exist at the expected sizes", () => {
  for (const f of [
    "public/brand/wordmark.png",
    "public/brand/wordmark.webp",
    "public/brand/wordmark@2x.png",
    "public/brand/wordmark@2x.webp",
    "public/brand/wordmark-hero.png",
    "public/brand/wordmark-hero.webp",
    "public/brand/wordmark-hero@2x.png",
    "public/brand/wordmark-hero@2x.webp",
    "public/icons/icon-48.png",
    "src/app/icon.png",
    "src/app/favicon.ico",
    "public/og.png",
  ]) {
    assert.ok(existsSync(join(root, f)), f);
  }
  assert.deepEqual(pngSize("public/brand/wordmark.png"), { w: 94, h: 36 });
  assert.deepEqual(pngSize("public/brand/wordmark@2x.png"), { w: 188, h: 72 });
  assert.deepEqual(pngSize("public/icons/icon-192.png"), { w: 192, h: 192 });
  assert.deepEqual(pngSize("public/icons/icon-512.png"), { w: 512, h: 512 });
  assert.deepEqual(pngSize("public/icons/icon-512-maskable.png"), { w: 512, h: 512 });
  assert.deepEqual(pngSize("public/og.png"), { w: 1200, h: 630 });
  assert.deepEqual(pngSize("src/app/icon.png"), { w: 32, h: 32 });
  // Wordmark PNGs are transparent cuts (they start with a tRNS or are RGBA).
  const wm = readFileSync(join(root, "public/brand/wordmark@2x.png"));
  const colorType = wm[25]; // IHDR color type: 6 = RGBA
  assert.equal(colorType, 6, "wordmark@2x.png should be RGBA");
});

test("visible web source never uses the retired club name and names the brand CZarp Analytics", () => {
  const skipExt = new Set([".png", ".webp", ".ico", ".svg", ".woff", ".woff2", ".jpg", ".jpeg"]);
  const retired = "Analytics" + " Club";
  const retiredRe = new RegExp(retired, "i");
  let hits: string[] = [];
  for (const dir of ["src", "scripts"]) {
    const base = join(root, dir);
    if (!existsSync(base)) continue;
    for (const p of walkFiles(base)) {
      if (skipExt.has(p.slice(p.lastIndexOf(".")))) continue;
      const text = readFileSync(p, "utf8");
      if (retiredRe.test(text)) hits.push(p);
    }
  }
  assert.deepEqual(hits, []);
  assert.match(read("src/components/Logo.tsx"), /CZarp Analytics/);
  assert.match(read("src/components/SiteFooter.tsx"), /© 2026 CZarp Analytics/);
  assert.match(read("src/lib/pwa.ts"), /PWA_NAME = "CZarp Analytics"/);
  assert.match(read("src/lib/site.ts"), /https:\/\/www\.czarpanalytics\.com/);
});

test("brand and icon generator sources stay free of KenPom, paper, and v1/v2 copy", () => {
  for (const rel of ["scripts/generate-pwa-icons.py", "src/components/Logo.tsx", "src/app/layout.tsx"]) {
    assert.doesNotMatch(read(rel), /KenPom|kenpom|\bpaper\b|v1\/v2/i, rel);
  }
});
