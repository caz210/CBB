import { test } from "node:test";
import assert from "node:assert/strict";
import { existsSync, readdirSync, readFileSync, statSync } from "node:fs";
import { join, relative, resolve } from "node:path";

/** Resolve web/src whether tests run from src or from .test-build/. */
function srcRoot(): string {
  const candidates = [resolve(__dirname, "../src"), resolve(__dirname, "../../src")];
  const found = candidates.find((p) => existsSync(p));
  assert.ok(found, "web/src not found");
  return found!;
}

function walk(dir: string): string[] {
  const out: string[] = [];
  for (const name of readdirSync(dir)) {
    if (name === "node_modules" || name === ".git") continue;
    const p = join(dir, name);
    if (statSync(p).isDirectory()) out.push(...walk(p));
    else if (/\.(ts|tsx|js|jsx)$/.test(name)) out.push(p);
  }
  return out;
}

/** Strip line and block comments without touching string or template contents. */
function stripComments(src: string): string {
  let out = "";
  let i = 0;
  const n = src.length;
  while (i < n) {
    const c = src[i];
    const next = src[i + 1];
    if (c === '"' || c === "'" || c === "`") {
      const q = c;
      out += c;
      i++;
      while (i < n) {
        const ch = src[i];
        out += ch;
        if (ch === "\\") {
          if (i + 1 < n) {
            out += src[i + 1];
            i += 2;
            continue;
          }
        }
        if (ch === q) {
          i++;
          break;
        }
        i++;
      }
      continue;
    }
    if (c === "/" && next === "/") {
      while (i < n && src[i] !== "\n") i++;
      continue;
    }
    if (c === "/" && next === "*") {
      i += 2;
      while (i < n && !(src[i] === "*" && src[i + 1] === "/")) i++;
      i = Math.min(n, i + 2);
      continue;
    }
    out += c;
    i++;
  }
  return out;
}

test("customer-facing source has no visible 'paper' copy (comments and is_paper left alone)", () => {
  const root = srcRoot();
  const hits: string[] = [];
  for (const file of walk(root)) {
    const rel = relative(root, file);
    const stripped = stripComments(readFileSync(file, "utf8")).replace(/\bis_paper\b/g, "INTERNAL_FLAG");
    stripped.split("\n").forEach((line, idx) => {
      if (/paper/i.test(line)) hits.push(`${rel}:${idx + 1}: ${line.trim()}`);
    });
  }
  assert.deepEqual(hits, [], `customer-visible 'paper' copy still present:\n${hits.join("\n")}`);
});
