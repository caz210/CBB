import { test } from "node:test";
import assert from "node:assert/strict";
import { fmtUnitsTick, niceStep, unitsScale } from "../src/lib/chartScale";

const covers = (raw: number[]) => {
  const data = raw.filter((v) => Number.isFinite(v));
  const s = unitsScale(raw);
  assert.ok(s.min <= Math.min(0, ...data), `min ${s.min} covers data`);
  assert.ok(s.max >= Math.max(0, ...data), `max ${s.max} covers data`);
  assert.ok(s.ticks.includes(0), "0 is a tick");
  assert.equal(s.ticks[0], s.min);
  assert.equal(s.ticks[s.ticks.length - 1], s.max);
  assert.ok(s.ticks.length >= 3 && s.ticks.length <= 10, `tick count ${s.ticks.length}`);
  return s;
};

test("niceStep rounds up to 1/2/5 x 10^n", () => {
  assert.equal(niceStep(0.7), 1); assert.equal(niceStep(1.2), 2); assert.equal(niceStep(3), 5);
  assert.equal(niceStep(6), 10); assert.equal(niceStep(0.03), 0.05); assert.equal(niceStep(130), 200);
});

test("the old cap: +5u official record is no longer clipped at +2u", () => {
  const s = covers([0, 1.2, 0.4, 3.1, 5.11]);
  assert.ok(s.max >= 5.11);
  assert.ok(s.ticks.some((t) => t >= 5), "a gridline at or above +5u");
});

test("scales up to hundreds of units", () => {
  const s = covers([0, 40, 120, 310, 455.5]);
  assert.ok(s.max >= 455.5);
  assert.ok(s.step >= 100 && s.step <= 200, `step ${s.step}`);
});

test("scales down to negative hundreds", () => {
  const s = covers([0, -30, -150, -380, -412]);
  assert.ok(s.min <= -412);
  assert.ok(s.max >= 0 && s.max <= s.step, "little headroom above zero when the run is all losses");
});

test("mixed positive and negative", () => {
  const s = covers([0, 25, -80, 140, -35, 60]);
  assert.ok(s.min <= -80 && s.max >= 140);
});

test("thousands", () => { covers([0, 2500, -1800, 9000]); });

test("small fractional range still has sensible ticks", () => {
  const s = covers([0, 0.12, -0.07, 0.3]);
  assert.ok(s.decimals >= 1);
  assert.ok(s.ticks.every((t) => Math.abs(t * 10 ** s.decimals - Math.round(t * 10 ** s.decimals)) < 1e-6), "no float noise");
});

test("all zeros, empty and non-finite input do not break the axis", () => {
  assert.deepEqual(unitsScale([0, 0, 0]).ticks.includes(0), true);
  covers([]); covers([NaN, Infinity, 1, 2]);
});

test("padding keeps the extreme point off the frame", () => {
  const s = unitsScale([0, 10]);
  assert.ok(s.max > 10 || s.ticks.includes(10) && s.max >= 10);
});

test("tick labels", () => {
  assert.equal(fmtUnitsTick(0, 0), "0u"); assert.equal(fmtUnitsTick(0, 1), "0u");
  assert.equal(fmtUnitsTick(5, 0), "+5u");
  assert.equal(fmtUnitsTick(-1250, 0), "−1,250u");
  assert.equal(fmtUnitsTick(0.5, 1), "+0.5u");
});
