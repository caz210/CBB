/** Auto-scaling y-axis for the units charts. No fixed domain and no cap: the axis always covers the
 *  data in both directions (positive and negative), always includes 0, and picks round tick steps
 *  (1, 2, 5 × 10^n) so it reads the same at ±2 units or ±hundreds of units. */

export interface UnitsScale {
  min: number;
  max: number;
  step: number;
  /** Tick values, ascending, always includes 0. */
  ticks: number[];
  /** Decimal places needed to print a tick at this step. */
  decimals: number;
}

/** Round step (1, 2 or 5 × 10^n) that is at least `raw`. */
export function niceStep(raw: number): number {
  if (!Number.isFinite(raw) || raw <= 0) return 1;
  const pow = Math.pow(10, Math.floor(Math.log10(raw)));
  const f = raw / pow;
  const nice = f <= 1 ? 1 : f <= 2 ? 2 : f <= 5 ? 5 : 10;
  return nice * pow;
}

/** The next round step above `step` (1 → 2 → 5 → 10 → 20 …). */
export function nextNiceStep(step: number): number {
  const pow = Math.pow(10, Math.floor(Math.log10(step) + 1e-9));
  const f = step / pow;
  return (f < 1.5 ? 2 : f < 3.5 ? 5 : 10) * pow;
}

const clean = (v: number) => Math.round(v * 1e9) / 1e9 + 0; // + 0 turns -0 into 0

/** `padFrac` pads the outer edge on each side that has data (top if any value > 0, bottom if any < 0)
 *  by that share of the data range. `targetTicks` is the approximate number of gridlines wanted. */
export function unitsScale(values: number[], opts: { padFrac?: number; targetTicks?: number } = {}): UnitsScale {
  const padFrac = opts.padFrac ?? 0.08;
  const targetTicks = Math.max(3, opts.targetTicks ?? 5);
  const vals = values.filter((v) => Number.isFinite(v));
  let lo = Math.min(0, ...vals);
  let hi = Math.max(0, ...vals);
  if (lo === hi) { lo = -1; hi = 1; } // nothing but zeros: show ±1u
  const span = hi - lo;
  const pad = span * padFrac;
  const top = hi > 0 ? hi + pad : hi;
  const bottom = lo < 0 ? lo - pad : lo;
  // Smallest round step that keeps the gridlines to about `targetTicks` (at most one extra).
  const maxTicks = targetTicks + 1;
  let step = niceStep(span / (maxTicks * 2));
  let kMin = 0, kMax = 0;
  for (let guard = 0; guard < 60; guard++) {
    kMin = Math.floor(bottom / step + 1e-9);
    kMax = Math.ceil(top / step - 1e-9);
    if (kMax - kMin + 1 <= maxTicks) break;
    step = nextNiceStep(step);
  }
  const ticks: number[] = [];
  for (let k = kMin; k <= kMax; k++) ticks.push(clean(k * step));
  const decimals = step >= 1 ? 0 : Math.min(6, Math.ceil(-Math.log10(step) - 1e-9));
  return { min: clean(kMin * step), max: clean(kMax * step), step, ticks, decimals };
}

/** "+1,250u" / "−40u" / "0u" with a true minus sign, matching fmtUnits. */
export function fmtUnitsTick(v: number, decimals: number): string {
  if (v === 0) return "0u";
  const body = Math.abs(v).toLocaleString("en-US", { minimumFractionDigits: decimals, maximumFractionDigits: decimals });
  return `${v > 0 ? "+" : v < 0 ? "−" : ""}${body}u`;
}
