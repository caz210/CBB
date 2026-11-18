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
  const step = niceStep(span / (targetTicks - 1));
  const pad = span * padFrac;
  const kMin = Math.floor((lo < 0 ? lo - pad : lo) / step + 1e-9);
  const kMax = Math.ceil((hi > 0 ? hi + pad : hi) / step - 1e-9);
  const ticks: number[] = [];
  for (let k = kMin; k <= kMax; k++) ticks.push(clean(k * step));
  const decimals = step >= 1 ? 0 : Math.min(6, Math.ceil(-Math.log10(step) - 1e-9));
  return { min: clean(kMin * step), max: clean(kMax * step), step, ticks, decimals };
}

/** "+1,250u" / "−40u" / "0u" with a true minus sign, matching fmtUnits. */
export function fmtUnitsTick(v: number, decimals: number): string {
  const body = Math.abs(v).toLocaleString("en-US", { minimumFractionDigits: decimals, maximumFractionDigits: decimals });
  return `${v > 0 ? "+" : v < 0 ? "−" : ""}${body}u`;
}
