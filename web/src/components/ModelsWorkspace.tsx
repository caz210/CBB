"use client";
import Link from "next/link";
import { useState } from "react";
import { BASELINE_FEATURES, EXAMPLE_USER_MODELS, MODEL_LIMIT, type UserModel } from "@/lib/example-data";
import { TIER_LABEL, type Tier } from "@/lib/labels";

const PRO_EXTRA: UserModel[] = [
  { ...EXAMPLE_USER_MODELS[0], id: "x2", name: "Defense Wins", created_at: "2026-10-30T19:00:00-05:00" },
  { ...EXAMPLE_USER_MODELS[0], id: "x3", name: "Rest & Travel", created_at: "2026-11-01T09:00:00-05:00" },
];
const FAKE_RECORD: Record<string, { ats: string; roi: string; good: boolean }> = {
  "Pace & Space v1": { ats: "53.4%", roi: "+2.6%", good: true },
  "Defense Wins": { ats: "49.1%", roi: "−4.0%", good: false },
  "Rest & Travel": { ats: "51.0%", roi: "+0.3%", good: true },
};

export function ModelsWorkspace() {
  // Mock: in production this is profiles.tier for the signed-in user.
  const [tier, setTier] = useState<Tier>("free");
  const [features, setFeatures] = useState(BASELINE_FEATURES);
  const [name, setName] = useState("Pace & Space v1");
  const [notice, setNotice] = useState<string | null>(null);

  const models = tier === "free" ? EXAMPLE_USER_MODELS : [...EXAMPLE_USER_MODELS, ...PRO_EXTRA];
  const limit = MODEL_LIMIT[tier];
  const atLimit = models.length >= limit;
  const total = features.reduce((a, f) => a + (f.enabled ? f.weight : 0), 0) || 1;

  function newModel() {
    if (atLimit) {
      setNotice("limit");
      return;
    }
    setNotice("Mockup only: this would insert a new row into user_models.");
  }

  return (
    <div className="space-y-4">
      {/* demo-only tier switch */}
      <div className="flex items-center justify-between gap-3 rounded-xl border border-dashed border-ink-600 px-3 py-2 text-xs text-zinc-400">
        <span>Preview as (mockup only)</span>
        <div className="inline-flex rounded-lg bg-ink-850 p-0.5">
          {(["free", "pro"] as Tier[]).map((t) => (
            <button key={t} onClick={() => { setTier(t); setNotice(null); }}
              className={`min-h-9 rounded-md px-4 font-semibold ${tier === t ? "bg-court-500 text-ink-950" : "text-zinc-400"}`}>
              {TIER_LABEL[t]}
            </button>
          ))}
        </div>
      </div>

      <div className="grid gap-4 lg:grid-cols-[1fr_1.4fr] lg:gap-6">
        {/* saved models */}
        <div className="card p-4 md:p-5">
          <div className="flex items-center justify-between">
            <h2 className="font-bold">Saved models</h2>
            <span className="rounded-full bg-ink-800 px-2.5 py-1 font-mono text-xs text-zinc-300">
              {models.length} / {limit === Infinity ? "∞" : limit}
            </span>
          </div>
          <div className="mt-3 h-1.5 overflow-hidden rounded-full bg-ink-700">
            <div className="h-full bg-court-500" style={{ width: limit === Infinity ? "8%" : `${Math.min(100, (models.length / limit) * 100)}%` }} />
          </div>
          <p className="mt-2 text-[11px] text-zinc-500">
            {tier === "free" ? "Free includes 1 saved model. Pro is unlimited." : "Pro: unlimited saved models."}
          </p>

          <div className="mt-4 space-y-2">
            <ModelRow name="CZarp Baseline" tag="Official" ats="51.9%" roi="+1.1%" good sub="Not counted toward your limit" />
            {models.map((m) => {
              const r = FAKE_RECORD[m.name];
              return <ModelRow key={m.id} name={m.name} tag="Yours" ats={r.ats} roi={r.roi} good={r.good} sub="Paper picks since Oct 28" />;
            })}
          </div>

          <button onClick={newModel}
            className={`mt-3 flex min-h-12 w-full items-center justify-center gap-2 rounded-xl border border-dashed text-sm font-semibold ${atLimit ? "border-ink-600 text-zinc-500" : "border-court-500/60 text-court-400 hover:bg-court-500/10"}`}>
            {atLimit ? "New model (Pro)" : "+ New model"}
          </button>

          {notice === "limit" ? (
            <div className="mt-3 rounded-xl border border-court-500/40 bg-court-500/10 p-3 text-sm">
              <div className="font-semibold">You&apos;ve used your free model</div>
              <p className="mt-0.5 text-xs text-zinc-400">Edit or replace it anytime, or go Pro for unlimited models.</p>
              <Link href="/pricing" className="mt-2 inline-flex min-h-10 items-center rounded-lg bg-court-500 px-4 text-sm font-semibold text-ink-950">See Pro</Link>
            </div>
          ) : notice ? (
            <p className="mt-3 rounded-lg bg-ink-800 px-3 py-2 text-xs text-amber-300">{notice}</p>
          ) : null}
        </div>

        {/* builder */}
        <div className="card p-4 md:p-6">
          <label className="text-[11px] font-semibold uppercase tracking-wider text-zinc-500">Editing</label>
          <div className="mt-1 flex flex-col gap-2 sm:flex-row">
            <input value={name} onChange={(e) => setName(e.target.value)}
              className="min-h-11 flex-1 rounded-lg border border-ink-600 bg-ink-950 px-3 font-semibold outline-none focus:border-court-500" />
            <button onClick={() => setNotice("Mockup only: this would update the row in user_models (features + weights JSON).")}
              className="min-h-11 rounded-lg bg-court-500 px-5 text-sm font-semibold text-ink-950 hover:bg-court-400">Save</button>
          </div>

          <div className="mt-5 space-y-1">
            {features.map((f, i) => {
              const share = f.enabled ? (f.weight / total) * 100 : 0;
              const set = (patch: Partial<typeof f>) => setFeatures(features.map((x, j) => (j === i ? { ...x, ...patch } : x)));
              return (
                <div key={f.name} className={`rounded-xl px-2 py-2.5 ${f.enabled ? "" : "opacity-45"}`}>
                  <div className="flex items-center gap-3">
                    <input type="checkbox" checked={f.enabled} onChange={(e) => set({ enabled: e.target.checked })}
                      className="h-5 w-5 shrink-0 accent-orange-500" aria-label={`Use ${f.label}`} />
                    <span className="min-w-0 flex-1 text-sm">{f.label}</span>
                    <span className="font-mono text-xs text-zinc-500">{share.toFixed(0)}%</span>
                  </div>
                  <input type="range" min={0} max={0.5} step={0.01} value={f.weight} disabled={!f.enabled}
                    onChange={(e) => set({ weight: +e.target.value })}
                    className="mt-2 h-6 w-full accent-orange-500" aria-label={`${f.label} weight`} />
                </div>
              );
            })}
          </div>
          <button className="mt-2 min-h-12 w-full rounded-xl border border-dashed border-ink-600 text-sm text-zinc-400 hover:bg-ink-850">
            + Add a data point
          </button>
          <p className="mt-3 text-[11px] text-zinc-500">Features are CZarp-derived only. No raw third-party stats.</p>
        </div>
      </div>
    </div>
  );
}

function ModelRow({ name, tag, ats, roi, good, sub }: { name: string; tag: string; ats: string; roi: string; good: boolean; sub: string }) {
  return (
    <div className="flex min-h-14 items-center justify-between gap-3 rounded-xl border border-ink-700 bg-ink-850 px-3.5 py-2.5">
      <div className="min-w-0">
        <div className="flex items-center gap-2 font-semibold">
          <span className="truncate">{name}</span>
          <span className={`shrink-0 rounded px-1.5 py-px text-[10px] font-bold uppercase ${tag === "Official" ? "bg-court-500/20 text-court-400" : "bg-ink-600 text-zinc-300"}`}>{tag}</span>
        </div>
        <div className="text-[11px] text-zinc-500">{sub}</div>
      </div>
      <div className="shrink-0 text-right font-mono text-xs">
        <div>{ats} ATS</div>
        <div className={good ? "text-edge-400" : "text-fade-400"}>{roi} ROI</div>
      </div>
    </div>
  );
}
