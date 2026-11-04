import Link from "next/link";

// Tier keys match profiles.tier ('free' | 'pro' | 'syndicate').
const TIERS = [
  {
    key: "free", name: "Free", price: "$0", period: "forever",
    blurb: "Follow along while the model earns its track record.",
    cta: "Create free account", href: "/signup",
    features: ["Daily CBB slate: score, spread, total", "Win & cover probabilities", "Team archetype tags", "Full public results ledger", "Value bets revealed at tip-off", "1 saved custom model"],
  },
  {
    key: "pro", name: "Pro", price: "$—", period: "/ mo (TBD)",
    blurb: "Real-time edges, stake sizing and unlimited models.",
    cta: "Join the Pro waitlist", href: "/signup", highlight: true,
    features: ["Everything in Free", "¼-Kelly stake sizing", "Unlimited saved custom models", "Line-move alerts", "New sports as they launch (NBA, WNBA…)"],
  },
  {
    key: "syndicate", name: "Syndicate", price: "Let's talk", period: "",
    blurb: "Placeholder for a higher tier: API, exports, teams.",
    cta: "Contact us", href: "/signup",
    features: ["Everything in Pro", "API / CSV data access", "Team seats"],
  },
];

const COMPARE: [string, string, string][] = [
  ["Daily slate & projections", "✓", "✓"],
  ["Public results ledger", "✓", "✓"],
  ["Value bets", "At tip-off", "Before tip"],
  ["¼-Kelly stakes", "—", "✓"],
  ["Saved custom models", "1", "Unlimited"],
  ["Alerts", "—", "✓"],
];

export default function PricingPage() {
  return (
    <div className="mx-auto max-w-6xl px-4 py-8 md:px-6 md:py-16">
      <div className="text-center">
        <div className="text-[11px] font-bold uppercase tracking-wider text-court-400">Pricing · placeholder</div>
        <h1 className="mt-2 text-3xl font-black tracking-tight md:text-4xl">Free while we prove it.</h1>
        <p className="mx-auto mt-3 max-w-2xl text-sm text-zinc-400 md:text-base">
          This season is paper-traded in public. Paid tiers open once the ledger shows a real edge.
        </p>
        <div className="mx-auto mt-5 inline-flex rounded-xl border border-ink-700 bg-ink-850 p-1 text-sm">
          <span className="flex min-h-9 items-center rounded-lg bg-ink-700 px-4 font-semibold">Monthly</span>
          <span className="flex min-h-9 items-center px-4 text-zinc-400">Season pass <span className="ml-1 text-edge-400">(save)</span></span>
        </div>
      </div>

      <div className="mt-8 grid gap-4 md:mt-12 md:grid-cols-3 md:gap-6">
        {TIERS.map((t) => (
          <div key={t.key}
            className={`relative flex flex-col rounded-2xl border p-5 md:p-7 ${t.highlight ? "order-first border-court-500 bg-gradient-to-b from-court-500/10 to-ink-900 shadow-2xl shadow-court-500/10 md:order-none" : "border-ink-700 bg-ink-900/80"}`}>
            {t.highlight && <span className="absolute -top-3 left-5 rounded-full bg-court-500 px-3 py-1 text-[11px] font-bold uppercase text-ink-950 md:left-7">Most popular</span>}
            <div className="flex items-baseline justify-between gap-3 md:block">
              <h2 className="text-lg font-bold">{t.name}</h2>
              <div className="flex items-baseline gap-1 md:mt-5">
                <span className="text-3xl font-black md:text-4xl">{t.price}</span>
                <span className="text-xs text-zinc-500 md:text-sm">{t.period}</span>
              </div>
            </div>
            <p className="mt-1 text-sm text-zinc-400 md:order-first">{t.blurb}</p>
            <ul className="mt-4 flex-1 space-y-2 text-sm md:mt-6">
              {t.features.map((f) => (
                <li key={f} className="flex gap-2"><span className="text-edge-400">✓</span><span className={f.includes("custom model") ? "font-semibold text-white" : "text-zinc-300"}>{f}</span></li>
              ))}
            </ul>
            <Link href={t.href}
              className={`mt-6 flex min-h-12 items-center justify-center rounded-xl font-semibold md:mt-8 ${t.highlight ? "bg-court-500 text-ink-950 hover:bg-court-400" : "border border-ink-600 hover:bg-ink-800"}`}>
              {t.cta}
            </Link>
          </div>
        ))}
      </div>

      <div className="card mt-8 overflow-hidden md:mt-12">
        <table className="w-full text-sm">
          <thead className="bg-ink-850 text-[11px] uppercase tracking-wider text-zinc-500">
            <tr><th className="px-4 py-2.5 text-left">Compare</th><th className="px-2 py-2.5">Free</th><th className="px-4 py-2.5 text-court-400">Pro</th></tr>
          </thead>
          <tbody>
            {COMPARE.map(([k, f, p]) => (
              <tr key={k} className="border-t border-ink-700/60">
                <td className="px-4 py-3 text-zinc-300">{k}</td>
                <td className="px-2 py-3 text-center text-zinc-400">{f}</td>
                <td className="px-4 py-3 text-center font-semibold">{p}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <p className="mt-6 text-center text-xs text-zinc-500">Billing via Stripe. Prices are placeholders.</p>
    </div>
  );
}
