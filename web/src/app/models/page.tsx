import type { Metadata } from "next";
import { PageHeader } from "@/components/PageHeader";

export const metadata: Metadata = { title: "My Models" };
import { ModelsWorkspace } from "@/components/ModelsWorkspace";
import { ExampleBanner } from "@/components/ExampleBadge";

export default function ModelsPage() {
  return (
    <>
    <ExampleBanner />
    <div className="mx-auto max-w-7xl px-4 py-5 md:px-6 md:py-10">
      <PageHeader
        eyebrow="My Models · Preview"
        title="Build your own model"
        sub="Pick the data points that matter, weight them, and track your model against CZarp's baseline."
      />
      <div className="mt-4 md:mt-6">
        <ModelsWorkspace />
      </div>
      <div className="card mt-4 p-4 text-sm text-zinc-400 md:mt-6 md:p-5">
        <h2 className="font-bold text-zinc-100">How it works</h2>
        <ol className="mt-2 space-y-1.5">
          <li><b className="text-zinc-200">1.</b> Start from CZarp&apos;s baseline and toggle or re-weight features.</li>
          <li><b className="text-zinc-200">2.</b> Backtest on past seasons, then run it on the live slate daily.</li>
          <li><b className="text-zinc-200">3.</b> Each model gets its own paper ledger. Free: 1 model. Pro: unlimited.</li>
        </ol>
      </div>
    </div>
    </>
  );
}
