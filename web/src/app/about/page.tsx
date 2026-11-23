import type { Metadata } from "next";
import Link from "next/link";
import { H, LegalPage } from "@/components/LegalPage";
import { pageMetadata } from "@/lib/seo";

export const metadata: Metadata = pageMetadata({
  title: "About",
  description: "CZarp prices every D1 men's game and only plays when the edge is real. A public, time-stamped record in units. For entertainment, not betting advice.",
  path: "/about",
});

export default function AboutPage() {
  return (
    <LegalPage eyebrow="CZarp Analytics" title="About">
      <p>
        CZarp prices every D1 men&apos;s game and only plays when the edge is real. Every official pick is time-stamped before tip. Every loss stays on the record.
      </p>
      <H>What you get</H>
      <p>
        A daily slate with price, fair value, and edge on every game. Official picks posted and time-stamped before tip. A public record with units, ROI, closing-line value, and the losing streaks — not just the wins.
      </p>
      <H>How to read it</H>
      <p>
        Edge is not a promise you will win tonight. It is a measure of whether the market price is off our number, as a share of the expected total score. One night proves nothing, and the record is still an early sample. Read the <Link href="/methodology" className="font-semibold text-court-400 hover:underline">methodology</Link> for how the numbers are built.
      </p>
      <H>Responsible use</H>
      <p>
        This is not a sportsbook and not betting advice. You must be 21 or older. If gambling is a problem, call 1-800-GAMBLER or visit{" "}
        <a href="https://www.ncpgambling.org" target="_blank" rel="noopener noreferrer" className="font-semibold text-court-400 hover:underline">ncpgambling.org</a>.
      </p>
    </LegalPage>
  );
}
