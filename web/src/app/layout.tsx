import type { Metadata } from "next";
import "./globals.css";
import { SiteHeader } from "@/components/SiteHeader";
import { SiteFooter } from "@/components/SiteFooter";
import { BottomNav } from "@/components/BottomNav";
import { SITE_URL } from "@/lib/site";

export const viewport = { width: "device-width", initialScale: 1, viewportFit: "cover", themeColor: "#07090d" };

export const metadata: Metadata = {
  metadataBase: new URL(SITE_URL),
  title: {
    default: "CZarp Analytics · College basketball fair value vs price",
    template: "%s · CZarp Analytics",
  },
  description: "Daily college basketball fair values vs the market price, edge % on every game, and a public, time-stamped paper-trading record. For entertainment and information, not betting advice.",
  openGraph: { siteName: "CZarp Analytics", type: "website", url: SITE_URL },
};

export default function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  return (
    <html lang="en">
      <body className="min-h-screen pb-[calc(3.5rem+env(safe-area-inset-bottom))] antialiased md:pb-0">
        <SiteHeader />
        <main>{children}</main>
        <SiteFooter />
        <BottomNav />
      </body>
    </html>
  );
}
