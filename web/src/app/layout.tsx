import type { Metadata } from "next";
import "./globals.css";
import { SiteHeader } from "@/components/SiteHeader";
import { SiteFooter } from "@/components/SiteFooter";
import { BottomNav } from "@/components/BottomNav";

export const viewport = { width: "device-width", initialScale: 1, viewportFit: "cover", themeColor: "#07090d" };

export const metadata: Metadata = {
  title: "CZarpAnalytics · Model-driven sports projections",
  description: "Daily college basketball projections, value flags and a fully transparent results ledger.",
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
