import type { Metadata } from "next";
import Script from "next/script";
import "./globals.css";
import { SiteHeader } from "@/components/SiteHeader";
import { SiteFooter } from "@/components/SiteFooter";
import { BottomNav } from "@/components/BottomNav";
import { HomeScreenHint } from "@/components/HomeScreenHint";
import { SITE_URL } from "@/lib/site";
import { PWA_NAME, PWA_THEME_COLOR } from "@/lib/pwa";
import { HOME_DESCRIPTION, HOME_TITLE, OG_IMAGE, TITLE_SUFFIX } from "@/lib/seo";

export const viewport = { width: "device-width", initialScale: 1, viewportFit: "cover" as const, themeColor: PWA_THEME_COLOR };

export const metadata: Metadata = {
  metadataBase: new URL(SITE_URL),
  applicationName: PWA_NAME,
  title: {
    default: HOME_TITLE,
    template: `%s${TITLE_SUFFIX}`,
  },
  description: HOME_DESCRIPTION,
  openGraph: {
    siteName: "CZarp Analytics",
    type: "website",
    url: SITE_URL,
    title: HOME_TITLE,
    description: HOME_DESCRIPTION,
    images: [OG_IMAGE],
  },
  twitter: {
    card: "summary_large_image",
    title: HOME_TITLE,
    description: HOME_DESCRIPTION,
    images: [OG_IMAGE.url],
  },
  appleWebApp: {
    capable: true,
    title: PWA_NAME,
    statusBarStyle: "black",
  },
  // Next's appleWebApp.capable emits mobile-web-app-capable; iOS still looks for the apple-prefixed name.
  other: { "apple-mobile-web-app-capable": "yes" },
  icons: {
    icon: [
      { url: "/icons/icon-192.png", sizes: "192x192", type: "image/png" },
      { url: "/icons/icon-512.png", sizes: "512x512", type: "image/png" },
    ],
    apple: [{ url: "/icons/apple-touch-icon.png", sizes: "180x180", type: "image/png" }],
  },
};

export default function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  return (
    <html lang="en">
      <Script id="czarp-bip" strategy="beforeInteractive">{`(function(){window.addEventListener("beforeinstallprompt",function(e){e.preventDefault();window.__bip=e;});})();`}</Script>
      <body className="min-h-screen pb-[calc(3.5rem+env(safe-area-inset-bottom))] antialiased lg:pb-0">
        <SiteHeader />
        <HomeScreenHint />
        <main>{children}</main>
        <SiteFooter />
        <BottomNav />
      </body>
    </html>
  );
}
