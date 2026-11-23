import type { Metadata } from "next";
import { SITE_URL } from "./site";

export const OG_IMAGE = { url: "/og.png", width: 1200, height: 630, alt: "CZarp Analytics" } as const;
export const TITLE_SUFFIX = " | CZarp Analytics";

export const HOME_TITLE = "CZarp Analytics — Daily D1 Men's Hoops Picks & Public Record";
export const HOME_DESCRIPTION =
  "Every D1 men's basketball game, priced. Official picks posted each morning, time-stamped and graded in public, in units only. For entertainment, not betting advice.";

/** Shared title, description, canonical, and OG/Twitter fields. Page `title` is
 *  the unsuffixed segment; the root layout template appends `TITLE_SUFFIX`.
 *  Pass `absolute` for a full document title that must not use the template
 *  (the homepage). OG/Twitter always get the full visible title. */
export function pageMetadata(opts: {
  title: string;
  description: string;
  path: string;
  absolute?: boolean;
}): Metadata {
  const path = opts.path.startsWith("/") ? opts.path : `/${opts.path}`;
  const canonical = `${SITE_URL}${path}`;
  const fullTitle = opts.absolute ? opts.title : `${opts.title}${TITLE_SUFFIX}`;
  return {
    title: opts.absolute ? { absolute: opts.title } : opts.title,
    description: opts.description,
    alternates: { canonical },
    openGraph: {
      title: fullTitle,
      description: opts.description,
      url: canonical,
      images: [OG_IMAGE],
    },
    twitter: {
      card: "summary_large_image",
      title: fullTitle,
      description: opts.description,
      images: [OG_IMAGE.url],
    },
  };
}
