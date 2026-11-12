import type { MetadataRoute } from "next";
import { SITE_URL } from "@/lib/site";
import { SPORTS } from "@/lib/sports";

export default function sitemap(): MetadataRoute.Sitemap {
  const now = new Date();
  const live = SPORTS.filter((s) => s.status === "live");
  return [
    { url: `${SITE_URL}/`, lastModified: now, changeFrequency: "daily", priority: 1 },
    ...live.flatMap((s) => [
      { url: `${SITE_URL}/${s.key}/slate`, lastModified: now, changeFrequency: "hourly" as const, priority: 0.9 },
      { url: `${SITE_URL}/${s.key}/results`, lastModified: now, changeFrequency: "daily" as const, priority: 0.8 },
    ]),
    ...["methodology", "about", "pricing", "terms", "privacy"].map((p) => ({
      url: `${SITE_URL}/${p}`, lastModified: now, changeFrequency: "monthly" as const, priority: 0.5,
    })),
  ];
}
