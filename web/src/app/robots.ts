import type { MetadataRoute } from "next";
import { SITE_URL } from "@/lib/site";
import { SHOW_ACCOUNTS } from "@/lib/flags";

export default function robots(): MetadataRoute.Robots {
  return {
    rules: [{ userAgent: "*", allow: "/", disallow: ["/models", "/login", "/signup", "/api/", ...(SHOW_ACCOUNTS ? [] : ["/pricing"])] }],
    sitemap: `${SITE_URL}/sitemap.xml`,
    host: SITE_URL,
  };
}
