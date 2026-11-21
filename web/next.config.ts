import type { NextConfig } from "next";
import { sportRedirects } from "./src/lib/sports";

const nextConfig: NextConfig = {
  async redirects() {
    return sportRedirects();
  },
  // Dev-only rankings fixtures. Keep them out of the production page trace.
  outputFileTracingExcludes: {
    "*": ["./src/lib/rankingsQa.ts"],
  },
};

export default nextConfig;
