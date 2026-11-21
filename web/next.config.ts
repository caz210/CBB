import type { NextConfig } from "next";
import { sportRedirects } from "./src/lib/sports";

const nextConfig: NextConfig = {
  async redirects() {
    return sportRedirects();
  },
};

export default nextConfig;
