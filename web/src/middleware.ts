import { NextResponse } from "next/server";
import type { NextRequest } from "next/server";
import { hiddenAccountRedirect } from "./lib/accounts";

/** Bounce hidden account surfaces (login, signup, models, pricing, and future callback/account routes). */
export function middleware(req: NextRequest) {
  const to = hiddenAccountRedirect(req.nextUrl.pathname);
  if (to) return NextResponse.redirect(new URL(to, req.url));
  return NextResponse.next();
}

export const config = {
  matcher: [
    "/login",
    "/login/:path*",
    "/signup",
    "/signup/:path*",
    "/auth",
    "/auth/:path*",
    "/account",
    "/account/:path*",
    "/callback",
    "/callback/:path*",
    "/models",
    "/models/:path*",
    "/pricing",
    "/pricing/:path*",
  ],
};
