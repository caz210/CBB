import { NextResponse } from "next/server";
import type { NextRequest } from "next/server";
import { authRouteRedirect } from "./lib/accounts";

/** Bounce hidden auth surfaces (login, signup, and future callback/account routes) to /. */
export function middleware(req: NextRequest) {
  const to = authRouteRedirect(req.nextUrl.pathname);
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
  ],
};
