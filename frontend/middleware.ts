import { NextRequest, NextResponse } from "next/server";

const COOKIE_NAME = "pm_admin";
const LOGIN_PATH = "/admin/login";

export function middleware(req: NextRequest) {
  const { pathname } = req.nextUrl;

  // allow next internals + public files
  if (
    pathname.startsWith("/_next") ||
    pathname.startsWith("/favicon.ico") ||
    pathname.startsWith("/robots.txt") ||
    pathname.startsWith("/sitemap.xml") ||
    pathname.startsWith("/api")
  ) {
    return NextResponse.next();
  }

  // allow login page without auth
  if (pathname === LOGIN_PATH) {
    return NextResponse.next();
  }

  // protect everything else
  const v = req.cookies.get(COOKIE_NAME)?.value;
  if (v === "1") return NextResponse.next();

  const url = req.nextUrl.clone();
  url.pathname = LOGIN_PATH;
  url.searchParams.set("next", pathname);
  return NextResponse.redirect(url);
}

export const config = {
  matcher: ["/:path*"],
};
