import { NextRequest, NextResponse } from "next/server";

const COOKIE_NAME = "pm_admin";

export async function POST(req: NextRequest) {
  const body = await req.json().catch(() => null);

  const username = String(body?.username ?? "");
  const password = String(body?.password ?? "");

  const adminUser = process.env.ADMIN_USERNAME ?? "";
  const adminPass = process.env.ADMIN_PASSWORD ?? "";

  if (!adminUser || !adminPass) {
    return NextResponse.json(
      { message: "Admin credentials not configured (ADMIN_USERNAME/ADMIN_PASSWORD)." },
      { status: 500 }
    );
  }

  if (username !== adminUser || password !== adminPass) {
    return NextResponse.json({ message: "Invalid username or password." }, { status: 401 });
  }

  const res = NextResponse.json({ ok: true }, { status: 200 });
  res.cookies.set(COOKIE_NAME, "1", {
    httpOnly: true,
    sameSite: "lax",
    secure: process.env.NODE_ENV === "production",
    path: "/",
  });
  return res;
}
