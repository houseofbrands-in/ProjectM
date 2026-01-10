import { NextRequest, NextResponse } from "next/server";

export const runtime = "nodejs";

// ✅ Increase body limit for large CSV uploads (Flipkart events can exceed 10MB)
export const config = {
  api: {
    bodyParser: false,
  },
};

// Next.js uses this for app route handlers to allow larger bodies
export const maxDuration = 300; // optional, gives more time on serverless-like envs

const BACKEND =
  process.env.INTERNAL_API_URL ??
  process.env.NEXT_PUBLIC_API_URL ??
  "http://127.0.0.1:8000";

async function proxy(req: NextRequest, ctx: { params: { path: string[] } }) {
  const { path } = ctx.params;

  const incomingUrl = new URL(req.url);
  const targetUrl = new URL(`${BACKEND}/${path.join("/")}`);
  targetUrl.search = incomingUrl.search;

  const headers = new Headers(req.headers);
  headers.delete("host");

  const method = req.method.toUpperCase();
  const hasBody = !["GET", "HEAD"].includes(method);
  const body = hasBody ? await req.arrayBuffer() : undefined;

  const upstream = await fetch(targetUrl.toString(), {
    method,
    headers,
    body: body ? Buffer.from(body) : undefined,
    redirect: "manual",
    cache: "no-store",
  });

  const resHeaders = new Headers(upstream.headers);
  const data = await upstream.arrayBuffer();

  return new NextResponse(data, {
    status: upstream.status,
    headers: resHeaders,
  });
}

export const GET = proxy;
export const POST = proxy;
export const PUT = proxy;
export const PATCH = proxy;
export const DELETE = proxy;
export const OPTIONS = proxy;
