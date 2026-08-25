import { NextRequest, NextResponse } from "next/server";

const backendUrl = process.env.BACKEND_URL ?? "http://localhost:8088";

export async function GET() {
  return proxy(`${backendUrl}/ops/api/v1/operation-requests`);
}

export async function POST(request: NextRequest) {
  return proxy(`${backendUrl}/ops/api/v1/operation-requests`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Idempotency-Key": request.headers.get("Idempotency-Key") ?? crypto.randomUUID(),
    },
    body: await request.text(),
  });
}

async function proxy(url: string, init?: RequestInit) {
  const response = await fetch(url, { ...init, cache: "no-store" });
  return new NextResponse(await response.text(), {
    status: response.status,
    headers: { "Content-Type": response.headers.get("Content-Type") ?? "application/json" },
  });
}
