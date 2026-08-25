import { NextRequest, NextResponse } from "next/server";

const backendUrl = process.env.BACKEND_URL ?? "http://localhost:8088";

export async function POST(request: NextRequest) {
  const response = await fetch(`${backendUrl}/ops/api/v1/operation-requests/bulk`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Idempotency-Key": request.headers.get("Idempotency-Key") ?? crypto.randomUUID(),
    },
    body: await request.text(),
    cache: "no-store",
  });
  return new NextResponse(await response.text(), {
    status: response.status,
    headers: { "Content-Type": response.headers.get("Content-Type") ?? "application/json" },
  });
}
