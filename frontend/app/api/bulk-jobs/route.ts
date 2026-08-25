import { NextRequest, NextResponse } from "next/server";

const backendUrl = process.env.BACKEND_URL ?? "http://localhost:8088";

export async function GET(request: NextRequest) {
  const response = await fetch(`${backendUrl}/ops/api/v1/bulk-jobs${request.nextUrl.search}`, { cache: "no-store" });
  return new NextResponse(await response.text(), { status: response.status, headers: { "Content-Type": "application/json" } });
}
