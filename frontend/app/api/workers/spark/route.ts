import { NextResponse } from "next/server";

const backendUrl = process.env.BACKEND_URL ?? "http://localhost:8088";

export async function GET() {
  try {
    const response = await fetch(`${backendUrl}/ops/api/v1/workers/spark`, { cache: "no-store" });
    return new NextResponse(await response.text(), {
      status: response.status,
      headers: { "Content-Type": response.headers.get("Content-Type") ?? "application/json" },
    });
  } catch {
    return NextResponse.json(
      { id: "spark", engineType: "SPARK", status: "OFFLINE", online: false, details: {} },
      { status: 200 },
    );
  }
}
