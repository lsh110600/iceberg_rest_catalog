import { NextRequest, NextResponse } from "next/server";

const backendUrl = process.env.BACKEND_URL ?? "http://localhost:8088";
const getActions = new Set(["status", "catalogs"]);
const postActions = new Set(["bootstrap", "query", "catalogs", "namespaces", "tables", "tables-bulk", "tables-bulk-jobs"]);
const backendActions: Record<string, string> = {
  "tables-bulk": "tables/bulk",
  "tables-bulk-jobs": "tables/bulk-jobs",
};

type RouteContext = { params: Promise<{ action: string }> };

export async function GET(_: NextRequest, context: RouteContext) {
  const { action } = await context.params;
  if (!getActions.has(action)) return methodNotAllowed();
  return proxy(action, { method: "GET" });
}

export async function POST(request: NextRequest, context: RouteContext) {
  const { action } = await context.params;
  if (!postActions.has(action)) return methodNotAllowed();
  return proxy(action, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Idempotency-Key": request.headers.get("Idempotency-Key") ?? crypto.randomUUID(),
      "X-Actor": "web.operator",
    },
    body: action === "bootstrap" ? undefined : await request.text(),
  });
}

function methodNotAllowed() {
  return NextResponse.json({ detail: "Unsupported playground action." }, { status: 405 });
}

async function proxy(action: string, init: RequestInit) {
  try {
    const response = await fetch(`${backendUrl}/ops/api/v1/playground/${backendActions[action] ?? action}`, {
      ...init,
      cache: "no-store",
    });
    return new NextResponse(await response.text(), {
      status: response.status,
      headers: { "Content-Type": response.headers.get("Content-Type") ?? "application/json" },
    });
  } catch {
    return NextResponse.json({ detail: "Iceberg Ops API is unavailable." }, { status: 503 });
  }
}
