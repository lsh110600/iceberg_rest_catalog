const backendUrl = process.env.BACKEND_URL ?? "http://localhost:8088";

export const dynamic = "force-dynamic";

export async function GET() {
  const response = await fetch(`${backendUrl}/ops/api/v1/operation-stream`, { cache: "no-store" });
  return new Response(response.body, {
    status: response.status,
    headers: { "Content-Type": "text/event-stream", "Cache-Control": "no-cache" },
  });
}
