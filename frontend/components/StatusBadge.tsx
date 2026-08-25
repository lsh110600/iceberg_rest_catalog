import type { Health } from "@/lib/api";

export function StatusBadge({ status }: { status: Health | string }) {
  return <span className={`status status-${status.toLowerCase().replace("_", "-")}`}><i />{status}</span>;
}
