import Link from "next/link";
import { formatBytes, formatDate, formatNumber, type TableSummary } from "@/lib/api";
import { StatusBadge } from "./StatusBadge";

export function TableList({ tables }: { tables: TableSummary[] }) {
  return (
    <div className="table-wrap">
      <table>
        <thead>
          <tr><th>Table</th><th>Health</th><th>Storage</th><th>Size</th><th>Files</th><th>Small files</th><th>Last commit</th></tr>
        </thead>
        <tbody>
          {tables.map((table) => (
            <tr key={table.id}>
              <td>
                <Link href={`/tables/${table.id}`} className="table-name">{table.namespaceName}.{table.tableName}</Link>
                <span className="subline">{table.ownerName ?? "owner 미지정"} · {table.sloProfile}</span>
              </td>
              <td><StatusBadge status={table.health} /></td>
              <td><span className="storage-pill">{table.storageType ?? "—"}</span></td>
              <td>{formatBytes(table.totalBytes)}</td>
              <td>{formatNumber(table.totalFiles)}</td>
              <td>{table.smallFileRatio == null ? "—" : `${(table.smallFileRatio * 100).toFixed(1)}%`}</td>
              <td>{formatDate(table.lastCommitAt)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
