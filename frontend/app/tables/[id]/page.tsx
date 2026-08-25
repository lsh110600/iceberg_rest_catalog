import Link from "next/link";
import { MetricSparkline } from "@/components/MetricSparkline";
import { StatusBadge } from "@/components/StatusBadge";
import { api, formatBytes, formatDate, formatNumber } from "@/lib/api";

export const dynamic = "force-dynamic";

export default async function TableDetailPage({ params }: { params: Promise<{ id: string }> }) {
  const { id } = await params;
  const [detail, metrics] = await Promise.all([api.table(id), api.metrics(id)]);
  const smallFiles = metrics.filter((point) => point.metricName === "small_file_ratio");
  const planning = metrics.filter((point) => point.metricName === "scan_planning_ms_p95");
  const table = detail.table;

  return (
    <div className="page-shell">
      <Link href="/" className="back-link">← 테이블 운영 현황</Link>
      <section className="page-heading detail-heading">
        <div>
          <p className="eyebrow">{table.catalogName} / {table.namespaceName}</p>
          <div className="title-line"><h1>{table.tableName}</h1><StatusBadge status={table.health} /></div>
          <p className="lede">{table.ownerName ?? "owner 미지정"} · {table.sloProfile} · Iceberg v{table.formatVersion}</p>
        </div>
        <Link href={`/operations?table=${table.id}`} className="primary-button">이 테이블 작업 요청</Link>
      </section>

      <section className="stat-grid detail-stats">
        <article className="stat-card"><span>Data size</span><strong>{formatBytes(table.totalBytes)}</strong><small>{formatNumber(table.totalRecords)} records</small></article>
        <article className="stat-card"><span>Data files</span><strong>{formatNumber(table.totalFiles)}</strong><small>{table.smallFileRatio == null ? "unknown" : `${(table.smallFileRatio * 100).toFixed(1)}% small`}</small></article>
        <article className="stat-card"><span>Snapshots</span><strong>{formatNumber(table.snapshotCount)}</strong><small>current {detail.currentSnapshotId ?? "—"}</small></article>
        <article className="stat-card"><span>Last commit</span><strong className="date-value">{formatDate(table.lastCommitAt)}</strong><small>observed {formatDate(table.observedAt)}</small></article>
      </section>

      <section className="split-grid">
        <article className="panel metric-panel">
          <div className="panel-title"><div><p className="eyebrow">FILE HEALTH</p><h2>Small file ratio</h2></div><strong>{smallFiles.length ? `${(smallFiles.at(-1)!.metricValue * 100).toFixed(1)}%` : "—"}</strong></div>
          <MetricSparkline points={smallFiles} color="#f5b942" />
        </article>
        <article className="panel metric-panel">
          <div className="panel-title"><div><p className="eyebrow">TRINO</p><h2>Planning p95</h2></div><strong>{planning.length ? `${Math.round(planning.at(-1)!.metricValue)} ms` : "—"}</strong></div>
          <MetricSparkline points={planning} />
        </article>
      </section>

      <section className="split-grid detail-grid">
        <article className="panel">
          <div className="panel-title"><div><p className="eyebrow">FINDINGS</p><h2>Active diagnostics</h2></div><span className="count-badge">{detail.findings.length}</span></div>
          <div className="finding-list">
            {detail.findings.length === 0 && <p className="empty-copy">열린 finding이 없습니다.</p>}
            {detail.findings.map((finding) => (
              <div className={`finding finding-${finding.severity.toLowerCase()}`} key={finding.id}>
                <span className="finding-dot" /><div><strong>{finding.title}</strong><p>{finding.recommendation}</p><small>{finding.ruleCode}</small></div>
              </div>
            ))}
          </div>
        </article>
        <article className="panel metadata-panel">
          <div className="panel-title"><div><p className="eyebrow">ICEBERG METADATA</p><h2>Current state</h2></div><span className="storage-pill">{table.storageType}</span></div>
          <dl>
            <div><dt>Table UUID</dt><dd>{detail.tableUuid ?? "—"}</dd></div>
            <div><dt>Metadata location</dt><dd><code>{detail.metadataLocation ?? "—"}</code></dd></div>
            {Object.entries(detail.properties).map(([key, value]) => <div key={key}><dt>{key}</dt><dd>{value}</dd></div>)}
          </dl>
        </article>
      </section>
    </div>
  );
}
