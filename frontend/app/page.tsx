import Link from "next/link";
import { api, formatDate } from "@/lib/api";
import { StatusBadge } from "@/components/StatusBadge";
import { TableList } from "@/components/TableList";
import { DashboardSummary } from "@/components/DashboardSummary";

export const dynamic = "force-dynamic";

export default async function DashboardPage() {
  const [dashboard, total, critical, warning, healthy, unknown] = await Promise.all([
    api.dashboard(),
    api.tablePage("limit=3"),
    api.tablePage("limit=3&health=CRITICAL"),
    api.tablePage("limit=3&health=WARNING"),
    api.tablePage("limit=3&health=HEALTHY"),
    api.tablePage("limit=3&health=UNKNOWN"),
  ]);

  return (
    <div className="page-shell">
      <section className="page-heading">
        <div>
          <p className="eyebrow">LAKEHOUSE CONTROL PLANE</p>
          <h1>테이블 운영 현황</h1>
          <p className="lede">HDFS의 Iceberg 테이블과 세 실행 엔진을 한곳에서 관측합니다.</p>
        </div>
        <Link href="/operations" className="primary-button">작업 요청</Link>
      </section>

      <DashboardSummary
        counts={dashboard.counts}
        samples={{
          total: total.items,
          critical: critical.items,
          warning: warning.items,
          healthy: healthy.items,
          unknown: unknown.items,
        }}
      />

      <section className="split-grid">
        <article className="panel">
          <div className="panel-title"><div><p className="eyebrow">COMPUTE</p><h2>실행 엔진 연결</h2></div><span className="muted">지원 작업 기준</span></div>
          <div className="engine-list">
            {dashboard.engines.map((engine) => (
              <div className="engine-row" key={engine.id}>
                <div className={`engine-icon engine-${engine.engineType.toLowerCase()}`}>{engine.engineType[0]}</div>
                <div className="engine-copy"><strong>{engine.name}</strong><span>{engine.engineType} {engine.version} · {engine.capabilities.length} capabilities</span></div>
                <StatusBadge status={engine.health} />
              </div>
            ))}
          </div>
        </article>

        <article className="panel">
          <div className="panel-title"><div><p className="eyebrow">STORAGE</p><h2>스토리지 연결</h2></div><span className="muted">HDFS → S3 준비</span></div>
          <div className="storage-list">
            {dashboard.storageProfiles.map((storage) => (
              <div className="storage-row" key={storage.id}>
                <div><div className="storage-heading"><strong>{storage.name}</strong><span className="storage-pill">{storage.storageType}</span></div><code>{storage.uriPrefix}</code></div>
                <StatusBadge status={storage.health} />
              </div>
            ))}
          </div>
        </article>
      </section>

      <section className="panel table-panel">
        <div className="panel-title"><div><p className="eyebrow">NEEDS ATTENTION</p><h2>확인이 필요한 테이블</h2></div><span className="muted">조회 {formatDate(new Date().toISOString())}</span></div>
        <TableList tables={dashboard.attentionTables} />
      </section>
    </div>
  );
}
