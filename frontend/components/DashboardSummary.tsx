"use client";

import Link from "next/link";
import { useState } from "react";
import type { TableStatusCounts, TablePage, TableSummary } from "@/lib/api";
import { TableList } from "./TableList";

type TableHealth = "HEALTHY" | "WARNING" | "CRITICAL" | "UNKNOWN";
type SummaryKey = "total" | "critical" | "warning" | "healthy" | "unknown" | "operations";

interface SummaryDefinition {
  key: SummaryKey;
  label: string;
  value: number;
  caption: string;
  description: string;
  health?: TableHealth;
  className?: string;
}

export function DashboardSummary({
  counts,
  samples,
}: {
  counts: TableStatusCounts;
  samples: Record<Exclude<SummaryKey, "operations">, TableSummary[]>;
}) {
  const [selected, setSelected] = useState<SummaryDefinition | null>(null);
  const [tables, setTables] = useState<TableSummary[]>([]);
  const [nextCursor, setNextCursor] = useState<string | undefined>();
  const [hasMore, setHasMore] = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  const definitions: SummaryDefinition[] = [
    {
      key: "total",
      label: "관리 테이블",
      value: counts.total,
      caption: "등록·관측 대상 전체",
      description: "Iceberg Ops에 등록되어 상태와 메트릭을 관리하는 모든 테이블입니다.",
      className: "stat-total",
    },
    {
      key: "critical",
      label: "긴급 확인",
      value: counts.critical,
      caption: "즉시 확인 필요",
      description: "데이터 유실 위험, freshness SLO 위반 등 즉시 조치가 필요한 테이블입니다.",
      health: "CRITICAL",
      className: "stat-critical",
    },
    {
      key: "warning",
      label: "주의 필요",
      value: counts.warning,
      caption: `${counts.openFindings}개 열린 진단`,
      description: "작은 파일 증가나 delete 증폭처럼 성능·운영 상태를 점검해야 하는 테이블입니다.",
      health: "WARNING",
      className: "stat-warning",
    },
    {
      key: "healthy",
      label: "정상",
      value: counts.healthy,
      caption: "최근 관측 상태 정상",
      description: "현재 적용된 상태 규칙에서 경고나 긴급 조건이 발견되지 않은 테이블입니다.",
      health: "HEALTHY",
      className: "stat-healthy",
    },
    {
      key: "unknown",
      label: "상태 미확인",
      value: counts.unknown,
      caption: "메트릭 확인 필요",
      description: "최근 관측값이 없거나 상태 판정에 필요한 메트릭이 부족한 테이블입니다.",
      health: "UNKNOWN",
    },
    {
      key: "operations",
      label: "진행 중 작업",
      value: counts.pendingOperations,
      caption: "승인 대기·실행 중",
      description: "승인 대기, 실행 대기, Spark 실행 또는 결과 검증 단계에 있는 작업입니다.",
    },
  ];

  async function loadTables(definition: SummaryDefinition, cursor?: string) {
    setLoading(true);
    setError("");
    const parameters = new URLSearchParams({ limit: "100" });
    if (definition.health) parameters.set("health", definition.health);
    if (cursor) parameters.set("cursor", cursor);
    try {
      const response = await fetch(`/api/tables/page?${parameters}`, { cache: "no-store" });
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.message ?? payload.detail ?? "목록을 불러오지 못했습니다.");
      const page = payload as TablePage;
      setTables((current) => cursor
        ? [...current, ...page.items.filter((item) => !current.some((old) => old.id === item.id))]
        : page.items);
      setNextCursor(page.nextCursor);
      setHasMore(page.hasMore);
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "목록을 불러오지 못했습니다.");
    } finally {
      setLoading(false);
    }
  }

  function selectDefinition(definition: SummaryDefinition) {
    if (selected?.key === definition.key) {
      setSelected(null);
      return;
    }
    setSelected(definition);
    setTables([]);
    setNextCursor(undefined);
    setHasMore(false);
    if (definition.key !== "operations") void loadTables(definition);
  }

  return (
    <>
      <section className="stat-grid dashboard-stat-grid" aria-label="테이블 상태 요약">
        {definitions.map((definition) => {
          const preview = definition.key === "operations" ? [] : samples[definition.key];
          const tooltipId = `summary-tooltip-${definition.key}`;
          return (
            <button
              type="button"
              className={`stat-card summary-card ${definition.className ?? ""} ${selected?.key === definition.key ? "selected" : ""}`}
              key={definition.key}
              onClick={() => selectDefinition(definition)}
              aria-expanded={selected?.key === definition.key}
              aria-describedby={tooltipId}
            >
              <span>{definition.label}</span>
              <strong>{definition.value}</strong>
              <small>{definition.caption} · 클릭하여 상세 보기</small>
              <span className="summary-tooltip" id={tooltipId} role="tooltip">
                <b>{definition.description}</b>
                {preview.length > 0 && <><em>대표 테이블</em><span className="summary-tooltip-list">{preview.map((table) => <span key={table.id}>{table.catalogName}.{table.namespaceName}.{table.tableName}</span>)}</span></>}
                {definition.key !== "operations" && preview.length === 0 && <em>해당 테이블이 없습니다.</em>}
                <i>클릭하면 전체 목록을 확인할 수 있습니다.</i>
              </span>
            </button>
          );
        })}
      </section>

      {selected && <section className="panel summary-detail" aria-live="polite">
        <div className="panel-title">
          <div><p className="eyebrow">SUMMARY DETAIL</p><h2>{selected.label} {selected.value}개</h2></div>
          <button type="button" className="summary-close" onClick={() => setSelected(null)} aria-label="상세 닫기">닫기</button>
        </div>
        <p className="summary-description">{selected.description}</p>
        {selected.key === "operations" ? <div className="summary-action"><span>작업별 승인·실행·취소 상태는 작업 관리 화면에서 확인할 수 있습니다.</span><Link href="/operations" className="primary-button">작업 목록 열기</Link></div> : <>
          {loading && tables.length === 0 && <p className="summary-empty">테이블 목록을 불러오는 중입니다.</p>}
          {error && <p className="summary-error">{error}</p>}
          {!loading && !error && tables.length === 0 && <p className="summary-empty">해당 상태의 테이블이 없습니다.</p>}
          {tables.length > 0 && <TableList tables={tables} />}
          {hasMore && <button type="button" className="secondary-button summary-load-more" disabled={loading} onClick={() => nextCursor && loadTables(selected, nextCursor)}>{loading ? "불러오는 중…" : "테이블 100개 더 보기"}</button>}
        </>}
      </section>}
    </>
  );
}
