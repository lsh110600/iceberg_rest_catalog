"use client";

import { useEffect, useMemo, useState } from "react";
import type {
  BulkJob,
  PlaygroundBootstrapResult,
  PlaygroundCatalog,
  PlaygroundQueryResult,
  PlaygroundStatus,
  PlaygroundTableBulkResult,
  TableSummary,
} from "@/lib/api";
import { formatBytes, formatDate } from "@/lib/api";

const identifierPattern = /^[A-Za-z_][A-Za-z0-9_]*$/;

function quoteIdentifier(value: string): string {
  return value.split(".").map((part) => `\`${part.replaceAll("`", "``")}\``).join(".");
}

function queryPresets(table: string) {
  return [
    {
      label: "테이블 메타데이터",
      sql: `DESCRIBE TABLE EXTENDED ${table}`,
    },
    {
      label: "파티션 통계",
      sql: `SELECT partition, record_count, file_count,
       total_data_file_size_in_bytes, last_updated_at,
       last_updated_snapshot_id
FROM ${table}.partitions
ORDER BY partition`,
    },
    {
      label: "스냅샷 이력",
      sql: `SELECT committed_at, snapshot_id, parent_id, operation,
       summary['added-data-files'] AS added_data_files,
       summary['added-records'] AS added_records,
       summary['total-data-files'] AS total_data_files,
       summary['total-records'] AS total_records
FROM ${table}.snapshots
ORDER BY committed_at DESC`,
    },
    {
      label: "파일 메트릭",
      sql: `SELECT CASE content
         WHEN 0 THEN 'DATA'
         WHEN 1 THEN 'POSITION_DELETE'
         WHEN 2 THEN 'EQUALITY_DELETE'
       END AS file_content,
       count(*) AS file_count,
       sum(record_count) AS record_count,
       sum(file_size_in_bytes) AS total_bytes,
       round(avg(file_size_in_bytes), 0) AS avg_file_bytes,
       min(file_size_in_bytes) AS min_file_bytes,
       max(file_size_in_bytes) AS max_file_bytes
FROM ${table}.files
GROUP BY content
ORDER BY content`,
    },
  ];
}

function displayValue(value: unknown): string {
  if (value === null || value === undefined) return "NULL";
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

function parseTableNames(value: string): string[] {
  return [...new Set(value.split(/[\s,]+/).map((name) => name.trim()).filter(Boolean))];
}

function qualifiedName(table: TableSummary): string {
  return `${table.catalogName}.${table.namespaceName}.${table.tableName}`;
}

async function responsePayload(response: Response) {
  const payload = await response.json();
  if (!response.ok) throw new Error(payload.detail ?? payload.message ?? "요청이 실패했습니다.");
  return payload;
}

export function PlaygroundClient({
  initialStatus,
  initialCatalogs,
}: {
  initialStatus: PlaygroundStatus;
  initialCatalogs: PlaygroundCatalog[];
}) {
  const defaultCatalog = initialCatalogs.find((catalog) => catalog.name === "playground")
    ?? initialCatalogs[0];
  const defaultNamespace = defaultCatalog?.namespaces.includes("demo")
    ? "demo"
    : defaultCatalog?.namespaces[0] ?? "";
  const initialTable = defaultCatalog?.name === "playground" && defaultNamespace === "demo"
    ? "orders"
    : "";
  const initialQualifiedTable = [
    defaultCatalog?.name ?? "catalog",
    defaultNamespace || "namespace",
    initialTable || "table_name",
  ].map(quoteIdentifier).join(".");

  const [status, setStatus] = useState(initialStatus);
  const [catalogs, setCatalogs] = useState(initialCatalogs);
  const [catalogName, setCatalogName] = useState(defaultCatalog?.name ?? "");
  const [namespaceName, setNamespaceName] = useState(defaultNamespace);
  const [tableName, setTableName] = useState(initialTable);
  const [targetMode, setTargetMode] = useState<"single" | "bulk">("single");
  const [bulkTableNames, setBulkTableNames] = useState(initialTable);
  const [activePreset, setActivePreset] = useState<number | null>(0);
  const [sql, setSql] = useState(queryPresets(initialQualifiedTable)[0].sql);
  const [result, setResult] = useState<PlaygroundQueryResult | null>(null);
  const [bulkResult, setBulkResult] = useState<PlaygroundTableBulkResult | null>(null);
  const [bulkJob, setBulkJob] = useState<BulkJob | null>(null);
  const [focusedTableId, setFocusedTableId] = useState<string | null>(null);
  const [bootstrap, setBootstrap] = useState<PlaygroundBootstrapResult | null>(null);
  const [message, setMessage] = useState(initialStatus.message ?? "");
  const [busy, setBusy] = useState<
    "status" | "bootstrap" | "query" | "bulk" | "catalog" | "namespace" | null
  >(null);
  const [newCatalogName, setNewCatalogName] = useState("");
  const [newWarehouse, setNewWarehouse] = useState("hdfs://namenode:8020/warehouse");
  const [newNamespace, setNewNamespace] = useState("");

  const selectedCatalog = useMemo(
    () => catalogs.find((catalog) => catalog.name === catalogName),
    [catalogName, catalogs],
  );
  const targetValid = Boolean(
    selectedCatalog
    && namespaceName.split(".").every((part) => identifierPattern.test(part))
    && identifierPattern.test(tableName),
  );
  const parsedBulkTableNames = parseTableNames(bulkTableNames);
  const bulkTargetValid = Boolean(
    selectedCatalog
    && namespaceName.split(".").every((part) => identifierPattern.test(part))
    && parsedBulkTableNames.length > 0
    && parsedBulkTableNames.length <= 200
    && parsedBulkTableNames.every((name) => identifierPattern.test(name)),
  );
  const qualifiedTable = [
    catalogName || "catalog",
    namespaceName || "namespace",
    tableName || "table_name",
  ].map(quoteIdentifier).join(".");
  const displayTable = [
    catalogName || "catalog",
    namespaceName || "namespace",
    tableName || "table_name",
  ].join(".");
  const queries = useMemo(() => queryPresets(qualifiedTable), [qualifiedTable]);
  const focusedTable = bulkResult?.tables.find((table) => table.id === focusedTableId)
    ?? bulkResult?.tables[0];

  useEffect(() => {
    if (activePreset !== null) setSql(queries[activePreset].sql);
  }, [activePreset, queries]);

  function upsertCatalog(next: PlaygroundCatalog) {
    setCatalogs((current) => [...current.filter((catalog) => catalog.id !== next.id), next]
      .sort((left, right) => left.name.localeCompare(right.name)));
  }

  function selectCatalog(name: string) {
    const next = catalogs.find((catalog) => catalog.name === name);
    setCatalogName(name);
    setNamespaceName(next?.namespaces[0] ?? "");
    setResult(null);
    setBulkResult(null);
  }

  async function refreshStatus() {
    setBusy("status");
    setMessage("");
    try {
      const payload = await responsePayload(
        await fetch("/api/playground/status", { cache: "no-store" }),
      );
      setStatus(payload);
      setMessage(payload.message ?? "서비스 상태를 갱신했습니다.");
    } catch (error) {
      setMessage(error instanceof Error ? error.message : "상태 확인에 실패했습니다.");
    } finally {
      setBusy(null);
    }
  }

  async function createSample() {
    setBusy("bootstrap");
    setMessage("");
    try {
      const payload = await responsePayload(
        await fetch("/api/playground/bootstrap", { method: "POST" }),
      );
      setBootstrap(payload);
      setStatus((current) => ({ ...current, sampleExists: true, ready: true }));
      const catalogPayload = await responsePayload(
        await fetch("/api/playground/catalogs", { cache: "no-store" }),
      );
      setCatalogs(catalogPayload);
      setCatalogName("playground");
      setNamespaceName("demo");
      setTableName("orders");
      setActivePreset(0);
      setMessage(
        payload.insertedRows > 0
          ? "Sample table과 8개 주문을 생성했습니다."
          : "기존 sample table을 확인했습니다.",
      );
    } catch (error) {
      setMessage(error instanceof Error ? error.message : "Sample table 생성에 실패했습니다.");
    } finally {
      setBusy(null);
    }
  }

  async function registerCatalog(event: React.FormEvent) {
    event.preventDefault();
    setBusy("catalog");
    setMessage("");
    try {
      const payload = await responsePayload(await fetch("/api/playground/catalogs", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ name: newCatalogName, warehouse: newWarehouse }),
      }));
      upsertCatalog(payload);
      setCatalogName(payload.name);
      setNamespaceName(payload.namespaces[0] ?? "");
      setNewCatalogName("");
      setMessage(`${payload.name} Catalog을 등록했습니다.`);
    } catch (error) {
      setMessage(error instanceof Error ? error.message : "Catalog 등록에 실패했습니다.");
    } finally {
      setBusy(null);
    }
  }

  async function registerNamespace(event: React.FormEvent) {
    event.preventDefault();
    setBusy("namespace");
    setMessage("");
    try {
      const registeredName = newNamespace;
      const payload = await responsePayload(await fetch("/api/playground/namespaces", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ catalogName, name: registeredName }),
      }));
      upsertCatalog(payload);
      setNamespaceName(registeredName);
      setNewNamespace("");
      setMessage(`${payload.name}.${registeredName} Namespace를 등록했습니다.`);
    } catch (error) {
      setMessage(error instanceof Error ? error.message : "Namespace 등록에 실패했습니다.");
    } finally {
      setBusy(null);
    }
  }

  async function runQuery() {
    if (!targetValid) {
      setMessage("등록된 Catalog와 Namespace를 선택하고 유효한 table_name을 입력하세요.");
      return;
    }
    setBusy("query");
    setMessage("");
    try {
      const payload = await responsePayload(await fetch("/api/playground/query", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ sql, maxRows: 100, catalogName }),
      }));
      setResult(payload);
      setMessage(
        `${displayTable}에서 ${payload.rowCount}개 행을 ${payload.durationMs}ms에 조회했습니다.`,
      );
    } catch (error) {
      setResult(null);
      setMessage(error instanceof Error ? error.message : "SQL 실행에 실패했습니다.");
    } finally {
      setBusy(null);
    }
  }

  async function runBulkMetrics() {
    if (!bulkTargetValid) {
      setMessage("유효한 table_name을 최대 200개까지 입력하세요.");
      return;
    }
    setBusy("bulk");
    setMessage("");
    try {
      let job: BulkJob = await responsePayload(
        await fetch("/api/playground/tables-bulk-jobs", {
          method: "POST",
          headers: { "Content-Type": "application/json", "Idempotency-Key": crypto.randomUUID() },
          body: JSON.stringify({
            catalogName,
            namespaceName,
            tableNames: parsedBulkTableNames,
          }),
        }),
      );
      setBulkJob(job);
      const terminal = new Set(["SUCCEEDED", "FAILED", "PARTIAL_SUCCESS", "CANCELED"]);
      while (!terminal.has(job.state)) {
        await new Promise((resolve) => window.setTimeout(resolve, 750));
        job = await responsePayload(await fetch(`/api/bulk-jobs/${job.id}`, { cache: "no-store" }));
        setBulkJob(job);
        setMessage(`${job.completedItems}/${job.totalItems}개 테이블 메트릭 처리 중…`);
      }
      const payload: PlaygroundTableBulkResult = {
        requestedCount: job.totalItems,
        tables: job.items.filter((item) => item.state === "SUCCEEDED").map((item) => item.result as unknown as TableSummary),
        errors: job.items.filter((item) => item.state === "FAILED").map((item) => ({ tableName: item.tableName ?? "unknown", detail: item.errorMessage ?? "조회 실패" })),
      };
      setBulkResult(payload);
      setFocusedTableId(payload.tables[0]?.id ?? null);
      setResult(null);
      setMessage(
        `${payload.tables.length}개 테이블 메트릭을 조회했습니다.`
        + (payload.errors.length ? ` ${payload.errors.length}개는 실패했습니다.` : ""),
      );
    } catch (error) {
      setBulkResult(null);
      setMessage(error instanceof Error ? error.message : "일괄 메트릭 조회에 실패했습니다.");
    } finally {
      setBusy(null);
    }
  }

  function openTableInSql(table: TableSummary) {
    setTargetMode("single");
    setTableName(table.tableName);
    setActivePreset(0);
    setResult(null);
  }

  return (
    <>
      <section className="playground-status-grid">
        <article className="panel playground-service-card">
          <div className={`service-orb ${status.hdfs.status === "HEALTHY" ? "online" : ""}`}>H</div>
          <div><p className="eyebrow">STORAGE</p><h2>HDFS</h2><code>{status.hdfs.endpoint}</code></div>
          <span className={`state ${status.hdfs.status === "HEALTHY" ? "state-queued" : ""}`}>{status.hdfs.status}</span>
        </article>
        <article className="panel playground-service-card">
          <div className={`service-orb spark-orb ${status.spark.status === "HEALTHY" ? "online" : ""}`}>S</div>
          <div><p className="eyebrow">COMPUTE</p><h2>Spark</h2><code>{status.spark.endpoint}</code></div>
          <span className={`state ${status.spark.status === "HEALTHY" ? "state-queued" : ""}`}>{status.spark.status}</span>
        </article>
        <article className="panel playground-service-card sample-card">
          <div className={`service-orb table-orb ${targetMode === "bulk" ? bulkTargetValid ? "online" : "" : targetValid ? "online" : ""}`}>I</div>
          <div><p className="eyebrow">QUERY TARGET</p><h2>{targetMode === "bulk" ? `${parsedBulkTableNames.length} tables` : targetValid ? "Ready" : "Select table"}</h2><code>{targetMode === "bulk" ? `${catalogName}.${namespaceName} · BULK` : displayTable}</code></div>
          <button className="secondary-button" onClick={refreshStatus} disabled={busy !== null}>{busy === "status" ? "확인 중…" : "상태 확인"}</button>
        </article>
      </section>

      <section className="playground-layout">
        <article className="panel playground-setup">
          <div className="panel-title"><div><p className="eyebrow">STEP 01</p><h2>조회 대상</h2></div><span className="muted">Hadoop Catalog</span></div>
          <div className="target-form">
            <div className="target-mode-switch playground-mode-switch" role="group" aria-label="조회 대상 선택 방식"><button type="button" className={targetMode === "single" ? "active" : ""} onClick={() => setTargetMode("single")}>단일 테이블 SQL</button><button type="button" className={targetMode === "bulk" ? "active" : ""} onClick={() => setTargetMode("bulk")}>Bulk 메트릭</button></div>
            <label>Catalog<select value={catalogName} onChange={(event) => selectCatalog(event.target.value)}>{catalogs.map((catalog) => <option key={catalog.id} value={catalog.name}>{catalog.name}</option>)}</select></label>
            <small>{selectedCatalog?.warehouse ?? "Catalog을 먼저 등록하세요."}</small>
            <label>Namespace<select value={namespaceName} onChange={(event) => { setNamespaceName(event.target.value); setResult(null); setBulkResult(null); }}><option value="" disabled>Namespace 선택</option>{selectedCatalog?.namespaces.map((namespace) => <option key={namespace} value={namespace}>{namespace}</option>)}</select></label>
            {targetMode === "single" ? <label>table_name<input type="text" value={tableName} onChange={(event) => { setTableName(event.target.value); setResult(null); }} placeholder="orders" /></label> : <label>table_name 목록<textarea className="bulk-table-input" value={bulkTableNames} onChange={(event) => { setBulkTableNames(event.target.value); setBulkResult(null); }} placeholder={"orders\nevents\ncustomers"} /></label>}
            <div className="qualified-target"><span>{targetMode === "bulk" ? `BULK TARGET · ${parsedBulkTableNames.length}/200` : "QUALIFIED TABLE"}</span><code>{targetMode === "bulk" ? parsedBulkTableNames.slice(0, 4).join(", ") || "table_name 입력 필요" : displayTable}{targetMode === "bulk" && parsedBulkTableNames.length > 4 ? ` 외 ${parsedBulkTableNames.length - 4}개` : ""}</code></div>
          </div>

          <details className="registry-panel">
            <summary>Catalog / Namespace 등록</summary>
            <form onSubmit={registerCatalog}>
              <h3>HDFS Catalog</h3>
              <label>Catalog name<input type="text" required pattern="[A-Za-z_][A-Za-z0-9_]*" value={newCatalogName} onChange={(event) => setNewCatalogName(event.target.value)} placeholder="existing_lake" /></label>
              <label>Warehouse URI<input type="text" required value={newWarehouse} onChange={(event) => setNewWarehouse(event.target.value)} placeholder="hdfs://namenode:8020/warehouse" /></label>
              <button className="secondary-button" disabled={busy !== null}>{busy === "catalog" ? "등록 중…" : "Catalog 등록"}</button>
            </form>
            <form onSubmit={registerNamespace}>
              <h3>Namespace</h3>
              <p><code>{catalogName || "Catalog 선택 필요"}</code></p>
              <label>Namespace name<input type="text" required pattern="[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*" value={newNamespace} onChange={(event) => setNewNamespace(event.target.value)} placeholder="analytics" /></label>
              <button className="secondary-button" disabled={!selectedCatalog || busy !== null}>{busy === "namespace" ? "등록 중…" : "Namespace 등록"}</button>
            </form>
          </details>

          <div className="sample-helper">
            <p>테스트 데이터가 필요하면 기본 <code>playground.demo.orders</code>를 준비할 수 있습니다.</p>
            <button className="secondary-button" onClick={createSample} disabled={!status.ready || busy !== null}>{busy === "bootstrap" ? "Spark 작업 실행 중…" : status.sampleExists ? "Sample 상태 재확인" : "Sample table 생성"}</button>
            {bootstrap && <div className="bootstrap-result"><code>{bootstrap.location}</code><span>{bootstrap.totalRows} rows · {bootstrap.snapshotCount} snapshots</span></div>}
          </div>
        </article>

        <article className="panel sql-panel">
          {targetMode === "single" ? <><div className="panel-title"><div><p className="eyebrow">STEP 02</p><h2>Read-only Spark SQL</h2></div><span className="muted">최대 200 rows</span></div><div className="query-presets">{queries.map((query, index) => <button key={query.label} className={activePreset === index ? "active" : ""} onClick={() => { setActivePreset(index); setSql(query.sql); }}>{query.label}</button>)}</div><textarea className="sql-editor" spellCheck={false} value={sql} onChange={(event) => { setActivePreset(null); setSql(event.target.value); }} /><div className="sql-actions"><span>{message}</span><button className="primary-button" onClick={runQuery} disabled={!status.ready || !targetValid || busy !== null}>{busy === "query" ? "실행 중…" : "Run query"}</button></div></> : <><div className="panel-title"><div><p className="eyebrow">STEP 02</p><h2>Bulk metadata metrics</h2></div><span className="muted">최대 200 tables</span></div><div className="bulk-query-copy"><p>각 테이블의 Iceberg metadata table을 비동기 작업으로 조회합니다. 화면을 기다리는 동안 서버는 테이블별 진행 상태와 부분 실패를 저장합니다.</p><ul><li>format version · current snapshot · last commit</li><li>partition · data/delete file · record · storage size</li><li>snapshot count · small file ratio</li></ul></div>{bulkJob && busy === "bulk" && <div className="bulk-job-card"><div><strong>Bulk inspection</strong><span>{bulkJob.state}</span></div><progress max={bulkJob.totalItems} value={bulkJob.completedItems} /><small>{bulkJob.completedItems}/{bulkJob.totalItems} 완료</small></div>}<div className="sql-actions"><span>{message}</span><button className="primary-button" onClick={runBulkMetrics} disabled={!status.ready || !bulkTargetValid || busy !== null}>{busy === "bulk" ? "메트릭 조회 중…" : `${parsedBulkTableNames.length}개 테이블 조회`}</button></div></>}
        </article>
      </section>

      <section className="panel playground-results">
        <div className="panel-title"><div><p className="eyebrow">RESULT</p><h2>{targetMode === "bulk" ? "Table metric summary" : "Query output"}</h2></div>{targetMode === "single" && result && <span className="muted">{result.rowCount} rows · {result.durationMs}ms{result.truncated ? " · truncated" : ""}</span>}{targetMode === "bulk" && bulkResult && <span className="muted">{bulkResult.tables.length} success · {bulkResult.errors.length} failed</span>}</div>
        {targetMode === "single" ? (!result ? <div className="result-empty">조회 대상을 선택하고 SQL을 실행하면 결과가 여기에 표시됩니다.</div> : <div className="table-wrap"><table><thead><tr>{result.columns.map((column) => <th key={column.name}>{column.name}<small>{column.dataType}</small></th>)}</tr></thead><tbody>{result.rows.map((row, index) => <tr key={index}>{result.columns.map((column) => <td key={column.name}>{displayValue(row[column.name])}</td>)}</tr>)}</tbody></table></div>) : (!bulkResult ? <div className="result-empty">여러 table_name을 입력하고 메트릭 조회를 실행하면 비교표가 표시됩니다.</div> : <div className="bulk-result-layout"><div className="table-wrap bulk-metric-table"><table><thead><tr><th>Table</th><th>Format</th><th>Partitions</th><th>Files</th><th>Records</th><th>Size</th><th>Snapshots</th><th>Last commit</th></tr></thead><tbody>{bulkResult.tables.map((table) => <tr key={table.id} className={focusedTable?.id === table.id ? "selected" : ""} onClick={() => setFocusedTableId(table.id)}><td><strong>{table.tableName}</strong><small>{table.catalogName}.{table.namespaceName}</small></td><td>v{table.formatVersion ?? "—"}</td><td>{table.partitionCount ?? "—"}</td><td>{table.totalFiles}</td><td>{table.totalRecords.toLocaleString("ko-KR")}</td><td>{formatBytes(table.totalBytes)}</td><td>{table.snapshotCount}</td><td>{formatDate(table.lastCommitAt)}</td></tr>)}</tbody></table></div>{focusedTable && <aside className="bulk-table-detail"><div><span>SELECTED TABLE</span><h3>{qualifiedName(focusedTable)}</h3></div><dl><div><dt>Current snapshot</dt><dd>{focusedTable.currentSnapshotId ?? "—"}</dd></div><div><dt>Small files</dt><dd>{focusedTable.smallFileRatio == null ? "—" : `${(focusedTable.smallFileRatio * 100).toFixed(1)}%`}</dd></div><div><dt>Delete files</dt><dd>{focusedTable.deleteFileRatio == null ? "—" : `${(focusedTable.deleteFileRatio * 100).toFixed(1)}%`}</dd></div><div><dt>Observed</dt><dd>{formatDate(focusedTable.observedAt)}</dd></div></dl><button className="secondary-button" onClick={() => openTableInSql(focusedTable)}>이 테이블 SQL 상세 조회</button></aside>}{bulkResult.errors.length > 0 && <div className="bulk-errors"><strong>조회 실패 {bulkResult.errors.length}개</strong>{bulkResult.errors.map((error) => <p key={error.tableName}><code>{error.tableName}</code><span>{error.detail}</span></p>)}</div>}</div>)}
      </section>
    </>
  );
}
