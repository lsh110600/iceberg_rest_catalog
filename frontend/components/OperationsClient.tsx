"use client";

import { useEffect, useMemo, useState } from "react";
import { useSearchParams } from "next/navigation";
import type {
  BulkJob,
  OperationExecution,
  OperationPage,
  OperationPlan,
  OperationRequest,
  PlaygroundCatalog,
  TableSummary,
  WorkerStatus,
} from "@/lib/api";

const commands = [
  ["REWRITE_DATA_FILES", "Rewrite data files"],
  ["REWRITE_MANIFESTS", "Rewrite manifests"],
  ["REWRITE_POSITION_DELETES", "Rewrite position deletes"],
  ["COMPUTE_STATS", "Compute statistics"],
  ["EXPIRE_SNAPSHOTS", "Expire snapshots"],
  ["REMOVE_ORPHAN_FILES", "Remove orphan files"],
  ["ROLLBACK", "Rollback snapshot"],
];

const runningStates = new Set(["QUEUED", "RUNNING", "VERIFYING", "CANCEL_REQUESTED"]);
const identifierPattern = /^[A-Za-z_][A-Za-z0-9_]*$/;

function parseTableNames(value: string): string[] {
  return [...new Set(value.split(/[\s,]+/).map((name) => name.trim()).filter(Boolean))];
}

function errorMessage(payload: { message?: string; detail?: string }) {
  if (payload.message) return payload.message;
  if (typeof payload.detail === "string") return payload.detail;
  if (payload.detail) return JSON.stringify(payload.detail);
  return "요청을 처리하지 못했습니다.";
}

function resultState(execution?: OperationExecution) {
  const before = execution?.result.before as Record<string, unknown> | undefined;
  const after = execution?.result.after as Record<string, unknown> | undefined;
  if (!before || !after) return null;
  return { before, after };
}

export function OperationsClient({
  initialOperations,
  initialOperationCursor,
  initialHasMoreOperations,
  initialTables,
  initialTableCursor,
  initialHasMoreTables,
  initialWorker,
  initialCatalogs,
}: {
  initialOperations: OperationRequest[];
  initialOperationCursor?: string;
  initialHasMoreOperations: boolean;
  initialTables: TableSummary[];
  initialTableCursor?: string;
  initialHasMoreTables: boolean;
  initialWorker: WorkerStatus;
  initialCatalogs: PlaygroundCatalog[];
}) {
  const searchParams = useSearchParams();
  const requestedTable = searchParams.get("table");
  const initialCatalogNames = new Set(initialCatalogs.map((catalog) => catalog.name));
  const scopedTable = initialTables.find((table) => initialCatalogNames.has(table.catalogName));
  const defaultTable = initialTables.find((table) => table.id === requestedTable)
    ?? scopedTable
    ?? initialTables[0];
  const [operations, setOperations] = useState(initialOperations);
  const [bulkJobs, setBulkJobs] = useState<BulkJob[]>([]);
  const [nextCursor, setNextCursor] = useState(initialOperationCursor);
  const [hasMore, setHasMore] = useState(initialHasMoreOperations);
  const [plan, setPlan] = useState<OperationPlan | null>(null);
  const [planning, setPlanning] = useState(false);
  const [tables, setTables] = useState(initialTables);
  const [tableCursor, setTableCursor] = useState(initialTableCursor);
  const [hasMoreTables, setHasMoreTables] = useState(initialHasMoreTables);
  const [catalogs, setCatalogs] = useState(initialCatalogs);
  const [worker, setWorker] = useState(initialWorker);
  const [tableId, setTableId] = useState(defaultTable?.id ?? "");
  const [targetMode, setTargetMode] = useState<"single" | "bulk">("single");
  const [bulkTableIds, setBulkTableIds] = useState<string[]>(defaultTable ? [defaultTable.id] : []);
  const [tableSearch, setTableSearch] = useState("");
  const [command, setCommand] = useState("REWRITE_DATA_FILES");
  const [reason, setReason] = useState("");
  const [rewriteAll, setRewriteAll] = useState(true);
  const [olderThanHours, setOlderThanHours] = useState(168);
  const [retainLast, setRetainLast] = useState(1);
  const [dryRun, setDryRun] = useState(true);
  const [snapshotId, setSnapshotId] = useState("");
  const [message, setMessage] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [registryBusy, setRegistryBusy] = useState<"catalog" | "namespace" | "table" | null>(null);
  const [targetCatalogName, setTargetCatalogName] = useState(
    initialCatalogNames.has(defaultTable?.catalogName ?? "")
      ? defaultTable?.catalogName ?? ""
      : initialCatalogs[0]?.name ?? "",
  );
  const defaultTargetCatalog = initialCatalogs.find(
    (catalog) => catalog.name === defaultTable?.catalogName,
  ) ?? initialCatalogs[0];
  const [targetNamespaceName, setTargetNamespaceName] = useState(
    defaultTargetCatalog?.namespaces.includes(defaultTable?.namespaceName ?? "")
      ? defaultTable?.namespaceName ?? ""
      : defaultTargetCatalog?.namespaces[0] ?? "",
  );
  const [targetTableName, setTargetTableName] = useState(
    initialCatalogNames.has(defaultTable?.catalogName ?? "") ? defaultTable?.tableName ?? "" : "",
  );
  const [registrationMode, setRegistrationMode] = useState<"single" | "bulk">("single");
  const [newCatalogName, setNewCatalogName] = useState("");
  const [newWarehouse, setNewWarehouse] = useState("hdfs://namenode:8020/warehouse");
  const [newNamespace, setNewNamespace] = useState("");
  const selected = useMemo(() => tables.find((table) => table.id === tableId), [tableId, tables]);
  const selectedTargetCatalog = useMemo(
    () => catalogs.find((catalog) => catalog.name === targetCatalogName),
    [catalogs, targetCatalogName],
  );
  const registeredCatalogNames = useMemo(
    () => new Set(catalogs.map((catalog) => catalog.name)),
    [catalogs],
  );
  const scopedManagedTable = tables.find((table) => registeredCatalogNames.has(table.catalogName));
  const executionMode = worker.details.executionMode === "livy" ? "livy" : "playground";
  const catalogScope = worker.details.catalogScope === "production" ? "production" : "playground";
  const visibleTables = useMemo(() => {
    const query = tableSearch.trim().toLocaleLowerCase();
    return tables.filter((table) => {
      const inScope = catalogScope === "production"
        ? !registeredCatalogNames.has(table.catalogName)
        : registeredCatalogNames.has(table.catalogName);
      const qualifiedName = `${table.catalogName}.${table.namespaceName}.${table.tableName}`;
      return inScope && (!query || qualifiedName.toLocaleLowerCase().includes(query));
    });
  }, [catalogScope, registeredCatalogNames, tableSearch, tables]);
  const operationTargets = targetMode === "bulk"
    ? tables.filter((table) => bulkTableIds.includes(table.id))
    : selected ? [selected] : [];
  const executable = operationTargets.length > 0 && operationTargets.every((table) => (
    catalogScope === "production"
      ? !registeredCatalogNames.has(table.catalogName)
      : registeredCatalogNames.has(table.catalogName)
  ));
  const registrationTableNames = parseTableNames(targetTableName);
  const registrationValid = registrationTableNames.length > 0
    && registrationTableNames.every((name) => identifierPattern.test(name));
  const executionLabel = executionMode === "livy"
    ? catalogScope === "playground" ? "Playground Livy" : "Livy batch"
    : "Local Spark";
  const workerAvailable = worker.online && !["DEGRADED", "OFFLINE"].includes(worker.status);

  useEffect(() => {
    async function refresh() {
      try {
        const [operationsResponse, workerResponse, jobsResponse] = await Promise.all([
          fetch("/api/operation-requests/page?limit=30", { cache: "no-store" }),
          fetch("/api/workers/spark", { cache: "no-store" }),
          fetch("/api/bulk-jobs?limit=20", { cache: "no-store" }),
        ]);
        if (operationsResponse.ok) {
          const page: OperationPage = await operationsResponse.json();
          setOperations((current) => [
            ...page.items,
            ...current.filter((item) => !page.items.some((fresh) => fresh.id === item.id)),
          ]);
          setNextCursor(page.nextCursor);
          setHasMore(page.hasMore);
        }
        if (workerResponse.ok) setWorker(await workerResponse.json());
        if (jobsResponse.ok) setBulkJobs(await jobsResponse.json());
      } catch {
        setWorker((current) => ({ ...current, status: "OFFLINE", online: false }));
      }
    }
    void refresh();
    const events = new EventSource("/api/operation-stream");
    events.addEventListener("operations", () => void refresh());
    const workerTimer = window.setInterval(() => void refresh(), 15000);
    return () => { events.close(); window.clearInterval(workerTimer); };
  }, []);

  useEffect(() => setPlan(null), [command, tableId, bulkTableIds, rewriteAll, olderThanHours, retainLast, dryRun, snapshotId, targetMode]);

  function upsertCatalog(next: PlaygroundCatalog) {
    setCatalogs((current) => [...current.filter((catalog) => catalog.id !== next.id), next]
      .sort((left, right) => left.name.localeCompare(right.name)));
  }

  function selectTargetCatalog(name: string) {
    const catalog = catalogs.find((item) => item.name === name);
    setTargetCatalogName(name);
    setTargetNamespaceName(catalog?.namespaces[0] ?? "");
    setTargetTableName("");
  }

  async function registerCatalog() {
    setRegistryBusy("catalog");
    setMessage("");
    try {
      const response = await fetch("/api/playground/catalogs", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ name: newCatalogName, warehouse: newWarehouse }),
      });
      const payload = await response.json();
      if (!response.ok) throw new Error(errorMessage(payload));
      upsertCatalog(payload);
      setTargetCatalogName(payload.name);
      setTargetNamespaceName(payload.namespaces[0] ?? "");
      setNewCatalogName("");
      setMessage(`${payload.name} Catalog을 등록했습니다.`);
    } catch (error) {
      setMessage(error instanceof Error ? error.message : "Catalog 등록에 실패했습니다.");
    } finally {
      setRegistryBusy(null);
    }
  }

  async function registerNamespace() {
    setRegistryBusy("namespace");
    setMessage("");
    try {
      const registeredName = newNamespace;
      const response = await fetch("/api/playground/namespaces", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ catalogName: targetCatalogName, name: registeredName }),
      });
      const payload = await response.json();
      if (!response.ok) throw new Error(errorMessage(payload));
      upsertCatalog(payload);
      setTargetNamespaceName(registeredName);
      setNewNamespace("");
      setMessage(`${payload.name}.${registeredName} Namespace를 등록했습니다.`);
    } catch (error) {
      setMessage(error instanceof Error ? error.message : "Namespace 등록에 실패했습니다.");
    } finally {
      setRegistryBusy(null);
    }
  }

  async function registerTable() {
    setRegistryBusy("table");
    setMessage("");
    try {
      const bulk = registrationMode === "bulk";
      const response = await fetch(`/api/playground/${bulk ? "tables-bulk-jobs" : "tables"}`, {
        method: "POST",
        headers: { "Content-Type": "application/json", "Idempotency-Key": crypto.randomUUID() },
        body: JSON.stringify(bulk ? {
          catalogName: targetCatalogName,
          namespaceName: targetNamespaceName,
          tableNames: registrationTableNames,
        } : {
          catalogName: targetCatalogName,
          namespaceName: targetNamespaceName,
          tableName: registrationTableNames[0],
        }),
      });
      let payload = await response.json();
      if (!response.ok) throw new Error(errorMessage(payload));
      if (bulk) {
        let job = payload as BulkJob;
        setBulkJobs((current) => [job, ...current.filter((item) => item.id !== job.id)]);
        const terminal = new Set(["SUCCEEDED", "FAILED", "PARTIAL_SUCCESS", "CANCELED"]);
        while (!terminal.has(job.state)) {
          await new Promise((resolve) => window.setTimeout(resolve, 750));
          const jobResponse = await fetch(`/api/bulk-jobs/${job.id}`, { cache: "no-store" });
          job = await jobResponse.json();
          if (!jobResponse.ok) throw new Error(errorMessage(job as { detail?: string }));
          setBulkJobs((current) => [job, ...current.filter((item) => item.id !== job.id)]);
          setMessage(`${job.completedItems}/${job.totalItems}개 테이블 확인 중…`);
        }
        payload = {
          tables: job.items.filter((item) => item.state === "SUCCEEDED").map((item) => item.result),
          errors: job.items.filter((item) => item.state === "FAILED").map((item) => ({
            tableName: item.tableName,
            detail: item.errorMessage,
          })),
        };
      }
      const registeredTables: TableSummary[] = bulk ? payload.tables : [payload];
      setTables((current) => [
        ...current.filter((table) => !registeredTables.some((item) => item.id === table.id)),
        ...registeredTables,
      ].sort((left, right) => (
        `${left.catalogName}.${left.namespaceName}.${left.tableName}`
          .localeCompare(`${right.catalogName}.${right.namespaceName}.${right.tableName}`)
      )));
      if (bulk) {
        setTargetMode("bulk");
        setBulkTableIds((current) => [...new Set([
          ...current,
          ...registeredTables.map((table) => table.id),
        ])]);
        setMessage(
          `${registeredTables.length}개 테이블을 등록했습니다.`
          + (payload.errors.length ? ` ${payload.errors.length}개는 확인에 실패했습니다.` : ""),
        );
      } else if (registeredTables[0]) {
        setTargetMode("single");
        setTableId(registeredTables[0].id);
        setMessage(`${registeredTables[0].catalogName}.${registeredTables[0].namespaceName}.${registeredTables[0].tableName}을 작업 대상으로 등록했습니다.`);
      }
    } catch (error) {
      setMessage(error instanceof Error ? error.message : "작업 대상 테이블 등록에 실패했습니다.");
    } finally {
      setRegistryBusy(null);
    }
  }

  function parameters() {
    if (command === "REWRITE_DATA_FILES") {
      return { rewriteAll, targetFileSizeBytes: 134217728 };
    }
    if (command === "REWRITE_POSITION_DELETES") return { rewriteAll };
    if (command === "EXPIRE_SNAPSHOTS") return { olderThanHours, retainLast };
    if (command === "REMOVE_ORPHAN_FILES") return { olderThanHours, dryRun };
    if (command === "ROLLBACK") return { snapshotId };
    return {};
  }

  async function calculatePlan() {
    if (!operationTargets.length) return;
    setPlanning(true);
    setMessage("");
    try {
      const response = await fetch("/api/operation-plans", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          tableIds: operationTargets.map((table) => table.id),
          command,
          preferredEngine: "SPARK",
          parameters: parameters(),
        }),
      });
      const payload = await response.json();
      if (!response.ok) throw new Error(errorMessage(payload));
      setPlan(payload);
      setMessage(`계획 완료: ${payload.tables.length}개 테이블, 위험도 ${payload.highestRisk}`);
    } catch (error) {
      setMessage(error instanceof Error ? error.message : "작업 계획 계산에 실패했습니다.");
    } finally {
      setPlanning(false);
    }
  }

  async function submit(event: React.FormEvent) {
    event.preventDefault();
    if (!executable) {
      setMessage(`현재 ${executionLabel} worker가 이 catalog를 실행할 수 없습니다.`);
      return;
    }
    if (!plan) {
      setMessage("먼저 계획 계산을 실행하고 영향 범위를 확인하세요.");
      return;
    }
    setSubmitting(true);
    setMessage("");
    const bulk = targetMode === "bulk";
    const response = await fetch(`/api/operation-requests${bulk ? "/bulk" : ""}`, {
      method: "POST",
      headers: { "Content-Type": "application/json", "Idempotency-Key": crypto.randomUUID() },
      body: JSON.stringify({
        ...(bulk ? { tableIds: operationTargets.map((table) => table.id) } : { tableId }),
        command,
        preferredEngine: "SPARK",
        parameters: parameters(),
        reason,
        requester: "web.operator",
      }),
    });
    const payload = await response.json();
    if (response.ok) {
      const created: OperationRequest[] = bulk ? payload.operations : [payload];
      if (bulk && payload.bulkJob) setBulkJobs((current) => [payload.bulkJob, ...current]);
      setOperations((current) => [...created, ...current]);
      setReason("");
      setMessage(`${created.length}개 작업 요청이 승인 대기열에 등록되었습니다.`);
    } else {
      setMessage(errorMessage(payload));
    }
    setSubmitting(false);
  }

  async function decide(id: string, decision: "APPROVED" | "REJECTED") {
    const response = await fetch(`/api/operation-requests/${id}/approvals`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        approver: "web.approver",
        decision,
        comment: "Spark execution console decision",
      }),
    });
    const payload = await response.json();
    if (response.ok) {
      setOperations((current) => current.map((operation) => operation.id === id ? payload : operation));
      setMessage(decision === "APPROVED" ? "승인되었습니다. Spark worker가 곧 실행합니다." : "반려되었습니다.");
    } else {
      setMessage(errorMessage(payload));
    }
  }

  async function retry(id: string) {
    const response = await fetch(`/api/operation-requests/${id}/retry`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ actor: "web.operator" }),
    });
    const payload = await response.json();
    if (response.ok) {
      setOperations((current) => current.map((operation) => operation.id === id ? payload : operation));
      setMessage("실패 작업을 다시 대기열에 등록했습니다.");
    } else {
      setMessage(errorMessage(payload));
    }
  }

  async function cancel(id: string, version: number) {
    const response = await fetch(`/api/operation-requests/${id}/cancel`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ actor: "web.operator", reason: "Canceled from Operations UI", expectedVersion: version }),
    });
    const payload = await response.json();
    if (response.ok) {
      setOperations((current) => current.map((operation) => operation.id === id ? payload : operation));
      setMessage("취소 요청을 반영했습니다.");
    } else setMessage(errorMessage(payload));
  }

  async function bulkAction(id: string, action: "cancel" | "retry-failed") {
    const response = await fetch(`/api/bulk-jobs/${id}/${action}`, {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ actor: "web.operator", reason: "Requested from Operations UI" }),
    });
    const payload = await response.json();
    if (response.ok) setBulkJobs((current) => current.map((job) => job.id === id ? payload : job));
    else setMessage(errorMessage(payload));
  }

  async function loadMore() {
    if (!nextCursor) return;
    const response = await fetch(`/api/operation-requests/page?limit=30&cursor=${encodeURIComponent(nextCursor)}`, { cache: "no-store" });
    if (!response.ok) return;
    const page: OperationPage = await response.json();
    setOperations((current) => [...current, ...page.items.filter((item) => !current.some((old) => old.id === item.id))]);
    setNextCursor(page.nextCursor);
    setHasMore(page.hasMore);
  }

  async function loadMoreTables() {
    if (!tableCursor) return;
    const response = await fetch(`/api/tables/page?limit=100&cursor=${encodeURIComponent(tableCursor)}`, { cache: "no-store" });
    if (!response.ok) return;
    const page = await response.json();
    setTables((current) => [...current, ...page.items.filter((item: TableSummary) => !current.some((old) => old.id === item.id))]);
    setTableCursor(page.nextCursor);
    setHasMoreTables(page.hasMore);
  }

  return (
    <>
      <section className="worker-strip panel">
        <div className={`worker-orb ${worker.online ? "online" : ""}`}>S</div>
        <div className="worker-copy">
          <p className="eyebrow">EXECUTION WORKER</p>
          <strong>{executionLabel} operation worker</strong>
          <span>{worker.workerId ?? "아직 heartbeat가 없습니다."}</span>
        </div>
        <div className="worker-facts">
          <span className={`state state-${worker.status.toLowerCase()}`}>{worker.status}</span>
          <code>{worker.currentOperationRequestId ?? "queue polling"}</code>
          <small>{worker.lastSeenAt ? `${new Date(worker.lastSeenAt).toLocaleTimeString("ko-KR")} heartbeat` : "offline"}</small>
        </div>
      </section>

      <div className="operations-layout">
        <form className="panel operation-form" onSubmit={submit}>
          <div className="panel-title"><div><p className="eyebrow">NEW REQUEST</p><h2>Spark maintenance plan</h2></div></div>
          <div className="target-mode-switch" role="group" aria-label="작업 대상 선택 방식">
            <button type="button" className={targetMode === "single" ? "active" : ""} onClick={() => setTargetMode("single")}>단일 테이블</button>
            <button type="button" className={targetMode === "bulk" ? "active" : ""} onClick={() => setTargetMode("bulk")}>Bulk 작업</button>
          </div>
          {targetMode === "single" ? (
            <label>작업 대상<select value={tableId} onChange={(event) => setTableId(event.target.value)}>{tables.map((table) => <option key={table.id} value={table.id}>{table.catalogName}.{table.namespaceName}.{table.tableName}</option>)}</select></label>
          ) : (
            <div className="bulk-target-picker">
              <div className="bulk-target-head"><strong>작업 대상 {bulkTableIds.length}개</strong><button type="button" onClick={() => setBulkTableIds([])}>선택 해제</button></div>
              <input type="text" value={tableSearch} onChange={(event) => setTableSearch(event.target.value)} placeholder="Catalog, Namespace 또는 table 검색" />
              <div className="bulk-target-actions"><button type="button" onClick={() => setBulkTableIds((current) => [...new Set([...current, ...visibleTables.map((table) => table.id)])].slice(0, 200))}>검색 결과 전체 선택</button><span>{visibleTables.length} tables · 최대 200개</span></div>
              <div className="bulk-target-list">{visibleTables.map((table) => {
                const checked = bulkTableIds.includes(table.id);
                const qualifiedName = `${table.catalogName}.${table.namespaceName}.${table.tableName}`;
                return <label key={table.id}><input type="checkbox" checked={checked} disabled={!checked && bulkTableIds.length >= 200} onChange={() => setBulkTableIds((current) => checked ? current.filter((id) => id !== table.id) : [...current, table.id].slice(0, 200))} /><span><strong>{qualifiedName}</strong><small>{table.totalFiles} files · {table.snapshotCount} snapshots</small></span></label>;
              })}</div>{hasMoreTables && <button type="button" className="secondary-button" onClick={loadMoreTables}>테이블 100개 더 불러오기</button>}
            </div>
          )}
          {catalogScope === "playground" && !scopedManagedTable && <div className="execution-notice">실행 가능한 HDFS 테이블이 없습니다. 아래에서 Catalog, Namespace와 table_name을 등록하세요.</div>}
          {operationTargets.length > 0 && !executable && <div className="execution-notice warning">{catalogScope === "production" ? "등록된 HDFS Hadoop Catalog 테이블은 Playground worker에서 실행합니다." : "선택한 테이블 중 일부는 production Livy worker를 구성한 뒤 실행할 수 있습니다."}</div>}

          <details className="operation-registry">
            <summary>Catalog / Namespace / table 등록</summary>
            <div className="operation-registry-body">
              <h3>작업 대상 테이블</h3>
              <label>Catalog<select value={targetCatalogName} onChange={(event) => selectTargetCatalog(event.target.value)}>{catalogs.map((catalog) => <option key={catalog.id} value={catalog.name}>{catalog.name}</option>)}</select></label>
              <small>{selectedTargetCatalog?.warehouse ?? "Catalog을 먼저 등록하세요."}</small>
              <label>Namespace<select value={targetNamespaceName} onChange={(event) => setTargetNamespaceName(event.target.value)}><option value="" disabled>Namespace 선택</option>{selectedTargetCatalog?.namespaces.map((namespace) => <option key={namespace} value={namespace}>{namespace}</option>)}</select></label>
              <div className="inline-mode-switch" role="group" aria-label="테이블 등록 방식"><button type="button" className={registrationMode === "single" ? "active" : ""} onClick={() => { setRegistrationMode("single"); setTargetTableName(registrationTableNames[0] ?? ""); }}>단일</button><button type="button" className={registrationMode === "bulk" ? "active" : ""} onClick={() => setRegistrationMode("bulk")}>Bulk</button></div>
              <label>{registrationMode === "bulk" ? "table_name 목록" : "table_name"}{registrationMode === "bulk" ? <textarea value={targetTableName} onChange={(event) => setTargetTableName(event.target.value)} placeholder={"orders\nevents\ncustomers"} /> : <input type="text" pattern="[A-Za-z_][A-Za-z0-9_]*" value={targetTableName} onChange={(event) => setTargetTableName(event.target.value)} placeholder="orders" />}</label>
              {registrationMode === "bulk" && <small>줄바꿈·공백·쉼표로 구분 · 중복 제외 {registrationTableNames.length}개 · 최대 200개</small>}
              <button type="button" className="secondary-button" onClick={registerTable} disabled={!selectedTargetCatalog || !targetNamespaceName || !registrationValid || registryBusy !== null}>{registryBusy === "table" ? "확인·등록 중…" : registrationMode === "bulk" ? `${registrationTableNames.length}개 일괄 확인·등록` : "작업 대상 등록·선택"}</button>
            </div>
            <div className="operation-registry-body registry-subsection">
              <h3>HDFS Catalog 등록</h3>
              <label>Catalog name<input type="text" pattern="[A-Za-z_][A-Za-z0-9_]*" value={newCatalogName} onChange={(event) => setNewCatalogName(event.target.value)} placeholder="existing_lake" /></label>
              <label>Warehouse URI<input type="text" value={newWarehouse} onChange={(event) => setNewWarehouse(event.target.value)} /></label>
              <button type="button" className="secondary-button" onClick={registerCatalog} disabled={!newCatalogName || !newWarehouse || registryBusy !== null}>{registryBusy === "catalog" ? "등록 중…" : "Catalog 등록"}</button>
            </div>
            <div className="operation-registry-body registry-subsection">
              <h3>Namespace 등록</h3>
              <p><code>{targetCatalogName || "Catalog 선택 필요"}</code></p>
              <label>Namespace name<input type="text" pattern="[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*" value={newNamespace} onChange={(event) => setNewNamespace(event.target.value)} placeholder="analytics" /></label>
              <button type="button" className="secondary-button" onClick={registerNamespace} disabled={!selectedTargetCatalog || !newNamespace || registryBusy !== null}>{registryBusy === "namespace" ? "등록 중…" : "Namespace 등록"}</button>
            </div>
          </details>
          <div className="form-row">
            <label>Command<select value={command} onChange={(event) => setCommand(event.target.value)}>{commands.map(([value, label]) => <option key={value} value={value}>{label}</option>)}</select></label>
            <label>Execution engine<select value="SPARK" disabled><option>SPARK</option></select></label>
          </div>
          {(command === "REWRITE_DATA_FILES" || command === "REWRITE_POSITION_DELETES") && <label className="checkbox-label"><input type="checkbox" checked={rewriteAll} onChange={(event) => setRewriteAll(event.target.checked)} />작은 샘플에서도 실행 확인을 위해 모든 대상 파일 rewrite</label>}
          {(command === "EXPIRE_SNAPSHOTS" || command === "REMOVE_ORPHAN_FILES") && <div className="form-row"><label>Older than (hours)<input type="number" min={command === "EXPIRE_SNAPSHOTS" ? 120 : 72} value={olderThanHours} onChange={(event) => setOlderThanHours(Number(event.target.value))} /></label>{command === "EXPIRE_SNAPSHOTS" && <label>Retain last<input type="number" min={1} value={retainLast} onChange={(event) => setRetainLast(Number(event.target.value))} /></label>}</div>}
          {command === "REMOVE_ORPHAN_FILES" && <label className="checkbox-label"><input type="checkbox" checked={dryRun} onChange={(event) => setDryRun(event.target.checked)} />Dry run — 삭제 후보만 조회</label>}
          {command === "ROLLBACK" && <label>Target snapshot ID<input type="number" min={1} required value={snapshotId} onChange={(event) => setSnapshotId(event.target.value)} /></label>}
          <label>Reason<textarea required minLength={5} value={reason} onChange={(event) => setReason(event.target.value)} placeholder="작업의 목적과 근거를 입력하세요." /></label>
          <div className="request-preview"><span>{targetMode === "bulk" ? `BULK TARGET · ${operationTargets.length} TABLES` : "TARGET"}</span><strong>{operationTargets.length ? operationTargets.slice(0, 3).map((table) => `${table.catalogName}.${table.namespaceName}.${table.tableName}`).join(", ") : "—"}{operationTargets.length > 3 ? ` 외 ${operationTargets.length - 3}개` : ""}</strong><small>{targetMode === "single" ? `Planned snapshot ${selected?.currentSnapshotId ?? "current"}` : "각 테이블의 현재 snapshot을 개별 계획에 고정"} · 승인 후 실행 직전에 실제 snapshot을 다시 검사합니다.</small></div>
          <div className="plan-actions"><button type="button" className="secondary-button" onClick={calculatePlan} disabled={planning || !executable}>{planning ? "계획 계산 중…" : "영향 범위 계산"}</button>{plan && <span className={`risk risk-${plan.highestRisk.toLowerCase()}`}>{plan.highestRisk}</span>}</div>
          {plan && <div className="operation-plan"><strong>{plan.tables.length} tables · {plan.totalFiles.toLocaleString("ko-KR")} files</strong><span>예상 rewrite {(plan.estimatedRewriteBytes / 1024 / 1024).toFixed(1)} MiB</span>{plan.tables.flatMap((item) => item.warnings).slice(0, 4).map((warning, index) => <small key={`${warning}-${index}`}>⚠ {warning}</small>)}</div>}
          <button className="primary-button" disabled={!plan || submitting || registryBusy !== null || operationTargets.length === 0 || !executable || !workerAvailable}>{submitting ? "등록 중…" : workerAvailable ? targetMode === "bulk" ? `${operationTargets.length}개 승인 요청 등록` : "승인 요청" : `${executionLabel} worker unavailable`}</button>
          {message && <p className="form-message">{message}</p>}
        </form>

        <section className="panel operation-queue">
          {bulkJobs.length > 0 && <div className="bulk-job-groups"><div className="panel-title"><div><p className="eyebrow">BULK GROUPS</p><h2>작업 그룹</h2></div></div>{bulkJobs.map((job) => <article className="bulk-job-card" key={job.id}><div><strong>{job.kind === "TABLE_INSPECT" ? "TABLE METRIC REGISTRATION" : job.command?.replaceAll("_", " ")}</strong><span className={`state state-${job.state.toLowerCase()}`}>{job.state.replaceAll("_", " ")}</span></div><progress max={job.totalItems} value={job.completedItems} /><small>{job.completedItems}/{job.totalItems} 완료 · {job.succeededItems} 성공 · {job.failedItems} 실패 · {job.canceledItems} 취소</small><div className="approval-actions">{!['SUCCEEDED','FAILED','CANCELED','PARTIAL_SUCCESS'].includes(job.state) && <button type="button" className="secondary-button" onClick={() => bulkAction(job.id, "cancel")}>그룹 취소</button>}{job.failedItems > 0 && <button type="button" className="secondary-button" onClick={() => bulkAction(job.id, "retry-failed")}>실패만 재시도</button>}</div></article>)}</div>}
          <div className="panel-title"><div><p className="eyebrow">WORKFLOW</p><h2>Execution queue</h2></div><span className="count-badge">{operations.length}</span></div>
          <div className="request-list">
            {operations.map((operation) => {
              const latest = operation.executions?.[0];
              const state = resultState(latest);
              const operationCatalog = operation.qualifiedTableName.split(".", 1)[0];
              const isRegisteredHadoopOperation = registeredCatalogNames.has(operationCatalog);
              const canExecute = catalogScope === "production"
                ? !isRegisteredHadoopOperation
                : isRegisteredHadoopOperation;
              return (
                <article className="request-card" key={operation.id}>
                  <div className="request-top"><div><span className={`risk risk-${operation.risk.toLowerCase()}`}>{operation.risk}</span><strong>{operation.command.replaceAll("_", " ")}</strong></div><span className={`state state-${operation.state.toLowerCase()}`}>{operation.state.replaceAll("_", " ")}</span></div>
                  <p className="request-table">{operation.qualifiedTableName}</p>
                  <p>{operation.reason}</p>
                  <div className="request-meta"><span>{operation.requester}</span><span>{operation.preferredEngine}</span><span>snapshot {operation.plannedSnapshotId ?? "—"}</span><span>{new Date(operation.createdAt).toLocaleString("ko-KR")}</span></div>
                  {runningStates.has(operation.state) && <div className="execution-progress"><i /><span>{operation.state === "QUEUED" ? "Worker 배정 대기" : operation.state === "RUNNING" ? `${executionLabel} job 실행 중` : operation.state === "CANCEL_REQUESTED" ? "실행기에 취소 전달 중" : "결과 검증 중"}</span></div>}
                  {state && <div className="execution-delta"><div><span>Before</span><strong>{latest?.beforeSnapshotId ?? String(state.before.snapshot_id ?? "—")}</strong><small>{String(state.before.total_files ?? 0)} files</small></div><b>→</b><div><span>After</span><strong>{latest?.afterSnapshotId ?? String(state.after.snapshot_id ?? "—")}</strong><small>{String(state.after.total_files ?? 0)} files</small></div></div>}
                  {latest?.errorMessage && <div className="execution-error">{latest.errorMessage}</div>}
                  {latest && <details className="execution-details"><summary>Attempt {latest.attempt} 실행 로그 · {latest.events.length} events</summary>{latest.externalId && <code>{latest.externalId}</code>}<div className="event-list">{latest.events.map((event) => <div key={event.id} className={`event event-${event.level.toLowerCase()}`}><time>{new Date(event.occurredAt).toLocaleTimeString("ko-KR")}</time><strong>{event.eventType}</strong><span>{event.message}</span></div>)}</div>{Object.keys(latest.result).length > 0 && <pre>{JSON.stringify(latest.result.output ?? latest.result, null, 2)}</pre>}</details>}
                  {operation.state === "WAITING_APPROVAL" && <div className="approval-actions">{!canExecute && <span className="unroutable">현재 worker에서 실행 불가</span>}<button onClick={() => decide(operation.id, "REJECTED")} type="button" className="secondary-button">반려</button><button onClick={() => decide(operation.id, "APPROVED")} type="button" className="primary-button small" disabled={!canExecute}>승인·실행</button></div>}
                  {["WAITING_APPROVAL", "QUEUED", "RUNNING", "VERIFYING", "CANCEL_REQUESTED"].includes(operation.state) && <div className="approval-actions"><button onClick={() => cancel(operation.id, operation.version)} type="button" className="secondary-button" disabled={operation.state === "CANCEL_REQUESTED"}>{operation.state === "CANCEL_REQUESTED" ? "취소 처리 중" : "취소"}</button></div>}
                  {operation.state === "FAILED" && <div className="approval-actions"><button onClick={() => retry(operation.id)} type="button" className="secondary-button">재시도</button></div>}
                </article>
              );
            })}
          </div>
          {hasMore && <button type="button" className="secondary-button load-more" onClick={loadMore}>이전 작업 더 보기</button>}
        </section>
      </div>
    </>
  );
}
