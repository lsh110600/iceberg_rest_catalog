const backendUrl = process.env.BACKEND_URL ?? "http://localhost:8088";

export type Health = "HEALTHY" | "WARNING" | "CRITICAL" | "UNKNOWN" | "NOT_CONFIGURED";

export interface TableStatusCounts {
  total: number;
  healthy: number;
  warning: number;
  critical: number;
  unknown: number;
  openFindings: number;
  pendingOperations: number;
}

export interface StorageProfile {
  id: string;
  name: string;
  storageType: "HDFS" | "S3";
  uriPrefix: string;
  fileIoImpl: string;
  region?: string;
  encryptionPolicy?: string;
  health: Health;
}

export interface EngineInstance {
  id: string;
  name: string;
  engineType: "SPARK" | "TRINO" | "FLINK";
  version?: string;
  endpoint?: string;
  health: Health;
  capabilities: string[];
  lastCheckedAt?: string;
}

export interface TableSummary {
  id: string;
  catalogName: string;
  namespaceName: string;
  tableName: string;
  ownerName?: string;
  sloProfile: string;
  formatVersion?: number;
  currentSnapshotId?: string;
  storageType?: "HDFS" | "S3";
  lastCommitAt?: string;
  totalBytes: number;
  totalRecords: number;
  totalFiles: number;
  smallFileRatio?: number;
  deleteFileRatio?: number;
  snapshotCount: number;
  partitionCount?: number;
  health: Health;
  observedAt?: string;
}

export interface Finding {
  id: string;
  ruleCode: string;
  severity: "CRITICAL" | "WARNING" | "INFO";
  title: string;
  evidence: Record<string, unknown>;
  recommendation?: string;
  status: string;
  firstSeenAt: string;
  lastSeenAt: string;
}

export interface TableDetail {
  table: TableSummary;
  tableUuid?: string;
  metadataLocation?: string;
  currentSnapshotId?: string;
  properties: Record<string, string>;
  findings: Finding[];
}

export interface MetricPoint {
  metricName: string;
  metricValue: number;
  engineType?: string;
  labels: Record<string, string>;
  observedAt: string;
}

export interface Dashboard {
  counts: TableStatusCounts;
  storageProfiles: StorageProfile[];
  engines: EngineInstance[];
  attentionTables: TableSummary[];
}

export interface OperationRequest {
  id: string;
  tableId: string;
  qualifiedTableName: string;
  command: string;
  preferredEngine: string;
  parameters: Record<string, unknown>;
  reason: string;
  requester: string;
  risk: string;
  state: string;
  plannedSnapshotId?: string;
  bulkJobId?: string;
  version: number;
  cancellationRequestedAt?: string;
  canceledAt?: string;
  plan: Record<string, unknown>;
  executions: OperationExecution[];
  createdAt: string;
  updatedAt: string;
}

export interface OperationRequestBulkResult {
  operations: OperationRequest[];
  requestedCount: number;
  bulkJob: BulkJob;
}

export interface OperationPlanItem {
  tableId: string;
  qualifiedTableName: string;
  currentSnapshotId?: string;
  totalFiles: number;
  totalBytes: number;
  estimatedRewriteBytes: number;
  estimatedOutputFiles?: number;
  affectedSnapshots?: number;
  risk: string;
  warnings: string[];
  executable: boolean;
}

export interface OperationPlan {
  command: string;
  tables: OperationPlanItem[];
  totalFiles: number;
  totalBytes: number;
  estimatedRewriteBytes: number;
  highestRisk: string;
  requiresApproval: boolean;
}

export interface BulkJobItem {
  id: string;
  ordinal: number;
  tableName?: string;
  tableId?: string;
  operationRequestId?: string;
  state: string;
  result: Record<string, unknown>;
  errorMessage?: string;
  startedAt?: string;
  finishedAt?: string;
}

export interface BulkJob {
  id: string;
  kind: string;
  state: string;
  namespaceName?: string;
  command?: string;
  requestedBy: string;
  totalItems: number;
  completedItems: number;
  succeededItems: number;
  failedItems: number;
  canceledItems: number;
  errorMessage?: string;
  cancellationRequestedAt?: string;
  items: BulkJobItem[];
  createdAt: string;
  updatedAt: string;
}

export interface OperationPage {
  items: OperationRequest[];
  nextCursor?: string;
  hasMore: boolean;
}

export interface TablePage {
  items: TableSummary[];
  nextCursor?: string;
  hasMore: boolean;
}

export interface OperationEvent {
  id: number;
  level: string;
  eventType: string;
  message: string;
  details: Record<string, unknown>;
  occurredAt: string;
}

export interface OperationExecution {
  id: string;
  attempt: number;
  engineType: string;
  engineInstanceId?: string;
  state: string;
  workerId?: string;
  externalId?: string;
  plannedSnapshotId?: string;
  beforeSnapshotId?: string;
  afterSnapshotId?: string;
  result: Record<string, unknown>;
  errorMessage?: string;
  queuedAt: string;
  startedAt?: string;
  finishedAt?: string;
  events: OperationEvent[];
}

export interface WorkerStatus {
  id: string;
  workerId?: string;
  engineType: string;
  status: string;
  online: boolean;
  currentOperationRequestId?: string;
  details: Record<string, unknown>;
  lastSeenAt?: string;
}

export interface PlaygroundComponent {
  status: Health;
  endpoint: string;
}

export interface PlaygroundStatus {
  enabled: boolean;
  ready: boolean;
  hdfs: PlaygroundComponent;
  spark: PlaygroundComponent;
  sampleTable: string;
  sampleExists: boolean;
  versions: Record<string, string>;
  message?: string;
}

export interface PlaygroundBootstrapResult {
  table: string;
  location: string;
  insertedRows: number;
  totalRows: number;
  snapshotCount: number;
  message: string;
}

export interface PlaygroundCatalog {
  id: string;
  name: string;
  warehouse: string;
  namespaces: string[];
}

export interface PlaygroundTableError {
  tableName: string;
  detail: string;
}

export interface PlaygroundTableBulkResult {
  tables: TableSummary[];
  errors: PlaygroundTableError[];
  requestedCount: number;
}

export interface PlaygroundColumn {
  name: string;
  dataType: string;
  nullable: boolean;
}

export interface PlaygroundQueryResult {
  columns: PlaygroundColumn[];
  rows: Record<string, unknown>[];
  rowCount: number;
  truncated: boolean;
  durationMs: number;
}

async function get<T>(path: string): Promise<T> {
  const response = await fetch(`${backendUrl}${path}`, { cache: "no-store" });
  if (!response.ok) {
    throw new Error(`API request failed: ${response.status} ${path}`);
  }
  return response.json() as Promise<T>;
}

export const api = {
  dashboard: () => get<Dashboard>("/ops/api/v1/dashboard"),
  tables: () => get<TableSummary[]>("/ops/api/v1/tables"),
  tablePage: (query = "limit=100") => get<TablePage>(`/ops/api/v1/tables/page?${query}`),
  table: (id: string) => get<TableDetail>(`/ops/api/v1/tables/${id}`),
  metrics: (id: string) => get<MetricPoint[]>(`/ops/api/v1/tables/${id}/metrics`),
  operations: () => get<OperationRequest[]>("/ops/api/v1/operation-requests"),
  operationPage: () => get<OperationPage>("/ops/api/v1/operation-requests/page?limit=30"),
  bulkJobs: () => get<BulkJob[]>("/ops/api/v1/bulk-jobs"),
  sparkWorker: () => get<WorkerStatus>("/ops/api/v1/workers/spark"),
  playgroundStatus: () => get<PlaygroundStatus>("/ops/api/v1/playground/status"),
  playgroundCatalogs: () => get<PlaygroundCatalog[]>("/ops/api/v1/playground/catalogs"),
};

export function formatBytes(value: number): string {
  if (value === 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB", "PB"];
  const unit = Math.min(Math.floor(Math.log(value) / Math.log(1024)), units.length - 1);
  return `${(value / 1024 ** unit).toFixed(unit > 2 ? 1 : 0)} ${units[unit]}`;
}

export function formatNumber(value: number): string {
  return new Intl.NumberFormat("ko-KR", { notation: "compact", maximumFractionDigits: 1 }).format(value);
}

export function formatDate(value?: string): string {
  if (!value) return "—";
  return new Intl.DateTimeFormat("ko-KR", {
    month: "short", day: "numeric", hour: "2-digit", minute: "2-digit",
  }).format(new Date(value));
}
