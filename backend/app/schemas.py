import re
import uuid
from datetime import datetime
from enum import StrEnum
from typing import Any
from urllib.parse import urlparse

from pydantic import BaseModel, ConfigDict, Field, field_serializer, field_validator
from pydantic.alias_generators import to_camel


class ApiModel(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True, from_attributes=True)


class MaintenanceCommand(StrEnum):
    COMPUTE_STATS = "COMPUTE_STATS"
    REWRITE_MANIFESTS = "REWRITE_MANIFESTS"
    REWRITE_DATA_FILES = "REWRITE_DATA_FILES"
    REWRITE_POSITION_DELETES = "REWRITE_POSITION_DELETES"
    EXPIRE_SNAPSHOTS = "EXPIRE_SNAPSHOTS"
    REMOVE_ORPHAN_FILES = "REMOVE_ORPHAN_FILES"
    STORAGE_MIGRATION = "STORAGE_MIGRATION"
    ROLLBACK = "ROLLBACK"


class EngineType(StrEnum):
    AUTO = "AUTO"
    SPARK = "SPARK"
    TRINO = "TRINO"
    FLINK = "FLINK"


class TableStatusCounts(ApiModel):
    total: int
    healthy: int
    warning: int
    critical: int
    unknown: int
    open_findings: int
    pending_operations: int


class StorageProfileOut(ApiModel):
    id: uuid.UUID
    name: str
    storage_type: str
    uri_prefix: str
    file_io_impl: str
    region: str | None = None
    encryption_policy: str | None = None
    health: str


class EngineInstanceOut(ApiModel):
    id: uuid.UUID
    name: str
    engine_type: str
    version: str | None = None
    endpoint: str | None = None
    health: str
    capabilities: list[str]
    last_checked_at: datetime | None = None


class TableSummary(ApiModel):
    id: uuid.UUID
    catalog_name: str
    namespace_name: str
    table_name: str
    owner_name: str | None = None
    slo_profile: str
    format_version: int | None = None
    current_snapshot_id: int | None = None
    storage_type: str | None = None
    last_commit_at: datetime | None = None
    total_bytes: int
    total_records: int
    total_files: int
    small_file_ratio: float | None = None
    delete_file_ratio: float | None = None
    snapshot_count: int
    partition_count: int | None = None
    health: str
    observed_at: datetime | None = None

    @field_serializer("current_snapshot_id", when_used="json")
    def serialize_current_snapshot_id(self, value: int | None) -> str | None:
        return str(value) if value is not None else None


class FindingOut(ApiModel):
    id: uuid.UUID
    rule_code: str
    severity: str
    title: str
    evidence: dict[str, Any]
    recommendation: str | None = None
    status: str
    first_seen_at: datetime
    last_seen_at: datetime


class TableDetail(ApiModel):
    table: TableSummary
    table_uuid: uuid.UUID | None = None
    metadata_location: str | None = None
    current_snapshot_id: int | None = None
    properties: dict[str, Any]
    findings: list[FindingOut]

    @field_serializer("current_snapshot_id", when_used="json")
    def serialize_current_snapshot_id(self, value: int | None) -> str | None:
        return str(value) if value is not None else None


class MetricPointOut(ApiModel):
    metric_name: str
    metric_value: float
    engine_type: str | None = None
    labels: dict[str, Any]
    observed_at: datetime


class Dashboard(ApiModel):
    counts: TableStatusCounts
    storage_profiles: list[StorageProfileOut]
    engines: list[EngineInstanceOut]
    attention_tables: list[TableSummary]


class OperationRequestCreate(ApiModel):
    table_id: uuid.UUID
    command: MaintenanceCommand
    preferred_engine: EngineType = EngineType.AUTO
    parameters: dict[str, Any] = Field(default_factory=dict)
    reason: str = Field(min_length=5, max_length=1000)
    requester: str = Field(min_length=2, max_length=200)


class OperationRequestBulkCreate(ApiModel):
    table_ids: list[uuid.UUID] = Field(min_length=1, max_length=200)
    command: MaintenanceCommand
    preferred_engine: EngineType = EngineType.AUTO
    parameters: dict[str, Any] = Field(default_factory=dict)
    reason: str = Field(min_length=5, max_length=1000)
    requester: str = Field(min_length=2, max_length=200)

    @field_validator("table_ids")
    @classmethod
    def unique_table_ids(cls, value: list[uuid.UUID]) -> list[uuid.UUID]:
        return list(dict.fromkeys(value))


class OperationPlanRequest(ApiModel):
    table_ids: list[uuid.UUID] = Field(min_length=1, max_length=200)
    command: MaintenanceCommand
    preferred_engine: EngineType = EngineType.SPARK
    parameters: dict[str, Any] = Field(default_factory=dict)

    @field_validator("table_ids")
    @classmethod
    def unique_plan_table_ids(cls, value: list[uuid.UUID]) -> list[uuid.UUID]:
        return list(dict.fromkeys(value))


class OperationPlanItem(ApiModel):
    table_id: uuid.UUID
    qualified_table_name: str
    current_snapshot_id: int | None = None
    total_files: int
    total_bytes: int
    estimated_rewrite_bytes: int
    estimated_output_files: int | None = None
    affected_snapshots: int | None = None
    risk: str
    warnings: list[str] = Field(default_factory=list)
    executable: bool = True

    @field_serializer("current_snapshot_id", when_used="json")
    def serialize_plan_snapshot_id(self, value: int | None) -> str | None:
        return str(value) if value is not None else None


class OperationPlanOut(ApiModel):
    command: str
    tables: list[OperationPlanItem]
    total_files: int
    total_bytes: int
    estimated_rewrite_bytes: int
    highest_risk: str
    requires_approval: bool = True


class ApprovalCreate(ApiModel):
    approver: str = Field(min_length=2, max_length=200)
    decision: str
    comment: str | None = Field(default=None, max_length=1000)


class RetryOperation(ApiModel):
    actor: str = Field(min_length=2, max_length=200)


class CancelOperation(ApiModel):
    actor: str = Field(min_length=2, max_length=200)
    reason: str = Field(min_length=3, max_length=1000)
    expected_version: int | None = Field(default=None, ge=1)


class OperationEventOut(ApiModel):
    id: int
    level: str
    event_type: str
    message: str
    details: dict[str, Any]
    occurred_at: datetime


class OperationExecutionOut(ApiModel):
    id: uuid.UUID
    attempt: int
    engine_type: str
    engine_instance_id: uuid.UUID | None = None
    state: str
    worker_id: str | None = None
    external_id: str | None = None
    planned_snapshot_id: int | None = None
    before_snapshot_id: int | None = None
    after_snapshot_id: int | None = None
    result: dict[str, Any]
    error_message: str | None = None
    queued_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None
    events: list[OperationEventOut] = Field(default_factory=list)

    @field_serializer(
        "planned_snapshot_id",
        "before_snapshot_id",
        "after_snapshot_id",
        when_used="json",
    )
    def serialize_snapshot_id(self, value: int | None) -> str | None:
        return str(value) if value is not None else None


class OperationRequestOut(ApiModel):
    id: uuid.UUID
    table_id: uuid.UUID
    qualified_table_name: str
    command: str
    preferred_engine: str
    parameters: dict[str, Any]
    reason: str
    requester: str
    risk: str
    state: str
    planned_snapshot_id: int | None = None
    bulk_job_id: uuid.UUID | None = None
    version: int = 1
    cancellation_requested_at: datetime | None = None
    canceled_at: datetime | None = None
    plan: dict[str, Any] = Field(default_factory=dict)
    executions: list[OperationExecutionOut] = Field(default_factory=list)
    created_at: datetime
    updated_at: datetime

    @field_serializer("planned_snapshot_id", when_used="json")
    def serialize_planned_snapshot_id(self, value: int | None) -> str | None:
        return str(value) if value is not None else None


class BulkJobItemOut(ApiModel):
    id: uuid.UUID
    ordinal: int
    table_name: str | None = None
    table_id: uuid.UUID | None = None
    operation_request_id: uuid.UUID | None = None
    state: str
    result: dict[str, Any] = Field(default_factory=dict)
    error_message: str | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None


class BulkJobOut(ApiModel):
    id: uuid.UUID
    kind: str
    state: str
    namespace_name: str | None = None
    command: str | None = None
    requested_by: str
    total_items: int
    completed_items: int
    succeeded_items: int
    failed_items: int
    canceled_items: int
    error_message: str | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    cancellation_requested_at: datetime | None = None
    items: list[BulkJobItemOut] = Field(default_factory=list)
    created_at: datetime
    updated_at: datetime


class BulkJobAction(ApiModel):
    actor: str = Field(min_length=2, max_length=200)
    reason: str | None = Field(default=None, max_length=1000)


class OperationRequestBulkOut(ApiModel):
    operations: list[OperationRequestOut]
    requested_count: int
    bulk_job: BulkJobOut


class OperationPage(ApiModel):
    items: list[OperationRequestOut]
    next_cursor: str | None = None
    has_more: bool


class TablePage(ApiModel):
    items: list[TableSummary]
    next_cursor: str | None = None
    has_more: bool


class WorkerStatus(ApiModel):
    id: str
    worker_id: str | None = None
    engine_type: str
    status: str
    online: bool
    current_operation_request_id: uuid.UUID | None = None
    details: dict[str, Any] = Field(default_factory=dict)
    last_seen_at: datetime | None = None


class ErrorResponse(ApiModel):
    code: str
    message: str
    timestamp: datetime


class PlaygroundComponent(ApiModel):
    status: str
    endpoint: str


class PlaygroundStatus(ApiModel):
    enabled: bool
    ready: bool
    hdfs: PlaygroundComponent
    spark: PlaygroundComponent
    sample_table: str
    sample_exists: bool
    versions: dict[str, str]
    message: str | None = None


class PlaygroundBootstrapResult(ApiModel):
    table: str
    location: str
    inserted_rows: int
    total_rows: int
    snapshot_count: int
    current_snapshot_id: int
    total_files: int
    total_bytes: int
    total_records: int
    message: str


CATALOG_IDENTIFIER_PATTERN = r"^[A-Za-z_][A-Za-z0-9_]*$"
NAMESPACE_PATTERN = r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$"


class PlaygroundCatalogCreate(ApiModel):
    name: str = Field(min_length=1, max_length=120, pattern=CATALOG_IDENTIFIER_PATTERN)
    warehouse: str = Field(min_length=1, max_length=2000)

    @field_validator("warehouse")
    @classmethod
    def hdfs_warehouse_only(cls, value: str) -> str:
        normalized = value.rstrip("/")
        parsed = urlparse(normalized)
        if parsed.scheme.lower() != "hdfs" or not parsed.path.startswith("/"):
            raise ValueError("warehouse must be an absolute hdfs:// URI")
        return normalized


class PlaygroundNamespaceCreate(ApiModel):
    catalog_name: str = Field(min_length=1, max_length=120, pattern=CATALOG_IDENTIFIER_PATTERN)
    name: str = Field(min_length=1, max_length=500, pattern=NAMESPACE_PATTERN)


class PlaygroundCatalogOut(ApiModel):
    id: uuid.UUID
    name: str
    warehouse: str
    namespaces: list[str] = Field(default_factory=list)


class PlaygroundTableRegister(ApiModel):
    catalog_name: str = Field(min_length=1, max_length=120, pattern=CATALOG_IDENTIFIER_PATTERN)
    namespace_name: str = Field(min_length=1, max_length=500, pattern=NAMESPACE_PATTERN)
    table_name: str = Field(min_length=1, max_length=500, pattern=CATALOG_IDENTIFIER_PATTERN)


class PlaygroundTableBulkRegister(ApiModel):
    catalog_name: str = Field(min_length=1, max_length=120, pattern=CATALOG_IDENTIFIER_PATTERN)
    namespace_name: str = Field(min_length=1, max_length=500, pattern=NAMESPACE_PATTERN)
    table_names: list[str] = Field(min_length=1, max_length=200)

    @field_validator("table_names")
    @classmethod
    def valid_unique_table_names(cls, value: list[str]) -> list[str]:
        normalized: list[str] = []
        for table_name in value:
            name = table_name.strip()
            if not name or len(name) > 500:
                raise ValueError("table names must contain 1 to 500 characters")
            if re.fullmatch(CATALOG_IDENTIFIER_PATTERN, name) is None:
                raise ValueError(f"unsupported table identifier: {name}")
            if name not in normalized:
                normalized.append(name)
        return normalized


class PlaygroundTableError(ApiModel):
    table_name: str
    detail: str


class PlaygroundTableBulkResult(ApiModel):
    tables: list[TableSummary]
    errors: list[PlaygroundTableError] = Field(default_factory=list)
    requested_count: int


class PlaygroundQueryRequest(ApiModel):
    sql: str = Field(min_length=1, max_length=4000)
    max_rows: int = Field(default=100, ge=1, le=200)
    catalog_name: str = Field(
        default="playground", min_length=1, max_length=120, pattern=CATALOG_IDENTIFIER_PATTERN
    )


class PlaygroundColumn(ApiModel):
    name: str
    data_type: str
    nullable: bool


class PlaygroundQueryResult(ApiModel):
    columns: list[PlaygroundColumn]
    rows: list[dict[str, Any]]
    row_count: int
    truncated: bool
    duration_ms: int
