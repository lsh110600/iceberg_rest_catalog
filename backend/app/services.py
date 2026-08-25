import base64
import copy
import hashlib
import json
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import case, delete, func, or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app import models
from app.schemas import (
    BulkJobOut,
    CancelOperation,
    Dashboard,
    EngineInstanceOut,
    EngineType,
    FindingOut,
    MaintenanceCommand,
    MetricPointOut,
    OperationEventOut,
    OperationExecutionOut,
    OperationPage,
    OperationPlanItem,
    OperationPlanOut,
    OperationPlanRequest,
    OperationRequestBulkCreate,
    OperationRequestBulkOut,
    OperationRequestCreate,
    OperationRequestOut,
    StorageProfileOut,
    TableDetail,
    TablePage,
    TableStatusCounts,
    TableSummary,
    WorkerStatus,
)


class NotFoundError(Exception):
    pass


class InvalidOperationError(Exception):
    pass


COMMAND_RISK = {
    MaintenanceCommand.COMPUTE_STATS: "LOW",
    MaintenanceCommand.REWRITE_MANIFESTS: "MEDIUM",
    MaintenanceCommand.REWRITE_DATA_FILES: "MEDIUM",
    MaintenanceCommand.REWRITE_POSITION_DELETES: "MEDIUM",
    MaintenanceCommand.EXPIRE_SNAPSHOTS: "HIGH",
    MaintenanceCommand.REMOVE_ORPHAN_FILES: "VERY_HIGH",
    MaintenanceCommand.STORAGE_MIGRATION: "VERY_HIGH",
    MaintenanceCommand.ROLLBACK: "VERY_HIGH",
}

ENGINE_CAPABILITIES = {
    EngineType.AUTO: {
        MaintenanceCommand.COMPUTE_STATS,
        MaintenanceCommand.REWRITE_MANIFESTS,
        MaintenanceCommand.REWRITE_DATA_FILES,
        MaintenanceCommand.REWRITE_POSITION_DELETES,
        MaintenanceCommand.EXPIRE_SNAPSHOTS,
        MaintenanceCommand.REMOVE_ORPHAN_FILES,
        MaintenanceCommand.ROLLBACK,
    },
    EngineType.SPARK: {
        MaintenanceCommand.COMPUTE_STATS,
        MaintenanceCommand.REWRITE_MANIFESTS,
        MaintenanceCommand.REWRITE_DATA_FILES,
        MaintenanceCommand.REWRITE_POSITION_DELETES,
        MaintenanceCommand.EXPIRE_SNAPSHOTS,
        MaintenanceCommand.REMOVE_ORPHAN_FILES,
        MaintenanceCommand.ROLLBACK,
    },
    EngineType.TRINO: {
        MaintenanceCommand.COMPUTE_STATS,
        MaintenanceCommand.REWRITE_MANIFESTS,
        MaintenanceCommand.REWRITE_DATA_FILES,
        MaintenanceCommand.EXPIRE_SNAPSHOTS,
        MaintenanceCommand.REMOVE_ORPHAN_FILES,
    },
    EngineType.FLINK: {
        MaintenanceCommand.REWRITE_DATA_FILES,
        MaintenanceCommand.EXPIRE_SNAPSHOTS,
        MaintenanceCommand.REMOVE_ORPHAN_FILES,
    },
}

SENSITIVE_METRIC_FIELDS = {
    "filter",
    "projected-field-names",
    "projectedFieldNames",
    "query",
    "queryText",
    "sql",
}
RISK_ORDER = {"LOW": 0, "MEDIUM": 1, "HIGH": 2, "VERY_HIGH": 3}
COMMAND_PARAMETERS = {
    MaintenanceCommand.REWRITE_DATA_FILES: {
        "targetFileSizeBytes",
        "minInputFiles",
        "maxFileGroupSizeBytes",
        "rewriteAll",
        "removeDanglingDeletes",
    },
    MaintenanceCommand.REWRITE_MANIFESTS: {"useCaching"},
    MaintenanceCommand.REWRITE_POSITION_DELETES: {"rewriteAll"},
    MaintenanceCommand.COMPUTE_STATS: {"columns"},
    MaintenanceCommand.EXPIRE_SNAPSHOTS: {"olderThanHours", "retainLast"},
    MaintenanceCommand.REMOVE_ORPHAN_FILES: {"olderThanHours", "dryRun"},
    MaintenanceCommand.ROLLBACK: {"snapshotId"},
}


def validate_parameters(command: MaintenanceCommand, parameters: dict[str, Any]) -> None:
    unknown = set(parameters) - COMMAND_PARAMETERS.get(command, set())
    if unknown:
        raise InvalidOperationError(f"Unsupported parameters for {command}: {', '.join(sorted(unknown))}")

    def integer(name: str, minimum: int, maximum: int) -> None:
        if name not in parameters:
            return
        value = parameters[name]
        if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
            raise InvalidOperationError(f"{name} must be an integer from {minimum} to {maximum}")

    for name in {"rewriteAll", "removeDanglingDeletes", "useCaching", "dryRun"}:
        if name in parameters and not isinstance(parameters[name], bool):
            raise InvalidOperationError(f"{name} must be a boolean")
    integer("targetFileSizeBytes", 1_048_576, 10_737_418_240)
    integer("minInputFiles", 1, 10_000)
    integer("maxFileGroupSizeBytes", 1_048_576, 109_951_162_777_600)
    integer("retainLast", 1, 10_000)
    integer("snapshotId", 1, 9_223_372_036_854_775_807)
    if command == MaintenanceCommand.EXPIRE_SNAPSHOTS:
        integer("olderThanHours", 120, 87_600)
    elif command == MaintenanceCommand.REMOVE_ORPHAN_FILES:
        integer("olderThanHours", 72, 87_600)
    columns = parameters.get("columns")
    if columns is not None and (
        not isinstance(columns, list)
        or not 1 <= len(columns) <= 100
        or any(not isinstance(column, str) or not column or len(column) > 500 for column in columns)
    ):
        raise InvalidOperationError("columns must contain 1 to 100 non-empty column names")


def request_fingerprint(payload: dict[str, Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode()
    return hashlib.sha256(encoded).hexdigest()


def encode_cursor(created_at: datetime, item_id: uuid.UUID) -> str:
    raw = f"{created_at.isoformat()}|{item_id}".encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def decode_cursor(value: str) -> tuple[datetime, uuid.UUID]:
    try:
        padded = value + "=" * (-len(value) % 4)
        created_at, item_id = base64.urlsafe_b64decode(padded).decode().split("|", 1)
        return datetime.fromisoformat(created_at), uuid.UUID(item_id)
    except (ValueError, UnicodeDecodeError) as exc:
        raise InvalidOperationError("Invalid pagination cursor") from exc


def bulk_job_out(job: models.BulkJob) -> BulkJobOut:
    return BulkJobOut.model_validate(job)


def plan_table(
    table: models.ManagedTable,
    command: MaintenanceCommand,
    parameters: dict[str, Any],
) -> OperationPlanItem:
    warnings: list[str] = []
    estimated_rewrite_bytes = 0
    estimated_output_files: int | None = None
    affected_snapshots: int | None = None
    target_size = int(parameters.get("targetFileSizeBytes", 134_217_728))

    if command in {MaintenanceCommand.REWRITE_DATA_FILES, MaintenanceCommand.REWRITE_POSITION_DELETES}:
        estimated_rewrite_bytes = table.total_bytes
        estimated_output_files = max(1, (table.total_bytes + target_size - 1) // target_size)
        if table.small_file_ratio is not None and float(table.small_file_ratio) < 0.1:
            warnings.append("Small file ratio is already below 10%; rewrite benefit may be limited.")
    elif command == MaintenanceCommand.REWRITE_MANIFESTS:
        estimated_rewrite_bytes = min(table.total_bytes, table.total_files * 8_388_608)
    elif command == MaintenanceCommand.EXPIRE_SNAPSHOTS:
        retain_last = int(parameters.get("retainLast", 1))
        affected_snapshots = max(0, table.snapshot_count - retain_last)
        if affected_snapshots == 0:
            warnings.append("No snapshot is currently eligible by retain-last estimate.")
    elif command == MaintenanceCommand.REMOVE_ORPHAN_FILES:
        if not parameters.get("dryRun", True):
            warnings.append("This request can permanently delete files; run dry-run first.")
    elif command == MaintenanceCommand.ROLLBACK:
        warnings.append("Rollback changes the current snapshot and requires lineage verification.")

    if table.current_snapshot_id is None:
        warnings.append("Table has no current snapshot and cannot be executed safely.")
    if table.observed_at is None:
        warnings.append("Table metrics have never been observed.")

    return OperationPlanItem(
        table_id=table.id,
        qualified_table_name=f"{table.catalog.name}.{table.namespace_name}.{table.table_name}",
        current_snapshot_id=table.current_snapshot_id,
        total_files=table.total_files,
        total_bytes=table.total_bytes,
        estimated_rewrite_bytes=estimated_rewrite_bytes,
        estimated_output_files=estimated_output_files,
        affected_snapshots=affected_snapshots,
        risk=COMMAND_RISK[command],
        warnings=warnings,
        executable=table.current_snapshot_id is not None,
    )


def redact_metric_payload(payload: dict[str, Any]) -> dict[str, Any]:
    redacted = copy.deepcopy(payload)

    def visit(value: Any) -> None:
        if isinstance(value, dict):
            for key in list(value):
                if key in SENSITIVE_METRIC_FIELDS:
                    value.pop(key)
                else:
                    visit(value[key])
        elif isinstance(value, list):
            for item in value:
                visit(item)

    visit(redacted)
    return redacted


def table_summary(table: models.ManagedTable) -> TableSummary:
    partition_count = (table.properties or {}).get("metric.partition-count")
    return TableSummary(
        id=table.id,
        catalog_name=table.catalog.name,
        namespace_name=table.namespace_name,
        table_name=table.table_name,
        owner_name=table.owner_name,
        slo_profile=table.slo_profile,
        format_version=table.format_version,
        current_snapshot_id=table.current_snapshot_id,
        storage_type=table.storage_profile.storage_type if table.storage_profile else None,
        last_commit_at=table.last_commit_at,
        total_bytes=table.total_bytes,
        total_records=table.total_records,
        total_files=table.total_files,
        small_file_ratio=float(table.small_file_ratio) if table.small_file_ratio is not None else None,
        delete_file_ratio=float(table.delete_file_ratio) if table.delete_file_ratio is not None else None,
        snapshot_count=table.snapshot_count,
        partition_count=int(partition_count) if partition_count is not None else None,
        health=table.health,
        observed_at=table.observed_at,
    )


def operation_out(operation: models.OperationRequest) -> OperationRequestOut:
    table = operation.table
    return OperationRequestOut(
        id=operation.id,
        table_id=operation.table_id,
        qualified_table_name=f"{table.catalog.name}.{table.namespace_name}.{table.table_name}",
        command=operation.command,
        preferred_engine=operation.preferred_engine,
        parameters=operation.parameters,
        reason=operation.reason,
        requester=operation.requester,
        risk=operation.risk,
        state=operation.state,
        planned_snapshot_id=operation.planned_snapshot_id,
        bulk_job_id=operation.bulk_job_id,
        version=operation.version,
        cancellation_requested_at=operation.cancellation_requested_at,
        canceled_at=operation.canceled_at,
        plan=operation.plan,
        executions=[
            OperationExecutionOut(
                id=execution.id,
                attempt=execution.attempt,
                engine_type=execution.engine_type,
                engine_instance_id=execution.engine_instance_id,
                state=execution.state,
                worker_id=execution.worker_id,
                external_id=execution.external_id,
                planned_snapshot_id=execution.planned_snapshot_id,
                before_snapshot_id=execution.before_snapshot_id,
                after_snapshot_id=execution.after_snapshot_id,
                result=execution.result,
                error_message=execution.error_message,
                queued_at=execution.queued_at,
                started_at=execution.started_at,
                finished_at=execution.finished_at,
                events=[OperationEventOut.model_validate(event) for event in execution.events],
            )
            for execution in operation.executions
        ],
        created_at=operation.created_at,
        updated_at=operation.updated_at,
    )


class OpsService:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def dashboard(self) -> Dashboard:
        counts_row = (
            await self.session.execute(
                select(
                    func.count(models.ManagedTable.id).label("total"),
                    func.count(models.ManagedTable.id)
                    .filter(models.ManagedTable.health == "HEALTHY")
                    .label("healthy"),
                    func.count(models.ManagedTable.id)
                    .filter(models.ManagedTable.health == "WARNING")
                    .label("warning"),
                    func.count(models.ManagedTable.id)
                    .filter(models.ManagedTable.health == "CRITICAL")
                    .label("critical"),
                    func.count(models.ManagedTable.id)
                    .filter(models.ManagedTable.health == "UNKNOWN")
                    .label("unknown"),
                )
            )
        ).one()
        open_findings = await self.session.scalar(
            select(func.count(models.Finding.id)).where(models.Finding.status == "OPEN")
        )
        pending_operations = await self.session.scalar(
            select(func.count(models.OperationRequest.id)).where(
                models.OperationRequest.state.in_(
                    ["PENDING", "PLANNING", "WAITING_APPROVAL", "QUEUED", "RUNNING", "VERIFYING"]
                )
            )
        )
        storage = (
            await self.session.scalars(select(models.StorageProfile).order_by(models.StorageProfile.name))
        ).all()
        engines = (
            await self.session.scalars(
                select(models.EngineInstance).order_by(models.EngineInstance.engine_type)
            )
        ).all()
        attention = (
            (
                await self.session.scalars(
                    select(models.ManagedTable)
                    .where(models.ManagedTable.health != "HEALTHY")
                    .order_by(
                        case(
                            (models.ManagedTable.health == "CRITICAL", 0),
                            (models.ManagedTable.health == "WARNING", 1),
                            else_=2,
                        ),
                        models.ManagedTable.updated_at.desc(),
                    )
                    .limit(8)
                )
            )
            .unique()
            .all()
        )
        return Dashboard(
            counts=TableStatusCounts(
                total=counts_row.total,
                healthy=counts_row.healthy,
                warning=counts_row.warning,
                critical=counts_row.critical,
                unknown=counts_row.unknown,
                open_findings=open_findings or 0,
                pending_operations=pending_operations or 0,
            ),
            storage_profiles=[StorageProfileOut.model_validate(item) for item in storage],
            engines=[EngineInstanceOut.model_validate(item) for item in engines],
            attention_tables=[table_summary(item) for item in attention],
        )

    async def tables(self, query: str | None = None) -> list[TableSummary]:
        statement = select(models.ManagedTable)
        if query and query.strip():
            pattern = f"%{query.strip()}%"
            statement = statement.where(
                or_(
                    (models.ManagedTable.namespace_name + "." + models.ManagedTable.table_name).ilike(
                        pattern
                    ),
                    models.ManagedTable.owner_name.ilike(pattern),
                )
            )
        statement = statement.order_by(
            case(
                (models.ManagedTable.health == "CRITICAL", 0),
                (models.ManagedTable.health == "WARNING", 1),
                (models.ManagedTable.health == "UNKNOWN", 2),
                else_=3,
            ),
            models.ManagedTable.namespace_name,
            models.ManagedTable.table_name,
        )
        rows = (await self.session.scalars(statement)).unique().all()
        return [table_summary(row) for row in rows]

    async def table(self, table_id: uuid.UUID) -> TableDetail:
        statement = (
            select(models.ManagedTable)
            .options(selectinload(models.ManagedTable.findings))
            .where(models.ManagedTable.id == table_id)
        )
        table = (await self.session.scalars(statement)).unique().one_or_none()
        if table is None:
            raise NotFoundError(f"Table not found: {table_id}")
        findings = sorted(
            table.findings,
            key=lambda item: (
                {"CRITICAL": 0, "WARNING": 1, "INFO": 2}.get(item.severity, 3),
                -item.last_seen_at.timestamp(),
            ),
        )
        return TableDetail(
            table=table_summary(table),
            table_uuid=table.table_uuid,
            metadata_location=table.metadata_location,
            current_snapshot_id=table.current_snapshot_id,
            properties=table.properties,
            findings=[FindingOut.model_validate(item) for item in findings],
        )

    async def metrics(self, table_id: uuid.UUID, metric: str | None) -> list[MetricPointOut]:
        if await self.session.get(models.ManagedTable, table_id) is None:
            raise NotFoundError(f"Table not found: {table_id}")
        statement = select(models.MetricPoint).where(models.MetricPoint.table_id == table_id)
        if metric:
            statement = statement.where(models.MetricPoint.metric_name == metric)
        rows = (
            await self.session.scalars(statement.order_by(models.MetricPoint.observed_at).limit(2000))
        ).all()
        return [MetricPointOut.model_validate(row) for row in rows]

    async def storage_profiles(self) -> list[StorageProfileOut]:
        rows = (
            await self.session.scalars(select(models.StorageProfile).order_by(models.StorageProfile.name))
        ).all()
        return [StorageProfileOut.model_validate(row) for row in rows]

    async def engines(self) -> list[EngineInstanceOut]:
        rows = (
            await self.session.scalars(
                select(models.EngineInstance).order_by(models.EngineInstance.engine_type)
            )
        ).all()
        return [EngineInstanceOut.model_validate(row) for row in rows]

    async def operations(self) -> list[OperationRequestOut]:
        rows = (
            (
                await self.session.scalars(
                    select(models.OperationRequest)
                    .options(
                        selectinload(models.OperationRequest.executions).selectinload(
                            models.OperationExecution.events
                        )
                    )
                    .order_by(models.OperationRequest.created_at.desc())
                )
            )
            .unique()
            .all()
        )
        return [operation_out(row) for row in rows]

    async def operation_page(
        self,
        limit: int,
        cursor: str | None,
        state: str | None = None,
        catalog: str | None = None,
    ) -> OperationPage:
        statement = select(models.OperationRequest).options(
            selectinload(models.OperationRequest.executions).selectinload(
                models.OperationExecution.events
            )
        )
        if state:
            statement = statement.where(models.OperationRequest.state == state.upper())
        if catalog:
            statement = statement.join(models.ManagedTable).join(models.Catalog).where(
                models.Catalog.name == catalog
            )
        if cursor:
            created_at, item_id = decode_cursor(cursor)
            statement = statement.where(
                or_(
                    models.OperationRequest.created_at < created_at,
                    (models.OperationRequest.created_at == created_at)
                    & (models.OperationRequest.id < item_id),
                )
            )
        rows = (
            await self.session.scalars(
                statement.order_by(
                    models.OperationRequest.created_at.desc(),
                    models.OperationRequest.id.desc(),
                ).limit(limit + 1)
            )
        ).unique().all()
        has_more = len(rows) > limit
        page = rows[:limit]
        return OperationPage(
            items=[operation_out(row) for row in page],
            has_more=has_more,
            next_cursor=(
                encode_cursor(page[-1].created_at, page[-1].id) if has_more and page else None
            ),
        )

    async def table_page(
        self,
        limit: int,
        cursor: str | None,
        query: str | None = None,
        catalog: str | None = None,
        health: str | None = None,
    ) -> TablePage:
        statement = select(models.ManagedTable)
        if query and query.strip():
            pattern = f"%{query.strip()}%"
            statement = statement.where(
                (models.ManagedTable.namespace_name + "." + models.ManagedTable.table_name).ilike(
                    pattern
                )
            )
        if catalog:
            statement = statement.join(models.Catalog).where(models.Catalog.name == catalog)
        if health:
            normalized_health = health.upper()
            if normalized_health not in {"HEALTHY", "WARNING", "CRITICAL", "UNKNOWN"}:
                raise InvalidOperationError(f"Unsupported table health filter: {health}")
            statement = statement.where(models.ManagedTable.health == normalized_health)
        if cursor:
            created_at, item_id = decode_cursor(cursor)
            statement = statement.where(
                or_(
                    models.ManagedTable.created_at < created_at,
                    (models.ManagedTable.created_at == created_at)
                    & (models.ManagedTable.id < item_id),
                )
            )
        rows = (
            await self.session.scalars(
                statement.order_by(
                    models.ManagedTable.created_at.desc(), models.ManagedTable.id.desc()
                ).limit(limit + 1)
            )
        ).unique().all()
        has_more = len(rows) > limit
        page = rows[:limit]
        return TablePage(
            items=[table_summary(row) for row in page],
            has_more=has_more,
            next_cursor=(
                encode_cursor(page[-1].created_at, page[-1].id) if has_more and page else None
            ),
        )

    async def operation_plan(self, request: OperationPlanRequest) -> OperationPlanOut:
        validate_parameters(request.command, request.parameters)
        if request.command not in ENGINE_CAPABILITIES[request.preferred_engine]:
            raise InvalidOperationError(
                f"{request.preferred_engine} does not support {request.command}"
            )
        tables = list(
            await self.session.scalars(
                select(models.ManagedTable).where(models.ManagedTable.id.in_(request.table_ids))
            )
        )
        by_id = {table.id: table for table in tables}
        missing = [table_id for table_id in request.table_ids if table_id not in by_id]
        if missing:
            raise NotFoundError(f"Tables not found: {', '.join(str(item) for item in missing)}")
        items = [
            plan_table(by_id[table_id], request.command, request.parameters)
            for table_id in request.table_ids
        ]
        return OperationPlanOut(
            command=request.command.value,
            tables=items,
            total_files=sum(item.total_files for item in items),
            total_bytes=sum(item.total_bytes for item in items),
            estimated_rewrite_bytes=sum(item.estimated_rewrite_bytes for item in items),
            highest_risk=max((item.risk for item in items), key=RISK_ORDER.get),
        )

    async def create_operation(
        self, request: OperationRequestCreate, idempotency_key: str | None = None
    ) -> OperationRequestOut:
        validate_parameters(request.command, request.parameters)
        fingerprint = request_fingerprint(request.model_dump(mode="json"))
        if idempotency_key:
            existing = await self.session.scalar(
                select(models.OperationRequest).where(
                    models.OperationRequest.idempotency_key == idempotency_key
                )
            )
            if existing is not None:
                if existing.request_fingerprint != fingerprint:
                    raise InvalidOperationError(
                        "Idempotency key was already used with a different request"
                    )
                await self.session.refresh(existing, attribute_names=["table", "executions"])
                return operation_out(existing)
        table = await self.session.get(models.ManagedTable, request.table_id)
        if table is None:
            raise NotFoundError(f"Table not found: {request.table_id}")
        if request.command not in ENGINE_CAPABILITIES[request.preferred_engine]:
            raise InvalidOperationError(f"{request.preferred_engine} does not support {request.command}")
        computed_plan = plan_table(table, request.command, request.parameters)
        if not computed_plan.executable:
            raise InvalidOperationError("Table has no current snapshot and is not safely executable")
        operation = models.OperationRequest(
            table_id=request.table_id,
            command=request.command.value,
            preferred_engine=request.preferred_engine.value,
            parameters=request.parameters,
            reason=request.reason,
            requester=request.requester,
            risk=COMMAND_RISK[request.command],
            state="WAITING_APPROVAL",
            planned_snapshot_id=table.current_snapshot_id,
            idempotency_key=idempotency_key,
            request_fingerprint=fingerprint,
            plan=computed_plan.model_dump(mode="json"),
        )
        self.session.add(operation)
        await self.session.flush()
        self.session.add(
            models.AuditEvent(
                actor=request.requester,
                action="OPERATION_REQUESTED",
                resource_type="OPERATION_REQUEST",
                resource_id=str(operation.id),
                payload={
                    "command": request.command.value,
                    "preferredEngine": request.preferred_engine.value,
                    "risk": operation.risk,
                },
            )
        )
        try:
            await self.session.commit()
        except IntegrityError as exc:
            await self.session.rollback()
            if not idempotency_key:
                raise
            existing = await self.session.scalar(
                select(models.OperationRequest).where(
                    models.OperationRequest.idempotency_key == idempotency_key
                )
            )
            if existing is None or existing.request_fingerprint != fingerprint:
                raise InvalidOperationError("Idempotency key conflict") from exc
            await self.session.refresh(existing, attribute_names=["table", "executions"])
            return operation_out(existing)
        await self.session.refresh(operation, attribute_names=["table", "executions"])
        return operation_out(operation)

    async def create_operations(
        self, request: OperationRequestBulkCreate, idempotency_key: str | None = None
    ) -> OperationRequestBulkOut:
        validate_parameters(request.command, request.parameters)
        fingerprint = request_fingerprint(request.model_dump(mode="json"))
        if idempotency_key:
            existing_job = await self.session.scalar(
                select(models.BulkJob).where(models.BulkJob.idempotency_key == idempotency_key)
            )
            if existing_job is not None:
                if existing_job.request_fingerprint != fingerprint:
                    raise InvalidOperationError(
                        "Idempotency key was already used with a different bulk request"
                    )
                await self.session.refresh(existing_job, attribute_names=["items"])
                operations = list(
                    await self.session.scalars(
                        select(models.OperationRequest)
                        .options(
                            selectinload(models.OperationRequest.executions).selectinload(
                                models.OperationExecution.events
                            )
                        )
                        .where(models.OperationRequest.bulk_job_id == existing_job.id)
                        .order_by(models.OperationRequest.created_at)
                    )
                )
                return OperationRequestBulkOut(
                    operations=[operation_out(operation) for operation in operations],
                    requested_count=existing_job.total_items,
                    bulk_job=bulk_job_out(existing_job),
                )
        if request.command not in ENGINE_CAPABILITIES[request.preferred_engine]:
            raise InvalidOperationError(f"{request.preferred_engine} does not support {request.command}")

        tables = list(
            await self.session.scalars(
                select(models.ManagedTable).where(models.ManagedTable.id.in_(request.table_ids))
            )
        )
        tables_by_id = {table.id: table for table in tables}
        missing = [table_id for table_id in request.table_ids if table_id not in tables_by_id]
        if missing:
            raise NotFoundError(f"Tables not found: {', '.join(str(table_id) for table_id in missing)}")

        job = models.BulkJob(
            kind="OPERATION_REQUEST",
            state="WAITING_APPROVAL",
            idempotency_key=idempotency_key,
            request_fingerprint=fingerprint,
            command=request.command.value,
            parameters=request.parameters,
            requested_by=request.requester,
            total_items=len(request.table_ids),
        )
        self.session.add(job)
        await self.session.flush()
        operations: list[models.OperationRequest] = []
        for ordinal, table_id in enumerate(request.table_ids):
            table = tables_by_id[table_id]
            computed_plan = plan_table(table, request.command, request.parameters)
            if not computed_plan.executable:
                raise InvalidOperationError(
                    f"Table has no current snapshot and is not safely executable: {table.table_name}"
                )
            operation = models.OperationRequest(
                table_id=table_id,
                command=request.command.value,
                preferred_engine=request.preferred_engine.value,
                parameters=request.parameters,
                reason=request.reason,
                requester=request.requester,
                risk=COMMAND_RISK[request.command],
                state="WAITING_APPROVAL",
                planned_snapshot_id=table.current_snapshot_id,
                idempotency_key=(
                    "bulk:"
                    + hashlib.sha256(f"{idempotency_key}:{table_id}".encode()).hexdigest()
                    if idempotency_key
                    else None
                ),
                request_fingerprint=fingerprint,
                plan=computed_plan.model_dump(mode="json"),
                bulk_job_id=job.id,
            )
            self.session.add(operation)
            await self.session.flush()
            self.session.add(
                models.AuditEvent(
                    actor=request.requester,
                    action="OPERATION_REQUESTED",
                    resource_type="OPERATION_REQUEST",
                    resource_id=str(operation.id),
                    payload={
                        "command": request.command.value,
                        "preferredEngine": request.preferred_engine.value,
                        "risk": operation.risk,
                        "bulkRequestedCount": len(request.table_ids),
                    },
                )
            )
            self.session.add(
                models.BulkJobItem(
                    bulk_job_id=job.id,
                    ordinal=ordinal,
                    table_name=table.table_name,
                    table_id=table.id,
                    operation_request_id=operation.id,
                    state="WAITING_APPROVAL",
                    result={
                        "qualifiedTableName": (
                            f"{table.catalog.name}.{table.namespace_name}.{table.table_name}"
                        )
                    },
                )
            )
            operations.append(operation)

        try:
            await self.session.commit()
        except IntegrityError as exc:
            await self.session.rollback()
            if not idempotency_key:
                raise
            existing_job = await self.session.scalar(
                select(models.BulkJob).where(
                    models.BulkJob.idempotency_key == idempotency_key
                )
            )
            if existing_job is None or existing_job.request_fingerprint != fingerprint:
                raise InvalidOperationError("Bulk idempotency key conflict") from exc
            await self.session.refresh(existing_job, attribute_names=["items"])
            existing_operations = list(
                await self.session.scalars(
                    select(models.OperationRequest)
                    .options(
                        selectinload(models.OperationRequest.executions).selectinload(
                            models.OperationExecution.events
                        )
                    )
                    .where(models.OperationRequest.bulk_job_id == existing_job.id)
                    .order_by(models.OperationRequest.created_at)
                )
            )
            return OperationRequestBulkOut(
                operations=[operation_out(item) for item in existing_operations],
                requested_count=existing_job.total_items,
                bulk_job=bulk_job_out(existing_job),
            )
        await self.session.refresh(job, attribute_names=["items"])
        for operation in operations:
            await self.session.refresh(operation, attribute_names=["table", "executions"])
        return OperationRequestBulkOut(
            operations=[operation_out(operation) for operation in operations],
            requested_count=len(request.table_ids),
            bulk_job=bulk_job_out(job),
        )

    async def decide_operation(
        self, request_id: uuid.UUID, approver: str, decision: str, comment: str | None
    ) -> OperationRequestOut:
        operation = await self.session.scalar(
            select(models.OperationRequest)
            .where(models.OperationRequest.id == request_id)
            .with_for_update(of=models.OperationRequest)
        )
        if operation is None:
            raise NotFoundError(f"Operation request not found: {request_id}")
        if operation.state != "WAITING_APPROVAL":
            raise InvalidOperationError("Operation is not waiting for approval")
        if operation.requester.casefold() == approver.casefold():
            raise InvalidOperationError("Requester cannot approve their own operation")
        normalized = decision.upper()
        if normalized not in {"APPROVED", "REJECTED"}:
            raise InvalidOperationError("Decision must be APPROVED or REJECTED")
        if normalized == "APPROVED":
            existing_lock = await self.session.get(models.OperationTableLock, operation.table_id)
            if existing_lock is not None and existing_lock.operation_request_id != operation.id:
                raise InvalidOperationError(
                    "Another active operation already owns the table execution lock"
                )
            if existing_lock is None:
                self.session.add(
                    models.OperationTableLock(
                        table_id=operation.table_id,
                        operation_request_id=operation.id,
                    )
                )
        self.session.add(
            models.Approval(
                operation_request_id=request_id,
                approver=approver,
                decision=normalized,
                comment=comment,
            )
        )
        operation.state = "QUEUED" if normalized == "APPROVED" else "REJECTED"
        operation.updated_at = datetime.now(UTC)
        operation.version += 1
        if operation.bulk_job_id:
            item = await self.session.scalar(
                select(models.BulkJobItem).where(
                    models.BulkJobItem.operation_request_id == operation.id
                )
            )
            if item is not None:
                item.state = operation.state
        self.session.add(
            models.AuditEvent(
                actor=approver,
                action=f"OPERATION_{normalized}",
                resource_type="OPERATION_REQUEST",
                resource_id=str(request_id),
                payload={"decision": normalized, "comment": comment},
            )
        )
        try:
            await self.session.commit()
        except IntegrityError as exc:
            await self.session.rollback()
            raise InvalidOperationError(
                "Another active operation acquired the table execution lock"
            ) from exc
        await self.session.refresh(operation, attribute_names=["table", "executions"])
        return operation_out(operation)

    async def operation(self, request_id: uuid.UUID) -> OperationRequestOut:
        statement = (
            select(models.OperationRequest)
            .options(
                selectinload(models.OperationRequest.executions).selectinload(
                    models.OperationExecution.events
                )
            )
            .where(models.OperationRequest.id == request_id)
        )
        operation = (await self.session.scalars(statement)).unique().one_or_none()
        if operation is None:
            raise NotFoundError(f"Operation request not found: {request_id}")
        return operation_out(operation)

    async def retry_operation(self, request_id: uuid.UUID, actor: str) -> OperationRequestOut:
        operation = await self.session.scalar(
            select(models.OperationRequest)
            .where(models.OperationRequest.id == request_id)
            .with_for_update(of=models.OperationRequest)
        )
        if operation is None:
            raise NotFoundError(f"Operation request not found: {request_id}")
        if operation.state != "FAILED":
            raise InvalidOperationError("Only FAILED operations can be retried")
        await self.session.refresh(operation, attribute_names=["table"])
        existing_lock = await self.session.get(models.OperationTableLock, operation.table_id)
        if existing_lock is not None and existing_lock.operation_request_id != operation.id:
            raise InvalidOperationError(
                "Another active operation already owns the table execution lock"
            )
        if existing_lock is None:
            self.session.add(
                models.OperationTableLock(
                    table_id=operation.table_id,
                    operation_request_id=operation.id,
                )
            )
        operation.state = "QUEUED"
        operation.planned_snapshot_id = operation.table.current_snapshot_id
        operation.updated_at = datetime.now(UTC)
        operation.version += 1
        operation.cancellation_requested_at = None
        operation.canceled_at = None
        self.session.add(
            models.AuditEvent(
                actor=actor,
                action="OPERATION_RETRIED",
                resource_type="OPERATION_REQUEST",
                resource_id=str(request_id),
                payload={"plannedSnapshotId": operation.planned_snapshot_id},
            )
        )
        await self.session.commit()
        return await self.operation(request_id)

    async def cancel_operation(
        self, request_id: uuid.UUID, request: CancelOperation
    ) -> OperationRequestOut:
        operation = await self.session.scalar(
            select(models.OperationRequest)
            .where(models.OperationRequest.id == request_id)
            .with_for_update(of=models.OperationRequest)
        )
        if operation is None:
            raise NotFoundError(f"Operation request not found: {request_id}")
        if request.expected_version is not None and operation.version != request.expected_version:
            raise InvalidOperationError(
                f"Operation version changed: expected {request.expected_version}, found {operation.version}"
            )
        if operation.state == "CANCELED":
            await self.session.refresh(operation, attribute_names=["table", "executions"])
            return operation_out(operation)
        if operation.state in {"SUCCEEDED", "FAILED", "REJECTED"}:
            raise InvalidOperationError(f"Terminal operation cannot be canceled: {operation.state}")

        now = datetime.now(UTC)
        operation.cancellation_requested_at = now
        operation.version += 1
        operation.updated_at = now
        if operation.state in {"WAITING_APPROVAL", "QUEUED"}:
            operation.state = "CANCELED"
            operation.canceled_at = now
            await self.session.execute(
                delete(models.OperationTableLock).where(
                    models.OperationTableLock.operation_request_id == operation.id
                )
            )
        else:
            operation.state = "CANCEL_REQUESTED"
        if operation.bulk_job_id:
            item = await self.session.scalar(
                select(models.BulkJobItem).where(
                    models.BulkJobItem.operation_request_id == operation.id
                )
            )
            if item is not None:
                item.state = operation.state
                if operation.state == "CANCELED":
                    item.finished_at = now
        self.session.add(
            models.AuditEvent(
                actor=request.actor,
                action="OPERATION_CANCEL_REQUESTED",
                resource_type="OPERATION_REQUEST",
                resource_id=str(operation.id),
                payload={"reason": request.reason, "state": operation.state},
            )
        )
        await self.session.commit()
        return await self.operation(request_id)

    async def _refresh_bulk_job(self, job: models.BulkJob) -> models.BulkJob:
        await self.session.refresh(job, attribute_names=["items"])
        operation_ids = [
            item.operation_request_id for item in job.items if item.operation_request_id is not None
        ]
        if operation_ids:
            operations = list(
                await self.session.scalars(
                    select(models.OperationRequest).where(
                        models.OperationRequest.id.in_(operation_ids)
                    )
                )
            )
            by_id = {operation.id: operation for operation in operations}
            for item in job.items:
                operation = by_id.get(item.operation_request_id)
                if operation is not None:
                    item.state = operation.state
                    if operation.state in {"SUCCEEDED", "FAILED", "REJECTED", "CANCELED"}:
                        item.finished_at = operation.updated_at

        terminal = {"SUCCEEDED", "FAILED", "REJECTED", "CANCELED"}
        states = [item.state for item in job.items]
        job.completed_items = sum(state in terminal for state in states)
        job.succeeded_items = states.count("SUCCEEDED")
        job.failed_items = states.count("FAILED") + states.count("REJECTED")
        job.canceled_items = states.count("CANCELED")
        if not states:
            job.state = "FAILED"
        elif all(state in terminal for state in states):
            job.finished_at = job.finished_at or datetime.now(UTC)
            if job.succeeded_items == len(states):
                job.state = "SUCCEEDED"
            elif job.canceled_items == len(states):
                job.state = "CANCELED"
            elif job.succeeded_items:
                job.state = "PARTIAL_SUCCESS"
            else:
                job.state = "FAILED"
        elif any(state == "CANCEL_REQUESTED" for state in states):
            job.state = "CANCEL_REQUESTED"
        elif any(state in {"RUNNING", "VERIFYING"} for state in states):
            job.state = "RUNNING"
        elif any(state == "QUEUED" for state in states):
            job.state = "QUEUED"
        else:
            job.state = "WAITING_APPROVAL"
        await self.session.commit()
        await self.session.refresh(job, attribute_names=["updated_at"])
        return job

    async def bulk_jobs(self, limit: int = 20) -> list[BulkJobOut]:
        jobs = list(
            await self.session.scalars(
                select(models.BulkJob)
                .options(selectinload(models.BulkJob.items))
                .order_by(models.BulkJob.created_at.desc())
                .limit(limit)
            )
        )
        results = []
        for job in jobs:
            results.append(bulk_job_out(await self._refresh_bulk_job(job)))
        return results

    async def bulk_job(self, job_id: uuid.UUID) -> BulkJobOut:
        job = await self.session.scalar(
            select(models.BulkJob)
            .options(selectinload(models.BulkJob.items))
            .where(models.BulkJob.id == job_id)
        )
        if job is None:
            raise NotFoundError(f"Bulk job not found: {job_id}")
        return bulk_job_out(await self._refresh_bulk_job(job))

    async def cancel_bulk_job(
        self, job_id: uuid.UUID, request: CancelOperation
    ) -> BulkJobOut:
        job = await self.session.scalar(
            select(models.BulkJob)
            .options(selectinload(models.BulkJob.items))
            .where(models.BulkJob.id == job_id)
            .with_for_update()
        )
        if job is None:
            raise NotFoundError(f"Bulk job not found: {job_id}")
        now = datetime.now(UTC)
        job.cancellation_requested_at = now
        for item in job.items:
            if item.operation_request_id is None and item.state == "QUEUED":
                item.state = "CANCELED"
                item.finished_at = now
                continue
            if item.operation_request_id is None and item.state == "RUNNING":
                item.state = "CANCEL_REQUESTED"
                continue
            if item.operation_request_id is None:
                continue
            operation = await self.session.get(models.OperationRequest, item.operation_request_id)
            if operation is None or operation.state in {"SUCCEEDED", "FAILED", "REJECTED", "CANCELED"}:
                continue
            operation.cancellation_requested_at = now
            operation.version += 1
            operation.updated_at = now
            if operation.state in {"WAITING_APPROVAL", "QUEUED"}:
                operation.state = "CANCELED"
                operation.canceled_at = now
                item.state = "CANCELED"
                item.finished_at = now
                await self.session.execute(
                    delete(models.OperationTableLock).where(
                        models.OperationTableLock.operation_request_id == operation.id
                    )
                )
            else:
                operation.state = "CANCEL_REQUESTED"
                item.state = "CANCEL_REQUESTED"
        job.state = "CANCEL_REQUESTED"
        self.session.add(
            models.AuditEvent(
                actor=request.actor,
                action="BULK_JOB_CANCEL_REQUESTED",
                resource_type="BULK_JOB",
                resource_id=str(job.id),
                payload={"reason": request.reason},
            )
        )
        await self.session.commit()
        return await self.bulk_job(job.id)

    async def retry_failed_bulk_job(self, job_id: uuid.UUID, actor: str) -> BulkJobOut:
        job = await self.session.scalar(
            select(models.BulkJob)
            .options(selectinload(models.BulkJob.items))
            .where(models.BulkJob.id == job_id)
            .with_for_update()
        )
        if job is None:
            raise NotFoundError(f"Bulk job not found: {job_id}")
        retried = 0
        for item in job.items:
            if item.state != "FAILED":
                continue
            if item.operation_request_id is None:
                item.state = "QUEUED"
                item.error_message = None
                item.started_at = None
                item.finished_at = None
                retried += 1
                continue
            operation = await self.session.get(models.OperationRequest, item.operation_request_id)
            if operation is None:
                continue
            existing_lock = await self.session.get(models.OperationTableLock, operation.table_id)
            if existing_lock is not None and existing_lock.operation_request_id != operation.id:
                item.error_message = "Another operation owns the table lock"
                continue
            if existing_lock is None:
                self.session.add(
                    models.OperationTableLock(
                        table_id=operation.table_id,
                        operation_request_id=operation.id,
                    )
                )
            operation.state = "QUEUED"
            await self.session.refresh(operation, attribute_names=["table"])
            operation.planned_snapshot_id = operation.table.current_snapshot_id
            operation.version += 1
            operation.updated_at = datetime.now(UTC)
            operation.cancellation_requested_at = None
            item.state = "QUEUED"
            item.error_message = None
            item.finished_at = None
            retried += 1
        if not retried:
            raise InvalidOperationError("No failed bulk operation could be retried")
        job.state = "QUEUED"
        job.finished_at = None
        job.cancellation_requested_at = None
        self.session.add(
            models.AuditEvent(
                actor=actor,
                action="BULK_JOB_RETRIED",
                resource_type="BULK_JOB",
                resource_id=str(job.id),
                payload={"retriedItems": retried},
            )
        )
        await self.session.commit()
        return await self.bulk_job(job.id)

    async def worker_status(self, worker_id: str = "spark") -> WorkerStatus:
        heartbeat = await self.session.get(models.WorkerHeartbeat, worker_id)
        if heartbeat is None:
            return WorkerStatus(
                id=worker_id,
                engine_type="SPARK",
                status="OFFLINE",
                online=False,
            )
        online = datetime.now(UTC) - heartbeat.last_seen_at < timedelta(seconds=15)
        return WorkerStatus(
            id=heartbeat.id,
            worker_id=heartbeat.worker_id,
            engine_type=heartbeat.engine_type,
            status=heartbeat.status if online else "OFFLINE",
            online=online,
            current_operation_request_id=heartbeat.current_operation_request_id,
            details=heartbeat.details,
            last_seen_at=heartbeat.last_seen_at,
        )

    async def ingest_metric(
        self, prefix: str | None, namespace: str, table_name: str, payload: dict[str, Any]
    ) -> None:
        normalized_namespace = namespace.replace("\x1f", ".")
        table_id = await self.session.scalar(
            select(models.ManagedTable.id)
            .where(
                models.ManagedTable.namespace_name == normalized_namespace,
                models.ManagedTable.table_name == table_name,
            )
            .limit(1)
        )
        metadata = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
        engine = str(metadata.get("engine") or metadata.get("engine-name") or "").upper() or None
        if engine not in {"SPARK", "TRINO", "FLINK"}:
            engine = None
        snapshot_id = payload.get("snapshot-id", payload.get("snapshotId"))
        self.session.add(
            models.MetricReport(
                table_id=table_id,
                catalog_prefix=prefix,
                namespace_name=normalized_namespace,
                table_name=table_name,
                report_type=str(payload.get("report-type", payload.get("reportType", "unknown"))),
                snapshot_id=snapshot_id if isinstance(snapshot_id, int) else None,
                engine_type=engine,
                payload=redact_metric_payload(payload),
            )
        )
        await self.session.commit()
