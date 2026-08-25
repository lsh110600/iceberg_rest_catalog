import asyncio
import contextlib
import logging
import os
import socket
import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import delete, func, or_, select
from sqlalchemy.orm import selectinload

from app import models
from app.config import get_settings
from app.database import SessionFactory
from app.executors import OperationCanceled, create_spark_executor

settings = get_settings()
executor = create_spark_executor(settings)
WORKER_KEY = "spark"
WORKER_ID = f"{socket.gethostname()}:{os.getpid()}:{str(uuid.uuid4())[:8]}"
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("iceberg-ops-worker")


def optional_int(value: Any) -> int | None:
    return int(value) if value is not None else None


async def heartbeat(
    status: str,
    current_operation_request_id: uuid.UUID | None = None,
    details: dict[str, Any] | None = None,
) -> None:
    async with SessionFactory() as session:
        row = await session.get(models.WorkerHeartbeat, WORKER_KEY)
        if row is None:
            row = models.WorkerHeartbeat(
                id=WORKER_KEY,
                worker_id=WORKER_ID,
                engine_type="SPARK",
                status=status,
                last_seen_at=datetime.now(UTC),
            )
            session.add(row)
        row.worker_id = WORKER_ID
        row.status = status
        row.current_operation_request_id = current_operation_request_id
        row.details = {**executor.heartbeat_details(), **(details or {})}
        row.last_seen_at = datetime.now(UTC)
        await session.commit()


async def heartbeat_while_running(operation_id: uuid.UUID) -> None:
    while True:
        await heartbeat("BUSY", operation_id)
        await asyncio.sleep(5)


async def refresh_idle_heartbeat() -> bool:
    try:
        details = await executor.probe()
    except Exception as exc:
        logger.warning("Spark execution endpoint probe failed: %s", exc)
        await heartbeat("DEGRADED", details={"engineHealth": "UNAVAILABLE", "error": str(exc)[:1000]})
        return False
    await heartbeat("IDLE", details=details)
    return True


def operation_payload(operation: models.OperationRequest) -> dict[str, Any]:
    return {
        "operation_id": str(operation.id),
        "catalog": operation.table.catalog.name,
        "catalog_type": operation.table.catalog.catalog_type,
        "warehouse": operation.table.catalog.warehouse,
        "namespace": operation.table.namespace_name,
        "table": operation.table.table_name,
        "command": operation.command,
        "parameters": operation.parameters,
        "expected_snapshot_id": operation.planned_snapshot_id,
    }


async def cancellation_requested(operation_id: uuid.UUID) -> bool:
    async with SessionFactory() as session:
        state = await session.scalar(
            select(models.OperationRequest.state).where(models.OperationRequest.id == operation_id)
        )
        return state in {"CANCEL_REQUESTED", "CANCELED"}


async def recover_stale_executions() -> None:
    async with SessionFactory() as session:
        rows = (
            await session.scalars(
                select(models.OperationExecution).where(
                    models.OperationExecution.state.in_(["RUNNING", "VERIFYING"]),
                    or_(
                        models.OperationExecution.worker_id.is_(None),
                        models.OperationExecution.worker_id != WORKER_ID,
                    ),
                )
            )
        ).all()
        resumable: list[tuple[uuid.UUID, uuid.UUID, dict[str, Any], str]] = []
        for execution in rows:
            operation = await session.get(models.OperationRequest, execution.operation_request_id)
            if operation is not None and execution.external_id and settings.spark_execution_mode == "livy":
                execution.worker_id = WORKER_ID
                resumable.append(
                    (
                        operation.id,
                        execution.id,
                        operation_payload(operation),
                        execution.external_id,
                    )
                )
                session.add(
                    models.OperationEvent(
                        execution_id=execution.id,
                        event_type="RECOVERY_ATTACHED",
                        message=f"Worker attached to existing execution {execution.external_id}.",
                    )
                )
                continue
            execution.state = "FAILED"
            execution.error_message = (
                "Worker heartbeat expired while execution was in progress. "
                "Inspect Spark and table state before retrying."
            )
            execution.finished_at = datetime.now(UTC)
            if operation is not None:
                operation.state = "FAILED"
                operation.updated_at = datetime.now(UTC)
                operation.version += 1
                await session.execute(
                    delete(models.OperationTableLock).where(
                        models.OperationTableLock.operation_request_id == operation.id
                    )
                )
                await finish_bulk_item(
                    session,
                    operation,
                    "FAILED",
                    "Worker heartbeat expired while execution was in progress.",
                )
            session.add(
                models.OperationEvent(
                    execution_id=execution.id,
                    level="ERROR",
                    event_type="STALE_EXECUTION",
                    message="Execution was marked failed after its worker heartbeat expired.",
                )
            )
        if rows:
            await session.commit()
    for operation_id, execution_id, payload, external_id in resumable:
        await execute_claim(operation_id, execution_id, payload, external_id=external_id)


async def claim_operation() -> tuple[uuid.UUID, uuid.UUID, dict[str, Any]] | None:
    async with SessionFactory() as session:
        async with session.begin():
            execution_scope = []
            if settings.spark_catalog_scope == "playground":
                execution_scope.append(models.Catalog.catalog_type == "HADOOP")
            else:
                execution_scope.append(models.Catalog.catalog_type != "HADOOP")
            statement = (
                select(models.OperationRequest)
                .join(
                    models.ManagedTable,
                    models.ManagedTable.id == models.OperationRequest.table_id,
                )
                .join(models.Catalog, models.Catalog.id == models.ManagedTable.catalog_id)
                .outerjoin(
                    models.OperationTableLock,
                    models.OperationTableLock.table_id == models.OperationRequest.table_id,
                )
                .options(selectinload(models.OperationRequest.executions))
                .where(
                    models.OperationRequest.state == "QUEUED",
                    models.OperationRequest.preferred_engine.in_(["AUTO", "SPARK"]),
                    or_(
                        models.OperationTableLock.table_id.is_(None),
                        models.OperationTableLock.operation_request_id
                        == models.OperationRequest.id,
                    ),
                    *execution_scope,
                )
                .order_by(models.OperationRequest.created_at)
                .with_for_update(skip_locked=True, of=models.OperationRequest)
                .limit(1)
            )
            operation = (await session.scalars(statement)).unique().one_or_none()
            if operation is None:
                return None

            table_lock = await session.get(models.OperationTableLock, operation.table_id)
            if table_lock is None:
                session.add(
                    models.OperationTableLock(
                        table_id=operation.table_id,
                        operation_request_id=operation.id,
                    )
                )
                await session.flush()

            attempt = (
                await session.scalar(
                    select(func.coalesce(func.max(models.OperationExecution.attempt), 0)).where(
                        models.OperationExecution.operation_request_id == operation.id
                    )
                )
            ) + 1
            engine = await session.scalar(
                select(models.EngineInstance).where(
                    models.EngineInstance.name == settings.spark_engine_name
                )
            )
            execution = models.OperationExecution(
                operation_request_id=operation.id,
                attempt=attempt,
                engine_type="SPARK",
                engine_instance_id=engine.id if engine else None,
                state="RUNNING",
                worker_id=WORKER_ID,
                external_id=None,
                planned_snapshot_id=operation.planned_snapshot_id,
                queued_at=operation.updated_at,
                started_at=datetime.now(UTC),
            )
            session.add(execution)
            await session.flush()
            operation.state = "RUNNING"
            operation.updated_at = datetime.now(UTC)
            session.add(
                models.OperationEvent(
                    execution_id=execution.id,
                    event_type="CLAIMED",
                    message=f"Spark worker {WORKER_ID} claimed the queued request.",
                    details={"attempt": attempt},
                )
            )
            session.add(
                models.AuditEvent(
                    actor=WORKER_ID,
                    action="OPERATION_STARTED",
                    resource_type="OPERATION_REQUEST",
                    resource_id=str(operation.id),
                    payload={"executionId": str(execution.id), "attempt": attempt},
                )
            )
            payload = operation_payload(operation)
            return operation.id, execution.id, payload


async def mark_submitted(
    execution_id: uuid.UUID,
    external_id: str,
    details: dict[str, Any],
) -> None:
    async with SessionFactory() as session:
        execution = await session.get(models.OperationExecution, execution_id)
        if execution is None:
            return
        execution.external_id = external_id
        session.add(
            models.OperationEvent(
                execution_id=execution.id,
                event_type="ENGINE_SUBMITTED",
                message=f"Spark execution was submitted as {external_id}.",
                details={"executionMode": settings.spark_execution_mode, **details},
            )
        )
        await session.commit()


async def mark_verifying(execution_id: uuid.UUID, result: dict[str, Any]) -> None:
    async with SessionFactory() as session:
        execution = await session.get(models.OperationExecution, execution_id)
        if execution is None:
            return
        operation = await session.get(models.OperationRequest, execution.operation_request_id)
        before = result.get("before", {})
        after = result.get("after", {})
        execution.state = "VERIFYING"
        execution.before_snapshot_id = optional_int(before.get("snapshot_id"))
        execution.after_snapshot_id = optional_int(after.get("snapshot_id"))
        execution.result = result
        if operation is not None:
            operation.state = "VERIFYING"
            operation.updated_at = datetime.now(UTC)
        session.add(
            models.OperationEvent(
                execution_id=execution.id,
                event_type="SPARK_COMPLETED",
                message="Spark procedure completed; verifying the resulting Iceberg state.",
                details={
                    "beforeSnapshotId": execution.before_snapshot_id,
                    "afterSnapshotId": execution.after_snapshot_id,
                    "durationMs": result.get("duration_ms"),
                },
            )
        )
        await session.commit()


def verify_result(
    command: str, result: dict[str, Any], parameters: dict[str, Any] | None = None
) -> None:
    parameters = parameters or {}
    before = result.get("before")
    after = result.get("after")
    if not isinstance(before, dict) or not isinstance(after, dict):
        raise RuntimeError("Spark result did not contain before/after table state.")
    if command != "ROLLBACK" and before.get("total_records") != after.get("total_records"):
        raise RuntimeError("Record count changed unexpectedly during maintenance verification.")
    if after.get("snapshot_id") is None:
        raise RuntimeError("The table has no current snapshot after maintenance.")
    for key in ["table_uuid", "location", "schema_id", "spec_id", "sort_order_id"]:
        if before.get(key) is not None and after.get(key) != before.get(key):
            raise RuntimeError(f"Table invariant changed unexpectedly: {key}")
    for key in ["total_files", "total_bytes", "data_files", "delete_files", "partition_count"]:
        if after.get(key) is not None and int(after[key]) < 0:
            raise RuntimeError(f"Table metric became negative: {key}")
    if command == "EXPIRE_SNAPSHOTS" and after.get("snapshot_count", 0) > before.get(
        "snapshot_count", 0
    ):
        raise RuntimeError("Snapshot count increased during snapshot expiration.")
    if command == "ROLLBACK":
        target = parameters.get("snapshotId")
        if target is not None and int(after["snapshot_id"]) != int(target):
            raise RuntimeError("Rollback did not select the requested snapshot.")


async def finish_bulk_item(
    session: Any, operation: models.OperationRequest, state: str, error: str | None = None
) -> None:
    if operation.bulk_job_id is None:
        return
    item = await session.scalar(
        select(models.BulkJobItem).where(
            models.BulkJobItem.operation_request_id == operation.id
        )
    )
    if item is not None:
        item.state = state
        item.error_message = error
        item.finished_at = datetime.now(UTC)


async def mark_succeeded(
    operation_id: uuid.UUID, execution_id: uuid.UUID, result: dict[str, Any]
) -> None:
    async with SessionFactory() as session:
        execution = await session.get(models.OperationExecution, execution_id)
        operation = await session.get(models.OperationRequest, operation_id)
        if execution is None or operation is None:
            return
        verify_result(operation.command, result, operation.parameters)
        after = result["after"]
        execution.state = "SUCCEEDED"
        execution.finished_at = datetime.now(UTC)
        operation.state = "SUCCEEDED"
        operation.updated_at = datetime.now(UTC)
        operation.version += 1
        operation.table.current_snapshot_id = optional_int(after.get("snapshot_id"))
        operation.table.snapshot_count = after.get("snapshot_count", operation.table.snapshot_count)
        operation.table.total_files = after.get("total_files", operation.table.total_files)
        operation.table.total_bytes = after.get("total_bytes", operation.table.total_bytes)
        operation.table.total_records = after.get("total_records", operation.table.total_records)
        operation.table.observed_at = datetime.now(UTC)
        session.add(
            models.OperationEvent(
                execution_id=execution.id,
                event_type="VERIFIED",
                message="Iceberg table state verification succeeded.",
                details={"after": after},
            )
        )
        session.add(
            models.AuditEvent(
                actor=WORKER_ID,
                action="OPERATION_SUCCEEDED",
                resource_type="OPERATION_REQUEST",
                resource_id=str(operation.id),
                payload={"executionId": str(execution.id), "result": result},
            )
        )
        await session.execute(
            delete(models.OperationTableLock).where(
                models.OperationTableLock.operation_request_id == operation.id
            )
        )
        await finish_bulk_item(session, operation, "SUCCEEDED")
        await session.commit()


async def mark_failed(
    operation_id: uuid.UUID, execution_id: uuid.UUID, error: Exception
) -> None:
    message = str(error)[:4000]
    async with SessionFactory() as session:
        execution = await session.get(models.OperationExecution, execution_id)
        operation = await session.get(models.OperationRequest, operation_id)
        if execution is not None:
            execution.state = "FAILED"
            execution.error_message = message
            execution.finished_at = datetime.now(UTC)
            session.add(
                models.OperationEvent(
                    execution_id=execution.id,
                    level="ERROR",
                    event_type="FAILED",
                    message=message[:1000],
                )
            )
        if operation is not None:
            operation.state = "FAILED"
            operation.updated_at = datetime.now(UTC)
            operation.version += 1
            session.add(
                models.AuditEvent(
                    actor=WORKER_ID,
                    action="OPERATION_FAILED",
                    resource_type="OPERATION_REQUEST",
                    resource_id=str(operation.id),
                    payload={"executionId": str(execution_id), "error": message},
                )
            )
            await session.execute(
                delete(models.OperationTableLock).where(
                    models.OperationTableLock.operation_request_id == operation.id
                )
            )
            await finish_bulk_item(session, operation, "FAILED", message)
        await session.commit()


async def mark_canceled(operation_id: uuid.UUID, execution_id: uuid.UUID, message: str) -> None:
    async with SessionFactory() as session:
        execution = await session.get(models.OperationExecution, execution_id)
        operation = await session.get(models.OperationRequest, operation_id)
        now = datetime.now(UTC)
        if execution is not None:
            execution.state = "CANCELED"
            execution.error_message = message
            execution.finished_at = now
            session.add(
                models.OperationEvent(
                    execution_id=execution.id,
                    level="WARNING",
                    event_type="CANCELED",
                    message=message[:1000],
                )
            )
        if operation is not None:
            operation.state = "CANCELED"
            operation.canceled_at = now
            operation.updated_at = now
            operation.version += 1
            await session.execute(
                delete(models.OperationTableLock).where(
                    models.OperationTableLock.operation_request_id == operation.id
                )
            )
            await finish_bulk_item(session, operation, "CANCELED", message)
            session.add(
                models.AuditEvent(
                    actor=WORKER_ID,
                    action="OPERATION_CANCELED",
                    resource_type="OPERATION_REQUEST",
                    resource_id=str(operation.id),
                    payload={"executionId": str(execution_id), "message": message},
                )
            )
        await session.commit()


async def execute_claim(
    operation_id: uuid.UUID,
    execution_id: uuid.UUID,
    payload: dict[str, Any],
    external_id: str | None = None,
) -> None:
    heartbeat_task = asyncio.create_task(heartbeat_while_running(operation_id))
    try:
        async def cancel_check() -> bool:
            return await cancellation_requested(operation_id)

        if external_id:
            result = await executor.resume(payload, external_id, cancel_check)
        else:
            result = await executor.execute(
                payload,
                lambda submitted_id, details: mark_submitted(
                    execution_id, submitted_id, details
                ),
                cancel_check,
            )
        await mark_verifying(execution_id, result)
        await mark_succeeded(operation_id, execution_id, result)
    except OperationCanceled as exc:
        await mark_canceled(operation_id, execution_id, str(exc))
    except Exception as exc:
        await mark_failed(operation_id, execution_id, exc)
    finally:
        heartbeat_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await heartbeat_task
        await heartbeat("IDLE")


async def run() -> None:
    await recover_stale_executions()
    while True:
        if not await refresh_idle_heartbeat():
            await asyncio.sleep(settings.operation_worker_poll_seconds)
            continue
        try:
            claim = await claim_operation()
        except Exception as exc:
            logger.exception("Queue polling failed")
            with contextlib.suppress(Exception):
                await heartbeat("DEGRADED", details={"error": str(exc)[:1000]})
            await asyncio.sleep(settings.operation_worker_poll_seconds)
            continue
        if claim is None:
            await asyncio.sleep(settings.operation_worker_poll_seconds)
            continue
        await execute_claim(*claim)


if __name__ == "__main__":
    asyncio.run(run())
