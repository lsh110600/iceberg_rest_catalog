import uuid
from datetime import UTC, datetime
from typing import Annotated, Any

import httpx
from fastapi import APIRouter, Depends, Header, HTTPException, status
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app import models
from app.config import get_settings
from app.database import get_session
from app.schemas import (
    BulkJobOut,
    PlaygroundBootstrapResult,
    PlaygroundCatalogCreate,
    PlaygroundCatalogOut,
    PlaygroundComponent,
    PlaygroundNamespaceCreate,
    PlaygroundQueryRequest,
    PlaygroundQueryResult,
    PlaygroundStatus,
    PlaygroundTableBulkRegister,
    PlaygroundTableBulkResult,
    PlaygroundTableRegister,
    TableSummary,
)
from app.services import bulk_job_out, request_fingerprint, table_summary

router = APIRouter(prefix="/ops/api/v1/playground", tags=["playground"])
settings = get_settings()
Session = Annotated[AsyncSession, Depends(get_session)]
PLAYGROUND_IDS = {
    "catalog": uuid.UUID("10000000-0000-0000-0000-000000000010"),
    "storage": uuid.UUID("20000000-0000-0000-0000-000000000010"),
    "engine": uuid.UUID("30000000-0000-0000-0000-000000000010"),
    "table": uuid.UUID("40000000-0000-0000-0000-000000000010"),
}
PLAYGROUND_WAREHOUSE = "hdfs://namenode:8020/warehouse"


def disabled_status(message: str) -> PlaygroundStatus:
    return PlaygroundStatus(
        enabled=settings.playground_enabled,
        ready=False,
        hdfs=PlaygroundComponent(status="UNKNOWN", endpoint="hdfs://namenode:8020"),
        spark=PlaygroundComponent(status="UNKNOWN", endpoint="spark://spark-master:7077"),
        sample_table="playground.demo.orders",
        sample_exists=False,
        versions={"spark": "3.5.9", "iceberg": "1.11.0", "hadoop": "3.5.0"},
        message=message,
    )


async def runner_request(method: str, path: str, body: dict[str, Any] | None = None) -> Any:
    if not settings.playground_enabled:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Playground is disabled. Start Docker Compose with the playground profile.",
        )
    try:
        async with httpx.AsyncClient(timeout=settings.playground_timeout_seconds) as client:
            response = await client.request(
                method, f"{settings.playground_runner_url}{path}", json=body
            )
    except httpx.HTTPError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Playground runner is unavailable: {exc.__class__.__name__}",
        ) from exc

    if response.is_error:
        try:
            detail = response.json().get("detail", response.text)
        except ValueError:
            detail = response.text
        raise HTTPException(status_code=response.status_code, detail=detail)
    return response.json()


async def catalog_output(session: AsyncSession, catalog: models.Catalog) -> PlaygroundCatalogOut:
    namespaces = list(
        await session.scalars(
            select(models.CatalogNamespace.name)
            .where(models.CatalogNamespace.catalog_id == catalog.id)
            .order_by(models.CatalogNamespace.name)
        )
    )
    return PlaygroundCatalogOut(
        id=catalog.id,
        name=catalog.name,
        warehouse=catalog.warehouse or "",
        namespaces=namespaces,
    )


async def ensure_playground_registry(session: AsyncSession) -> models.Catalog:
    catalog = await session.scalar(select(models.Catalog).where(models.Catalog.name == "playground"))
    if catalog is None:
        catalog = models.Catalog(
            id=PLAYGROUND_IDS["catalog"],
            name="playground",
            catalog_type="HADOOP",
            endpoint=settings.playground_runner_url,
            warehouse=PLAYGROUND_WAREHOUSE,
            health="UNKNOWN",
        )
        session.add(catalog)
        await session.flush()

    namespace = await session.scalar(
        select(models.CatalogNamespace).where(
            models.CatalogNamespace.catalog_id == catalog.id,
            models.CatalogNamespace.name == "demo",
        )
    )
    if namespace is None:
        session.add(models.CatalogNamespace(catalog_id=catalog.id, name="demo"))
    await session.commit()
    return catalog


async def sync_playground_table(
    session: AsyncSession, payload: PlaygroundBootstrapResult
) -> None:
    catalog = await ensure_playground_registry(session)
    catalog.endpoint = settings.playground_runner_url
    catalog.warehouse = PLAYGROUND_WAREHOUSE
    catalog.health = "HEALTHY"
    catalog.last_synced_at = datetime.now(UTC)

    storage = await session.scalar(
        select(models.StorageProfile).where(models.StorageProfile.name == "hdfs-playground")
    )
    if storage is None:
        storage = models.StorageProfile(
            id=PLAYGROUND_IDS["storage"],
            name="hdfs-playground",
            storage_type="HDFS",
            uri_prefix="hdfs://namenode:8020/warehouse",
            file_io_impl="org.apache.iceberg.hadoop.HadoopFileIO",
        )
        session.add(storage)
    storage.health = "HEALTHY"

    engine = await session.scalar(
        select(models.EngineInstance).where(models.EngineInstance.name == "spark-playground")
    )
    if engine is None:
        engine = models.EngineInstance(
            id=PLAYGROUND_IDS["engine"],
            name="spark-playground",
            engine_type="SPARK",
        )
        session.add(engine)
    engine.version = "3.5.9"
    engine.endpoint = "spark://spark-master:7077"
    engine.health = "HEALTHY"
    engine.capabilities = [
        "REWRITE_DATA_FILES",
        "REWRITE_MANIFESTS",
        "REWRITE_POSITION_DELETES",
        "EXPIRE_SNAPSHOTS",
        "REMOVE_ORPHAN_FILES",
        "COMPUTE_STATS",
        "ROLLBACK",
    ]
    engine.last_checked_at = datetime.now(UTC)
    await session.flush()

    table = await session.scalar(
        select(models.ManagedTable).where(
            models.ManagedTable.catalog_id == catalog.id,
            models.ManagedTable.namespace_name == "demo",
            models.ManagedTable.table_name == "orders",
        )
    )
    if table is None:
        table = models.ManagedTable(
            id=PLAYGROUND_IDS["table"],
            catalog_id=catalog.id,
            namespace_name="demo",
            table_name="orders",
        )
        session.add(table)
    table.storage_profile_id = storage.id
    table.owner_name = "playground"
    table.slo_profile = "local-playground"
    table.format_version = 2
    table.metadata_location = payload.location
    table.current_snapshot_id = payload.current_snapshot_id
    table.last_commit_at = datetime.now(UTC)
    table.total_bytes = payload.total_bytes
    table.total_records = payload.total_records
    table.total_files = payload.total_files
    table.snapshot_count = payload.snapshot_count
    table.health = "HEALTHY"
    table.observed_at = datetime.now(UTC)
    table.properties = {"execution.enabled": "true", "catalog.type": "hadoop"}
    await session.commit()


@router.get("/status", response_model=PlaygroundStatus)
async def playground_status() -> PlaygroundStatus:
    if not settings.playground_enabled:
        return disabled_status("Playground Docker profile is not enabled.")
    try:
        payload = await runner_request("GET", "/status")
    except HTTPException as exc:
        return disabled_status(str(exc.detail))
    return PlaygroundStatus.model_validate(payload)


@router.post("/bootstrap", response_model=PlaygroundBootstrapResult)
async def bootstrap_playground(session: Session) -> PlaygroundBootstrapResult:
    payload = await runner_request("POST", "/bootstrap")
    result = PlaygroundBootstrapResult.model_validate(payload)
    await sync_playground_table(session, result)
    return result


@router.get("/catalogs", response_model=list[PlaygroundCatalogOut])
async def list_playground_catalogs(session: Session) -> list[PlaygroundCatalogOut]:
    catalogs = list(
        await session.scalars(
            select(models.Catalog)
            .where(models.Catalog.catalog_type == "HADOOP", models.Catalog.warehouse.is_not(None))
            .order_by(models.Catalog.name)
        )
    )
    return [await catalog_output(session, catalog) for catalog in catalogs]


@router.post(
    "/catalogs",
    response_model=PlaygroundCatalogOut,
    status_code=status.HTTP_201_CREATED,
)
async def register_playground_catalog(
    request: PlaygroundCatalogCreate, session: Session
) -> PlaygroundCatalogOut:
    existing = await session.scalar(select(models.Catalog).where(models.Catalog.name == request.name))
    if existing is not None:
        if existing.catalog_type == "HADOOP" and existing.warehouse == request.warehouse:
            return await catalog_output(session, existing)
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Catalog name is already registered with different settings: {request.name}",
        )

    catalog = models.Catalog(
        name=request.name,
        catalog_type="HADOOP",
        endpoint=settings.playground_runner_url,
        warehouse=request.warehouse,
        health="UNKNOWN",
    )
    session.add(catalog)
    await session.commit()
    return await catalog_output(session, catalog)


@router.post(
    "/namespaces",
    response_model=PlaygroundCatalogOut,
    status_code=status.HTTP_201_CREATED,
)
async def register_playground_namespace(
    request: PlaygroundNamespaceCreate, session: Session
) -> PlaygroundCatalogOut:
    catalog = await session.scalar(
        select(models.Catalog).where(
            models.Catalog.name == request.catalog_name,
            models.Catalog.catalog_type == "HADOOP",
        )
    )
    if catalog is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Registered Hadoop catalog was not found: {request.catalog_name}",
        )
    existing = await session.scalar(
        select(models.CatalogNamespace).where(
            models.CatalogNamespace.catalog_id == catalog.id,
            models.CatalogNamespace.name == request.name,
        )
    )
    if existing is None:
        session.add(models.CatalogNamespace(catalog_id=catalog.id, name=request.name))
        await session.commit()
    return await catalog_output(session, catalog)


async def registered_table_scope(
    session: AsyncSession, catalog_name: str, namespace_name: str
) -> models.Catalog:
    catalog = await session.scalar(
        select(models.Catalog).where(
            models.Catalog.name == catalog_name,
            models.Catalog.catalog_type == "HADOOP",
        )
    )
    if catalog is None or catalog.warehouse is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Registered Hadoop catalog was not found: {catalog_name}",
        )
    namespace = await session.scalar(
        select(models.CatalogNamespace).where(
            models.CatalogNamespace.catalog_id == catalog.id,
            models.CatalogNamespace.name == namespace_name,
        )
    )
    if namespace is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                "Registered namespace was not found: "
                f"{catalog_name}.{namespace_name}"
            ),
        )
    return catalog


async def sync_inspected_table(
    session: AsyncSession,
    catalog: models.Catalog,
    namespace_name: str,
    table_name: str,
    payload: dict[str, Any],
) -> models.ManagedTable:
    table = await session.scalar(
        select(models.ManagedTable).where(
            models.ManagedTable.catalog_id == catalog.id,
            models.ManagedTable.namespace_name == namespace_name,
            models.ManagedTable.table_name == table_name,
        )
    )
    if table is None:
        table = models.ManagedTable(
            catalog_id=catalog.id,
            namespace_name=namespace_name,
            table_name=table_name,
        )
        session.add(table)

    data_files = int(payload.get("data_files", 0))
    delete_files = int(payload.get("delete_files", 0))
    small_data_files = int(payload.get("small_data_files", 0))
    table.slo_profile = "operator-managed"
    table.format_version = payload.get("format_version")
    table.metadata_location = payload.get("location")
    snapshot_id = payload.get("snapshot_id")
    table.current_snapshot_id = int(snapshot_id) if snapshot_id is not None else None
    last_commit_at = payload.get("last_commit_at")
    table.last_commit_at = (
        datetime.fromisoformat(last_commit_at) if isinstance(last_commit_at, str) else last_commit_at
    )
    table.total_bytes = payload.get("total_bytes", 0)
    table.total_records = payload.get("total_records", 0)
    table.total_files = payload.get("total_files", 0)
    table.snapshot_count = payload.get("snapshot_count", 0)
    table.small_file_ratio = small_data_files / data_files if data_files else None
    table.delete_file_ratio = (
        delete_files / (data_files + delete_files) if data_files + delete_files else None
    )
    table.health = "HEALTHY"
    table.observed_at = datetime.now(UTC)
    table.properties = {
        **(table.properties or {}),
        "execution.enabled": "true",
        "catalog.type": "hadoop",
        "warehouse": catalog.warehouse,
        "metric.partition-count": str(payload.get("partition_count", 0)),
    }
    return table


@router.post("/tables", response_model=TableSummary, status_code=status.HTTP_201_CREATED)
async def register_playground_table(
    request: PlaygroundTableRegister, session: Session
) -> TableSummary:
    catalog = await registered_table_scope(
        session, request.catalog_name, request.namespace_name
    )
    payload = await runner_request(
        "POST",
        "/tables/inspect",
        {
            "catalog": catalog.name,
            "warehouse": catalog.warehouse,
            "namespace": request.namespace_name,
            "table": request.table_name,
        },
    )
    table = await sync_inspected_table(
        session, catalog, request.namespace_name, request.table_name, payload
    )
    catalog.health = "HEALTHY"
    catalog.last_synced_at = datetime.now(UTC)
    await session.commit()
    await session.refresh(table, attribute_names=["catalog", "storage_profile"])
    return table_summary(table)


@router.post(
    "/tables/bulk",
    response_model=PlaygroundTableBulkResult,
    status_code=status.HTTP_201_CREATED,
)
async def register_playground_tables(
    request: PlaygroundTableBulkRegister, session: Session
) -> PlaygroundTableBulkResult:
    catalog = await registered_table_scope(
        session, request.catalog_name, request.namespace_name
    )
    payload = await runner_request(
        "POST",
        "/tables/inspect-bulk",
        {
            "catalog": catalog.name,
            "warehouse": catalog.warehouse,
            "namespace": request.namespace_name,
            "tables": request.table_names,
        },
    )
    tables = [
        await sync_inspected_table(
            session,
            catalog,
            request.namespace_name,
            item["table"],
            item,
        )
        for item in payload.get("tables", [])
    ]
    if tables:
        catalog.health = "HEALTHY"
        catalog.last_synced_at = datetime.now(UTC)
    await session.commit()
    for table in tables:
        await session.refresh(table, attribute_names=["catalog", "storage_profile"])
    return PlaygroundTableBulkResult(
        tables=[table_summary(table) for table in tables],
        errors=[
            {"table_name": item["table"], "detail": item["detail"]}
            for item in payload.get("errors", [])
        ],
        requested_count=payload.get("requested_count", len(request.table_names)),
    )


@router.post(
    "/tables/bulk-jobs",
    response_model=BulkJobOut,
    status_code=status.HTTP_202_ACCEPTED,
)
async def create_playground_table_bulk_job(
    request: PlaygroundTableBulkRegister,
    session: Session,
    idempotency_key: Annotated[
        str | None, Header(alias="Idempotency-Key", min_length=1, max_length=200)
    ] = None,
    actor: Annotated[str, Header(alias="X-Actor")] = "web.operator",
) -> BulkJobOut:
    catalog = await registered_table_scope(
        session, request.catalog_name, request.namespace_name
    )
    fingerprint = request_fingerprint(request.model_dump(mode="json"))
    if idempotency_key:
        existing = await session.scalar(
            select(models.BulkJob).where(models.BulkJob.idempotency_key == idempotency_key)
        )
        if existing is not None:
            if existing.request_fingerprint != fingerprint:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="Idempotency key was already used with a different request",
                )
            await session.refresh(existing, attribute_names=["items"])
            return bulk_job_out(existing)

    job = models.BulkJob(
        kind="TABLE_INSPECT",
        state="QUEUED",
        idempotency_key=idempotency_key,
        request_fingerprint=fingerprint,
        catalog_id=catalog.id,
        namespace_name=request.namespace_name,
        requested_by=actor,
        total_items=len(request.table_names),
    )
    session.add(job)
    await session.flush()
    session.add_all(
        [
            models.BulkJobItem(
                bulk_job_id=job.id,
                ordinal=ordinal,
                table_name=table_name,
                state="QUEUED",
            )
            for ordinal, table_name in enumerate(request.table_names)
        ]
    )
    session.add(
        models.AuditEvent(
            actor=actor,
            action="BULK_TABLE_INSPECTION_REQUESTED",
            resource_type="BULK_JOB",
            resource_id=str(job.id),
            payload={
                "catalog": catalog.name,
                "namespace": request.namespace_name,
                "tableCount": len(request.table_names),
            },
        )
    )
    try:
        await session.commit()
    except IntegrityError as exc:
        await session.rollback()
        if not idempotency_key:
            raise
        existing = await session.scalar(
            select(models.BulkJob).where(models.BulkJob.idempotency_key == idempotency_key)
        )
        if existing is None or existing.request_fingerprint != fingerprint:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Idempotency key conflict",
            ) from exc
        await session.refresh(existing, attribute_names=["items"])
        return bulk_job_out(existing)
    await session.refresh(job, attribute_names=["items"])
    return bulk_job_out(job)


@router.post("/query", response_model=PlaygroundQueryResult)
async def query_playground(request: PlaygroundQueryRequest, session: Session) -> PlaygroundQueryResult:
    catalog = await session.scalar(
        select(models.Catalog).where(
            models.Catalog.name == request.catalog_name,
            models.Catalog.catalog_type == "HADOOP",
        )
    )
    if catalog is None or catalog.warehouse is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Registered Hadoop catalog was not found: {request.catalog_name}",
        )
    payload = await runner_request(
        "POST",
        "/query",
        {
            "sql": request.sql,
            "max_rows": request.max_rows,
            "catalog": catalog.name,
            "warehouse": catalog.warehouse,
        },
    )
    return PlaygroundQueryResult.model_validate(payload)
