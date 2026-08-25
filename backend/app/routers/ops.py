import asyncio
import json
import uuid
from collections.abc import AsyncIterator
from typing import Annotated

from fastapi import APIRouter, Depends, Header, Query, status
from fastapi.responses import StreamingResponse
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app import models
from app.database import SessionFactory, get_session
from app.schemas import (
    ApprovalCreate,
    BulkJobAction,
    BulkJobOut,
    CancelOperation,
    Dashboard,
    EngineInstanceOut,
    MetricPointOut,
    OperationPage,
    OperationPlanOut,
    OperationPlanRequest,
    OperationRequestBulkCreate,
    OperationRequestBulkOut,
    OperationRequestCreate,
    OperationRequestOut,
    RetryOperation,
    StorageProfileOut,
    TableDetail,
    TablePage,
    TableSummary,
    WorkerStatus,
)
from app.services import OpsService

router = APIRouter(prefix="/ops/api/v1", tags=["operations"])
Session = Annotated[AsyncSession, Depends(get_session)]


@router.get("/dashboard", response_model=Dashboard)
async def dashboard(session: Session) -> Dashboard:
    return await OpsService(session).dashboard()


@router.get("/tables", response_model=list[TableSummary])
async def tables(session: Session, q: str | None = Query(default=None, max_length=200)) -> list[TableSummary]:
    return await OpsService(session).tables(q)


@router.get("/tables/page", response_model=TablePage)
async def table_page(
    session: Session,
    limit: int = Query(default=50, ge=1, le=200),
    cursor: str | None = None,
    q: str | None = Query(default=None, max_length=200),
    catalog: str | None = Query(default=None, max_length=120),
    health: str | None = Query(default=None, max_length=40),
) -> TablePage:
    return await OpsService(session).table_page(limit, cursor, q, catalog, health)


@router.get("/tables/{table_id}", response_model=TableDetail)
async def table(table_id: uuid.UUID, session: Session) -> TableDetail:
    return await OpsService(session).table(table_id)


@router.get("/tables/{table_id}/metrics", response_model=list[MetricPointOut])
async def metrics(table_id: uuid.UUID, session: Session, metric: str | None = None) -> list[MetricPointOut]:
    return await OpsService(session).metrics(table_id, metric)


@router.get("/storage-profiles", response_model=list[StorageProfileOut])
async def storage_profiles(session: Session) -> list[StorageProfileOut]:
    return await OpsService(session).storage_profiles()


@router.get("/engines", response_model=list[EngineInstanceOut])
async def engines(session: Session) -> list[EngineInstanceOut]:
    return await OpsService(session).engines()


@router.get("/operation-requests", response_model=list[OperationRequestOut])
async def operation_requests(session: Session) -> list[OperationRequestOut]:
    return await OpsService(session).operations()


@router.get("/operation-requests/page", response_model=OperationPage)
async def operation_request_page(
    session: Session,
    limit: int = Query(default=30, ge=1, le=100),
    cursor: str | None = None,
    state: str | None = Query(default=None, max_length=40),
    catalog: str | None = Query(default=None, max_length=120),
) -> OperationPage:
    return await OpsService(session).operation_page(limit, cursor, state, catalog)


@router.post("/operation-plans", response_model=OperationPlanOut)
async def operation_plan(request: OperationPlanRequest, session: Session) -> OperationPlanOut:
    return await OpsService(session).operation_plan(request)


@router.post(
    "/operation-requests/bulk",
    response_model=OperationRequestBulkOut,
    status_code=status.HTTP_201_CREATED,
)
async def create_operations(
    request: OperationRequestBulkCreate,
    session: Session,
    idempotency_key: Annotated[
        str | None, Header(alias="Idempotency-Key", min_length=1, max_length=200)
    ] = None,
) -> OperationRequestBulkOut:
    return await OpsService(session).create_operations(request, idempotency_key)


@router.get("/operation-requests/{request_id}", response_model=OperationRequestOut)
async def operation_request(request_id: uuid.UUID, session: Session) -> OperationRequestOut:
    return await OpsService(session).operation(request_id)


@router.post("/operation-requests", response_model=OperationRequestOut, status_code=status.HTTP_201_CREATED)
async def create_operation(
    request: OperationRequestCreate,
    session: Session,
    idempotency_key: Annotated[
        str | None, Header(alias="Idempotency-Key", min_length=1, max_length=200)
    ] = None,
) -> OperationRequestOut:
    return await OpsService(session).create_operation(request, idempotency_key)


@router.post("/operation-requests/{request_id}/approvals", response_model=OperationRequestOut)
async def approve_operation(
    request_id: uuid.UUID, approval: ApprovalCreate, session: Session
) -> OperationRequestOut:
    return await OpsService(session).decide_operation(
        request_id, approval.approver, approval.decision, approval.comment
    )


@router.post("/operation-requests/{request_id}/retry", response_model=OperationRequestOut)
async def retry_operation(
    request_id: uuid.UUID, retry: RetryOperation, session: Session
) -> OperationRequestOut:
    return await OpsService(session).retry_operation(request_id, retry.actor)


@router.post("/operation-requests/{request_id}/cancel", response_model=OperationRequestOut)
async def cancel_operation(
    request_id: uuid.UUID, request: CancelOperation, session: Session
) -> OperationRequestOut:
    return await OpsService(session).cancel_operation(request_id, request)


@router.get("/bulk-jobs", response_model=list[BulkJobOut])
async def bulk_jobs(
    session: Session, limit: int = Query(default=20, ge=1, le=100)
) -> list[BulkJobOut]:
    return await OpsService(session).bulk_jobs(limit)


@router.get("/bulk-jobs/{job_id}", response_model=BulkJobOut)
async def bulk_job(job_id: uuid.UUID, session: Session) -> BulkJobOut:
    return await OpsService(session).bulk_job(job_id)


@router.post("/bulk-jobs/{job_id}/cancel", response_model=BulkJobOut)
async def cancel_bulk_job(
    job_id: uuid.UUID, action: BulkJobAction, session: Session
) -> BulkJobOut:
    return await OpsService(session).cancel_bulk_job(
        job_id,
        CancelOperation(actor=action.actor, reason=action.reason or "Bulk job canceled"),
    )


@router.post("/bulk-jobs/{job_id}/retry-failed", response_model=BulkJobOut)
async def retry_failed_bulk_job(
    job_id: uuid.UUID, action: BulkJobAction, session: Session
) -> BulkJobOut:
    return await OpsService(session).retry_failed_bulk_job(job_id, action.actor)


async def operation_stream() -> AsyncIterator[str]:
    last_token = ""
    while True:
        async with SessionFactory() as session:
            operation_updated = await session.scalar(select(func.max(models.OperationRequest.updated_at)))
            bulk_updated = await session.scalar(select(func.max(models.BulkJob.updated_at)))
            token = f"{operation_updated}|{bulk_updated}"
        if token != last_token:
            yield f"event: operations\ndata: {json.dumps({'token': token})}\n\n"
            last_token = token
        else:
            yield ": heartbeat\n\n"
        await asyncio.sleep(2)


@router.get("/operation-stream")
async def stream_operations() -> StreamingResponse:
    return StreamingResponse(
        operation_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.get("/workers/spark", response_model=WorkerStatus)
async def spark_worker_status(session: Session) -> WorkerStatus:
    return await OpsService(session).worker_status("spark")
