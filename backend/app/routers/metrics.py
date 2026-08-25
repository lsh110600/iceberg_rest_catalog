from typing import Annotated, Any

from fastapi import APIRouter, Body, Depends, Response, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_session
from app.services import OpsService

router = APIRouter(tags=["iceberg-metrics"])
Session = Annotated[AsyncSession, Depends(get_session)]


@router.post(
    "/v1/namespaces/{namespace}/tables/{table}/metrics",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def ingest_metrics_without_prefix(
    namespace: str, table: str, session: Session, payload: Annotated[dict[str, Any], Body()]
) -> Response:
    await OpsService(session).ingest_metric(None, namespace, table, payload)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post(
    "/v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def ingest_metrics_with_prefix(
    prefix: str,
    namespace: str,
    table: str,
    session: Session,
    payload: Annotated[dict[str, Any], Body()],
) -> Response:
    await OpsService(session).ingest_metric(prefix, namespace, table, payload)
    return Response(status_code=status.HTTP_204_NO_CONTENT)
