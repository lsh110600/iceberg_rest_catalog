from contextlib import asynccontextmanager
from datetime import UTC, datetime

from fastapi import FastAPI, Request, Response, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from sqlalchemy import text

from app.config import get_settings
from app.database import SessionFactory
from app.demo import seed_demo_data
from app.routers import metrics, ops, playground
from app.schemas import ErrorResponse
from app.services import InvalidOperationError, NotFoundError

settings = get_settings()


@asynccontextmanager
async def lifespan(_: FastAPI):
    if settings.demo_data or settings.playground_enabled:
        async with SessionFactory() as session:
            if settings.demo_data:
                await seed_demo_data(session)
            if settings.playground_enabled:
                await playground.ensure_playground_registry(session)
    yield


app = FastAPI(
    title=settings.app_name,
    version="0.1.0",
    description="Control plane API for monitoring and operating Apache Iceberg tables.",
    lifespan=lifespan,
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)
app.include_router(ops.router)
app.include_router(metrics.router)
app.include_router(playground.router)


@app.get("/healthz", tags=["system"])
async def health() -> dict[str, str]:
    async with SessionFactory() as session:
        await session.execute(text("SELECT 1"))
    return {"status": "healthy", "service": settings.app_name}


@app.get("/metrics", include_in_schema=False)
async def prometheus_metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.exception_handler(NotFoundError)
async def not_found(_: Request, exception: NotFoundError) -> JSONResponse:
    body = ErrorResponse(code="NOT_FOUND", message=str(exception), timestamp=datetime.now(UTC))
    return JSONResponse(
        status_code=status.HTTP_404_NOT_FOUND, content=body.model_dump(mode="json", by_alias=True)
    )


@app.exception_handler(InvalidOperationError)
async def invalid_operation(_: Request, exception: InvalidOperationError) -> JSONResponse:
    body = ErrorResponse(code="BAD_REQUEST", message=str(exception), timestamp=datetime.now(UTC))
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST, content=body.model_dump(mode="json", by_alias=True)
    )
