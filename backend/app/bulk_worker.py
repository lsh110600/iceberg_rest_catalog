import asyncio
import logging
import uuid
from datetime import UTC, datetime, timedelta

from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app import models
from app.config import get_settings
from app.database import SessionFactory
from app.routers.playground import runner_request, sync_inspected_table
from app.services import table_summary

settings = get_settings()
logger = logging.getLogger("iceberg-ops-bulk-worker")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


async def recover_jobs() -> None:
    cutoff = datetime.now(UTC) - timedelta(seconds=settings.operation_worker_stale_seconds)
    async with SessionFactory() as session:
        jobs = list(
            await session.scalars(
                select(models.BulkJob)
                .options(selectinload(models.BulkJob.items))
                .where(
                    models.BulkJob.kind == "TABLE_INSPECT",
                    models.BulkJob.state == "RUNNING",
                    models.BulkJob.updated_at < cutoff,
                )
            )
        )
        for job in jobs:
            job.state = "QUEUED"
            job.started_at = None
            for item in job.items:
                if item.state == "RUNNING":
                    item.state = "QUEUED"
                    item.started_at = None
        if jobs:
            await session.commit()


async def claim_job() -> uuid.UUID | None:
    async with SessionFactory() as session:
        async with session.begin():
            job = await session.scalar(
                select(models.BulkJob)
                .where(models.BulkJob.kind == "TABLE_INSPECT", models.BulkJob.state == "QUEUED")
                .order_by(models.BulkJob.created_at)
                .with_for_update(skip_locked=True)
                .limit(1)
            )
            if job is None:
                return None
            job.state = "RUNNING"
            job.started_at = datetime.now(UTC)
            return job.id


async def process_item(job_id: uuid.UUID, item_id: uuid.UUID) -> None:
    async with SessionFactory() as session:
        job = await session.get(models.BulkJob, job_id)
        item = await session.get(models.BulkJobItem, item_id)
        if job is None or item is None or item.table_name is None:
            return
        if job.cancellation_requested_at is not None:
            item.state = "CANCELED"
            item.finished_at = datetime.now(UTC)
            await session.commit()
            return
        catalog = await session.get(models.Catalog, job.catalog_id)
        if catalog is None or catalog.warehouse is None or job.namespace_name is None:
            item.state = "FAILED"
            item.error_message = "Registered Catalog or Namespace is unavailable"
            item.finished_at = datetime.now(UTC)
            await session.commit()
            return
        item.state = "RUNNING"
        item.started_at = datetime.now(UTC)
        await session.commit()

        try:
            payload = await runner_request(
                "POST",
                "/tables/inspect",
                {
                    "catalog": catalog.name,
                    "warehouse": catalog.warehouse,
                    "namespace": job.namespace_name,
                    "table": item.table_name,
                },
            )
            await session.refresh(job)
            if job.cancellation_requested_at is not None:
                item.state = "CANCELED"
                item.finished_at = datetime.now(UTC)
                await session.commit()
                return
            table = await sync_inspected_table(
                session, catalog, job.namespace_name, item.table_name, payload
            )
            await session.commit()
            await session.refresh(table, attribute_names=["catalog", "storage_profile"])
            item.table_id = table.id
            item.result = table_summary(table).model_dump(mode="json", by_alias=True)
            item.state = "SUCCEEDED"
        except Exception as exc:
            logger.warning("Bulk table item failed: %s", item.table_name, exc_info=True)
            item.state = "FAILED"
            item.error_message = str(getattr(exc, "detail", exc))[:2000]
        item.finished_at = datetime.now(UTC)
        states = list(
            await session.scalars(
                select(models.BulkJobItem.state).where(
                    models.BulkJobItem.bulk_job_id == job.id
                )
            )
        )
        terminal = {"SUCCEEDED", "FAILED", "CANCELED"}
        job.completed_items = sum(state in terminal for state in states)
        job.succeeded_items = states.count("SUCCEEDED")
        job.failed_items = states.count("FAILED")
        job.canceled_items = states.count("CANCELED")
        await session.commit()


async def finish_job(job_id: uuid.UUID) -> None:
    async with SessionFactory() as session:
        job = await session.scalar(
            select(models.BulkJob)
            .options(selectinload(models.BulkJob.items))
            .where(models.BulkJob.id == job_id)
        )
        if job is None:
            return
        states = [item.state for item in job.items]
        job.completed_items = sum(
            state in {"SUCCEEDED", "FAILED", "CANCELED"} for state in states
        )
        job.succeeded_items = states.count("SUCCEEDED")
        job.failed_items = states.count("FAILED")
        job.canceled_items = states.count("CANCELED")
        if job.canceled_items == len(states):
            job.state = "CANCELED"
        elif job.succeeded_items == len(states):
            job.state = "SUCCEEDED"
        elif job.succeeded_items:
            job.state = "PARTIAL_SUCCESS"
        else:
            job.state = "FAILED"
        job.finished_at = datetime.now(UTC)
        await session.commit()


async def process_job(job_id: uuid.UUID) -> None:
    async with SessionFactory() as session:
        job = await session.scalar(
            select(models.BulkJob)
            .options(selectinload(models.BulkJob.items))
            .where(models.BulkJob.id == job_id)
        )
        if job is None:
            return
        item_ids = [item.id for item in job.items if item.state == "QUEUED"]
    for item_id in item_ids:
        await process_item(job_id, item_id)
    await finish_job(job_id)


async def run() -> None:
    await recover_jobs()
    while True:
        try:
            job_id = await claim_job()
            if job_id is None:
                await asyncio.sleep(settings.bulk_worker_poll_seconds)
                continue
            await process_job(job_id)
        except Exception:
            logger.exception("Bulk worker loop failed")
            await asyncio.sleep(settings.bulk_worker_poll_seconds)


if __name__ == "__main__":
    asyncio.run(run())
