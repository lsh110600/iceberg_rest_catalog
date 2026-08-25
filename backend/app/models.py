import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import (
    BigInteger,
    DateTime,
    ForeignKey,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class TimestampMixin:
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )


class Catalog(TimestampMixin, Base):
    __tablename__ = "catalogs"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(120), unique=True)
    catalog_type: Mapped[str] = mapped_column(String(40), default="REST")
    endpoint: Mapped[str | None] = mapped_column(Text)
    warehouse: Mapped[str | None] = mapped_column(Text)
    health: Mapped[str] = mapped_column(String(20), default="UNKNOWN")
    last_synced_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class CatalogNamespace(TimestampMixin, Base):
    __tablename__ = "catalog_namespaces"
    __table_args__ = (UniqueConstraint("catalog_id", "name"),)

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    catalog_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("catalogs.id", ondelete="CASCADE"), index=True
    )
    name: Mapped[str] = mapped_column(String(500))

    catalog: Mapped[Catalog] = relationship(lazy="joined")


class StorageProfile(TimestampMixin, Base):
    __tablename__ = "storage_profiles"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(120), unique=True)
    storage_type: Mapped[str] = mapped_column(String(20))
    uri_prefix: Mapped[str] = mapped_column(Text)
    file_io_impl: Mapped[str] = mapped_column(Text)
    config_ref: Mapped[str | None] = mapped_column(Text)
    credential_ref: Mapped[str | None] = mapped_column(Text)
    region: Mapped[str | None] = mapped_column(String(80))
    encryption_policy: Mapped[str | None] = mapped_column(String(120))
    health: Mapped[str] = mapped_column(String(20), default="UNKNOWN")


class EngineInstance(TimestampMixin, Base):
    __tablename__ = "engine_instances"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(120), unique=True)
    engine_type: Mapped[str] = mapped_column(String(20))
    version: Mapped[str | None] = mapped_column(String(80))
    endpoint: Mapped[str | None] = mapped_column(Text)
    health: Mapped[str] = mapped_column(String(20), default="UNKNOWN")
    capabilities: Mapped[list[str]] = mapped_column(JSONB, default=list)
    last_checked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class EngineStorageAccess(Base):
    __tablename__ = "engine_storage_access"

    engine_instance_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("engine_instances.id", ondelete="CASCADE"), primary_key=True
    )
    storage_profile_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("storage_profiles.id", ondelete="CASCADE"), primary_key=True
    )
    health: Mapped[str] = mapped_column(String(20), default="UNKNOWN")
    last_checked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class ManagedTable(TimestampMixin, Base):
    __tablename__ = "managed_tables"
    __table_args__ = (UniqueConstraint("catalog_id", "namespace_name", "table_name"),)

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    catalog_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("catalogs.id"))
    storage_profile_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("storage_profiles.id")
    )
    table_uuid: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))
    namespace_name: Mapped[str] = mapped_column(String(500))
    table_name: Mapped[str] = mapped_column(String(500))
    owner_name: Mapped[str | None] = mapped_column(String(200))
    slo_profile: Mapped[str] = mapped_column(String(80), default="batch-daily")
    format_version: Mapped[int | None] = mapped_column(Integer)
    metadata_location: Mapped[str | None] = mapped_column(Text)
    current_snapshot_id: Mapped[int | None] = mapped_column(BigInteger)
    last_commit_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    total_bytes: Mapped[int] = mapped_column(BigInteger, default=0)
    total_records: Mapped[int] = mapped_column(BigInteger, default=0)
    total_files: Mapped[int] = mapped_column(BigInteger, default=0)
    small_file_ratio: Mapped[Decimal | None] = mapped_column(Numeric(8, 5))
    delete_file_ratio: Mapped[Decimal | None] = mapped_column(Numeric(8, 5))
    snapshot_count: Mapped[int] = mapped_column(Integer, default=0)
    health: Mapped[str] = mapped_column(String(20), default="UNKNOWN")
    observed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    properties: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)

    catalog: Mapped[Catalog] = relationship(lazy="joined")
    storage_profile: Mapped[StorageProfile | None] = relationship(lazy="joined")
    findings: Mapped[list["Finding"]] = relationship(
        back_populates="table", cascade="all, delete-orphan", lazy="selectin"
    )


class Finding(Base):
    __tablename__ = "findings"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    table_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("managed_tables.id", ondelete="CASCADE")
    )
    rule_code: Mapped[str] = mapped_column(String(120))
    severity: Mapped[str] = mapped_column(String(20))
    title: Mapped[str] = mapped_column(String(300))
    evidence: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)
    recommendation: Mapped[str | None] = mapped_column(String(500))
    status: Mapped[str] = mapped_column(String(20), default="OPEN")
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    table: Mapped[ManagedTable] = relationship(back_populates="findings")


class MetricReport(Base):
    __tablename__ = "metric_reports"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    table_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("managed_tables.id", ondelete="SET NULL")
    )
    catalog_prefix: Mapped[str | None] = mapped_column(String(300))
    namespace_name: Mapped[str] = mapped_column(String(500))
    table_name: Mapped[str] = mapped_column(String(500))
    report_type: Mapped[str] = mapped_column(String(80))
    snapshot_id: Mapped[int | None] = mapped_column(BigInteger)
    engine_type: Mapped[str | None] = mapped_column(String(20))
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class MetricPoint(Base):
    __tablename__ = "metric_points"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    table_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("managed_tables.id", ondelete="CASCADE")
    )
    metric_name: Mapped[str] = mapped_column(String(160))
    metric_value: Mapped[float]
    engine_type: Mapped[str | None] = mapped_column(String(20))
    labels: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class OperationRequest(TimestampMixin, Base):
    __tablename__ = "operation_requests"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    table_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("managed_tables.id"))
    command: Mapped[str] = mapped_column(String(80))
    preferred_engine: Mapped[str] = mapped_column(String(20), default="AUTO")
    parameters: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)
    reason: Mapped[str] = mapped_column(String(1000))
    requester: Mapped[str] = mapped_column(String(200))
    risk: Mapped[str] = mapped_column(String(20))
    state: Mapped[str] = mapped_column(String(40))
    planned_snapshot_id: Mapped[int | None] = mapped_column(BigInteger)
    idempotency_key: Mapped[str | None] = mapped_column(String(200), unique=True)
    request_fingerprint: Mapped[str | None] = mapped_column(String(64))
    version: Mapped[int] = mapped_column(Integer, default=1)
    cancellation_requested_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    canceled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    plan: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)
    bulk_job_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("bulk_jobs.id", ondelete="SET NULL"), index=True
    )

    table: Mapped[ManagedTable] = relationship(lazy="joined")
    executions: Mapped[list["OperationExecution"]] = relationship(
        back_populates="operation_request",
        cascade="all, delete-orphan",
        lazy="selectin",
        order_by="OperationExecution.attempt.desc()",
    )


class OperationTableLock(Base):
    __tablename__ = "operation_table_locks"

    table_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("managed_tables.id", ondelete="CASCADE"), primary_key=True
    )
    operation_request_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("operation_requests.id", ondelete="CASCADE"),
        unique=True,
    )
    acquired_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class Approval(Base):
    __tablename__ = "approvals"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    operation_request_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("operation_requests.id", ondelete="CASCADE")
    )
    approver: Mapped[str] = mapped_column(String(200))
    decision: Mapped[str] = mapped_column(String(20))
    comment: Mapped[str | None] = mapped_column(String(1000))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class OperationExecution(Base):
    __tablename__ = "operation_executions"
    __table_args__ = (UniqueConstraint("operation_request_id", "attempt"),)

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    operation_request_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("operation_requests.id", ondelete="CASCADE"), index=True
    )
    attempt: Mapped[int] = mapped_column(Integer)
    engine_type: Mapped[str] = mapped_column(String(20), default="SPARK")
    engine_instance_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("engine_instances.id", ondelete="SET NULL")
    )
    state: Mapped[str] = mapped_column(String(40))
    worker_id: Mapped[str | None] = mapped_column(String(200))
    external_id: Mapped[str | None] = mapped_column(String(200))
    planned_snapshot_id: Mapped[int | None] = mapped_column(BigInteger)
    before_snapshot_id: Mapped[int | None] = mapped_column(BigInteger)
    after_snapshot_id: Mapped[int | None] = mapped_column(BigInteger)
    result: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)
    error_message: Mapped[str | None] = mapped_column(Text)
    queued_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    operation_request: Mapped[OperationRequest] = relationship(back_populates="executions")
    events: Mapped[list["OperationEvent"]] = relationship(
        back_populates="execution",
        cascade="all, delete-orphan",
        lazy="selectin",
        order_by="OperationEvent.occurred_at.asc()",
    )


class OperationEvent(Base):
    __tablename__ = "operation_events"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    execution_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("operation_executions.id", ondelete="CASCADE"), index=True
    )
    level: Mapped[str] = mapped_column(String(20), default="INFO")
    event_type: Mapped[str] = mapped_column(String(80))
    message: Mapped[str] = mapped_column(String(1000))
    details: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    execution: Mapped[OperationExecution] = relationship(back_populates="events")


class WorkerHeartbeat(Base):
    __tablename__ = "worker_heartbeats"

    id: Mapped[str] = mapped_column(String(80), primary_key=True)
    worker_id: Mapped[str] = mapped_column(String(200))
    engine_type: Mapped[str] = mapped_column(String(20))
    status: Mapped[str] = mapped_column(String(40))
    current_operation_request_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))
    details: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class AuditEvent(Base):
    __tablename__ = "audit_events"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    actor: Mapped[str] = mapped_column(String(200))
    action: Mapped[str] = mapped_column(String(120))
    resource_type: Mapped[str] = mapped_column(String(80))
    resource_id: Mapped[str] = mapped_column(String(200))
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class BulkJob(TimestampMixin, Base):
    __tablename__ = "bulk_jobs"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    kind: Mapped[str] = mapped_column(String(40))
    state: Mapped[str] = mapped_column(String(40), default="QUEUED", index=True)
    idempotency_key: Mapped[str | None] = mapped_column(String(200), unique=True)
    request_fingerprint: Mapped[str | None] = mapped_column(String(64))
    catalog_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("catalogs.id", ondelete="SET NULL")
    )
    namespace_name: Mapped[str | None] = mapped_column(String(500))
    command: Mapped[str | None] = mapped_column(String(80))
    parameters: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)
    requested_by: Mapped[str] = mapped_column(String(200))
    total_items: Mapped[int] = mapped_column(Integer)
    completed_items: Mapped[int] = mapped_column(Integer, default=0)
    succeeded_items: Mapped[int] = mapped_column(Integer, default=0)
    failed_items: Mapped[int] = mapped_column(Integer, default=0)
    canceled_items: Mapped[int] = mapped_column(Integer, default=0)
    error_message: Mapped[str | None] = mapped_column(Text)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    cancellation_requested_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    items: Mapped[list["BulkJobItem"]] = relationship(
        back_populates="bulk_job",
        cascade="all, delete-orphan",
        lazy="selectin",
        order_by="BulkJobItem.ordinal.asc()",
    )


class BulkJobItem(Base):
    __tablename__ = "bulk_job_items"
    __table_args__ = (UniqueConstraint("bulk_job_id", "ordinal"),)

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    bulk_job_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("bulk_jobs.id", ondelete="CASCADE"), index=True
    )
    ordinal: Mapped[int] = mapped_column(Integer)
    table_name: Mapped[str | None] = mapped_column(String(500))
    table_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("managed_tables.id", ondelete="SET NULL")
    )
    operation_request_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("operation_requests.id", ondelete="SET NULL")
    )
    state: Mapped[str] = mapped_column(String(40), default="QUEUED", index=True)
    result: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)
    error_message: Mapped[str | None] = mapped_column(Text)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    bulk_job: Mapped[BulkJob] = relationship(back_populates="items")
