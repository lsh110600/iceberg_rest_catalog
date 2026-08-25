"""Add operation safety, cancellation, idempotency and persistent bulk jobs."""

import sqlalchemy as sa
from sqlalchemy import inspect
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "20260825_04"
down_revision = "20260825_03"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)
    tables = set(inspector.get_table_names())

    if "bulk_jobs" not in tables:
        op.create_table(
            "bulk_jobs",
            sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
            sa.Column("kind", sa.String(40), nullable=False),
            sa.Column("state", sa.String(40), nullable=False, server_default="QUEUED"),
            sa.Column("idempotency_key", sa.String(200), unique=True),
            sa.Column("request_fingerprint", sa.String(64)),
            sa.Column(
                "catalog_id",
                postgresql.UUID(as_uuid=True),
                sa.ForeignKey("catalogs.id", ondelete="SET NULL"),
            ),
            sa.Column("namespace_name", sa.String(500)),
            sa.Column("command", sa.String(80)),
            sa.Column(
                "parameters",
                postgresql.JSONB(),
                nullable=False,
                server_default=sa.text("'{}'::jsonb"),
            ),
            sa.Column("requested_by", sa.String(200), nullable=False),
            sa.Column("total_items", sa.Integer(), nullable=False),
            sa.Column("completed_items", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("succeeded_items", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("failed_items", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("canceled_items", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("error_message", sa.Text()),
            sa.Column("started_at", sa.DateTime(timezone=True)),
            sa.Column("finished_at", sa.DateTime(timezone=True)),
            sa.Column("cancellation_requested_at", sa.DateTime(timezone=True)),
            sa.Column(
                "created_at",
                sa.DateTime(timezone=True),
                nullable=False,
                server_default=sa.func.now(),
            ),
            sa.Column(
                "updated_at",
                sa.DateTime(timezone=True),
                nullable=False,
                server_default=sa.func.now(),
            ),
        )
        op.create_index("ix_bulk_jobs_state", "bulk_jobs", ["state"])

    operation_columns = {
        column["name"] for column in inspect(bind).get_columns("operation_requests")
    }
    additions = [
        ("idempotency_key", sa.Column("idempotency_key", sa.String(200))),
        ("request_fingerprint", sa.Column("request_fingerprint", sa.String(64))),
        ("version", sa.Column("version", sa.Integer(), nullable=False, server_default="1")),
        (
            "cancellation_requested_at",
            sa.Column("cancellation_requested_at", sa.DateTime(timezone=True)),
        ),
        ("canceled_at", sa.Column("canceled_at", sa.DateTime(timezone=True))),
        (
            "plan",
            sa.Column(
                "plan",
                postgresql.JSONB(),
                nullable=False,
                server_default=sa.text("'{}'::jsonb"),
            ),
        ),
        (
            "bulk_job_id",
            sa.Column(
                "bulk_job_id",
                postgresql.UUID(as_uuid=True),
                sa.ForeignKey("bulk_jobs.id", ondelete="SET NULL"),
            ),
        ),
    ]
    for name, column in additions:
        if name not in operation_columns:
            op.add_column("operation_requests", column)
    indexes = {index["name"] for index in inspect(bind).get_indexes("operation_requests")}
    if "uq_operation_requests_idempotency_key" not in indexes:
        op.create_index(
            "uq_operation_requests_idempotency_key",
            "operation_requests",
            ["idempotency_key"],
            unique=True,
        )
    if "ix_operation_requests_bulk_job_id" not in indexes:
        op.create_index(
            "ix_operation_requests_bulk_job_id", "operation_requests", ["bulk_job_id"]
        )

    tables = set(inspect(bind).get_table_names())
    if "operation_table_locks" not in tables:
        op.create_table(
            "operation_table_locks",
            sa.Column(
                "table_id",
                postgresql.UUID(as_uuid=True),
                sa.ForeignKey("managed_tables.id", ondelete="CASCADE"),
                primary_key=True,
            ),
            sa.Column(
                "operation_request_id",
                postgresql.UUID(as_uuid=True),
                sa.ForeignKey("operation_requests.id", ondelete="CASCADE"),
                nullable=False,
                unique=True,
            ),
            sa.Column(
                "acquired_at",
                sa.DateTime(timezone=True),
                nullable=False,
                server_default=sa.func.now(),
            ),
        )

    if "bulk_job_items" not in tables:
        op.create_table(
            "bulk_job_items",
            sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
            sa.Column(
                "bulk_job_id",
                postgresql.UUID(as_uuid=True),
                sa.ForeignKey("bulk_jobs.id", ondelete="CASCADE"),
                nullable=False,
            ),
            sa.Column("ordinal", sa.Integer(), nullable=False),
            sa.Column("table_name", sa.String(500)),
            sa.Column(
                "table_id",
                postgresql.UUID(as_uuid=True),
                sa.ForeignKey("managed_tables.id", ondelete="SET NULL"),
            ),
            sa.Column(
                "operation_request_id",
                postgresql.UUID(as_uuid=True),
                sa.ForeignKey("operation_requests.id", ondelete="SET NULL"),
            ),
            sa.Column("state", sa.String(40), nullable=False, server_default="QUEUED"),
            sa.Column(
                "result",
                postgresql.JSONB(),
                nullable=False,
                server_default=sa.text("'{}'::jsonb"),
            ),
            sa.Column("error_message", sa.Text()),
            sa.Column("started_at", sa.DateTime(timezone=True)),
            sa.Column("finished_at", sa.DateTime(timezone=True)),
            sa.UniqueConstraint("bulk_job_id", "ordinal"),
        )
        op.create_index("ix_bulk_job_items_bulk_job_id", "bulk_job_items", ["bulk_job_id"])
        op.create_index("ix_bulk_job_items_state", "bulk_job_items", ["state"])


def downgrade() -> None:
    bind = op.get_bind()
    tables = set(inspect(bind).get_table_names())
    if "bulk_job_items" in tables:
        op.drop_table("bulk_job_items")
    if "operation_table_locks" in tables:
        op.drop_table("operation_table_locks")

    operation_columns = {
        column["name"] for column in inspect(bind).get_columns("operation_requests")
    }
    for name in [
        "bulk_job_id",
        "plan",
        "canceled_at",
        "cancellation_requested_at",
        "version",
        "request_fingerprint",
        "idempotency_key",
    ]:
        if name in operation_columns:
            op.drop_column("operation_requests", name)
    if "bulk_jobs" in tables:
        op.drop_table("bulk_jobs")
