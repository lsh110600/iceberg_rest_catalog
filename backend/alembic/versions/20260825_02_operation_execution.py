"""Add Spark operation execution history and worker heartbeat."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect
from sqlalchemy.dialects import postgresql

revision = "20260825_02"
down_revision = "20260824_01"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)
    operation_columns = {column["name"] for column in inspector.get_columns("operation_requests")}
    if "planned_snapshot_id" not in operation_columns:
        op.add_column("operation_requests", sa.Column("planned_snapshot_id", sa.BigInteger()))

    existing_tables = set(inspector.get_table_names())
    if "operation_executions" not in existing_tables:
        op.create_table(
            "operation_executions",
            sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
            sa.Column(
                "operation_request_id",
                postgresql.UUID(as_uuid=True),
                sa.ForeignKey("operation_requests.id", ondelete="CASCADE"),
                nullable=False,
            ),
            sa.Column("attempt", sa.Integer(), nullable=False),
            sa.Column("engine_type", sa.String(20), nullable=False),
            sa.Column(
                "engine_instance_id",
                postgresql.UUID(as_uuid=True),
                sa.ForeignKey("engine_instances.id", ondelete="SET NULL"),
            ),
            sa.Column("state", sa.String(40), nullable=False),
            sa.Column("worker_id", sa.String(200)),
            sa.Column("external_id", sa.String(200)),
            sa.Column("planned_snapshot_id", sa.BigInteger()),
            sa.Column("before_snapshot_id", sa.BigInteger()),
            sa.Column("after_snapshot_id", sa.BigInteger()),
            sa.Column("result", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
            sa.Column("error_message", sa.Text()),
            sa.Column("queued_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
            sa.Column("started_at", sa.DateTime(timezone=True)),
            sa.Column("finished_at", sa.DateTime(timezone=True)),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
            sa.UniqueConstraint("operation_request_id", "attempt"),
        )
        op.create_index(
            "ix_operation_executions_operation_request_id",
            "operation_executions",
            ["operation_request_id"],
        )

    if "operation_events" not in existing_tables:
        op.create_table(
            "operation_events",
            sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
            sa.Column(
                "execution_id",
                postgresql.UUID(as_uuid=True),
                sa.ForeignKey("operation_executions.id", ondelete="CASCADE"),
                nullable=False,
            ),
            sa.Column("level", sa.String(20), nullable=False),
            sa.Column("event_type", sa.String(80), nullable=False),
            sa.Column("message", sa.String(1000), nullable=False),
            sa.Column("details", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
            sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        )
        op.create_index("ix_operation_events_execution_id", "operation_events", ["execution_id"])

    if "worker_heartbeats" not in existing_tables:
        op.create_table(
            "worker_heartbeats",
            sa.Column("id", sa.String(80), primary_key=True),
            sa.Column("worker_id", sa.String(200), nullable=False),
            sa.Column("engine_type", sa.String(20), nullable=False),
            sa.Column("status", sa.String(40), nullable=False),
            sa.Column("current_operation_request_id", postgresql.UUID(as_uuid=True)),
            sa.Column("details", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
            sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False),
        )


def downgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)
    existing_tables = set(inspector.get_table_names())
    if "worker_heartbeats" in existing_tables:
        op.drop_table("worker_heartbeats")
    if "operation_events" in existing_tables:
        op.drop_index("ix_operation_events_execution_id", table_name="operation_events")
        op.drop_table("operation_events")
    if "operation_executions" in existing_tables:
        op.drop_index("ix_operation_executions_operation_request_id", table_name="operation_executions")
        op.drop_table("operation_executions")
    columns = {column["name"] for column in inspect(bind).get_columns("operation_requests")}
    if "planned_snapshot_id" in columns:
        op.drop_column("operation_requests", "planned_snapshot_id")
