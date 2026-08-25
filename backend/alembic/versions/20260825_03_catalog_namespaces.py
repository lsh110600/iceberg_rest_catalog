"""Add reusable namespaces for registered catalogs."""

import sqlalchemy as sa
from sqlalchemy import inspect
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "20260825_03"
down_revision = "20260825_02"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    if "catalog_namespaces" in inspect(bind).get_table_names():
        return
    op.create_table(
        "catalog_namespaces",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "catalog_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("catalogs.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("name", sa.String(500), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("catalog_id", "name"),
    )
    op.create_index("ix_catalog_namespaces_catalog_id", "catalog_namespaces", ["catalog_id"])


def downgrade() -> None:
    bind = op.get_bind()
    if "catalog_namespaces" not in inspect(bind).get_table_names():
        return
    op.drop_index("ix_catalog_namespaces_catalog_id", table_name="catalog_namespaces")
    op.drop_table("catalog_namespaces")
