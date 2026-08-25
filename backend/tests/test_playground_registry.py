import uuid

import pytest
from pydantic import ValidationError

from app.schemas import (
    OperationRequestBulkCreate,
    PlaygroundCatalogCreate,
    PlaygroundNamespaceCreate,
    PlaygroundTableBulkRegister,
    PlaygroundTableRegister,
    TableSummary,
)


def test_catalog_registration_normalizes_hdfs_warehouse() -> None:
    registration = PlaygroundCatalogCreate(
        name="existing_lake",
        warehouse="hdfs://namenode:8020/existing/",
    )

    assert registration.warehouse == "hdfs://namenode:8020/existing"


def test_catalog_registration_rejects_non_hdfs_warehouse() -> None:
    with pytest.raises(ValidationError, match="hdfs"):
        PlaygroundCatalogCreate(name="existing_lake", warehouse="s3://bucket/warehouse")


def test_namespace_registration_accepts_nested_namespace() -> None:
    registration = PlaygroundNamespaceCreate(
        catalog_name="existing_lake",
        name="analytics.daily",
    )

    assert registration.name == "analytics.daily"


def test_namespace_registration_rejects_sql_identifier_injection() -> None:
    with pytest.raises(ValidationError):
        PlaygroundNamespaceCreate(
            catalog_name="existing_lake",
            name="analytics; DROP TABLE x",
        )


def test_operation_table_registration_accepts_variable_target() -> None:
    registration = PlaygroundTableRegister(
        catalog_name="existing_lake",
        namespace_name="analytics.daily",
        table_name="events",
    )

    assert registration.model_dump() == {
        "catalog_name": "existing_lake",
        "namespace_name": "analytics.daily",
        "table_name": "events",
    }


def test_bulk_table_registration_deduplicates_targets() -> None:
    registration = PlaygroundTableBulkRegister(
        catalog_name="existing_lake",
        namespace_name="analytics",
        table_names=["events", "customers", "events"],
    )

    assert registration.table_names == ["events", "customers"]


def test_bulk_table_registration_rejects_invalid_identifier() -> None:
    with pytest.raises(ValidationError, match="unsupported table identifier"):
        PlaygroundTableBulkRegister(
            catalog_name="existing_lake",
            namespace_name="analytics",
            table_names=["events", "bad-table"],
        )


def test_bulk_operation_request_deduplicates_table_ids() -> None:
    first = uuid.uuid4()
    second = uuid.uuid4()
    request = OperationRequestBulkCreate(
        table_ids=[first, second, first],
        command="COMPUTE_STATS",
        preferred_engine="SPARK",
        reason="refresh table statistics",
        requester="test.operator",
    )

    assert request.table_ids == [first, second]


def test_table_summary_serializes_snapshot_id_without_javascript_precision_loss() -> None:
    summary = TableSummary(
        id=uuid.uuid4(),
        catalog_name="existing_lake",
        namespace_name="analytics",
        table_name="events",
        slo_profile="operator-managed",
        current_snapshot_id=5_893_406_121_893_130_298,
        total_bytes=1,
        total_records=1,
        total_files=1,
        snapshot_count=1,
        health="HEALTHY",
    )

    assert summary.model_dump(mode="json")["current_snapshot_id"] == "5893406121893130298"
