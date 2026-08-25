from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException
from pydantic import ValidationError

from app.main import (
    OperationExecuteRequest,
    TableBulkInspectRequest,
    configure_hadoop_catalog,
    json_value,
    procedure_sql,
)


def test_configure_hadoop_catalog_sets_spark_catalog_options() -> None:
    spark = MagicMock()
    spark.conf.get.return_value = None

    configure_hadoop_catalog(spark, "existing_lake", "hdfs://namenode:8020/existing/")

    spark.conf.set.assert_any_call(
        "spark.sql.catalog.existing_lake", "org.apache.iceberg.spark.SparkCatalog"
    )
    spark.conf.set.assert_any_call("spark.sql.catalog.existing_lake.type", "hadoop")
    spark.conf.set.assert_any_call(
        "spark.sql.catalog.existing_lake.warehouse", "hdfs://namenode:8020/existing"
    )


def test_configure_hadoop_catalog_rejects_non_hdfs_warehouse() -> None:
    spark = MagicMock()

    with pytest.raises(HTTPException, match="hdfs"):
        configure_hadoop_catalog(spark, "existing_lake", "s3://bucket/warehouse")


def test_configure_hadoop_catalog_rejects_changed_active_warehouse() -> None:
    spark = MagicMock()
    spark.conf.get.return_value = "hdfs://namenode:8020/first"

    with pytest.raises(HTTPException) as error:
        configure_hadoop_catalog(spark, "existing_lake", "hdfs://namenode:8020/second")

    assert error.value.status_code == 409


def test_json_value_preserves_large_iceberg_identifiers() -> None:
    snapshot_id = 5_893_406_121_893_130_298

    assert json_value(snapshot_id) == "5893406121893130298"
    assert json_value(42) == 42


def test_procedure_sql_accepts_registered_catalog_table_target() -> None:
    request = OperationExecuteRequest(
        operation_id="operation-1",
        catalog="existing_lake",
        namespace="analytics.daily",
        table="events",
        command="REWRITE_MANIFESTS",
    )

    assert procedure_sql(request).startswith(
        "CALL existing_lake.system.rewrite_manifests(table => 'analytics.daily.events'"
    )


def test_bulk_table_inspection_caps_request_size() -> None:
    with pytest.raises(ValidationError):
        TableBulkInspectRequest(
            catalog="existing_lake",
            warehouse="hdfs://namenode:8020/existing",
            namespace="analytics",
            tables=[f"table_{index}" for index in range(201)],
        )
