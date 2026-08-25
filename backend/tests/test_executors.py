import base64
import json

import httpx
import pytest

from app.config import Settings
from app.executors import RESULT_MARKER, LivyBatchExecutor, OperationCanceled


@pytest.mark.asyncio
async def test_livy_executor_submits_polls_and_reads_result_marker() -> None:
    expected_result = {
        "before": {"snapshot_id": 10, "total_records": 8},
        "after": {"snapshot_id": 11, "total_records": 8},
        "output": [],
    }
    marker = RESULT_MARKER + base64.b64encode(json.dumps(expected_result).encode()).decode()
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.method == "POST" and request.url.path == "/batches":
            return httpx.Response(201, json={"id": 42, "state": "starting", "appId": None})
        if request.method == "GET" and request.url.path == "/batches/42":
            return httpx.Response(200, json={"id": 42, "state": "success", "appId": "app-7"})
        if request.method == "GET" and request.url.path == "/batches/42/log":
            return httpx.Response(200, json={"id": 42, "from": 0, "size": 1, "log": [marker]})
        raise AssertionError(f"Unexpected request: {request.method} {request.url}")

    settings = Settings(
        _env_file=None,
        spark_execution_mode="livy",
        livy_url="https://livy.internal:8998",
        livy_job_file="hdfs:///apps/iceberg-ops/iceberg_maintenance.py",
        livy_poll_seconds=0,
        livy_conf_json='{"spark.sql.catalog.production.type":"rest"}',
    )
    executor = LivyBatchExecutor(settings, transport=httpx.MockTransport(handler))
    submissions: list[tuple[str, dict]] = []

    async def submitted(external_id: str, details: dict) -> None:
        submissions.append((external_id, details))

    result = await executor.execute(
        {
            "operation_id": "operation-1",
            "catalog": "production",
            "namespace": "events",
            "table": "clickstream",
            "command": "REWRITE_DATA_FILES",
            "parameters": {"rewriteAll": True},
            "expected_snapshot_id": 10,
        },
        submitted,
    )

    assert submissions == [("livy-batch:42", {"batchId": 42, "appId": None, "state": "starting"})]
    assert result["before"]["snapshot_id"] == 10
    assert result["livy"]["batch_id"] == 42
    submission = json.loads(requests[0].content)
    assert submission["file"] == "hdfs:///apps/iceberg-ops/iceberg_maintenance.py"
    assert submission["conf"] == {"spark.sql.catalog.production.type": "rest"}
    assert "--expected-snapshot-id" in submission["args"]


def test_livy_result_parser_rejects_missing_marker() -> None:
    with pytest.raises(RuntimeError, match="without an Iceberg Ops result marker"):
        LivyBatchExecutor.result_from_logs(["ordinary Spark output"])


def test_empty_optional_livy_environment_values_are_ignored() -> None:
    settings = Settings(
        _env_file=None,
        livy_queue="",
        livy_driver_memory="",
        livy_num_executors="",
    )

    assert settings.livy_queue is None
    assert settings.livy_driver_memory is None
    assert settings.livy_num_executors is None


def test_livy_executor_adds_registered_hadoop_catalog_configuration() -> None:
    executor = LivyBatchExecutor(Settings(_env_file=None, livy_conf_json="{}"))

    submission = executor.batch_payload(
        {
            "operation_id": "operation-2",
            "catalog": "existing_lake",
            "catalog_type": "HADOOP",
            "warehouse": "hdfs://namenode:8020/existing",
            "namespace": "analytics",
            "table": "events",
            "command": "REWRITE_MANIFESTS",
            "parameters": {},
        }
    )

    assert submission["conf"] == {
        "spark.sql.catalog.existing_lake": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.existing_lake.type": "hadoop",
        "spark.sql.catalog.existing_lake.warehouse": "hdfs://namenode:8020/existing",
    }


@pytest.mark.asyncio
async def test_livy_executor_cancels_running_batch() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.method == "POST":
            return httpx.Response(201, json={"id": 9, "state": "running"})
        if request.method == "DELETE":
            return httpx.Response(200, json={"id": 9, "state": "killed"})
        raise AssertionError(f"Unexpected request: {request.method} {request.url}")

    executor = LivyBatchExecutor(
        Settings(_env_file=None, livy_poll_seconds=0),
        transport=httpx.MockTransport(handler),
    )

    async def submitted(_external_id: str, _details: dict) -> None:
        pass

    async def canceled() -> bool:
        return True

    with pytest.raises(OperationCanceled):
        await executor.execute(
            {
                "operation_id": "operation-3",
                "catalog": "production",
                "namespace": "events",
                "table": "clickstream",
                "command": "REWRITE_MANIFESTS",
                "parameters": {},
            },
            submitted,
            canceled,
        )
    assert any(request.method == "DELETE" for request in requests)


@pytest.mark.asyncio
async def test_livy_executor_resumes_existing_batch() -> None:
    result = {"before": {"snapshot_id": 1}, "after": {"snapshot_id": 2}}
    marker = RESULT_MARKER + base64.b64encode(json.dumps(result).encode()).decode()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/batches/77":
            return httpx.Response(200, json={"id": 77, "state": "success"})
        if request.url.path == "/batches/77/log":
            return httpx.Response(200, json={"log": [marker]})
        raise AssertionError(f"Unexpected request: {request.method} {request.url}")

    executor = LivyBatchExecutor(
        Settings(_env_file=None), transport=httpx.MockTransport(handler)
    )
    resumed = await executor.resume({}, "livy-batch:77")
    assert resumed["livy"]["batch_id"] == 77
