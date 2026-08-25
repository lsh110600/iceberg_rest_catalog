import asyncio
import base64
import json
import re
import ssl
import time
from collections.abc import Awaitable, Callable
from typing import Any, Protocol

import httpx

from app.config import Settings

SubmittedCallback = Callable[[str, dict[str, Any]], Awaitable[None]]
CancellationCallback = Callable[[], Awaitable[bool]]
RESULT_MARKER = "ICEBERG_OPS_RESULT="
LIVY_SUCCESS_STATES = {"success"}
LIVY_FAILURE_STATES = {"dead", "error", "killed"}


class OperationCanceled(RuntimeError):
    pass


class SparkOperationExecutor(Protocol):
    async def execute(
        self,
        payload: dict[str, Any],
        on_submitted: SubmittedCallback,
        should_cancel: CancellationCallback | None = None,
    ) -> dict[str, Any]: ...

    async def resume(
        self,
        payload: dict[str, Any],
        external_id: str,
        should_cancel: CancellationCallback | None = None,
    ) -> dict[str, Any]: ...

    def heartbeat_details(self) -> dict[str, Any]: ...

    async def probe(self) -> dict[str, Any]: ...


def response_error(response: httpx.Response, service: str) -> str:
    try:
        payload = response.json()
        if isinstance(payload, dict):
            detail = payload.get("detail") or payload.get("msg")
            if detail:
                return str(detail)
    except ValueError:
        pass
    return f"{service} returned HTTP {response.status_code}: {response.text[:1000]}"


class PlaygroundSparkExecutor:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def heartbeat_details(self) -> dict[str, Any]:
        return {
            "executionMode": "playground",
            "catalogScope": self.settings.spark_catalog_scope,
            "endpoint": self.settings.playground_runner_url,
            "pollSeconds": self.settings.operation_worker_poll_seconds,
        }

    async def probe(self) -> dict[str, Any]:
        timeout = httpx.Timeout(10, connect=5)
        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.get(f"{self.settings.playground_runner_url}/healthz")
        except httpx.HTTPError as exc:
            raise RuntimeError(f"Spark runner is unavailable: {exc.__class__.__name__}") from exc
        if response.is_error:
            raise RuntimeError(response_error(response, "Spark runner"))
        return {**self.heartbeat_details(), "engineHealth": "HEALTHY"}

    async def execute(
        self,
        payload: dict[str, Any],
        on_submitted: SubmittedCallback,
        should_cancel: CancellationCallback | None = None,
    ) -> dict[str, Any]:
        if should_cancel and await should_cancel():
            raise OperationCanceled("Operation was canceled before Spark submission")
        await on_submitted(str(payload["operation_id"]), {"executionMode": "playground"})
        timeout = httpx.Timeout(self.settings.operation_worker_timeout_seconds, connect=10)
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.post(
                f"{self.settings.playground_runner_url}/operations/execute",
                json=payload,
                headers={"X-Playground-Token": self.settings.playground_runner_token},
            )
        if response.is_error:
            raise RuntimeError(response_error(response, "Spark runner"))
        result = response.json()
        if not isinstance(result, dict):
            raise RuntimeError("Spark runner returned an invalid result.")
        if should_cancel and await should_cancel():
            raise OperationCanceled("Operation cancellation was requested during execution")
        return result

    async def resume(
        self,
        payload: dict[str, Any],
        external_id: str,
        should_cancel: CancellationCallback | None = None,
    ) -> dict[str, Any]:
        raise RuntimeError(f"Playground execution cannot be resumed: {external_id}")


class LivyBatchExecutor:
    def __init__(
        self,
        settings: Settings,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.settings = settings
        self.transport = transport

    def heartbeat_details(self) -> dict[str, Any]:
        return {
            "executionMode": "livy",
            "catalogScope": self.settings.spark_catalog_scope,
            "endpoint": self.settings.livy_url,
            "engineName": self.settings.spark_engine_name,
            "queue": self.settings.livy_queue,
            "pollSeconds": self.settings.livy_poll_seconds,
            "jobFile": self.settings.livy_job_file,
        }

    def client_options(self) -> dict[str, Any]:
        headers = {"Content-Type": "application/json"}
        auth = None
        if self.settings.livy_bearer_token:
            headers["Authorization"] = f"Bearer {self.settings.livy_bearer_token}"
        elif self.settings.livy_username:
            auth = httpx.BasicAuth(
                self.settings.livy_username,
                self.settings.livy_password or "",
            )

        verify: bool | ssl.SSLContext = self.settings.livy_verify_tls
        if self.settings.livy_ca_bundle:
            verify = ssl.create_default_context(cafile=self.settings.livy_ca_bundle)
        return {
            "base_url": self.settings.livy_url.rstrip("/"),
            "headers": headers,
            "auth": auth,
            "verify": verify,
            "transport": self.transport,
            "timeout": httpx.Timeout(self.settings.livy_request_timeout_seconds, connect=10),
        }

    async def probe(self) -> dict[str, Any]:
        async with httpx.AsyncClient(**self.client_options()) as client:
            response = await self.request(
                client,
                "GET",
                "/batches",
                params={"from": 0, "size": 1},
            )
            payload = response.json()
            if not isinstance(payload, dict) or not isinstance(payload.get("sessions", []), list):
                raise RuntimeError("Livy returned an invalid batches response.")
        return {**self.heartbeat_details(), "engineHealth": "HEALTHY"}

    def batch_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        arguments = [
            "--operation-id",
            str(payload["operation_id"]),
            "--catalog",
            str(payload["catalog"]),
            "--namespace",
            str(payload["namespace"]),
            "--table",
            str(payload["table"]),
            "--command",
            str(payload["command"]),
            "--parameters-json",
            json.dumps(payload.get("parameters", {}), separators=(",", ":"), sort_keys=True),
        ]
        if payload.get("expected_snapshot_id") is not None:
            arguments.extend(["--expected-snapshot-id", str(payload["expected_snapshot_id"])])

        conf = dict(self.settings.livy_conf)
        if payload.get("catalog_type") == "HADOOP" and payload.get("warehouse"):
            catalog = str(payload["catalog"])
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", catalog):
                raise RuntimeError(f"Unsupported Spark catalog identifier: {catalog}")
            prefix = f"spark.sql.catalog.{catalog}"
            conf[prefix] = "org.apache.iceberg.spark.SparkCatalog"
            conf[f"{prefix}.type"] = "hadoop"
            conf[f"{prefix}.warehouse"] = str(payload["warehouse"])

        batch: dict[str, Any] = {
            "file": self.settings.livy_job_file,
            "args": arguments,
            "name": f"iceberg-ops-{payload['operation_id']}",
            "conf": conf,
        }
        optional = {
            "proxyUser": self.settings.livy_proxy_user,
            "queue": self.settings.livy_queue,
            "driverMemory": self.settings.livy_driver_memory,
            "driverCores": self.settings.livy_driver_cores,
            "executorMemory": self.settings.livy_executor_memory,
            "executorCores": self.settings.livy_executor_cores,
            "numExecutors": self.settings.livy_num_executors,
        }
        batch.update({key: value for key, value in optional.items() if value is not None})
        return batch

    async def request(
        self,
        client: httpx.AsyncClient,
        method: str,
        path: str,
        **kwargs: Any,
    ) -> httpx.Response:
        try:
            response = await client.request(method, path, **kwargs)
        except httpx.HTTPError as exc:
            raise RuntimeError(f"Livy is unavailable: {exc.__class__.__name__}") from exc
        if response.is_error:
            raise RuntimeError(response_error(response, "Livy"))
        return response

    async def logs(self, client: httpx.AsyncClient, batch_id: int) -> list[str]:
        lines: list[str] = []
        offset = 0
        page_size = min(1000, self.settings.livy_log_max_lines)
        while len(lines) < self.settings.livy_log_max_lines:
            response = await self.request(
                client,
                "GET",
                f"/batches/{batch_id}/log",
                params={"from": offset, "size": page_size},
            )
            payload = response.json()
            page = payload.get("log", []) if isinstance(payload, dict) else []
            if not isinstance(page, list):
                raise RuntimeError("Livy returned an invalid batch log response.")
            normalized = [str(line) for line in page]
            lines.extend(normalized)
            if len(normalized) < page_size:
                break
            offset += len(normalized)
        return lines[: self.settings.livy_log_max_lines]

    @staticmethod
    def result_from_logs(lines: list[str]) -> dict[str, Any]:
        for line in reversed(lines):
            marker_at = line.find(RESULT_MARKER)
            if marker_at < 0:
                continue
            encoded = line[marker_at + len(RESULT_MARKER) :].strip()
            try:
                result = json.loads(base64.b64decode(encoded, validate=True))
            except (ValueError, json.JSONDecodeError) as exc:
                raise RuntimeError("Livy job returned an invalid result marker.") from exc
            if not isinstance(result, dict):
                raise RuntimeError("Livy job result must be a JSON object.")
            return result
        raise RuntimeError("Livy batch succeeded without an Iceberg Ops result marker.")

    async def execute(
        self,
        payload: dict[str, Any],
        on_submitted: SubmittedCallback,
        should_cancel: CancellationCallback | None = None,
    ) -> dict[str, Any]:
        deadline = time.monotonic() + self.settings.operation_worker_timeout_seconds
        async with httpx.AsyncClient(**self.client_options()) as client:
            response = await self.request(client, "POST", "/batches", json=self.batch_payload(payload))
            batch = response.json()
            if not isinstance(batch, dict) or not isinstance(batch.get("id"), int):
                raise RuntimeError("Livy returned an invalid batch submission response.")
            batch_id = batch["id"]
            external_id = f"livy-batch:{batch_id}"
            await on_submitted(
                external_id,
                {
                    "batchId": batch_id,
                    "appId": batch.get("appId"),
                    "state": batch.get("state"),
                },
            )

            return await self.wait_for_batch(
                client, batch_id, batch, deadline, should_cancel
            )

    async def wait_for_batch(
        self,
        client: httpx.AsyncClient,
        batch_id: int,
        batch: dict[str, Any],
        deadline: float,
        should_cancel: CancellationCallback | None,
    ) -> dict[str, Any]:
        state = str(batch.get("state", "starting")).lower()
        while state not in LIVY_SUCCESS_STATES | LIVY_FAILURE_STATES:
            if should_cancel and await should_cancel():
                try:
                    await self.request(client, "DELETE", f"/batches/{batch_id}")
                except RuntimeError:
                    pass
                raise OperationCanceled(f"Livy batch {batch_id} cancellation was requested")
            if time.monotonic() >= deadline:
                try:
                    await self.request(client, "DELETE", f"/batches/{batch_id}")
                except RuntimeError:
                    pass
                raise RuntimeError(f"Livy batch {batch_id} timed out and cancellation was requested.")
            await asyncio.sleep(self.settings.livy_poll_seconds)
            response = await self.request(client, "GET", f"/batches/{batch_id}")
            batch = response.json()
            if not isinstance(batch, dict):
                raise RuntimeError("Livy returned an invalid batch status response.")
            state = str(batch.get("state", "unknown")).lower()

        lines = await self.logs(client, batch_id)
        if state in LIVY_FAILURE_STATES:
            tail = "\n".join(lines[-40:])[-4000:]
            if state == "killed" and should_cancel and await should_cancel():
                raise OperationCanceled(f"Livy batch {batch_id} was canceled")
            raise RuntimeError(f"Livy batch {batch_id} finished as {state}.\n{tail}")
        result = self.result_from_logs(lines)
        result["livy"] = {
            "batch_id": batch_id,
            "app_id": batch.get("appId"),
            "state": state,
            "log_tail": lines[-40:],
        }
        return result

    async def resume(
        self,
        payload: dict[str, Any],
        external_id: str,
        should_cancel: CancellationCallback | None = None,
    ) -> dict[str, Any]:
        match = re.fullmatch(r"livy-batch:([0-9]+)", external_id)
        if match is None:
            raise RuntimeError(f"Unsupported Livy external id: {external_id}")
        batch_id = int(match.group(1))
        deadline = time.monotonic() + self.settings.operation_worker_timeout_seconds
        async with httpx.AsyncClient(**self.client_options()) as client:
            response = await self.request(client, "GET", f"/batches/{batch_id}")
            batch = response.json()
            if not isinstance(batch, dict):
                raise RuntimeError("Livy returned an invalid batch status response.")
            return await self.wait_for_batch(client, batch_id, batch, deadline, should_cancel)


def create_spark_executor(settings: Settings) -> SparkOperationExecutor:
    if settings.spark_execution_mode == "livy":
        return LivyBatchExecutor(settings)
    return PlaygroundSparkExecutor(settings)
