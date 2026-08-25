import json
from functools import lru_cache
from typing import Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_name: str = "Iceberg Ops API"
    environment: str = "development"
    database_url: str = "postgresql+asyncpg://iceberg_ops:iceberg_ops@localhost:5432/iceberg_ops"
    demo_data: bool = False
    cors_allowed_origins: str = Field(default="http://localhost:3000")
    playground_enabled: bool = False
    playground_runner_url: str = "http://playground-runner:8090"
    playground_timeout_seconds: float = 120.0
    playground_runner_token: str = "local-playground-token"
    operation_worker_poll_seconds: float = 2.0
    operation_worker_timeout_seconds: float = 1800.0
    operation_worker_stale_seconds: float = 7200.0
    bulk_worker_poll_seconds: float = 1.0
    spark_execution_mode: Literal["playground", "livy"] = "playground"
    spark_catalog_scope: Literal["playground", "production"] = "playground"
    spark_engine_name: str = "spark-playground"
    livy_url: str = "http://livy:8998"
    livy_job_file: str = "hdfs:///apps/iceberg-ops/iceberg_maintenance.py"
    livy_poll_seconds: float = 5.0
    livy_request_timeout_seconds: float = 30.0
    livy_verify_tls: bool = True
    livy_ca_bundle: str | None = None
    livy_username: str | None = None
    livy_password: str | None = None
    livy_bearer_token: str | None = None
    livy_proxy_user: str | None = None
    livy_queue: str | None = None
    livy_driver_memory: str | None = None
    livy_driver_cores: int | None = None
    livy_executor_memory: str | None = None
    livy_executor_cores: int | None = None
    livy_num_executors: int | None = None
    livy_conf_json: str = "{}"
    livy_log_max_lines: int = 10_000

    @field_validator(
        "livy_ca_bundle",
        "livy_username",
        "livy_password",
        "livy_bearer_token",
        "livy_proxy_user",
        "livy_queue",
        "livy_driver_memory",
        "livy_driver_cores",
        "livy_executor_memory",
        "livy_executor_cores",
        "livy_num_executors",
        mode="before",
    )
    @classmethod
    def empty_livy_value_is_none(cls, value: object) -> object:
        return None if value == "" else value

    @property
    def cors_origins(self) -> list[str]:
        return [origin.strip() for origin in self.cors_allowed_origins.split(",") if origin.strip()]

    @property
    def livy_conf(self) -> dict[str, str]:
        value = json.loads(self.livy_conf_json)
        if not isinstance(value, dict) or not all(
            isinstance(key, str) and isinstance(item, str) for key, item in value.items()
        ):
            raise ValueError("LIVY_CONF_JSON must be a JSON object containing string values.")
        return value


@lru_cache
def get_settings() -> Settings:
    return Settings()
