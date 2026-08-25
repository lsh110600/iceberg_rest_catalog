import logging
import os
import re
import socket
import threading
import time
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any
from urllib.parse import urlparse

from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel, Field
from pyspark.sql import Row, SparkSession
from pyspark.sql.context import SQLContext

SPARK_VERSION = "3.5.9"
ICEBERG_VERSION = "1.11.0"
HADOOP_VERSION = "3.5.0"
SAMPLE_TABLE = "playground.demo.orders"
HDFS_URI = "hdfs://namenode:8020"
SPARK_MASTER = "spark://spark-master:7077"

app = FastAPI(title="Iceberg Playground Runner", version="0.1.0")
logger = logging.getLogger("iceberg-playground-runner")
spark_lock = threading.Lock()
spark_session: SparkSession | None = None


class QueryRequest(BaseModel):
    sql: str = Field(min_length=1, max_length=4000)
    max_rows: int = Field(default=100, ge=1, le=200)
    catalog: str = Field(default="playground", min_length=1, max_length=120)
    warehouse: str = Field(default=f"{HDFS_URI}/warehouse", min_length=1, max_length=2000)


class OperationExecuteRequest(BaseModel):
    operation_id: str = Field(min_length=1, max_length=200)
    catalog: str = Field(min_length=1, max_length=200)
    namespace: str = Field(min_length=1, max_length=500)
    table: str = Field(min_length=1, max_length=500)
    command: str = Field(min_length=1, max_length=80)
    parameters: dict[str, Any] = Field(default_factory=dict)
    expected_snapshot_id: int | None = None
    warehouse: str = Field(default=f"{HDFS_URI}/warehouse", min_length=1, max_length=2000)


class TableInspectRequest(BaseModel):
    catalog: str = Field(min_length=1, max_length=120)
    warehouse: str = Field(min_length=1, max_length=2000)
    namespace: str = Field(min_length=1, max_length=500)
    table: str = Field(min_length=1, max_length=500)


class TableBulkInspectRequest(BaseModel):
    catalog: str = Field(min_length=1, max_length=120)
    warehouse: str = Field(min_length=1, max_length=2000)
    namespace: str = Field(min_length=1, max_length=500)
    tables: list[str] = Field(min_length=1, max_length=200)


def port_open(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=2):
            return True
    except OSError:
        return False


def spark_context_is_running(session: SparkSession) -> bool:
    """Return whether the JVM SparkContext behind a session can still run jobs."""
    try:
        java_context = session.sparkContext._jsc
        return java_context is not None and not java_context.sc().isStopped()
    except Exception:  # Py4J can fail while the JVM is shutting down.
        return False


def clear_stale_spark_session() -> None:
    """Remove Python/JVM session references so getOrCreate builds a new context."""
    global spark_session
    stale_session = spark_session
    spark_session = None
    if stale_session is not None:
        try:
            # Even when the JVM context is already stopped, PySpark's stop()
            # clears its Python-side _active_spark_context reference.
            stale_session.sparkContext.stop()
        except Exception:
            logger.warning("Could not finalize the stale SparkContext.", exc_info=True)
    try:
        if stale_session is not None and stale_session._jvm is not None:
            stale_session._jvm.SparkSession.clearActiveSession()
            stale_session._jvm.SparkSession.clearDefaultSession()
    except Exception:  # The gateway may briefly be unavailable during shutdown.
        logger.warning("Could not clear the JVM SparkSession references.", exc_info=True)
    finally:
        # PySpark 3.5 exposes these as internal class references rather than
        # public clearActiveSession/clearDefaultSession Python methods.
        SparkSession._instantiatedSession = None
        SparkSession._activeSession = None
        SQLContext._instantiatedContext = None


def build_spark_session() -> SparkSession:
    session = (
        SparkSession.builder.appName("iceberg-ops-playground")
        .master(SPARK_MASTER)
        .config("spark.driver.host", "playground-runner")
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.driver.port", "39000")
        .config("spark.blockManager.port", "39001")
        .config("spark.executor.memory", "1g")
        .config("spark.executor.cores", "1")
        .config("spark.cores.max", "1")
        .config("spark.sql.shuffle.partitions", "2")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.playground", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.playground.type", "hadoop")
        .config("spark.sql.catalog.playground.warehouse", f"{HDFS_URI}/warehouse")
        .config("spark.hadoop.fs.defaultFS", HDFS_URI)
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("WARN")
    return session


def get_spark() -> SparkSession:
    global spark_session
    if spark_session is not None and spark_context_is_running(spark_session):
        return spark_session

    if spark_session is not None:
        logger.warning("Detected a stopped SparkContext; creating a replacement session.")
        clear_stale_spark_session()

    spark_session = build_spark_session()
    if not spark_context_is_running(spark_session):
        clear_stale_spark_session()
        raise RuntimeError("Spark created a session with a stopped SparkContext.")
    return spark_session


def sample_exists(spark: SparkSession) -> bool:
    rows = spark.sql("SHOW TABLES IN playground.demo LIKE 'orders'").collect()
    return bool(rows)


def json_value(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, int) and abs(value) > 2**53 - 1:
        return str(value)
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, Row):
        return {key: json_value(item) for key, item in value.asDict(recursive=False).items()}
    if isinstance(value, list):
        return [json_value(item) for item in value]
    if isinstance(value, dict):
        return {str(key): json_value(item) for key, item in value.items()}
    return value


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def identifier(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise HTTPException(status_code=400, detail=f"Unsupported identifier: {value}")
    return value


def namespace_identifier(value: str) -> str:
    return ".".join(identifier(part) for part in value.split("."))


def configure_hadoop_catalog(spark: SparkSession, catalog: str, warehouse: str) -> None:
    catalog_name = identifier(catalog)
    normalized_warehouse = warehouse.rstrip("/")
    parsed = urlparse(normalized_warehouse)
    if parsed.scheme.lower() != "hdfs" or not parsed.path.startswith("/"):
        raise HTTPException(status_code=400, detail="warehouse must be an absolute hdfs:// URI")

    prefix = f"spark.sql.catalog.{catalog_name}"
    configured_warehouse = spark.conf.get(f"{prefix}.warehouse", None)
    if configured_warehouse is not None and configured_warehouse.rstrip("/") != normalized_warehouse:
        raise HTTPException(
            status_code=409,
            detail=f"Catalog {catalog_name} is already active with a different warehouse.",
        )
    spark.conf.set(prefix, "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set(f"{prefix}.type", "hadoop")
    spark.conf.set(f"{prefix}.warehouse", normalized_warehouse)


def bool_option(value: Any, name: str) -> str:
    if not isinstance(value, bool):
        raise HTTPException(status_code=400, detail=f"{name} must be boolean.")
    return "true" if value else "false"


def int_option(value: Any, name: str, minimum: int, maximum: int) -> str:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise HTTPException(
            status_code=400,
            detail=f"{name} must be between {minimum} and {maximum}.",
        )
    return str(value)


def options_sql(options: dict[str, str]) -> str:
    values = ", ".join(
        f"{sql_literal(key)}, {sql_literal(value)}" for key, value in options.items()
    )
    return f"map({values})"


def procedure_sql(request: OperationExecuteRequest) -> str:
    catalog = identifier(request.catalog)
    namespace = namespace_identifier(request.namespace)
    table = identifier(request.table)
    table_arg = sql_literal(f"{namespace}.{table}")
    parameters = request.parameters

    if request.command == "REWRITE_DATA_FILES":
        options: dict[str, str] = {}
        mappings = {
            "targetFileSizeBytes": ("target-file-size-bytes", 1_048_576, 10_737_418_240),
            "minInputFiles": ("min-input-files", 1, 10_000),
            "maxFileGroupSizeBytes": ("max-file-group-size-bytes", 1_048_576, 109_951_162_777_600),
        }
        for source, (target, minimum, maximum) in mappings.items():
            if source in parameters:
                options[target] = int_option(parameters[source], source, minimum, maximum)
        for source, target in {
            "rewriteAll": "rewrite-all",
            "removeDanglingDeletes": "remove-dangling-deletes",
        }.items():
            if source in parameters:
                options[target] = bool_option(parameters[source], source)
        suffix = f", options => {options_sql(options)}" if options else ""
        return f"CALL {catalog}.system.rewrite_data_files(table => {table_arg}{suffix})"

    if request.command == "REWRITE_MANIFESTS":
        use_caching = parameters.get("useCaching", False)
        return (
            f"CALL {catalog}.system.rewrite_manifests(table => {table_arg}, "
            f"use_caching => {bool_option(use_caching, 'useCaching')})"
        )

    if request.command == "REWRITE_POSITION_DELETES":
        options = {}
        if "rewriteAll" in parameters:
            options["rewrite-all"] = bool_option(parameters["rewriteAll"], "rewriteAll")
        suffix = f", options => {options_sql(options)}" if options else ""
        return f"CALL {catalog}.system.rewrite_position_delete_files(table => {table_arg}{suffix})"

    if request.command == "COMPUTE_STATS":
        columns = parameters.get("columns")
        if columns is None:
            return f"CALL {catalog}.system.compute_table_stats(table => {table_arg})"
        if not isinstance(columns, list) or not 1 <= len(columns) <= 100:
            raise HTTPException(status_code=400, detail="columns must contain 1 to 100 column names.")
        column_values = []
        for column in columns:
            if not isinstance(column, str) or not column or len(column) > 500 or "\x00" in column:
                raise HTTPException(status_code=400, detail="Invalid statistics column name.")
            column_values.append(sql_literal(column))
        return (
            f"CALL {catalog}.system.compute_table_stats(table => {table_arg}, "
            f"columns => array({', '.join(column_values)}))"
        )

    if request.command == "EXPIRE_SNAPSHOTS":
        older_hours = parameters.get("olderThanHours", 168)
        retain_last = parameters.get("retainLast", 1)
        hours = int(int_option(older_hours, "olderThanHours", 120, 87_600))
        retain = int_option(retain_last, "retainLast", 1, 10_000)
        older_than = datetime.now(timezone.utc) - timedelta(hours=hours)
        timestamp = older_than.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        return (
            f"CALL {catalog}.system.expire_snapshots(table => {table_arg}, "
            f"older_than => TIMESTAMP {sql_literal(timestamp)}, retain_last => {retain}, "
            "stream_results => true)"
        )

    if request.command == "REMOVE_ORPHAN_FILES":
        older_hours = parameters.get("olderThanHours", 72)
        hours = int(int_option(older_hours, "olderThanHours", 72, 87_600))
        dry_run = bool_option(parameters.get("dryRun", True), "dryRun")
        older_than = datetime.now(timezone.utc) - timedelta(hours=hours)
        timestamp = older_than.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        return (
            f"CALL {catalog}.system.remove_orphan_files(table => {table_arg}, "
            f"older_than => TIMESTAMP {sql_literal(timestamp)}, dry_run => {dry_run}, "
            "stream_results => true)"
        )

    if request.command == "ROLLBACK":
        snapshot_id = parameters.get("snapshotId")
        snapshot = int_option(snapshot_id, "snapshotId", 1, 9_223_372_036_854_775_807)
        return f"CALL {catalog}.system.rollback_to_snapshot(table => {table_arg}, snapshot_id => {snapshot})"

    raise HTTPException(status_code=400, detail=f"Unsupported Spark command: {request.command}")


def iceberg_table_state(spark: SparkSession, full_table: str) -> dict[str, Any]:
    if not spark.catalog.tableExists(full_table):
        raise HTTPException(status_code=404, detail=f"Iceberg table does not exist: {full_table}")
    current = spark.sql(f"SELECT snapshot_id FROM {full_table}.refs WHERE name = 'main'").first()
    snapshot_id = current["snapshot_id"] if current else None
    snapshot_count = spark.sql(f"SELECT count(*) AS count FROM {full_table}.snapshots").first()["count"]
    files = spark.sql(
        f"SELECT count(*) AS files, coalesce(sum(file_size_in_bytes), 0) AS bytes, "
        f"coalesce(sum(record_count), 0) AS records, "
        f"coalesce(sum(CASE WHEN content = 0 THEN 1 ELSE 0 END), 0) AS data_files, "
        f"coalesce(sum(CASE WHEN content <> 0 THEN 1 ELSE 0 END), 0) AS delete_files, "
        "coalesce(sum(CASE WHEN content = 0 AND file_size_in_bytes < 134217728 "
        f"THEN 1 ELSE 0 END), 0) AS small_data_files, "
        f"min(spec_id) AS spec_id, min(sort_order_id) AS sort_order_id "
        f"FROM {full_table}.files"
    ).first()
    partition_count = spark.sql(
        f"SELECT count(*) AS count FROM {full_table}.partitions"
    ).first()["count"]
    details = {
        str(row[0]).strip(): str(row[1]).strip()
        for row in spark.sql(f"DESCRIBE TABLE EXTENDED {full_table}").collect()
        if row[0] and row[1]
    }
    return {
        "snapshot_id": snapshot_id,
        "snapshot_count": snapshot_count,
        "total_files": files["files"],
        "total_bytes": files["bytes"],
        "total_records": files["records"],
        "data_files": files["data_files"],
        "delete_files": files["delete_files"],
        "small_data_files": files["small_data_files"],
        "partition_count": partition_count,
        "location": details.get("Location"),
        "schema_id": None,
        "spec_id": files["spec_id"],
        "sort_order_id": files["sort_order_id"],
    }


def iceberg_table_details(spark: SparkSession, full_table: str) -> dict[str, Any]:
    state = iceberg_table_state(spark, full_table)
    rows = spark.sql(f"DESCRIBE TABLE EXTENDED {full_table}").collect()
    details = {str(row[0]).strip(): str(row[1]).strip() for row in rows if row[0] and row[1]}
    properties = details.get("Table Properties", "")
    format_version_match = re.search(r"(?:^|[\[,])\s*format-version=([0-9]+)", properties)
    latest = spark.sql(
        f"SELECT committed_at FROM {full_table}.snapshots ORDER BY committed_at DESC LIMIT 1"
    ).first()
    return {
        **state,
        "location": details.get("Location"),
        "format_version": int(format_version_match.group(1)) if format_version_match else None,
        "last_commit_at": json_value(latest["committed_at"]) if latest else None,
    }


def verify_operation_token(x_playground_token: str | None) -> None:
    expected = os.environ.get("PLAYGROUND_RUNNER_TOKEN", "local-playground-token")
    if not x_playground_token or x_playground_token != expected:
        raise HTTPException(status_code=401, detail="Invalid playground runner token.")


def validate_read_only_sql(sql: str) -> str:
    without_comments = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)
    without_comments = re.sub(r"--[^\n]*", " ", without_comments).strip()
    normalized = without_comments[:-1].strip() if without_comments.endswith(";") else without_comments
    if not normalized or ";" in normalized:
        raise HTTPException(status_code=400, detail="Exactly one SQL statement is allowed.")

    first_word = normalized.split(None, 1)[0].upper()
    if first_word not in {"SELECT", "SHOW", "DESCRIBE", "DESC", "EXPLAIN", "WITH"}:
        raise HTTPException(status_code=400, detail="Playground accepts read-only SQL only.")

    forbidden = re.search(
        r"\b(INSERT|UPDATE|DELETE|MERGE|CREATE|DROP|ALTER|TRUNCATE|CALL|REFRESH|SET|USE|ADD|CACHE|UNCACHE)\b",
        normalized,
        flags=re.IGNORECASE,
    )
    if forbidden:
        raise HTTPException(
            status_code=400,
            detail=f"Read-only SQL cannot contain {forbidden.group(1).upper()}.",
        )
    return normalized


def table_location(spark: SparkSession) -> str:
    rows = spark.sql(f"DESCRIBE TABLE EXTENDED {SAMPLE_TABLE}").collect()
    for row in rows:
        if str(row[0]).strip() == "Location":
            return str(row[1])
    return f"{HDFS_URI}/warehouse/demo/orders"


@app.get("/healthz")
def health() -> dict[str, str]:
    return {"status": "healthy", "service": "Iceberg Playground Runner"}


@app.get("/status")
def status() -> dict[str, Any]:
    hdfs_ready = port_open("namenode", 8020)
    spark_endpoint_ready = port_open("spark-master", 7077)
    spark_ready = False
    exists = False
    message = None
    if hdfs_ready and spark_endpoint_ready:
        try:
            with spark_lock:
                spark = get_spark()
                # Force a real Spark job so a stopped context cannot look healthy
                # just because the master TCP port and catalog metadata are reachable.
                spark.range(1).count()
                spark.sql("CREATE NAMESPACE IF NOT EXISTS playground.demo")
                exists = sample_exists(spark)
                spark_ready = True
        except Exception as exc:  # Spark returns JVM exception types dynamically.
            logger.warning("Spark readiness check failed.", exc_info=True)
            message = f"Spark session is unavailable: {exc.__class__.__name__}"
    else:
        message = "Waiting for HDFS and Spark services."
    return {
        "enabled": True,
        "ready": hdfs_ready and spark_ready,
        "hdfs": {"status": "HEALTHY" if hdfs_ready else "UNKNOWN", "endpoint": HDFS_URI},
        "spark": {
            "status": "HEALTHY" if spark_ready else "UNKNOWN",
            "endpoint": SPARK_MASTER,
        },
        "sample_table": SAMPLE_TABLE,
        "sample_exists": exists,
        "versions": {
            "spark": SPARK_VERSION,
            "iceberg": ICEBERG_VERSION,
            "hadoop": HADOOP_VERSION,
        },
        "message": message,
    }


@app.post("/bootstrap")
def bootstrap() -> dict[str, Any]:
    with spark_lock:
        spark = get_spark()
        spark.sql("CREATE NAMESPACE IF NOT EXISTS playground.demo")
        spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {SAMPLE_TABLE} (
              order_id BIGINT,
              customer STRING,
              amount DECIMAL(12, 2),
              order_date DATE,
              status STRING
            ) USING iceberg
            PARTITIONED BY (days(order_date))
            TBLPROPERTIES (
              'format-version' = '2',
              'write.target-file-size-bytes' = '134217728'
            )
            """
        )
        before = spark.sql(f"SELECT count(*) AS count FROM {SAMPLE_TABLE}").first()["count"]
        inserted = 0
        if before == 0:
            spark.sql(
                f"""
                INSERT INTO {SAMPLE_TABLE} VALUES
                  (1001, 'Han River Shop', 125000.00, DATE '2026-08-20', 'PAID'),
                  (1002, 'Jeju Market', 89900.00, DATE '2026-08-20', 'SHIPPED'),
                  (1003, 'Busan Books', 42000.00, DATE '2026-08-21', 'PAID'),
                  (1004, 'Seoul Coffee', 18500.00, DATE '2026-08-21', 'CANCELED')
                """
            )
            spark.sql(
                f"""
                INSERT INTO {SAMPLE_TABLE} VALUES
                  (1005, 'Gangneung Store', 73000.00, DATE '2026-08-22', 'PAID'),
                  (1006, 'Incheon Parts', 210000.00, DATE '2026-08-22', 'SHIPPED'),
                  (1007, 'Daejeon Lab', 99000.00, DATE '2026-08-23', 'PAID'),
                  (1008, 'Daegu Design', 56000.00, DATE '2026-08-23', 'READY')
                """
            )
            inserted = 8

        total = spark.sql(f"SELECT count(*) AS count FROM {SAMPLE_TABLE}").first()["count"]
        state = iceberg_table_state(spark, SAMPLE_TABLE)
        return {
            "table": SAMPLE_TABLE,
            "location": table_location(spark),
            "inserted_rows": inserted,
            "total_rows": total,
            "snapshot_count": state["snapshot_count"],
            "current_snapshot_id": state["snapshot_id"],
            "total_files": state["total_files"],
            "total_bytes": state["total_bytes"],
            "total_records": state["total_records"],
            "message": "Sample Iceberg table is ready.",
        }


@app.post("/query")
def query(request: QueryRequest) -> dict[str, Any]:
    sql = validate_read_only_sql(request.sql)
    started = time.monotonic()
    with spark_lock:
        spark = get_spark()
        configure_hadoop_catalog(spark, request.catalog, request.warehouse)
        dataframe = spark.sql(sql)
        collected = dataframe.limit(request.max_rows + 1).collect()
    truncated = len(collected) > request.max_rows
    rows = collected[: request.max_rows]
    return {
        "columns": [
            {"name": field.name, "data_type": field.dataType.simpleString(), "nullable": field.nullable}
            for field in dataframe.schema.fields
        ],
        "rows": [
            {key: json_value(value) for key, value in row.asDict(recursive=False).items()}
            for row in rows
        ],
        "row_count": len(rows),
        "truncated": truncated,
        "duration_ms": round((time.monotonic() - started) * 1000),
    }


@app.post("/tables/inspect")
def inspect_table(request: TableInspectRequest) -> dict[str, Any]:
    catalog = identifier(request.catalog)
    namespace = namespace_identifier(request.namespace)
    table = identifier(request.table)
    full_table = f"{catalog}.{namespace}.{table}"
    with spark_lock:
        spark = get_spark()
        configure_hadoop_catalog(spark, catalog, request.warehouse)
        return iceberg_table_details(spark, full_table)


@app.post("/tables/inspect-bulk")
def inspect_tables(request: TableBulkInspectRequest) -> dict[str, Any]:
    catalog = identifier(request.catalog)
    namespace = namespace_identifier(request.namespace)
    table_names = list(dict.fromkeys(identifier(table) for table in request.tables))
    tables: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []
    with spark_lock:
        spark = get_spark()
        configure_hadoop_catalog(spark, catalog, request.warehouse)
        for table in table_names:
            try:
                tables.append({"table": table, **iceberg_table_details(spark, f"{catalog}.{namespace}.{table}")})
            except HTTPException as exc:
                errors.append({"table": table, "detail": str(exc.detail)})
            except Exception as exc:  # Spark exception classes are created dynamically.
                logger.warning("Bulk table inspection failed for %s.%s.%s", catalog, namespace, table)
                detail = str(exc).splitlines()[0] if str(exc) else exc.__class__.__name__
                errors.append({"table": table, "detail": detail[:500]})
    return {"tables": tables, "errors": errors, "requested_count": len(table_names)}


@app.post("/operations/execute")
def execute_operation(
    request: OperationExecuteRequest,
    x_playground_token: str | None = Header(default=None),
) -> dict[str, Any]:
    verify_operation_token(x_playground_token)
    sql = procedure_sql(request)
    catalog = identifier(request.catalog)
    namespace = namespace_identifier(request.namespace)
    table = identifier(request.table)
    full_table = f"{catalog}.{namespace}.{table}"
    started = time.monotonic()
    with spark_lock:
        spark = get_spark()
        configure_hadoop_catalog(spark, catalog, request.warehouse)
        before = iceberg_table_state(spark, full_table)
        if (
            request.expected_snapshot_id is not None
            and before["snapshot_id"] != request.expected_snapshot_id
        ):
            raise HTTPException(
                status_code=409,
                detail=(
                    "Snapshot precondition failed: expected "
                    f"{request.expected_snapshot_id}, found {before['snapshot_id']}."
                ),
            )
        spark.sparkContext.setJobGroup(
            request.operation_id,
            f"Iceberg Ops {request.command} on {full_table}",
            interruptOnCancel=True,
        )
        try:
            output_frame = spark.sql(sql)
            output_rows = output_frame.limit(1000).collect()
            output = [
                {key: json_value(value) for key, value in row.asDict(recursive=False).items()}
                for row in output_rows
            ]
            after = iceberg_table_state(spark, full_table)
        finally:
            spark.sparkContext.setLocalProperty("spark.jobGroup.id", None)
            spark.sparkContext.setLocalProperty("spark.job.description", None)
            spark.sparkContext.setLocalProperty("spark.job.interruptOnCancel", None)
    return {
        "operation_id": request.operation_id,
        "command": request.command,
        "table": full_table,
        "procedure": sql.split("(", 1)[0],
        "before": before,
        "after": after,
        "output": output,
        "duration_ms": round((time.monotonic() - started) * 1000),
    }
