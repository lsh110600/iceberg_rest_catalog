"""Standalone PySpark entrypoint submitted through Apache Livy /batches."""

import argparse
import base64
import json
import re
import time
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from pyspark.sql import Row, SparkSession

RESULT_MARKER = "ICEBERG_OPS_RESULT="


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--operation-id", required=True)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--table", required=True)
    parser.add_argument("--command", required=True)
    parser.add_argument("--parameters-json", default="{}")
    parser.add_argument("--expected-snapshot-id", type=int)
    return parser.parse_args()


def identifier(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"Unsupported identifier: {value}")
    return value


def namespace_identifier(value: str) -> str:
    return ".".join(identifier(part) for part in value.split("."))


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def bool_option(value: Any, name: str) -> str:
    if not isinstance(value, bool):
        raise ValueError(f"{name} must be boolean.")
    return "true" if value else "false"


def int_option(value: Any, name: str, minimum: int, maximum: int) -> str:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise ValueError(f"{name} must be between {minimum} and {maximum}.")
    return str(value)


def options_sql(options: dict[str, str]) -> str:
    values = ", ".join(
        f"{sql_literal(key)}, {sql_literal(value)}" for key, value in options.items()
    )
    return f"map({values})"


def procedure_sql(
    catalog: str,
    namespace: str,
    table: str,
    command: str,
    parameters: dict[str, Any],
) -> str:
    catalog_id = identifier(catalog)
    namespace_identifier(namespace)
    identifier(table)
    table_arg = sql_literal(f"{namespace}.{table}")

    if command == "REWRITE_DATA_FILES":
        options: dict[str, str] = {}
        mappings = {
            "targetFileSizeBytes": ("target-file-size-bytes", 1_048_576, 10_737_418_240),
            "minInputFiles": ("min-input-files", 1, 10_000),
            "maxFileGroupSizeBytes": (
                "max-file-group-size-bytes",
                1_048_576,
                109_951_162_777_600,
            ),
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
        return f"CALL {catalog_id}.system.rewrite_data_files(table => {table_arg}{suffix})"

    if command == "REWRITE_MANIFESTS":
        use_caching = parameters.get("useCaching", False)
        return (
            f"CALL {catalog_id}.system.rewrite_manifests(table => {table_arg}, "
            f"use_caching => {bool_option(use_caching, 'useCaching')})"
        )

    if command == "REWRITE_POSITION_DELETES":
        options = {}
        if "rewriteAll" in parameters:
            options["rewrite-all"] = bool_option(parameters["rewriteAll"], "rewriteAll")
        suffix = f", options => {options_sql(options)}" if options else ""
        return f"CALL {catalog_id}.system.rewrite_position_delete_files(table => {table_arg}{suffix})"

    if command == "COMPUTE_STATS":
        columns = parameters.get("columns")
        if columns is None:
            return f"CALL {catalog_id}.system.compute_table_stats(table => {table_arg})"
        if not isinstance(columns, list) or not 1 <= len(columns) <= 100:
            raise ValueError("columns must contain 1 to 100 column names.")
        values = []
        for column in columns:
            if not isinstance(column, str) or not column or len(column) > 500 or "\x00" in column:
                raise ValueError("Invalid statistics column name.")
            values.append(sql_literal(column))
        return (
            f"CALL {catalog_id}.system.compute_table_stats(table => {table_arg}, "
            f"columns => array({', '.join(values)}))"
        )

    if command == "EXPIRE_SNAPSHOTS":
        hours = int(int_option(parameters.get("olderThanHours", 168), "olderThanHours", 120, 87_600))
        retain = int_option(parameters.get("retainLast", 1), "retainLast", 1, 10_000)
        older_than = datetime.now(timezone.utc) - timedelta(hours=hours)
        timestamp = older_than.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        return (
            f"CALL {catalog_id}.system.expire_snapshots(table => {table_arg}, "
            f"older_than => TIMESTAMP {sql_literal(timestamp)}, retain_last => {retain}, "
            "stream_results => true)"
        )

    if command == "REMOVE_ORPHAN_FILES":
        hours = int(int_option(parameters.get("olderThanHours", 72), "olderThanHours", 72, 87_600))
        dry_run = bool_option(parameters.get("dryRun", True), "dryRun")
        older_than = datetime.now(timezone.utc) - timedelta(hours=hours)
        timestamp = older_than.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        return (
            f"CALL {catalog_id}.system.remove_orphan_files(table => {table_arg}, "
            f"older_than => TIMESTAMP {sql_literal(timestamp)}, dry_run => {dry_run}, "
            "stream_results => true)"
        )

    if command == "ROLLBACK":
        snapshot = int_option(
            parameters.get("snapshotId"),
            "snapshotId",
            1,
            9_223_372_036_854_775_807,
        )
        return (
            f"CALL {catalog_id}.system.rollback_to_snapshot("
            f"table => {table_arg}, snapshot_id => {snapshot})"
        )

    raise ValueError(f"Unsupported Spark command: {command}")


def json_value(value: Any) -> Any:
    if isinstance(value, date | datetime):
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


def table_state(spark: SparkSession, full_table: str) -> dict[str, Any]:
    current = spark.sql(f"SELECT snapshot_id FROM {full_table}.refs WHERE name = 'main'").first()
    snapshot_id = current["snapshot_id"] if current else None
    snapshot_count = spark.sql(f"SELECT count(*) AS count FROM {full_table}.snapshots").first()["count"]
    files = spark.sql(
        f"SELECT count(*) AS files, coalesce(sum(file_size_in_bytes), 0) AS bytes, "
        f"coalesce(sum(record_count), 0) AS records, "
        f"coalesce(sum(CASE WHEN content = 0 THEN 1 ELSE 0 END), 0) AS data_files, "
        f"coalesce(sum(CASE WHEN content <> 0 THEN 1 ELSE 0 END), 0) AS delete_files, "
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
        "partition_count": partition_count,
        "location": details.get("Location"),
        "schema_id": None,
        "spec_id": files["spec_id"],
        "sort_order_id": files["sort_order_id"],
    }


def emit_result(result: dict[str, Any]) -> None:
    encoded = base64.b64encode(
        json.dumps(result, separators=(",", ":"), default=json_value).encode("utf-8")
    ).decode("ascii")
    print(f"{RESULT_MARKER}{encoded}", flush=True)


def main() -> None:
    args = parse_args()
    catalog = identifier(args.catalog)
    namespace = namespace_identifier(args.namespace)
    table = identifier(args.table)
    full_table = f"{catalog}.{namespace}.{table}"
    parameters = json.loads(args.parameters_json)
    if not isinstance(parameters, dict):
        raise ValueError("parameters-json must contain a JSON object.")

    spark = SparkSession.builder.appName(f"iceberg-ops-{args.operation_id}").getOrCreate()
    started = time.monotonic()
    before = table_state(spark, full_table)
    if args.expected_snapshot_id is not None and before["snapshot_id"] != args.expected_snapshot_id:
        raise RuntimeError(
            "Snapshot precondition failed: expected "
            f"{args.expected_snapshot_id}, found {before['snapshot_id']}."
        )

    sql = procedure_sql(catalog, args.namespace, table, args.command, parameters)
    output_rows = spark.sql(sql).limit(100).collect()
    output = [
        {key: json_value(value) for key, value in row.asDict(recursive=False).items()}
        for row in output_rows
    ]
    after = table_state(spark, full_table)
    emit_result(
        {
            "operation_id": args.operation_id,
            "command": args.command,
            "table": full_table,
            "procedure": sql.split("(", 1)[0],
            "before": before,
            "after": after,
            "output": output,
            "duration_ms": round((time.monotonic() - started) * 1000),
        }
    )


if __name__ == "__main__":
    main()
