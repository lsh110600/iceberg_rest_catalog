import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app import models

IDS = {
    "catalog": uuid.UUID("10000000-0000-0000-0000-000000000001"),
    "hdfs": uuid.UUID("20000000-0000-0000-0000-000000000001"),
    "s3": uuid.UUID("20000000-0000-0000-0000-000000000002"),
    "spark": uuid.UUID("30000000-0000-0000-0000-000000000001"),
    "trino": uuid.UUID("30000000-0000-0000-0000-000000000002"),
    "flink": uuid.UUID("30000000-0000-0000-0000-000000000003"),
}


async def seed_demo_data(session: AsyncSession) -> None:
    if await session.scalar(select(func.count(models.Catalog.id))):
        return

    now = datetime.now(UTC)
    catalog = models.Catalog(
        id=IDS["catalog"],
        name="production",
        catalog_type="REST",
        endpoint="http://rest-catalog:8181",
        warehouse="hdfs://namenode:8020/warehouse",
        health="HEALTHY",
        last_synced_at=now - timedelta(seconds=22),
    )
    hdfs = models.StorageProfile(
        id=IDS["hdfs"],
        name="hdfs-production",
        storage_type="HDFS",
        uri_prefix="hdfs://namenode:8020/warehouse",
        file_io_impl="org.apache.iceberg.hadoop.HadoopFileIO",
        config_ref="secret://hadoop/production-config",
        health="HEALTHY",
    )
    s3 = models.StorageProfile(
        id=IDS["s3"],
        name="s3-future",
        storage_type="S3",
        uri_prefix="s3://company-lakehouse/warehouse",
        file_io_impl="org.apache.iceberg.aws.s3.S3FileIO",
        credential_ref="secret://aws/lakehouse-role",
        region="ap-northeast-2",
        encryption_policy="SSE-KMS",
        health="NOT_CONFIGURED",
    )
    engines = [
        models.EngineInstance(
            id=IDS["spark"],
            name="spark-production",
            engine_type="SPARK",
            version="3.5.x",
            endpoint="http://livy:8998",
            health="HEALTHY",
            capabilities=[
                "REWRITE_DATA_FILES",
                "REWRITE_MANIFESTS",
                "REWRITE_POSITION_DELETES",
                "EXPIRE_SNAPSHOTS",
                "REMOVE_ORPHAN_FILES",
                "COMPUTE_STATS",
                "ROLLBACK",
            ],
            last_checked_at=now,
        ),
        models.EngineInstance(
            id=IDS["trino"],
            name="trino-production",
            engine_type="TRINO",
            version="480+",
            endpoint="http://trino:8080",
            health="HEALTHY",
            capabilities=["METADATA_QUERY", "OPTIMIZE", "OPTIMIZE_MANIFESTS", "EXPIRE_SNAPSHOTS", "ANALYZE"],
            last_checked_at=now,
        ),
        models.EngineInstance(
            id=IDS["flink"],
            name="flink-streaming",
            engine_type="FLINK",
            version="2.x",
            endpoint="http://flink-jobmanager:8081",
            health="WARNING",
            capabilities=["REWRITE_DATA_FILES", "EXPIRE_SNAPSHOTS", "STREAMING_TELEMETRY"],
            last_checked_at=now - timedelta(minutes=2),
        ),
    ]
    session.add_all([catalog, hdfs, s3, *engines])
    await session.flush()
    session.add_all(
        [
            models.EngineStorageAccess(
                engine_instance_id=engine.id,
                storage_profile_id=hdfs.id,
                health="WARNING" if engine.engine_type == "FLINK" else "HEALTHY",
                last_checked_at=now,
            )
            for engine in engines
        ]
    )

    table_specs = [
        (
            "40000000-0000-0000-0000-000000000001",
            "41000000-0000-0000-0000-000000000001",
            "events",
            "clickstream",
            "growth-data",
            "streaming-hot",
            2,
            918273645001,
            timedelta(minutes=4),
            8 * 1024**4,
            32_100_000_000,
            18_432,
            Decimal("0.632"),
            Decimal("0.081"),
            824,
            "WARNING",
            timedelta(minutes=1),
            536_870_912,
        ),
        (
            "40000000-0000-0000-0000-000000000002",
            "41000000-0000-0000-0000-000000000002",
            "finance",
            "daily_settlement",
            "finance-platform",
            "batch-daily",
            2,
            918273645002,
            timedelta(hours=9),
            1536 * 1024**3,
            987_000_000,
            1_512,
            Decimal("0.091"),
            Decimal("0.004"),
            42,
            "HEALTHY",
            timedelta(minutes=4),
            536_870_912,
        ),
        (
            "40000000-0000-0000-0000-000000000003",
            "41000000-0000-0000-0000-000000000003",
            "cdc",
            "customer_changes",
            "customer-domain",
            "streaming-hot",
            2,
            918273645003,
            timedelta(minutes=47),
            3 * 1024**4,
            8_600_000_000,
            29_018,
            Decimal("0.421"),
            Decimal("0.238"),
            901,
            "CRITICAL",
            timedelta(minutes=2),
            268_435_456,
        ),
        (
            "40000000-0000-0000-0000-000000000004",
            "41000000-0000-0000-0000-000000000004",
            "archive",
            "legacy_orders",
            "data-archive",
            "archive",
            1,
            918273645004,
            timedelta(days=93),
            512 * 1024**3,
            412_000_000,
            704,
            None,
            None,
            8,
            "UNKNOWN",
            timedelta(days=2),
            None,
        ),
    ]
    tables: list[models.ManagedTable] = []
    for spec in table_specs:
        (
            table_id,
            table_uuid,
            namespace,
            name,
            owner,
            slo,
            format_version,
            snapshot_id,
            commit_age,
            total_bytes,
            total_records,
            total_files,
            small_ratio,
            delete_ratio,
            snapshot_count,
            health,
            observed_age,
            target_size,
        ) = spec
        table = models.ManagedTable(
            id=uuid.UUID(table_id),
            catalog_id=catalog.id,
            storage_profile_id=hdfs.id,
            table_uuid=uuid.UUID(table_uuid),
            namespace_name=namespace,
            table_name=name,
            owner_name=owner,
            slo_profile=slo,
            format_version=format_version,
            metadata_location=f"{hdfs.uri_prefix}/{namespace}/{name}/metadata/v{snapshot_count}.metadata.json",
            current_snapshot_id=snapshot_id,
            last_commit_at=now - commit_age,
            total_bytes=total_bytes,
            total_records=total_records,
            total_files=total_files,
            small_file_ratio=small_ratio,
            delete_file_ratio=delete_ratio,
            snapshot_count=snapshot_count,
            health=health,
            observed_at=now - observed_age,
            properties={"write.target-file-size-bytes": str(target_size)} if target_size else {},
        )
        tables.append(table)
    session.add_all(tables)
    await session.flush()

    session.add_all(
        [
            models.Finding(
                table_id=tables[0].id,
                rule_code="SMALL_FILE_RATIO",
                severity="WARNING",
                title="작은 파일 비율이 63.2%입니다",
                evidence={"ratio": 0.632, "eligibleBytes": 4_810_363_371_520},
                recommendation="최근 7일 partition에 rewrite data files를 계획하세요.",
                status="OPEN",
                first_seen_at=now - timedelta(hours=3),
                last_seen_at=now,
            ),
            models.Finding(
                table_id=tables[2].id,
                rule_code="FRESHNESS_LAG",
                severity="CRITICAL",
                title="Streaming freshness SLO를 위반했습니다",
                evidence={"lastCommitMinutes": 47, "sloMinutes": 10},
                recommendation="Flink upstream job과 checkpoint 상태를 확인하세요.",
                status="OPEN",
                first_seen_at=now - timedelta(minutes=37),
                last_seen_at=now,
            ),
            models.Finding(
                table_id=tables[2].id,
                rule_code="DELETE_AMPLIFICATION",
                severity="WARNING",
                title="Delete record 비율이 23.8%입니다",
                evidence={"ratio": 0.238},
                recommendation="position delete file rewrite를 계획하세요.",
                status="OPEN",
                first_seen_at=now - timedelta(hours=2),
                last_seen_at=now,
            ),
            models.Finding(
                table_id=tables[3].id,
                rule_code="COLLECTOR_STALE",
                severity="INFO",
                title="최근 inventory를 확인할 수 없습니다",
                evidence={"collectorLagHours": 48},
                recommendation="HDFS 접근 권한과 collector 상태를 확인하세요.",
                status="OPEN",
                first_seen_at=now - timedelta(days=1),
                last_seen_at=now,
            ),
        ]
    )
    for hour in range(25):
        observed = now - timedelta(hours=24 - hour)
        session.add_all(
            [
                models.MetricPoint(
                    table_id=tables[0].id,
                    metric_name="small_file_ratio",
                    metric_value=0.32 + hour * 0.013,
                    engine_type="SPARK",
                    labels={},
                    observed_at=observed,
                ),
                models.MetricPoint(
                    table_id=tables[0].id,
                    metric_name="scan_planning_ms_p95",
                    metric_value=850 + hour * 73,
                    engine_type="TRINO",
                    labels={},
                    observed_at=observed,
                ),
            ]
        )
    session.add(
        models.OperationRequest(
            id=uuid.UUID("60000000-0000-0000-0000-000000000001"),
            table_id=tables[0].id,
            command="REWRITE_DATA_FILES",
            preferred_engine="SPARK",
            parameters={"maxRewriteBytes": 1_099_511_627_776},
            reason="작은 파일 증가로 인한 Trino planning 지연 개선",
            requester="demo.operator",
            risk="MEDIUM",
            state="WAITING_APPROVAL",
            created_at=now - timedelta(minutes=25),
            updated_at=now - timedelta(minutes=25),
        )
    )
    await session.commit()
