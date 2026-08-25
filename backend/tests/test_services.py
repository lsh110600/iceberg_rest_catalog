import pytest

from app.schemas import EngineType, MaintenanceCommand
from app.services import (
    ENGINE_CAPABILITIES,
    InvalidOperationError,
    redact_metric_payload,
    validate_parameters,
)
from app.worker import verify_result


def test_metric_redaction_is_recursive_and_does_not_mutate_source() -> None:
    source = {
        "report-type": "scan-report",
        "filter": "email = 'secret@example.com'",
        "context": {"queryText": "select * from secret", "safe": "value"},
        "projected-field-names": ["email"],
    }

    result = redact_metric_payload(source)

    assert "filter" not in result
    assert "projected-field-names" not in result
    assert "queryText" not in result["context"]
    assert result["context"]["safe"] == "value"
    assert "filter" in source


def test_engine_capabilities_only_advertise_implemented_spark_commands() -> None:
    assert MaintenanceCommand.REWRITE_DATA_FILES in ENGINE_CAPABILITIES[EngineType.SPARK]
    assert MaintenanceCommand.COMPUTE_STATS in ENGINE_CAPABILITIES[EngineType.SPARK]
    assert MaintenanceCommand.STORAGE_MIGRATION not in ENGINE_CAPABILITIES[EngineType.SPARK]
    assert MaintenanceCommand.STORAGE_MIGRATION not in ENGINE_CAPABILITIES[EngineType.TRINO]
    assert MaintenanceCommand.STORAGE_MIGRATION not in ENGINE_CAPABILITIES[EngineType.FLINK]


def test_worker_verification_preserves_records_for_maintenance() -> None:
    verify_result(
        "REWRITE_DATA_FILES",
        {
            "before": {"snapshot_id": 1, "total_records": 8},
            "after": {"snapshot_id": 2, "total_records": 8},
        },
    )


def test_worker_verification_rejects_unexpected_record_count_change() -> None:
    with pytest.raises(RuntimeError, match="Record count changed"):
        verify_result(
            "REWRITE_MANIFESTS",
            {
                "before": {"snapshot_id": 1, "total_records": 8},
                "after": {"snapshot_id": 2, "total_records": 7},
            },
        )


def test_operation_parameter_validation_rejects_unknown_and_unsafe_values() -> None:
    with pytest.raises(InvalidOperationError, match="Unsupported parameters"):
        validate_parameters(MaintenanceCommand.REWRITE_MANIFESTS, {"sql": "DROP TABLE x"})
    with pytest.raises(InvalidOperationError, match="olderThanHours"):
        validate_parameters(MaintenanceCommand.REMOVE_ORPHAN_FILES, {"olderThanHours": 1})


def test_worker_verification_rejects_metadata_invariant_change() -> None:
    with pytest.raises(RuntimeError, match="location"):
        verify_result(
            "REWRITE_DATA_FILES",
            {
                "before": {"snapshot_id": 1, "total_records": 8, "location": "hdfs:///a"},
                "after": {"snapshot_id": 2, "total_records": 8, "location": "hdfs:///b"},
            },
        )
