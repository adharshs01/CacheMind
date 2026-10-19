"""Test group K (infrastructure): failure injection against real Kafka/Spark/HBase.

Each scenario restarts or kills real processes and then VERIFIES the data:
Bronze exactly-once per Kafka offset, every clean event in Silver and HBase,
and no incomplete micro-batch. Slow (~10-15 minutes). Requires `make infra-up`.
"""

from __future__ import annotations

import pytest

from tests.conftest import require_service

pytestmark = [pytest.mark.e2e, pytest.mark.kafka, pytest.mark.hbase, pytest.mark.slow]


def test_failure_scenarios_recover_without_loss_or_duplication(tmp_path):
    require_service("Kafka", "127.0.0.1", 9092)
    require_service("HBase REST", "127.0.0.1", 8080)
    from cachemind.experiments.streaming_experiments import exp12_failures

    results = exp12_failures(tmp_path)
    failed = [(r["scenario"], r.get("checks"), r.get("notes")) for r in results if not r["passed"]]
    assert not failed, failed
    names = {r["scenario"] for r in results}
    assert {
        "duplicate_malformed_late",
        "spark_process_killed",
        "kafka_broker_restart",
        "hbase_unavailable",
        "partial_output_failure",
        "redrain_without_new_data",
    } <= names
    partial = next(r for r in results if r["scenario"] == "partial_output_failure")
    assert partial["fault_triggered"] and partial["parquet_written_before_restart"]
    assert not partial["manifest_written_before_restart"]  # the inconsistent window existed
