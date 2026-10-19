"""Test group J: the complete pipeline on real Kafka + Spark + HBase.

generate -> Bronze (files) -> Kafka -> Structured Streaming -> Silver/Gold -> HBase ->
features -> MLlib -> exported + Spark-batch predictions -> cache simulation -> HBase.

Runs in an isolated data directory and Kafka topic. Requires `make infra-up`.
"""

from __future__ import annotations

import uuid

import pytest

from cachemind.config import get_settings
from tests.conftest import require_service

pytestmark = [pytest.mark.e2e, pytest.mark.kafka, pytest.mark.hbase, pytest.mark.slow]


@pytest.fixture
def isolated(tmp_path, monkeypatch):
    monkeypatch.setenv("CACHEMIND_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("CACHEMIND_ARTIFACTS_DIR", str(tmp_path / "artifacts"))
    monkeypatch.setenv("CACHEMIND_REPORTS_DIR", str(tmp_path / "reports"))
    monkeypatch.setenv("CACHEMIND_STREAM_RUNTIME_DIR", str(tmp_path / "stream_runtime"))
    monkeypatch.setenv("CACHEMIND_KAFKA_TOPIC", f"cachemind.e2e.{uuid.uuid4().hex[:8]}")
    get_settings.cache_clear()
    from cachemind.ops.infra import ensure_topic

    ensure_topic()
    yield tmp_path
    get_settings.cache_clear()


def test_full_pipeline_end_to_end(isolated):
    require_service("Kafka", "127.0.0.1", 9092)
    require_service("HBase REST", "127.0.0.1", 8080)
    from cachemind.experiments.pipeline_demo import run_pipeline

    res = run_pipeline(events=8_000, items=800, capacity=40, seed=23)
    failed = {k: v for k, v in res["checks"].items() if not v["ok"]}
    assert res["all_checks_passed"], failed
    for required in (
        "bronze_rows_equal_raw_lines",
        "all_messages_delivered",
        "streaming_outputs_consistent",
        "silver_matches_ground_truth_for_historical_sources",
        "every_streamed_event_reached_silver_exactly_once",
        "hbase_item_statistics_match_gold",
        "mllib_export_parity",
        "trace_equals_silver",
        "experiment_rows_in_hbase",
        "lineage_event_in_hbase",
        "stream_windows_never_overcount",
        "stream_windows_count_every_on_time_event",
        "late_flags_bounded_by_injected_late_events",
    ):
        assert res["checks"][required]["ok"], required
    # the run streamed into its own namespace under the (isolated) data dir
    iso = res["isolation"]
    assert iso["data_dir"].startswith(str(isolated / "data" / "pipeline_runs"))
    assert iso["kafka_topic"] == f"cachemind.demo.{res['run_id']}"
    stages = res["stages"]
    assert len(stages) == 12
    lineage = stages["12_lineage"]["result"]
    assert lineage["hbase_cache_requests_row"]["request:event_id"] == lineage["event_id"]
