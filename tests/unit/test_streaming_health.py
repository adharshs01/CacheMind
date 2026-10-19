"""Streaming metrics and health (no JVM): watermark-drop accounting, current-run health
kept apart from the error history, and isolation of the pipeline demo."""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime

import pyarrow as pa
import pyarrow.parquet as pq

from cachemind.config import get_settings

# Spark 4.0.4 progress of a windows micro-batch whose 18 input rows were all behind the
# watermark (shape copied from .runtime/streaming/status.json): the aggregation is
# listed first and reports 0; the dedupe-within-watermark operator dropped the rows.
WINDOWS_PROGRESS = {
    "name": "gold_windows_v2",
    "batchId": 14,
    "timestamp": "2026-10-19T09:50:05.259Z",
    "numInputRows": 18,
    "eventTime": {"watermark": "2026-01-06T07:25:33.729Z"},
    "stateOperators": [
        {"operatorName": "stateStoreSave", "numRowsTotal": 98, "numRowsDroppedByWatermark": 0},
        {
            "operatorName": "dedupeWithinWatermark",
            "numRowsTotal": 161,
            "numRowsDroppedByWatermark": 18,
        },
    ],
    "sources": [],
}

PY4J_HBASE = (
    "py4j.Py4JException: An exception was raised by the Python Proxy. Return Message: "
    "Traceback (most recent call last):\n"
    '  File ".../streaming.py", line 259, in __call__\n'
    "    hbase_rows = publish_requests(self.hbase, (r.asDict() for r in records))\n"
    "cachemind.bigdata.hbase_store.HBaseUnavailable: HBase REST unreachable at "
    "http://127.0.0.1:8080: HTTPConnectionPool(host='127.0.0.1', port=8080)\n"
    "\n\tat py4j.Protocol.getReturnValue(Protocol.java:476)\n"
)
PY4J_INJECTED = (
    "py4j.Py4JException: An exception was raised by the Python Proxy. Return Message: "
    "Traceback (most recent call last):\n"
    '    raise RuntimeError(f"injected fault after Parquet write of silver batch {batch_id}")\n'
    "RuntimeError: injected fault after Parquet write of silver batch 0\n"
    "\n\tat py4j.Protocol.getReturnValue(Protocol.java:476)\n"
)
SCHEMA = (
    "org.apache.spark.sql.execution.streaming.state.StateStoreKeySchemaNotCompatible: "
    "[STATE_STORE_KEY_SCHEMA_NOT_COMPATIBLE] Provided key schema does not match\n"
    "Please check number and type of fields.\n"
    "\tat org.apache.spark.util.LazyTry.tryT(LazyTry.scala:46)\n"
)


def test_dropped_by_watermark_is_summed_over_all_state_operators():
    from cachemind.bigdata.streaming import progress_record

    rec = progress_record(WINDOWS_PROGRESS)
    assert rec["dropped_by_watermark"] == 18  # the first operator alone reports 0
    assert rec["state_rows"] == 98 + 161
    assert [o["operator"] for o in rec["state_operators"]] == [
        "stateStoreSave",
        "dedupeWithinWatermark",
    ]
    stateless = progress_record({"name": "bronze_ingest", "stateOperators": []})
    assert stateless["dropped_by_watermark"] is None and stateless["state_rows"] is None


def test_error_class_finds_the_real_error_inside_py4j_wrappers():
    from cachemind.monitoring.health import error_class

    assert error_class(PY4J_HBASE) == "HBaseUnavailable"
    assert error_class(PY4J_INJECTED) == "RuntimeError"
    assert error_class(SCHEMA) == "StateStoreKeySchemaNotCompatible"
    assert error_class("no class here") == "unknown"


def _write_jsonl(path, records):
    path.write_text("".join(json.dumps(r) + "\n" for r in records))


def test_health_separates_current_run_from_error_history(tmp_path, monkeypatch):
    from cachemind.monitoring import health

    rt = tmp_path / "stream_runtime"
    rt.mkdir()
    s = replace(get_settings(), stream_runtime_override=rt)
    monkeypatch.setattr(health, "get_settings", lambda: s)
    started = "2026-10-19T10:00:00+00:00"
    (rt / "status.json").write_text(
        json.dumps(
            {"run_id": "r2", "mode": "available_now", "state": "stopped", "started_at": started}
        )
    )
    _write_jsonl(
        rt / "progress.jsonl",
        [
            # legacy records (no run_id) from failure-injection runs before this run
            {"event": "terminated", "id": "a", "exception": PY4J_HBASE, "ts": "2026-10-19T08:00"},
            {
                "event": "terminated",
                "id": "b",
                "exception": PY4J_INJECTED,
                "ts": "2026-10-19T08:01",
            },
            {"event": "terminated", "id": "c", "exception": PY4J_HBASE, "run_id": "r1"},
            {**json.loads(json.dumps(WINDOWS_PROGRESS)), "event": "progress", "run_id": "r2"},
            {"event": "terminated", "id": "d", "exception": None, "run_id": "r2"},
        ],
    )
    h = health.streaming_health()
    assert h["current_run"]["run_id"] == "r2"
    assert h["current_run"]["queries_terminated_with_error"] == 0
    assert h["history"]["queries_terminated_with_error"] == 3
    assert h["history"]["by_error_class"] == {"HBaseUnavailable": 2, "RuntimeError": 1}
    assert "terminated_with_error" not in h  # the ambiguous mixed counter is gone

    with (rt / "progress.jsonl").open("a") as fh:
        fh.write(json.dumps({"event": "terminated", "exception": SCHEMA, "run_id": "r2"}) + "\n")
    with (rt / "progress.jsonl").open("a") as fh:
        fh.write('{"event": "progress", "name": "silver_st')  # being appended right now
    h = health.streaming_health()
    assert h["current_run"]["queries_terminated_with_error"] == 1
    assert h["current_run"]["errors"][0]["error"] == "StateStoreKeySchemaNotCompatible"


def test_stream_runtime_dir_defaults_to_shared_dir_and_can_be_isolated(tmp_path):
    s = get_settings()
    assert s.stream_runtime_dir == s.runtime_dir / "streaming"
    assert replace(s, stream_runtime_override=tmp_path).stream_runtime_dir == tmp_path


def test_pipeline_demo_streams_into_its_own_namespace(tmp_path, monkeypatch):
    from cachemind.experiments import pipeline_demo

    monkeypatch.setenv("CACHEMIND_DATA_DIR", str(tmp_path / "data"))
    get_settings.cache_clear()
    try:
        env = pipeline_demo.isolation_env("e2e-x-1")
        with pipeline_demo._environment(env):
            s = get_settings()
            assert s.data_dir == tmp_path / "data" / "pipeline_runs" / "e2e-x-1"
            assert s.checkpoint_dir.is_relative_to(s.data_dir)
            assert s.kafka_topic == "cachemind.demo.e2e-x-1"
            assert s.stream_runtime_dir.is_relative_to(s.data_dir)
        assert get_settings().data_dir == tmp_path / "data"  # restored afterwards
    finally:
        get_settings.cache_clear()


def test_demo_window_check_uses_latest_batch_and_detects_over_and_undercount(tmp_path):
    from cachemind.experiments.pipeline_demo import stream_window_check

    t0 = int(datetime(2026, 1, 5, tzinfo=UTC).timestamp() * 1000)
    events = [{"item_id": "i1", "event_time_ms": t0 + k * 1000} for k in range(3)]  # 3 in w0
    events += [{"item_id": "i2", "event_time_ms": t0 + 60_000}]  # 1 in w1

    def batch(b, rows):
        d = tmp_path / "w" / f"batch_id={b:010d}"
        d.mkdir(parents=True)
        pq.write_table(
            pa.table(
                {
                    "item_id": [r[0] for r in rows],
                    "window_start": pa.array([r[1] for r in rows], pa.timestamp("ms")),
                    "requests": [r[2] for r in rows],
                    "stream_batch_id": [b] * len(rows),
                }
            ),
            d / "part-0.parquet",
        )

    batch(0, [("i1", t0, 2)])  # running count, later updated
    batch(1, [("i1", t0, 3), ("i2", t0 + 60_000, 1)])
    ok = stream_window_check(tmp_path / "w", events)
    assert ok == {
        "events_expected": 4,
        "events_in_windows": 4,
        "windows_expected": 2,
        "windows_exact": 2,
        "windows_overcounted": 0,
    }
    batch(2, [("i2", t0 + 60_000, 2)])  # an overcount must be detected
    assert stream_window_check(tmp_path / "w", events)["windows_overcounted"] == 1
    missing = stream_window_check(tmp_path / "nothing", events)
    assert missing["events_in_windows"] == 0 and missing["windows_exact"] == 0
