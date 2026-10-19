"""Test groups H/K (Spark side, no Kafka): micro-batch writers, replay idempotency,
lateness classification and reconciliation.

The writers are invoked exactly as Spark's foreachBatch invokes them, with a
static DataFrame and a batch id, so replays can be simulated deterministically.
"""

from __future__ import annotations

import json
import os
import shutil
import time
from collections import Counter
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pyarrow.dataset as pads
import pytest

from cachemind.config import get_settings

pytestmark = pytest.mark.spark


@pytest.fixture(scope="module")
def spark():
    from cachemind.bigdata.spark_session import get_spark
    from cachemind.bigdata.streaming import stop_state_store

    s = get_spark("cachemind-stream-tests")
    yield s
    stop_state_store(s)  # Issue B: the JVM-wide StateStore must not outlive the session
    s.stop()


@pytest.fixture
def paths(tmp_path, monkeypatch):
    from cachemind.bigdata import streaming

    s = replace(get_settings(), data_dir=tmp_path / "data")
    monkeypatch.setattr(streaming, "get_settings", lambda: s)
    return streaming.StreamPaths(s)


def _event(i: int, ts_ms: int, item: str = "i0000001", **override) -> str:
    ev = {
        "schema_version": 1,
        "event_id": f"ev-{i}",
        "seq": i,
        "dataset_id": "unit",
        "event_time": datetime.fromtimestamp(ts_ms / 1000, UTC).strftime("%Y-%m-%dT%H:%M:%S.")
        + f"{ts_ms % 1000:03d}Z",
        "user_id": "u1",
        "item_id": item,
        "region": "us-east",
        "item_size_bytes": 10,
    }
    ev.update(override)
    return json.dumps(ev)


def bronze_batch(spark, payloads: list[str], first_offset: int = 0):
    rows = [
        (
            p,
            "kafka",
            None,
            None,
            "t",
            0,
            first_offset + i,
            None,
            "_stream",
            datetime.now(UTC),
            "test",
            1,
        )
        for i, p in enumerate(payloads)
    ]
    from cachemind.bigdata.etl import BRONZE_SCHEMA

    return spark.createDataFrame(rows, BRONZE_SCHEMA)


T0 = 1_767_571_200_000


def test_silver_writer_replay_is_idempotent(spark, paths):
    from cachemind.bigdata.streaming import SilverBatchWriter

    payloads = [_event(i, T0 + i * 1000) for i in range(50)]
    payloads += [payloads[3], "{not json", _event(99, T0, item_id="BAD ID!")]
    writer = SilverBatchWriter(paths, hbase=None)
    batch = bronze_batch(spark, payloads)
    writer(batch, 0)
    first = pads.dataset(str(paths.silver), format="parquet", partitioning="hive").to_table()
    writer(batch, 0)  # Spark replays an uncommitted batch after a crash
    second = pads.dataset(str(paths.silver), format="parquet", partitioning="hive").to_table()
    assert first.num_rows == second.num_rows == 50
    assert sorted(second.column("event_id").to_pylist()) == sorted(f"ev-{i}" for i in range(50))
    q = pads.dataset(str(paths.quarantine), format="parquet", partitioning="hive").to_table()
    reasons = sorted(q.column("dq_reason").to_pylist())
    assert reasons == ["exact_duplicate", "invalid_item_id", "malformed_json"]
    manifest = json.loads((paths.manifests / "silver_stream" / "batch_0000000000.json").read_text())
    assert manifest["silver_rows"] == 50 and manifest["quarantine_rows"] == 3


def test_late_events_flagged_against_previous_batches_and_stable_on_replay(spark, paths):
    from cachemind.bigdata.streaming import WATERMARK_MS, SilverBatchWriter

    writer = SilverBatchWriter(paths, hbase=None)
    writer(bronze_batch(spark, [_event(i, T0 + i * 60_000) for i in range(10)]), 0)
    max_seen = T0 + 9 * 60_000
    late = _event(100, max_seen - WATERMARK_MS - 1)  # beyond allowed lateness
    on_time = _event(101, max_seen - WATERMARK_MS + 1000)  # within allowed lateness
    batch1 = bronze_batch(spark, [late, on_time], first_offset=10)
    writer(batch1, 1)
    writer(batch1, 1)  # replay must classify identically
    assert writer.late_totals == {"unit": 1}  # ...and must not be counted twice
    t = pads.dataset(str(paths.silver / "batch_id=0000000001"), format="parquet").to_table()
    flags = dict(
        zip(t.column("event_id").to_pylist(), t.column("is_late").to_pylist(), strict=True)
    )
    assert flags == {"ev-100": True, "ev-101": False}


def test_window_writer_outputs_absolute_counts(spark, paths):
    from pyspark.sql import functions as F

    from cachemind.bigdata.streaming import WindowBatchWriter

    df = spark.createDataFrame(
        [("unit", "i1", datetime.fromtimestamp(T0 / 1000, UTC), 7)],
        "dataset_id string, item_id string, ts timestamp, count long",
    ).select("dataset_id", "item_id", F.window("ts", "1 minute").alias("window"), "count")
    w = WindowBatchWriter(paths, hbase=None)
    w(df, 3)
    w(df, 3)
    t = pads.dataset(str(paths.windows), format="parquet", partitioning="hive").to_table()
    assert t.num_rows == 1 and t.column("requests").to_pylist() == [7]


def test_reconcile_detects_committed_batches_without_outputs(spark, paths):
    from cachemind.bigdata.streaming import SilverBatchWriter, stream_output_consistency

    writer = SilverBatchWriter(paths, hbase=None)
    writer(bronze_batch(spark, [_event(i, T0 + i) for i in range(5)]), 0)
    commits = paths.checkpoint("silver_stream") / "commits"
    commits.mkdir(parents=True)
    (commits / "0").write_text("v1\n{}")
    (commits / "1").write_text("v1\n{}")  # Spark says batch 1 committed, but no outputs exist
    res = stream_output_consistency(paths)["silver_stream"]
    assert res["committed_batches"] == 2
    assert res["missing_manifest"] == [1] and res["missing_output_dir"] == [1]
    assert res["uncommitted_outputs"] == []


def test_fault_injection_is_one_shot(paths, monkeypatch):
    from cachemind.bigdata.streaming import _maybe_inject_fault

    _maybe_inject_fault(paths, 0)  # disabled by default: no exception
    monkeypatch.setenv("CACHEMIND_FAULT_SILVER_BATCH", "2")
    paths.ckpt.mkdir(parents=True, exist_ok=True)
    _maybe_inject_fault(paths, 1)  # other batch: no exception
    with pytest.raises(RuntimeError, match="injected fault"):
        _maybe_inject_fault(paths, 2)
    _maybe_inject_fault(paths, 2)  # replay after restart succeeds


# --- event time: exact window counts, lateness, namespaces (FINAL_CORRECTNESS_AUDIT) ------


def write_bronze_files(spark, paths, files: list[list[tuple[int, int, str]]], ds: str, tag: str):
    """One Parquet file per inner list in the Bronze stream dir, oldest mtime first.

    Each event is (i, event_time_ms, item_id); the Kafka partition is derived from the
    item (as with the item-keyed producer). Files are the unit the file source reads.
    """
    from cachemind.bigdata.etl import BRONZE_SCHEMA

    d = paths.bronze / "ingest_date=2026-10-19"
    d.mkdir(parents=True, exist_ok=True)
    mtime0 = time.time() - 10_000 + len(list(d.iterdir())) * 10
    for k, rows in enumerate(files):
        data = [
            (
                _event(i, ts, item=item, event_id=f"{ds}-ev-{i}", dataset_id=ds),
                "kafka",
                None,
                None,
                "t",
                int(item[1:]) % 3,
                i,
                None,
                "_stream",
                datetime.now(UTC),
                "test",
                1,
            )
            for i, ts, item in rows
        ]
        tmp = paths.bronze.parent / f"_tmp_{tag}_{k}"
        spark.createDataFrame(data, BRONZE_SCHEMA).coalesce(1).write.parquet(str(tmp))
        dest = d / f"part-{tag}-{k:03d}.parquet"
        shutil.move(str(next(tmp.glob("part-*.parquet"))), dest)
        shutil.rmtree(tmp)
        os.utime(dest, (mtime0 + 2 * k, mtime0 + 2 * k))


def on_time_events(n: int, start: int = T0, first: int = 0) -> list[tuple[int, int, str]]:
    """n events (ids first..first+n-1), one per second, in event-time order (never late)."""
    return [(first + i, start + i * 1000, f"i{((first + i) * 7) % 11:07d}") for i in range(n)]


def recount(events, ds: str) -> Counter:
    """Independent expectation: requests per (dataset, item, 1-minute window)."""
    return Counter((ds, item, ts // 60_000 * 60_000) for _, ts, item in events)


def run_query(start, spark, paths, max_files: int = 1):
    q = start(spark, paths, None, available_now=True, max_files=max_files, trigger_seconds=1)
    q.awaitTermination()
    assert q.exception() is None
    return q


def emitted_windows(paths) -> Counter:
    """Current count per window = row of the highest micro-batch (update mode)."""
    import pyarrow as pa

    if not paths.windows.exists():
        return Counter()
    t = pads.dataset(str(paths.windows), format="parquet", partitioning="hive").to_table()
    starts = t.column("window_start").cast(pa.timestamp("ms")).cast(pa.int64()).to_pylist()
    latest: dict[tuple, tuple[int, int]] = {}
    keys = zip(
        t.column("dataset_id").to_pylist(), t.column("item_id").to_pylist(), starts, strict=True
    )
    for key, n, b in zip(
        keys,
        t.column("requests").to_pylist(),
        t.column("stream_batch_id").to_pylist(),
        strict=True,
    ):
        if key not in latest or b > latest[key][1]:
            latest[key] = (n, b)
    return Counter({k: v[0] for k, v in latest.items()})


def watermark_drops(q) -> int:
    return sum(o.numRowsDroppedByWatermark for p in q.recentProgress for o in p.stateOperators)


def late_flags(paths) -> dict[str, bool]:
    t = pads.dataset(str(paths.silver), format="parquet", partitioning="hive").to_table(
        columns=["event_id", "is_late"]
    )
    return dict(zip(t.column("event_id").to_pylist(), t.column("is_late").to_pylist(), strict=True))


def namespace(tmp_path: Path, name: str, monkeypatch):
    from cachemind.bigdata import streaming

    s = replace(get_settings(), data_dir=tmp_path / name / "data")
    monkeypatch.setattr(streaming, "get_settings", lambda: s)
    return streaming.StreamPaths(s)


def test_windows_equal_independent_recount_and_only_the_late_event_is_dropped(spark, paths):
    from cachemind.bigdata.streaming import WATERMARK_MS, start_gold_windows, start_silver_stream

    events = on_time_events(600)  # 10 minutes
    late = (999, T0 + 100_000, "i0000005")  # event time 100 s, arrives after 599 s was seen
    tail = (1000, T0 + 599_500, "i0000005")
    write_bronze_files(spark, paths, [events[:300], events[300:], [late], [tail]], "unit", "a")
    run_query(start_silver_stream, spark, paths)
    q = run_query(start_gold_windows, spark, paths)

    # Silver: exactly the delayed event is late (100 s < 599 s - watermark delay)
    flags = late_flags(paths)
    assert T0 + 100_000 < T0 + 599_000 - WATERMARK_MS
    assert [e for e, is_late in flags.items() if is_late] == ["unit-ev-999"]
    # Windows: every on-time event counted exactly once; the late one dropped and counted
    assert emitted_windows(paths) == recount([*events, tail], "unit")
    assert watermark_drops(q) == 1
    # Restart with no new data changes nothing (idempotent replay from checkpoint)
    before = emitted_windows(paths)
    run_query(start_gold_windows, spark, paths)
    assert emitted_windows(paths) == before


def test_one_namespace_is_one_event_time_timeline(spark, tmp_path, monkeypatch):
    """dsB's timeline lies before dsA's: in a shared namespace dsB is entirely late and
    dropped (Spark's watermark is global per query); in its own namespace it is exact."""
    from cachemind.bigdata.streaming import start_gold_windows, start_silver_stream

    ev_a = on_time_events(600, T0 + 3_600_000)
    ev_b = on_time_events(600, T0)
    shared = namespace(tmp_path, "shared", monkeypatch)
    write_bronze_files(spark, shared, [ev_a[:300], ev_a[300:]], "dsa", "a")
    run_query(start_silver_stream, spark, shared)
    run_query(start_gold_windows, spark, shared)
    write_bronze_files(spark, shared, [ev_b[:300], ev_b[300:]], "dsb", "b")
    run_query(start_silver_stream, spark, shared)
    q = run_query(start_gold_windows, spark, shared)
    flags = late_flags(shared)
    assert sum(v for k, v in flags.items() if k.startswith("dsb")) == 600
    assert sum(v for k, v in flags.items() if k.startswith("dsa")) == 0
    got = emitted_windows(shared)
    assert Counter({k: v for k, v in got.items() if k[0] == "dsa"}) == recount(ev_a, "dsa")
    assert not [k for k in got if k[0] == "dsb"]
    assert watermark_drops(q) == 600  # visible in the metric, not silent

    for ds, evs in (("dsa", ev_a), ("dsb", ev_b)):
        own = namespace(tmp_path, f"own_{ds}", monkeypatch)
        write_bronze_files(spark, own, [evs[:300], evs[300:]], ds, "x")
        run_query(start_silver_stream, spark, own)
        run_query(start_gold_windows, spark, own)
        assert not any(late_flags(own).values())
        assert emitted_windows(own) == recount(evs, ds)


def test_splitting_a_bronze_batch_across_micro_batches_inflates_late_flags(
    spark, tmp_path, monkeypatch
):
    """Why Bronze is written as ONE file per micro-batch: with one file per Kafka
    partition, maxFilesPerTrigger can split a Bronze batch and on-time events of the
    lagging partitions look late. Whole Bronze batches flag nothing."""
    from cachemind.bigdata.streaming import start_silver_stream

    events = on_time_events(720)
    batches = [events[0:240], events[240:480], events[480:720]]
    per_partition = namespace(tmp_path, "per_partition", monkeypatch)
    files = [[e for e in b if int(e[2][1:]) % 3 == p] for b in batches for p in range(3)]
    write_bronze_files(spark, per_partition, files, "unit", "p")
    run_query(start_silver_stream, spark, per_partition, max_files=4)
    assert sum(late_flags(per_partition).values()) > 0  # 107 of 720 on-time events

    whole = namespace(tmp_path, "whole", monkeypatch)
    write_bronze_files(spark, whole, batches, "unit", "w")
    run_query(start_silver_stream, spark, whole, max_files=1)
    assert len(late_flags(whole)) == 720 and not any(late_flags(whole).values())


def test_bronze_projection_is_one_partition_per_micro_batch(spark):
    from cachemind.bigdata.streaming import BRONZE_STREAM_SCHEMA, bronze_projection

    rows = [
        (b"k", _event(i, T0 + i).encode(), "t", i % 3, i // 3, datetime.now(UTC), 0)
        for i in range(30)
    ]
    kafka_like = spark.createDataFrame(
        rows,
        "key binary, value binary, topic string, partition int, offset long, "
        "timestamp timestamp, timestampType int",
    ).repartition(3, "partition")
    out = bronze_projection(kafka_like, "run-x")
    assert out.rdd.getNumPartitions() == 1
    assert out.columns == [f.name for f in BRONZE_STREAM_SCHEMA]
    assert out.count() == 30


def test_stop_state_store_ends_maintenance_and_state_stays_usable(spark, paths):
    from cachemind.bigdata.streaming import start_gold_windows, stop_state_store

    store = spark._jvm.org.apache.spark.sql.execution.streaming.state.StateStore
    write_bronze_files(spark, paths, [on_time_events(120)], "unit", "s")
    run_query(start_gold_windows, spark, paths)
    assert store.isMaintenanceRunning()  # outlives the finished query
    stop_state_store(spark)
    assert not store.isMaintenanceRunning()
    later = on_time_events(10, T0 + 120_000, first=120)  # new event ids
    write_bronze_files(spark, paths, [later], "unit", "t")
    run_query(start_gold_windows, spark, paths)  # checkpointed state reloads fine
    assert emitted_windows(paths) == recount(on_time_events(120) + later, "unit")


def test_stop_state_store_refuses_while_a_query_is_active():
    from types import SimpleNamespace

    from cachemind.bigdata.streaming import stop_state_store

    fake = SimpleNamespace(streams=SimpleNamespace(active=[SimpleNamespace(name="q")]))
    with pytest.raises(RuntimeError, match="queries are active"):
        stop_state_store(fake)
