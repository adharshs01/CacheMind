"""Streaming event-time correctness on real Kafka (+ HBase): Bronze layout, lateness,
window conservation and HBase window cells, in an isolated namespace.

Requires `make infra-up`. Everything (data dir, topic, checkpoints, streaming status
files) is private to the test, so the shared namespace is never touched.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import uuid
from collections import Counter

import pyarrow as pa
import pyarrow.dataset as pads
import pytest

from cachemind.config import REPO_ROOT, get_settings
from tests.conftest import require_service

pytestmark = [pytest.mark.kafka, pytest.mark.hbase, pytest.mark.spark, pytest.mark.slow]


@pytest.fixture
def isolated(tmp_path, monkeypatch):
    monkeypatch.setenv("CACHEMIND_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("CACHEMIND_STREAM_RUNTIME_DIR", str(tmp_path / "stream_runtime"))
    monkeypatch.setenv("CACHEMIND_KAFKA_TOPIC", f"cachemind.test.{uuid.uuid4().hex[:8]}")
    get_settings.cache_clear()
    yield tmp_path
    get_settings.cache_clear()


def _stream_once() -> dict:
    """`cachemind stream run --available-now` as its own process (as `make streaming-once`).

    A separate JVM also guarantees the Kafka connector is on the classpath even when an
    earlier test in this pytest process already started Spark without it.
    """
    out = subprocess.run(
        [
            sys.executable,
            "-m",
            "cachemind.bigdata.streaming",
            "run",
            "--available-now",
            "--max-offsets",
            "500",
            "--max-files",
            "2",
        ],
        env={**os.environ, "CACHEMIND_SPARK_DRIVER_MEMORY": "768m"},
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
        timeout=900,
        check=False,
    )
    assert out.returncode == 0, out.stderr[-3000:]
    assert "RpcEndpointNotFoundException" not in out.stderr  # Issue B shutdown order
    return json.loads(out.stdout[out.stdout.index("{") :])


def _bronze_commits(paths) -> list[list[str]]:
    log = paths.bronze / "_spark_metadata"
    out = []
    for f in sorted((p for p in log.iterdir() if p.name.isdigit()), key=lambda p: int(p.name)):
        out.append([json.loads(x)["path"] for x in f.read_text().splitlines()[1:] if x])
    return out


def test_in_order_stream_keeps_bronze_batches_whole_and_windows_conserve_events(isolated):
    require_service("Kafka", "127.0.0.1", 9092)
    require_service("HBase REST", "127.0.0.1", 8080)
    from cachemind.bigdata.hbase_store import HBaseRestClient, ensure_tables, item_row_key
    from cachemind.bigdata.kafka_io import produce
    from cachemind.bigdata.streaming import WATERMARK_MS, StreamPaths
    from cachemind.data.generator import generate_dataset
    from cachemind.data.workloads import WorkloadConfig
    from cachemind.ops.infra import ensure_topic

    s = get_settings()
    ensure_topic(partitions=3)
    ensure_tables()
    ds = f"kafka-test-{uuid.uuid4().hex[:6]}"
    # 1 event/s as in the pipeline demo: a 500-offset Bronze batch spans ~8 minutes of
    # event time, so a split Bronze batch WOULD produce false late flags and lost events
    # (old layout, same input: 1 604 of 3 000 flagged late, 2 958 of 3 000 counted)
    cfg = WorkloadConfig(
        workload="zipf",
        n_events=3_000,
        n_items=300,
        n_users=100,
        seed=5,
        dataset_id=ds,
        event_rate_per_sec=1.0,
    )
    ddir = generate_dataset(cfg, s.raw_dir, batch_split=(0.0, 0.0))  # everything streamed
    rep = produce(dataset_dir=ddir, rate=0)  # in event-time order, no injected faults
    assert rep.failed == 0 and rep.delivered == 3_000

    # small batches: several Bronze micro-batches, two files per downstream trigger
    st = _stream_once()
    paths = StreamPaths(s)

    # 1. Bronze: exactly one file per committed micro-batch, every offset once
    commits = _bronze_commits(paths)
    assert len(commits) >= 6 and all(len(files) == 1 for files in commits), commits
    bronze = pads.dataset(str(paths.bronze), format="parquet", partitioning="hive").to_table()
    offsets = list(
        zip(
            bronze.column("kafka_partition").to_pylist(),
            bronze.column("kafka_offset").to_pylist(),
            strict=True,
        )
    )
    assert len(offsets) == len(set(offsets)) == 3_000

    # 2. No Silver micro-batch splits a Bronze micro-batch
    silver = pads.dataset(str(paths.silver), format="parquet", partitioning="hive").to_table()
    sv = silver.to_pandas()
    assert sv.event_id.nunique() == len(sv) == 3_000
    file_of = {}
    for i, files in enumerate(commits):
        t = pads.dataset(files[0].replace("file://", ""), format="parquet").to_table(
            columns=["kafka_partition", "kafka_offset"]
        )
        for key in zip(*(t.column(c).to_pylist() for c in t.column_names), strict=True):
            file_of[key] = i
    sv["bronze_batch"] = [
        file_of[(p, o)] for p, o in zip(sv.kafka_partition, sv.kafka_offset, strict=True)
    ]
    assert (sv.groupby("bronze_batch").stream_batch_id.nunique() == 1).all()

    # 3. Late flags == the documented policy recomputed independently from the outputs
    expected_late, running_max = {}, None
    for b in sorted(sv.stream_batch_id.unique()):
        rows = sv[sv.stream_batch_id == b]
        ref = running_max
        for e, t in zip(rows.event_id, rows.event_time_ms, strict=True):
            expected_late[e] = ref is not None and t < ref - WATERMARK_MS
        m = int(rows.event_time_ms.max())
        running_max = m if running_max is None else max(running_max, m)
    assert dict(zip(sv.event_id, sv.is_late, strict=True)) == expected_late
    assert st["event_time"]["silver_late_rows"] == sum(expected_late.values())

    # 4. Windows: never overcount; emitted + dropped-by-watermark == distinct events
    truth = Counter(
        (item, t // 60_000 * 60_000) for item, t in zip(sv.item_id, sv.event_time_ms, strict=True)
    )
    w = pads.dataset(str(paths.windows), format="parquet", partitioning="hive").to_table()
    starts = w.column("window_start").cast(pa.timestamp("ms")).cast(pa.int64()).to_pylist()
    latest: dict[tuple[str, int], tuple[int, int]] = {}
    for item, ws, n, b in zip(
        w.column("item_id").to_pylist(),
        starts,
        w.column("requests").to_pylist(),
        w.column("stream_batch_id").to_pylist(),
        strict=True,
    ):
        if (item, ws) not in latest or b > latest[(item, ws)][1]:
            latest[(item, ws)] = (n, b)
    emitted = {k: v[0] for k, v in latest.items()}
    assert all(n <= truth[k] for k, n in emitted.items())
    dropped = st["event_time"]["windows_dropped_by_watermark"]
    assert sum(emitted.values()) + dropped == 3_000
    # nothing was produced late, and whole Bronze batches keep every event countable
    assert dropped == 0 and emitted == dict(truth)
    assert st["event_time"]["windows_input_rows"] == 3_000

    # 5. HBase holds the same absolute window counts (sampled items)
    client = HBaseRestClient()
    for item in sorted({k[0] for k in emitted})[:25]:
        row = client.get_row("item_statistics", item_row_key(ds, item)) or {}
        cells = {
            int(k.split(":", 1)[1]): int(v) for k, v in row.items() if k.startswith("windows:")
        }
        want = {ws: n for (it, ws), n in emitted.items() if it == item}
        assert cells == want, item

    # 6. A restart with no new data commits nothing and changes nothing
    before = (len(_bronze_commits(paths)), len(list(paths.silver.glob("batch_id=*"))))
    st2 = _stream_once()
    assert (len(_bronze_commits(paths)), len(list(paths.silver.glob("batch_id=*")))) == before
    assert st2["event_time"]["silver_late_rows"] == 0
