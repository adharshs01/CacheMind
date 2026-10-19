"""Test group I: real Apache HBase through the REST gateway.

Requires `make infra-up` (or `python -m cachemind.ops.infra up hbase`).
Each test run uses a unique table prefix and drops its tables afterwards.
"""

from __future__ import annotations

import uuid

import pytest

from cachemind.bigdata.hbase_store import (
    TABLES,
    HBaseError,
    HBaseRestClient,
    ensure_tables,
    experiment_row_key,
    item_row_key,
    publish_experiment_results,
    publish_item_windows,
    publish_requests,
    request_row_key,
)
from tests.conftest import require_service

pytestmark = pytest.mark.hbase


@pytest.fixture(scope="module")
def client():
    require_service("HBase REST", "127.0.0.1", 8080)
    c = HBaseRestClient(table_prefix=f"t{uuid.uuid4().hex[:8]}_")
    ensure_tables(c)
    yield c
    for name in TABLES:
        c.delete_table(name)


def _req(i: int) -> dict:
    return {
        "event_id": f"ev-{i:05d}",
        "item_id": f"i{i % 7:07d}",
        "user_id": "u0000001",
        "event_time_ms": 1767571200000 + i,
        "seq": i,
        "region": "eu-west",
        "item_size_bytes": 1000 + i,
        "dataset_id": "unit",
        "category": "web",
        "source": "kafka",
        "kafka_topic": "t",
        "kafka_partition": i % 3,
        "kafka_offset": i,
    }


def test_cluster_is_live(client):
    st = client.cluster_status()
    assert st.get("LiveNodes"), st
    assert client.version()


def test_tables_have_expected_column_families(client):
    for name, families in TABLES.items():
        schema = client.table_schema(name)
        assert schema is not None
        assert {c["name"] for c in schema["ColumnSchema"]} == set(families)


def test_ensure_tables_is_idempotent(client):
    assert set(ensure_tables(client).values()) == {"exists"}


def test_insert_read_update(client):
    assert publish_requests(client, [_req(1)]) == 1
    row = client.get_row("cache_requests", request_row_key("ev-00001"))
    assert row["request:item_id"] == "i0000001"
    assert row["metadata:region"] == "eu-west"
    assert row["lineage:offset"] == "1"
    # update one cell (put of a new value replaces it; VERSIONS=1)
    client.put_rows("cache_requests", {request_row_key("ev-00001"): {"metadata:region": "us-east"}})
    row = client.get_row("cache_requests", request_row_key("ev-00001"))
    assert row["metadata:region"] == "us-east"
    assert row["request:item_id"] == "i0000001"  # other cells untouched


def test_missing_row_returns_none(client):
    assert client.get_row("cache_requests", request_row_key("does-not-exist")) is None


def test_batch_write_and_full_scan(client):
    n = publish_requests(client, [_req(i) for i in range(100, 350)])
    assert n == 250
    keys = [k for k, _ in client.scan("cache_requests")]
    assert len([k for k in keys if k.split(":", 1)[1].startswith("ev-00")]) >= 250
    assert keys == sorted(keys)  # HBase returns rows in key order


def test_repeated_writes_are_idempotent(client):
    batch = [_req(i) for i in range(500, 560)]
    publish_requests(client, batch)
    before = client.count_rows("cache_requests")
    publish_requests(client, batch)  # replayed micro-batch
    publish_requests(client, batch)
    assert client.count_rows("cache_requests") == before


def test_window_counts_are_absolute_not_incremented(client):
    publish_item_windows(client, [("ds-a", "i0000042", 1767571200000, 5)])
    publish_item_windows(client, [("ds-a", "i0000042", 1767571200000, 5)])  # replay
    publish_item_windows(client, [("ds-a", "i0000042", 1767571260000, 2)])
    publish_item_windows(client, [("ds-b", "i0000042", 1767571200000, 9)])  # other dataset
    row = client.get_row("item_statistics", item_row_key("ds-a", "i0000042"))
    assert row["windows:1767571200000"] == "5"
    assert row["windows:1767571260000"] == "2"
    other = client.get_row("item_statistics", item_row_key("ds-b", "i0000042"))
    assert other["windows:1767571200000"] == "9"  # datasets never overwrite each other


def test_experiment_prefix_scan(client):
    results = [
        {
            "experiment": "exp1",
            "workload": "zipf",
            "policy": p,
            "capacity": c,
            "seed": 1,
            "run_id": f"r-{p}-{c}",
            "hits": 1,
            "misses": 1,
            "hit_rate": 0.5,
            "miss_rate": 0.5,
            "evictions": 0,
            "wall_time_s": 0.1,
        }
        for p in ("lru", "lfu")
        for c in (10, 100, 1000)
    ]
    publish_experiment_results(client, results)
    lru = [k for k, _ in client.scan("cache_experiments", prefix="exp1#zipf#lru#")]
    assert len(lru) == 3
    assert [int(k.split("#")[3]) for k in lru] == [10, 100, 1000]  # capacity order via padding


def test_scan_with_column_projection_and_limit(client):
    publish_requests(client, [_req(i) for i in range(700, 720)])
    rows = list(client.scan("cache_requests", columns=["request:item_id"], limit=5))
    assert len(rows) == 5
    assert all(set(cells) == {"request:item_id"} for _, cells in rows)


def test_row_keys_are_unambiguous():
    with pytest.raises(ValueError):
        experiment_row_key("exp#1", "zipf", "lru", 10, 1)  # delimiter injection rejected
    assert request_row_key("abc") != request_row_key("abd")
    assert item_row_key("ds-a", "i0000001").endswith(":ds-a#i0000001")
    assert item_row_key("ds-a", "i0000001") != item_row_key("ds-b", "i0000001")
    with pytest.raises(ValueError):
        item_row_key("ds#a", "i0000001")


def test_bad_table_raises_clear_error(client):
    with pytest.raises(HBaseError):
        client.put_rows("no_such_table_xyz", {"k": {"cf:q": "v"}})
