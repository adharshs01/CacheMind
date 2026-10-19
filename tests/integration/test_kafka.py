"""Test group G: real Kafka broker (producer delivery, consumer delivery, identity, restart).

Requires `make infra-up` (or `python -m cachemind.ops.infra up kafka`). Every
test uses its own topic so runs are isolated and repeatable.
"""

from __future__ import annotations

import uuid
from pathlib import Path

import pytest

from cachemind.bigdata.kafka_io import consume, produce, topic_end_offsets
from cachemind.data.generator import generate_dataset
from cachemind.data.workloads import WorkloadConfig
from cachemind.ops.infra import ensure_topic
from tests.conftest import require_service

pytestmark = pytest.mark.kafka


@pytest.fixture(scope="module")
def dataset(tmp_path_factory) -> Path:
    require_service("Kafka", "127.0.0.1", 9092)
    root = tmp_path_factory.mktemp("raw")
    cfg = WorkloadConfig(workload="zipf", n_events=2_000, n_items=200, n_users=50, seed=21)
    return generate_dataset(cfg, root, batch_split=(0.4, 0.4))


@pytest.fixture
def topic():
    name = f"cachemind.test.{uuid.uuid4().hex[:8]}"
    ensure_topic(name, partitions=3)
    return name


def test_producer_delivery_reports_every_message(dataset, topic):
    rep = produce(dataset_dir=dataset, topic=topic, rate=0, max_events=300)
    assert rep.sent == rep.delivered == 300 and rep.failed == 0
    assert sum(topic_end_offsets(topic).values()) == 300
    assert len(rep.partitions) == 3  # keyed by item_id -> spread over partitions


def test_consumer_receives_every_event_with_identity_preserved(dataset, topic):
    produce(dataset_dir=dataset, topic=topic, rate=0, max_events=250)
    res = consume(topic=topic, group_id=f"g-{uuid.uuid4().hex[:6]}", idle_timeout=5)
    assert res["messages"] == 250 and res["valid"] == 250
    assert res["distinct_event_ids"] == 250 and res["duplicate_event_ids"] == 0


def test_injected_duplicates_keep_the_same_event_id(dataset, topic):
    rep = produce(
        dataset_dir=dataset,
        topic=topic,
        rate=0,
        max_events=300,
        duplicate_rate=0.1,
        malformed_rate=0.05,
        seed=3,
    )
    res = consume(topic=topic, group_id=f"g-{uuid.uuid4().hex[:6]}", idle_timeout=5)
    assert res["messages"] == rep.sent
    assert res["duplicate_event_ids"] == rep.injected["duplicate"]
    assert res["distinct_event_ids"] == 300
    assert res.get("invalid:malformed_json", 0) == rep.injected["malformed"]


def test_consumer_restart_resumes_from_committed_offsets(dataset, topic):
    produce(dataset_dir=dataset, topic=topic, rate=0, max_events=200)
    group = f"g-{uuid.uuid4().hex[:6]}"
    first = consume(topic=topic, group_id=group, max_messages=80, idle_timeout=5)
    # A new consumer in the same group; allow time for the group rebalance to complete
    # (on the 8 GB development laptop JVM stalls of up to ~56 s were observed).
    second = consume(topic=topic, group_id=group, idle_timeout=90)
    assert first["messages"] == 80
    assert second["messages"] == 120  # nothing re-read, nothing skipped
    assert first["distinct_event_ids"] + second["distinct_event_ids"] == 200


def test_producer_restart_reuses_event_ids(dataset, topic):
    """A restarted producer re-sending the same range creates logical duplicates that
    carry identical event ids (removed later by Silver de-duplication)."""
    produce(dataset_dir=dataset, topic=topic, rate=0, max_events=100)
    produce(dataset_dir=dataset, topic=topic, rate=0, max_events=100)
    res = consume(topic=topic, group_id=f"g-{uuid.uuid4().hex[:6]}", idle_timeout=5)
    assert res["messages"] == 200 and res["distinct_event_ids"] == 100
