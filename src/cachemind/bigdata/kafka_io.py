"""Kafka producer (real-time request generator) and a verification consumer.

Producer guarantees and limits
------------------------------
* ``enable.idempotence=true`` + ``acks=all``: librdkafka retries cannot create
  broker-side duplicates *within one producer session*.
* Event ids are deterministic (UUIDv5 of dataset_id/seq), so a message re-sent
  by an application-level retry or a restarted producer carries the same
  event_id and is removed by the Silver de-duplication. Kafka itself does not
  deduplicate across producer sessions — that is the documented boundary.
* The local queue is bounded (``queue.buffering.max.messages``); when it is full
  the producer blocks on ``poll`` instead of growing memory.
* Every message gets a delivery report; failures are counted and reported, and
  the exit code is non-zero when any message was not delivered.
* Message key = item_id, so all requests for one item land in one partition
  (per-item order). There is NO global order across partitions; replay order
  comes from the event's ``seq`` (see simulation.trace).
"""

from __future__ import annotations

import argparse
import json
import random
import signal
import sys
import time
from collections import Counter
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer, TopicPartition

from cachemind.config import get_settings
from cachemind.contracts import validate_wire_json
from cachemind.logging_utils import get_logger, log_event

log = get_logger("cachemind.kafka")

PRODUCER_CONFIG: dict[str, Any] = {
    "enable.idempotence": True,
    "acks": "all",
    "linger.ms": 20,
    "batch.num.messages": 1000,
    "compression.type": "lz4",
    "queue.buffering.max.messages": 20_000,
    "queue.buffering.max.kbytes": 32_768,
    "message.timeout.ms": 60_000,
    "retry.backoff.ms": 200,
    "client.id": "cachemind-producer",
}


@dataclass
class ProduceReport:
    topic: str
    requested: int = 0
    sent: int = 0
    delivered: int = 0
    failed: int = 0
    injected: Counter = field(default_factory=Counter)
    partitions: Counter = field(default_factory=Counter)
    errors: list[str] = field(default_factory=list)
    interrupted: bool = False
    elapsed_s: float = 0.0
    first_seq: int | None = None
    last_seq: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "topic": self.topic,
            "requested": self.requested,
            "sent": self.sent,
            "delivered": self.delivered,
            "failed": self.failed,
            "injected": dict(self.injected),
            "partitions": {str(k): v for k, v in sorted(self.partitions.items())},
            "errors": self.errors[:20],
            "interrupted": self.interrupted,
            "elapsed_s": round(self.elapsed_s, 3),
            "achieved_rate_per_s": round(self.sent / self.elapsed_s, 1) if self.elapsed_s else None,
            "first_seq": self.first_seq,
            "last_seq": self.last_seq,
        }


def _file_events(path: Path, skip: int, limit: int | None) -> Iterator[str]:
    with path.open() as fh:
        for i, line in enumerate(fh):
            if i < skip:
                continue
            if limit is not None and i >= skip + limit:
                return
            yield line.rstrip("\n")


def _live_events(seed: int, limit: int | None, workload: str) -> Iterator[str]:
    """New events stamped with the current wall-clock time (live demo mode)."""
    from cachemind.contracts import CanonicalEvent, make_event_id
    from cachemind.data.generator import iter_clean_events
    from cachemind.data.workloads import WorkloadConfig

    day = datetime.now(UTC).strftime("%Y%m%d%H%M%S")
    ds = f"live-{workload}-{day}-s{seed}"
    cfg = WorkloadConfig(
        workload=workload,
        n_events=limit or 10_000_000,
        n_items=5_000,
        n_users=2_000,
        seed=seed,
        dataset_id=ds,
    )
    for ev in iter_clean_events(cfg):
        now_ms = int(time.time() * 1000)
        live = CanonicalEvent(
            make_event_id(ds, ev.seq),
            ev.seq,
            ds,
            now_ms,
            ev.user_id,
            ev.item_id,
            ev.region,
            ev.item_size_bytes,
        )
        yield live.to_json()


def produce(
    *,
    dataset_dir: Path | None = None,
    live: bool = False,
    rate: float = 200.0,
    max_events: int | None = None,
    skip: int = 0,
    topic: str | None = None,
    seed: int = 7,
    duplicate_rate: float = 0.0,
    malformed_rate: float = 0.0,
    late_rate: float = 0.0,
    late_by: int = 500,
    workload: str = "zipf",
    flush_timeout: float = 60.0,
    extra_config: dict[str, Any] | None = None,
) -> ProduceReport:
    s = get_settings()
    topic = topic or s.kafka_topic
    report = ProduceReport(topic=topic)
    producer = Producer(
        {"bootstrap.servers": s.kafka_bootstrap, **PRODUCER_CONFIG, **(extra_config or {})}
    )
    rnd = random.Random(seed)
    stop = {"flag": False}

    def _sigint(_sig: int, _frm: Any) -> None:
        stop["flag"] = True

    previous = signal.signal(signal.SIGINT, _sigint)

    def on_delivery(err: KafkaError | None, msg: Any) -> None:
        if err is not None:
            report.failed += 1
            if len(report.errors) < 50:
                report.errors.append(str(err))
        else:
            report.delivered += 1
            report.partitions[msg.partition()] += 1

    def send(value: str, key: str | None) -> None:
        while True:
            try:
                producer.produce(
                    topic,
                    value=value.encode(),
                    key=key.encode() if key else None,
                    on_delivery=on_delivery,
                )
                report.sent += 1
                break
            except BufferError:
                producer.poll(0.5)  # bounded local queue is full: wait for deliveries
        producer.poll(0)

    if live:
        events = _live_events(seed, max_events, workload)
    else:
        if dataset_dir is None:
            raise ValueError("dataset_dir is required unless live=True")
        events = _file_events(dataset_dir / "stream_events.jsonl", skip, max_events)

    delayed: list[tuple[int, str, str]] = []  # (release_after_n_sent, value, key)
    interval = 1.0 / rate if rate > 0 else 0.0
    t0 = time.monotonic()
    next_at = t0
    try:
        for line in events:
            if stop["flag"]:
                report.interrupted = True
                break
            report.requested += 1
            ev = json.loads(line)
            key = ev.get("item_id")
            if report.first_seq is None:
                report.first_seq = ev.get("seq")
            report.last_seq = ev.get("seq")
            if interval:
                now = time.monotonic()
                if next_at > now:
                    time.sleep(next_at - now)
                next_at = max(next_at + interval, time.monotonic() - 1.0)
            if late_rate and rnd.random() < late_rate:
                delayed.append((report.sent + late_by, line, key))
                report.injected["late"] += 1
            else:
                send(line, key)
            if duplicate_rate and rnd.random() < duplicate_rate:
                send(line, key)  # same event_id again: a logical duplicate
                report.injected["duplicate"] += 1
            if malformed_rate and rnd.random() < malformed_rate:
                send(line[: len(line) // 2], key)  # truncated JSON
                report.injected["malformed"] += 1
            while delayed and delayed[0][0] <= report.sent:
                _, v, k = delayed.pop(0)
                send(v, k)
        for _, v, k in delayed:  # release remaining late events at the end
            send(v, k)
    finally:
        remaining = producer.flush(flush_timeout)
        if remaining:
            report.failed += remaining
            report.errors.append(f"{remaining} messages still queued after flush timeout")
        signal.signal(signal.SIGINT, previous)
        report.elapsed_s = time.monotonic() - t0
    log_event(log, "producer finished", **report.to_dict())
    out = s.runtime_dir / "producer"
    out.mkdir(parents=True, exist_ok=True)
    (out / "last_run.json").write_text(json.dumps(report.to_dict(), indent=2) + "\n")
    return report


def consume(
    *,
    topic: str | None = None,
    group_id: str = "cachemind-verify",
    max_messages: int | None = None,
    idle_timeout: float = 10.0,
    from_beginning: bool = True,
    commit: bool = True,
) -> dict[str, Any]:
    """Read and validate messages; offsets are committed after processing (at-least-once)."""
    s = get_settings()
    topic = topic or s.kafka_topic
    consumer = Consumer(
        {
            "bootstrap.servers": s.kafka_bootstrap,
            "group.id": group_id,
            "auto.offset.reset": "earliest" if from_beginning else "latest",
            "enable.auto.commit": False,
            "enable.partition.eof": False,
            "isolation.level": "read_committed",
        }
    )
    consumer.subscribe([topic])
    counts: Counter = Counter()
    event_ids: set[str] = set()
    duplicates = 0
    last_msg = time.monotonic()
    try:
        while max_messages is None or counts["messages"] < max_messages:
            msg = consumer.poll(1.0)
            if msg is None:
                if time.monotonic() - last_msg > idle_timeout:
                    break
                continue
            if msg.error():
                raise KafkaException(msg.error())
            last_msg = time.monotonic()
            counts["messages"] += 1
            res = validate_wire_json(msg.value())
            if res.ok and res.event is not None:
                counts["valid"] += 1
                if res.event.event_id in event_ids:
                    duplicates += 1
                event_ids.add(res.event.event_id)
            else:
                counts[f"invalid:{res.reason}"] += 1
            if commit:
                consumer.commit(message=msg, asynchronous=False)
    finally:
        positions = consumer.position(
            [
                TopicPartition(topic, p)
                for p in consumer.list_topics(topic, timeout=5).topics[topic].partitions
            ]
        )
        consumer.close()
    return {
        "topic": topic,
        "group_id": group_id,
        **counts,
        "distinct_event_ids": len(event_ids),
        "duplicate_event_ids": duplicates,
        "positions": {tp.partition: tp.offset for tp in positions},
    }


def topic_end_offsets(topic: str | None = None) -> dict[int, int]:
    s = get_settings()
    topic = topic or s.kafka_topic
    c = Consumer(
        {
            "bootstrap.servers": s.kafka_bootstrap,
            "group.id": "cachemind-offsets",
            "enable.auto.commit": False,
        }
    )
    try:
        parts = c.list_topics(topic, timeout=10).topics[topic].partitions
        return {p: c.get_watermark_offsets(TopicPartition(topic, p), timeout=10)[1] for p in parts}
    finally:
        c.close()


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="CacheMind Kafka request producer")
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--dataset", help="dataset id under data/raw (sends stream_events.jsonl)")
    src.add_argument("--live", action="store_true", help="generate new events stamped 'now'")
    ap.add_argument("--rate", type=float, default=200.0, help="events per second (0 = unthrottled)")
    ap.add_argument("--max-events", type=int)
    ap.add_argument("--skip", type=int, default=0, help="skip the first N stream events")
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--duplicate-rate", type=float, default=0.0)
    ap.add_argument("--malformed-rate", type=float, default=0.0)
    ap.add_argument("--late-rate", type=float, default=0.0)
    ap.add_argument("--late-by", type=int, default=500, help="delay late events by N messages")
    args = ap.parse_args(argv)
    from cachemind.config import safe_child

    dataset_dir = safe_child(get_settings().raw_dir, args.dataset) if args.dataset else None
    rep = produce(
        dataset_dir=dataset_dir,
        live=args.live,
        rate=args.rate,
        max_events=args.max_events,
        skip=args.skip,
        seed=args.seed,
        duplicate_rate=args.duplicate_rate,
        malformed_rate=args.malformed_rate,
        late_rate=args.late_rate,
        late_by=args.late_by,
    )
    print(json.dumps(rep.to_dict(), indent=2))
    return 0 if rep.failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
