"""Group D (Python side, no JVM): validation rules, raw formats, generator determinism,
and an independent pure-Python re-derivation of the generator's ground truth."""

from __future__ import annotations

import csv
import hashlib
import json
from collections import Counter, defaultdict
from itertools import pairwise
from pathlib import Path

import numpy as np
import pytest

from cachemind.contracts import (
    MAX_ITEM_SIZE_BYTES,
    Reason,
    format_iso_ms,
    json_text,
    key_component,
    make_event_id,
    normalize_category,
    normalize_region,
    parse_event_time_ms,
    validate_wire_record,
)
from cachemind.data.generator import generate_dataset, iter_clean_events
from cachemind.data.raw_formats import (
    SOURCE_API_JSON,
    SOURCE_LEGACY_CSV,
    api_json_to_wire,
    parse_legacy_csv_line,
    validate_raw,
)
from cachemind.data.workloads import WORKLOADS, WorkloadConfig, WorkloadGenerator


def wire(**over):
    base = {
        "schema_version": 1,
        "event_id": "e1",
        "seq": 5,
        "dataset_id": "ds",
        "event_time": "2026-01-05T00:00:00.000Z",
        "user_id": "u1",
        "item_id": "i1",
        "region": "us-east",
        "item_size_bytes": 10,
    }
    base.update(over)
    return {k: v for k, v in base.items() if v is not ...}


@pytest.mark.parametrize(
    ("record", "reason"),
    [
        ("not a dict", Reason.MALFORMED_JSON),
        (wire(schema_version=2), Reason.UNSUPPORTED_SCHEMA),
        (wire(schema_version=...), Reason.UNSUPPORTED_SCHEMA),
        (wire(item_id=...), f"{Reason.MISSING_FIELD}:item_id"),
        (wire(user_id="   "), f"{Reason.MISSING_FIELD}:user_id"),
        (wire(event_id="bad id"), Reason.INVALID_EVENT_ID),
        (wire(dataset_id="Bad Id"), Reason.INVALID_DATASET_ID),
        (wire(seq=-1), Reason.INVALID_SEQ),
        (wire(seq=1.5), Reason.INVALID_SEQ),
        (wire(event_time="2026-01-05T00:00:00"), Reason.INVALID_TIMESTAMP),  # naive rejected
        (wire(event_time="2026-02-30T00:00:00Z"), Reason.INVALID_TIMESTAMP),
        (wire(event_time="1999-12-31T23:59:59Z"), Reason.INVALID_TIMESTAMP),  # out of range
        (wire(item_id="I 1"), Reason.INVALID_ITEM_ID),
        (wire(user_id="u/1"), Reason.INVALID_USER_ID),
        (wire(region="mars"), Reason.INVALID_REGION),
        (wire(item_size_bytes=0), Reason.NON_POSITIVE_SIZE),
        (wire(item_size_bytes=-7), Reason.NON_POSITIVE_SIZE),
        (wire(item_size_bytes="5kb"), Reason.INVALID_SIZE),
        (wire(item_size_bytes=MAX_ITEM_SIZE_BYTES + 1), Reason.INVALID_SIZE),
    ],
)
def test_each_validation_rule(record, reason):
    res = validate_wire_record(record)
    assert not res.ok and res.reason == reason


def test_first_failing_rule_wins():
    res = validate_wire_record(wire(event_id="bad id", region="mars", item_size_bytes=-1))
    assert res.reason == Reason.INVALID_EVENT_ID


def test_valid_record_is_normalised():
    res = validate_wire_record(
        wire(
            item_id="  I0000042 ",
            user_id="U9",
            region="EUW",
            event_time="2026-01-05T05:30:00.123+05:30",
            item_size_bytes="12",
            seq="7",
        )
    )
    assert res.ok
    ev = res.event
    assert (ev.item_id, ev.user_id, ev.region, ev.seq, ev.item_size_bytes) == (
        "i0000042",
        "u9",
        "eu-west",
        7,
        12,
    )
    assert ev.event_time_ms == parse_event_time_ms("2026-01-05T00:00:00.123Z")


def test_timestamp_parsing_and_formatting_round_trip():
    ms = parse_event_time_ms("2026-01-04T16:01:09.847-08:00")
    assert format_iso_ms(ms) == "2026-01-05T00:01:09.847Z"
    assert parse_event_time_ms("2026-01-05 00:00:00Z") is not None  # space separator
    assert parse_event_time_ms("2026-01-05T00:00:00.123456789Z") % 1000 == 123  # truncation
    assert parse_event_time_ms(True) is None


def test_json_text_matches_spark_string_semantics():
    assert json_text(5) == "5" and json_text(True) == "true" and json_text(None) is None
    assert json_text({"a": 1}) == '{"a":1}'


def test_category_and_region_aliases():
    assert normalize_category("  Video ") == "video"
    assert normalize_category("vid") == "video"
    assert normalize_category("misc") == "unknown" and normalize_category(None) == "unknown"
    assert normalize_region("USE") == "us-east" and normalize_region("eu_west") == "eu-west"
    assert normalize_region(42) is None


def test_event_ids_are_stable_and_distinct():
    assert make_event_id("ds", 1) == make_event_id("ds", 1)
    assert make_event_id("ds", 1) != make_event_id("ds", 2) != make_event_id("ds2", 1)


def test_key_component_rejects_delimiters():
    assert key_component("exp_1.a-b") == "exp_1.a-b"
    for bad in ("a#b", "a:b", "", "A", "x" * 65):
        with pytest.raises(ValueError):
            key_component(bad)


def test_legacy_csv_parsing_rules():
    assert parse_legacy_csv_line("a,1,2,u,i,USE,3")["region_code"] == "USE"
    assert parse_legacy_csv_line("a,1,2,u,i,USE") is None  # 6 fields
    assert parse_legacy_csv_line('a,1,2,"u",i,USE,3') is None  # quoting is not allowed
    assert validate_raw(SOURCE_LEGACY_CSV, "a,1,2,u,i,USE", "ds").reason == Reason.MALFORMED_CSV


def test_api_json_nested_type_mismatch_is_malformed():
    assert api_json_to_wire({"item": "x"}) is None
    assert validate_raw(SOURCE_API_JSON, '{"item": "x"}', "ds").reason == Reason.MALFORMED_JSON
    assert validate_raw(SOURCE_API_JSON, "{truncated", "ds").reason == Reason.MALFORMED_JSON


# --- generator ---------------------------------------------------------------


@pytest.mark.parametrize("workload", WORKLOADS)
def test_every_workload_generates_valid_deterministic_events(workload):
    cfg = WorkloadConfig(workload=workload, n_events=6_000, n_items=400, n_users=50, seed=4)
    a = [e.to_json() for e in iter_clean_events(cfg)]
    b = [e.to_json() for e in iter_clean_events(cfg)]
    assert a == b and len(a) == 6_000
    for line in a[::97]:
        assert validate_wire_record(json.loads(line)).ok
    seqs = [json.loads(x)["seq"] for x in a]
    assert seqs == list(range(6_000))
    times = [parse_event_time_ms(json.loads(x)["event_time"]) for x in a]
    assert all(t1 <= t2 for t1, t2 in pairwise(times))


def test_seeds_change_the_trace():
    def items(seed):
        cfg = WorkloadConfig(workload="zipf", n_events=2_000, n_items=300, seed=seed)
        return [e.item_id for e in iter_clean_events(cfg)]

    assert items(1) != items(2)


def test_workload_shapes():
    def counts(**kw):
        g = WorkloadGenerator(WorkloadConfig(n_events=20_000, n_items=1_000, **kw))
        return np.concatenate([b["item_index"] for b in g.blocks()]), g

    uni, _ = counts(workload="uniform")
    zipf, _ = counts(workload="zipf")
    top = lambda x: np.sort(np.bincount(x))[-10:].sum() / len(x)  # noqa: E731
    assert top(zipf) > 3 * top(uni)  # skew
    scan, g = counts(workload="scan")
    assert g.max_item_index > 1_000  # mints never-seen items beyond the catalogue
    shift, _ = counts(workload="shift")
    first, second = shift[:10_000], shift[10_000:]
    assert np.argmax(np.bincount(first)) != np.argmax(np.bincount(second))


def test_invalid_config_rejected():
    with pytest.raises(ValueError):
        WorkloadConfig(workload="nope")
    with pytest.raises(ValueError):
        WorkloadConfig(n_items=0)


def _python_reference_etl(ddir: Path, dataset_id: str) -> tuple[Counter, int]:
    """Independent re-implementation of the Silver policy over the raw files."""
    reasons: Counter = Counter()
    valid: dict[str, list[str]] = defaultdict(list)  # event_id -> payload hashes
    with (ddir / "requests_legacy.csv").open() as fh:
        next(fh)
        lines = [(SOURCE_LEGACY_CSV, ln.rstrip("\n")) for ln in fh]
    with (ddir / "requests_api.jsonl").open() as fh:
        lines += [(SOURCE_API_JSON, ln.rstrip("\n")) for ln in fh]
    for source, payload in lines:
        res = validate_raw(source, payload, dataset_id)
        if res.ok:
            valid[res.event.event_id].append(res.event.payload_hash())
        else:
            reasons[res.reason] += 1
    silver = 0
    for versions in valid.values():
        if len(set(versions)) > 1:
            reasons[Reason.CONFLICTING_DUPLICATE] += len(versions)
        else:
            silver += 1
            if len(versions) > 1:
                reasons[Reason.EXACT_DUPLICATE] += len(versions) - 1
    return reasons, silver


def test_python_reference_etl_reproduces_generator_ground_truth(tmp_path):
    cfg = WorkloadConfig(workload="bursty", n_events=8_000, n_items=500, n_users=80, seed=9)
    ddir = generate_dataset(cfg, tmp_path, anomaly_rate=0.03)
    expected = json.loads((ddir / "expected.json").read_text())["batch_expected"]
    reasons, silver = _python_reference_etl(ddir, cfg.resolved_dataset_id)
    assert dict(reasons) == expected["quarantine_by_reason"]
    assert silver == expected["silver_count"]


def test_generated_files_are_deterministic_and_hashed(tmp_path):
    cfg = WorkloadConfig(workload="zipf", n_events=3_000, n_items=200, n_users=40, seed=3)
    a = generate_dataset(cfg, tmp_path / "a")
    b = generate_dataset(cfg, tmp_path / "b")
    for name in (
        "requests_legacy.csv",
        "requests_api.jsonl",
        "stream_events.jsonl",
        "items.csv",
        "users.csv",
    ):
        assert (a / name).read_bytes() == (b / name).read_bytes(), name
    manifest = json.loads((a / "manifest.json").read_text())
    for name, info in manifest["files"].items():
        assert hashlib.sha256((a / name).read_bytes()).hexdigest() == info["sha256"]
    with (a / "requests_legacy.csv").open() as fh:
        assert next(csv.reader(fh)) == [
            "request_id",
            "seq",
            "ts_epoch_ms",
            "user",
            "item",
            "region_code",
            "bytes",
        ]
    with pytest.raises(FileExistsError):
        generate_dataset(cfg, tmp_path / "a")  # never silently overwrites
