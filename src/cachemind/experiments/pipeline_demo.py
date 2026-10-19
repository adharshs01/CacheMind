"""End-to-end pipeline demonstration with stored, verifiable evidence (Profile C).

    python -m cachemind.experiments.pipeline_demo --events 20000 --capacity 100

Stages (each records counts + timings in reports/pipeline/<run_id>/evidence.json):

 1  generate      synthetic dataset: legacy CSV + API JSON + stream events + metadata
 2  bronze        Spark: historical files -> Bronze (raw lines + file/line lineage)
 3  produce       Kafka: stream portion -> topic (with injected duplicates/malformed/late)
 4  stream        Spark Structured Streaming (available-now): Kafka -> Bronze ->
                  provisional Silver + HBase cache_requests, event-time windows -> HBase
 5  reconcile     streaming outputs complete? HBase rows present?
 6  silver        Spark batch: ALL Bronze (files + Kafka) -> canonical Silver + quarantine,
                  verified against generator ground truth
 7  gold          Spark aggregates; item statistics published to HBase (absolute values)
 8  features      Spark window features + labels on canonical Silver
 9  mllib         Spark MLlib LR + RF: train/select/evaluate/save/reload/export/parity,
                  batch scoring of the whole trace with the reloaded Spark model
10  simulate      cache policies on the Silver trace: classical + Belady + ML using
                  (a) the exported model online and (b) the Spark model's batch predictions
11  publish       experiment results -> HBase cache_experiments; SQL summary view
12  lineage       one event traced from its Kafka offset to its eviction decision

Requires Kafka + HBase (``make infra-up``). Every stage fails loudly; nothing is
reported as successful unless it ran.

Isolation: every run uses its own data directory (``<data>/pipeline_runs/<run_id>/``),
Kafka topic (``cachemind.demo.<run_id>``) and streaming status files. All synthetic
datasets start on 2026-01-05, and a checkpoint namespace is one event-time timeline,
so a demo streamed into the shared namespace after any later-ending dataset was late
in its entirety and absent from the windows. Evidence, models and HBase rows are
written to the usual shared locations.
"""

from __future__ import annotations

import argparse
import json
import os
import time
import uuid
from collections import Counter
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from cachemind.config import get_settings, safe_child
from cachemind.logging_utils import get_logger, log_event

log = get_logger("cachemind.pipeline")


class Evidence:
    def __init__(self, run_id: str) -> None:
        self.run_id = run_id
        self.dir = safe_child(get_settings().reports_dir, "pipeline", run_id)
        self.dir.mkdir(parents=True, exist_ok=True)
        self.data: dict[str, Any] = {
            "run_id": run_id,
            "started_at": datetime.now(UTC).isoformat(),
            "stages": {},
            "checks": {},
        }

    def stage(self, name: str, result: Any, seconds: float) -> None:
        self.data["stages"][name] = {"seconds": round(seconds, 2), "result": result}
        self.save()
        log_event(log, "stage done", stage=name, seconds=round(seconds, 2))

    def check(self, name: str, ok: bool, detail: Any = None) -> None:
        self.data["checks"][name] = {"ok": bool(ok), "detail": detail}
        self.save()
        if not ok:
            raise AssertionError(f"pipeline check failed: {name}: {detail}")

    def save(self) -> None:
        (self.dir / "evidence.json").write_text(json.dumps(self.data, indent=2, default=str))


def isolation_env(run_id: str) -> dict[str, str]:
    """Environment of an isolated demo run (resolved against the CURRENT settings)."""
    root = get_settings().data_dir / "pipeline_runs" / run_id
    return {
        "CACHEMIND_DATA_DIR": str(root),
        "CACHEMIND_KAFKA_TOPIC": f"cachemind.demo.{run_id}",
        "CACHEMIND_STREAM_RUNTIME_DIR": str(root / "_stream_runtime"),
    }


@contextmanager
def _environment(values: dict[str, str]) -> Iterator[None]:
    old = {k: os.environ.get(k) for k in values}
    os.environ.update(values)
    get_settings.cache_clear()
    try:
        yield
    finally:
        for k, v in old.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
        get_settings.cache_clear()


def stream_window_check(windows_dir: Path, kafka_events: list[dict[str, Any]]) -> dict[str, Any]:
    """Streaming window counts vs counts recomputed from canonical Silver.

    ``kafka_events``: the distinct valid streamed events of the dataset (event_id,
    item_id, event_time_ms). In update mode the row of the highest batch id is a
    window's current count.
    """
    import pyarrow as pa
    import pyarrow.dataset as pads

    truth: Counter[tuple[str, int]] = Counter(
        (e["item_id"], e["event_time_ms"] // 60_000 * 60_000) for e in kafka_events
    )
    emitted: dict[tuple[str, int], tuple[int, int]] = {}
    if windows_dir.exists():
        t = pads.dataset(str(windows_dir), format="parquet", partitioning="hive").to_table(
            columns=["item_id", "window_start", "requests", "stream_batch_id"]
        )
        starts = t.column("window_start").cast(pa.timestamp("ms")).cast(pa.int64()).to_pylist()
        for item, ws, n, b in zip(
            t.column("item_id").to_pylist(),
            starts,
            t.column("requests").to_pylist(),
            t.column("stream_batch_id").to_pylist(),
            strict=True,
        ):
            if (item, ws) not in emitted or b > emitted[(item, ws)][1]:
                emitted[(item, ws)] = (n, b)
    keys = set(truth) | set(emitted)
    counts = {k: emitted.get(k, (0, 0))[0] for k in keys}
    return {
        "events_expected": sum(truth.values()),
        "events_in_windows": sum(counts.values()),
        "windows_expected": len(truth),
        "windows_exact": sum(1 for k in keys if counts[k] == truth.get(k, 0)),
        "windows_overcounted": sum(1 for k in keys if counts[k] > truth.get(k, 0)),
    }


def run_pipeline(
    events: int = 20_000,
    items: int = 1_500,
    capacity: int = 100,
    horizon_factor: int = 5,
    workload: str = "zipf",
    seed: int = 11,
    rate: float = 0.0,
) -> dict[str, Any]:
    run_id = f"e2e-{datetime.now(UTC).strftime('%Y%m%dT%H%M%S')}-{uuid.uuid4().hex[:4]}"
    with _environment(isolation_env(run_id)):
        return _run_pipeline(run_id, events, items, capacity, horizon_factor, workload, seed, rate)


def _run_pipeline(
    run_id: str,
    events: int,
    items: int,
    capacity: int,
    horizon_factor: int,
    workload: str,
    seed: int,
    rate: float,
) -> dict[str, Any]:
    from cachemind.bigdata.etl import build_silver, ingest_bronze_files, read_silver
    from cachemind.bigdata.features_spark import build_features
    from cachemind.bigdata.gold import build_gold
    from cachemind.bigdata.hbase_store import (
        HBaseRestClient,
        ensure_tables,
        item_row_key,
        publish_experiment_results,
        request_row_key,
    )
    from cachemind.bigdata.kafka_io import produce
    from cachemind.bigdata.mllib_train import load_trace_predictions, train_mllib_models
    from cachemind.bigdata.spark_session import get_spark
    from cachemind.bigdata.streaming import reconcile, run_streaming
    from cachemind.cli import cmd_hbase_publish
    from cachemind.data.generator import generate_dataset
    from cachemind.data.workloads import WorkloadConfig
    from cachemind.experiments.common import Workbench, make_policy, result_row, run_policy
    from cachemind.ml.dataset import build_reuse_dataset
    from cachemind.ml.model_spec import load_scorer
    from cachemind.monitoring.health import hbase_health, kafka_health
    from cachemind.ops.infra import ensure_topic
    from cachemind.simulation.trace import Trace

    s = get_settings()
    ev = Evidence(run_id)
    ev.data["isolation"] = {
        "data_dir": str(s.data_dir),
        "kafka_topic": s.kafka_topic,
        "checkpoints": str(s.checkpoint_dir),
        "stream_runtime": str(s.stream_runtime_dir),
    }
    k = kafka_health()
    ev.check("kafka_reachable", k["ok"], k)
    h = hbase_health()
    ev.check("hbase_reachable", h["ok"], h)
    ensure_topic()
    ensure_tables()

    # 1 generate -------------------------------------------------------------------
    t = time.time()
    ds_id = f"e2e-{workload}-n{events}-s{seed}-{run_id[-4:]}"
    cfg = WorkloadConfig(
        workload=workload,
        n_events=events,
        n_items=items,
        n_users=max(100, items // 3),
        seed=seed,
        event_rate_per_sec=1.0,
        dataset_id=ds_id,
    )
    ddir = generate_dataset(cfg, s.raw_dir, batch_split=(0.4, 0.4))
    expected = json.loads((ddir / "expected.json").read_text())
    ev.stage(
        "1_generate",
        {"dataset_id": ds_id, "raw_rows": expected["raw_rows"], "anomalies": expected["anomalies"]},
        time.time() - t,
    )

    spark = get_spark("cachemind-e2e", kafka=True)
    try:
        # 2 bronze (files) -----------------------------------------------------------
        t = time.time()
        b = ingest_bronze_files(spark, s, ddir)
        ev.stage("2_bronze_files", b, time.time() - t)
        ev.check(
            "bronze_rows_equal_raw_lines",
            b["sources"]["legacy_csv"]["rows"] == expected["raw_rows"]["legacy_csv"]
            and b["sources"]["api_json"]["rows"] == expected["raw_rows"]["api_json"],
            {"bronze": b["sources"], "raw": expected["raw_rows"]},
        )

        # 3 produce -------------------------------------------------------------------
        t = time.time()
        rep = produce(
            dataset_dir=ddir,
            rate=rate,
            seed=seed,
            duplicate_rate=0.01,
            malformed_rate=0.005,
            late_rate=0.01,
            late_by=200,
        )
        ev.stage("3_kafka_produce", rep.to_dict(), time.time() - t)
        ev.check(
            "all_messages_delivered",
            rep.failed == 0 and rep.delivered == rep.sent,
            {"sent": rep.sent, "delivered": rep.delivered, "failed": rep.failed},
        )
    finally:
        spark.stop()  # the streaming job builds its own session with identical config

    # 4 streaming ------------------------------------------------------------------------
    t = time.time()
    st = run_streaming(available_now=True, with_hbase=True, max_offsets=2000, max_files=4)
    ev.stage("4_streaming", {k: v for k, v in st.items() if k != "queries"}, time.time() - t)

    # 5 reconcile ---------------------------------------------------------------------
    t = time.time()
    rec = reconcile(repair=False)
    ev.stage("5_reconcile", rec, time.time() - t)
    ev.check("streaming_outputs_consistent", rec["consistent"], rec)

    spark = get_spark("cachemind-e2e", kafka=True)
    try:
        # 6 canonical silver (files + Kafka Bronze) -------------------------------------
        t = time.time()
        silver = build_silver(spark, s, ds_id, include_stream=True)
        ev.stage(
            "6_silver",
            {
                k: silver[k]
                for k in (
                    "bronze_rows_by_source",
                    "quarantine_by_reason",
                    "silver_rows",
                    "row_accounting_ok",
                    "verification",
                )
            },
            time.time() - t,
        )
        ver = silver["verification"]
        ev.check(
            "silver_matches_ground_truth_for_historical_sources",
            ver["batch_silver_count"]["ok"]
            and ver["batch_silver_checksum"]["ok"]
            and ver["batch_quarantine_by_reason"]["ok"],
            ver,
        )
        kafka_rows = spark.read.parquet(
            str(s.silver_dir / "requests" / f"dataset_id={ds_id}")
        ).where("source = 'kafka'")
        n_rows = kafka_rows.count()
        n_stream = kafka_rows.select("event_id").distinct().count()
        ev.check(
            "every_streamed_event_reached_silver_exactly_once",
            n_rows == n_stream == expected["stream_clean_events"],
            {
                "kafka_rows_in_silver": n_rows,
                "distinct_kafka_events_in_silver": n_stream,
                "clean_stream_events_sent": expected["stream_clean_events"],
            },
        )
        ev.check(
            "row_accounting",
            silver["row_accounting_ok"],
            "bronze rows == silver rows + quarantined rows",
        )

        # streaming event-time outputs vs canonical Silver (independent recount)
        import pyarrow.dataset as pads

        from cachemind.bigdata.streaming import StreamPaths

        win = stream_window_check(
            StreamPaths(s).windows,
            [r.asDict() for r in kafka_rows.select("item_id", "event_time_ms").collect()],
        )
        flags = (
            pads.dataset(
                str(s.lake_dir / "silver_stream" / "requests"),
                format="parquet",
                partitioning="hive",
            )
            .to_table(columns=["is_late"], filter=pads.field("dataset_id") == ds_id)
            .column("is_late")
            .to_pylist()
        )
        injected_late = int(rep.injected.get("late", 0))
        ev.data["stream_event_time"] = {
            **win,
            "streaming_run": st.get("event_time"),
            "provisional_silver_late_flags": int(sum(flags)),
            "injected_late_events": injected_late,
        }
        ev.check("stream_windows_never_overcount", win["windows_overcounted"] == 0, win)
        ev.check(
            "stream_windows_count_every_on_time_event",
            win["events_in_windows"] >= win["events_expected"] - injected_late,
            {**win, "injected_late_events": injected_late},
        )
        ev.check(
            "late_flags_bounded_by_injected_late_events",
            sum(flags) <= injected_late,
            {"late_flags": int(sum(flags)), "injected_late_events": injected_late},
        )

        # 7 gold + HBase item statistics ---------------------------------------------
        t = time.time()
        gold = build_gold(spark, s, ds_id)
        pub = argparse.Namespace(dataset=ds_id, experiments=False)
        cmd_hbase_publish(pub)
        client = HBaseRestClient()
        top = (
            spark.read.parquet(str(s.gold_dir / "item_request_counts" / f"dataset_id={ds_id}"))
            .orderBy("popularity_rank")
            .head(1)[0]
        )
        hrow = client.get_row("item_statistics", item_row_key(ds_id, top["item_id"]))
        ev.stage(
            "7_gold_and_hbase",
            {"tables": gold["tables"], "top_item": top["item_id"], "hbase_row": hrow},
            time.time() - t,
        )
        ev.check(
            "hbase_item_statistics_match_gold",
            hrow is not None and int(hrow["access:total_requests"]) == top["total_requests"],
            {"gold": top["total_requests"], "hbase": (hrow or {}).get("access:total_requests")},
        )

        # 8 features + 9 MLlib --------------------------------------------------------
        horizon = horizon_factor * capacity
        t = time.time()
        feats = build_features(spark, s, ds_id, horizon)
        ev.stage("8_features", feats, time.time() - t)
        t = time.time()
        models = train_mllib_models(spark, s, ds_id, horizon, seed=seed, max_train_rows=200_000)
        ev.stage(
            "9_mllib",
            [
                {
                    "artifact": m["artifact"],
                    "model_type": m["model_type"],
                    "version": m["model_version"],
                    "params": m["selected_params"],
                    "test": {
                        k2: m["metrics"]["test@val_threshold"][k2]
                        for k2 in ("roc_auc", "pr_auc", "f1", "brier", "positive_rate")
                    },
                    "baselines": {b2: v["roc_auc"] for b2, v in m["baselines_test"].items()},
                    "parity": m["parity"],
                    "latency": m["latency"],
                }
                for m in models
            ],
            time.time() - t,
        )
        ev.check(
            "mllib_export_parity",
            all(m["parity"]["passed"] for m in models),
            [m["parity"] for m in models],
        )
        silver_count = read_silver(spark, s, ds_id).count()
    finally:
        spark.stop()

    # 10 simulate ---------------------------------------------------------------------
    t = time.time()
    trace = Trace.from_silver(s.silver_dir / "requests", ds_id)
    ev.check(
        "trace_equals_silver",
        len(trace) == silver_count,
        {"trace": len(trace), "silver": silver_count},
    )
    wb = Workbench(trace=trace, dataset=build_reuse_dataset(trace, horizon))
    rows = []
    for name in ("fifo", "lru", "lfu", "belady"):
        r = run_policy(wb, make_policy(name), capacity)
        rows.append(
            result_row("e2e_pipeline", wb, r, workload=workload, seed=seed, horizon=horizon)
        )
    for m in models:
        art = Path(m["artifact"])
        wb.models[m["model_type"]] = load_scorer(art)
        wb.model_reports[m["model_type"]] = m
        r_online = run_policy(wb, make_policy("ml", wb, m["model_type"], horizon), capacity)
        rows.append(
            result_row(
                "e2e_pipeline",
                wb,
                r_online,
                workload=workload,
                seed=seed,
                model=m["model_type"],
                horizon=horizon,
                runtime="exported-numpy",
                extra={"predictions": "online"},
            )
        )
        pre = load_trace_predictions(art, len(trace))
        r_batch = run_policy(
            wb, make_policy("ml", wb, m["model_type"], horizon, precomputed=pre), capacity
        )
        rows.append(
            result_row(
                "e2e_pipeline",
                wb,
                r_batch,
                workload=workload,
                seed=seed,
                model=m["model_type"],
                horizon=horizon,
                runtime="spark-mllib",
                extra={"predictions": "spark-batch"},
            )
        )
        ev.check(
            f"online_export_equals_spark_batch_{m['model_type']}",
            (r_online.hits, r_online.evictions) == (r_batch.hits, r_batch.evictions),
            {"online_hits": r_online.hits, "spark_batch_hits": r_batch.hits},
        )
    ev.stage(
        "10_simulate",
        [
            {
                k2: r[k2]
                for k2 in (
                    "policy",
                    "runtime",
                    "capacity",
                    "hits",
                    "misses",
                    "hit_rate",
                    "evictions",
                    "wall_time_s",
                )
            }
            for r in rows
        ],
        time.time() - t,
    )
    ev.check(
        "hits_plus_misses", all(r["hits"] + r["misses"] == r["measured_requests"] for r in rows)
    )

    # 11 publish experiment results -----------------------------------------------------
    t = time.time()
    for row in rows:
        if row.get("runtime"):
            row["policy"] = f"{row['policy']}-{row['runtime'].split('-')[0]}"
        row["workload"] = workload
    n = publish_experiment_results(HBaseRestClient(), rows)
    from cachemind.experiments.common import save_results

    save_results("e2e_pipeline", rows, {"run_id": run_id, "dataset_id": ds_id})
    back = list(HBaseRestClient().scan("cache_experiments", prefix=f"e2e_pipeline#{workload}#"))
    ev.stage(
        "11_publish", {"hbase_rows_written": n, "hbase_rows_read_back": len(back)}, time.time() - t
    )
    ev.check("experiment_rows_in_hbase", len(back) >= n, {"written": n, "read": len(back)})

    # 12 lineage: follow one streamed event end to end --------------------------------
    import pyarrow.dataset as pads

    sv = (
        pads.dataset(
            str(s.silver_dir / "requests" / f"dataset_id={ds_id}"),
            format="parquet",
            partitioning="hive",
        )
        .to_table(
            columns=["event_id", "seq", "item_id", "source", "kafka_partition", "kafka_offset"],
            filter=pads.field("source") == "kafka",
        )
        .slice(0, 1)
        .to_pylist()[0]
    )
    pos = int((trace.seq == sv["seq"]).nonzero()[0][0])
    hb = HBaseRestClient().get_row("cache_requests", request_row_key(sv["event_id"]))
    ev.stage(
        "12_lineage",
        {
            "event_id": sv["event_id"],
            "kafka": {"partition": sv["kafka_partition"], "offset": sv["kafka_offset"]},
            "silver_seq": sv["seq"],
            "trace_position": pos,
            "item_id": sv["item_id"],
            "hbase_cache_requests_row": hb,
            "feature_row_label": int(wb.dataset.label[pos]),
            "in_test_block": pos >= wb.bounds.test_start,
        },
        0.0,
    )
    ev.check("lineage_event_in_hbase", hb is not None and hb["request:item_id"] == sv["item_id"])
    ev.data["finished_at"] = datetime.now(UTC).isoformat()
    ev.data["all_checks_passed"] = all(c["ok"] for c in ev.data["checks"].values())
    ev.save()
    return ev.data


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--events", type=int, default=20_000)
    ap.add_argument("--items", type=int, default=1_500)
    ap.add_argument("--capacity", type=int, default=100)
    ap.add_argument("--workload", default="zipf")
    ap.add_argument("--seed", type=int, default=11)
    ap.add_argument("--rate", type=float, default=0.0, help="producer events/s (0 = max)")
    args = ap.parse_args(argv)
    res = run_pipeline(
        events=args.events,
        items=args.items,
        capacity=args.capacity,
        workload=args.workload,
        seed=args.seed,
        rate=args.rate,
    )
    print(
        json.dumps(
            {
                "run_id": res["run_id"],
                "all_checks_passed": res["all_checks_passed"],
                "checks": {k: v["ok"] for k, v in res["checks"].items()},
                "evidence": str(get_settings().reports_dir / "pipeline" / res["run_id"]),
            },
            indent=2,
        )
    )
    return 0 if res["all_checks_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
