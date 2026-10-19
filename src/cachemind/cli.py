"""CacheMind command-line interface (``cachemind <command> ...`` or ``python -m cachemind``).

Lightweight (no JVM):   generate, simulate, train, evaluate, experiments
Spark (local JVM):      bronze, silver, gold, etl, features, mllib, rdd, sql, spark-experiments
Services (Kafka/HBase): produce, stream, reconcile, hbase-init, hbase-publish, hbase-verify,
                        health, pipeline-demo
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any

from cachemind.config import get_settings, safe_child


def _print(obj: Any) -> None:
    print(json.dumps(obj, indent=2, default=str))


def _dataset_dir(dataset: str) -> Path:
    d = safe_child(get_settings().raw_dir, dataset)
    if not d.exists():
        raise SystemExit(
            f"dataset {dataset!r} not found under {get_settings().raw_dir} "
            "(run `cachemind generate` first)"
        )
    return d


# --- lightweight ------------------------------------------------------------------


def cmd_generate(a: argparse.Namespace) -> int:
    from cachemind.data.generator import generate_dataset
    from cachemind.data.workloads import WorkloadConfig

    cfg = WorkloadConfig.from_profile(
        a.profile,
        workload=a.workload,
        seed=a.seed,
        **({"dataset_id": a.dataset_id} if a.dataset_id else {}),
    )
    t0 = time.time()
    out = generate_dataset(
        cfg, get_settings().raw_dir, anomaly_rate=a.anomaly_rate, overwrite=a.overwrite
    )
    expected = json.loads((out / "expected.json").read_text())
    _print(
        {
            "dataset_id": cfg.resolved_dataset_id,
            "path": str(out),
            "synthetic": True,
            "seconds": round(time.time() - t0, 1),
            "raw_rows": expected["raw_rows"],
            "anomalies": expected["anomalies"],
        }
    )
    return 0


def _load_trace(a: argparse.Namespace):
    from cachemind.data.workloads import WorkloadConfig
    from cachemind.simulation.trace import Trace

    if getattr(a, "dataset", None):
        return Trace.from_silver(get_settings().silver_dir / "requests", a.dataset)
    cfg = WorkloadConfig.from_profile(a.profile, workload=a.workload, seed=a.seed)
    return Trace.from_generator(cfg)


def cmd_simulate(a: argparse.Namespace) -> int:
    import numpy as np

    from cachemind.experiments.common import Workbench, make_policy
    from cachemind.ml.dataset import build_reuse_dataset
    from cachemind.ml.model_spec import ModelLoadError, load_scorer
    from cachemind.simulation.engine import simulate

    trace = _load_trace(a)
    policies = (
        ["fifo", "lru", "lfu", "belady"]
        if a.policy == "classical"
        else (["fifo", "lru", "lfu", "belady", "ml", "hybrid"] if a.policy == "all" else [a.policy])
    )
    wb, horizon, precomputed, model_type = None, None, None, None
    if {"ml", "hybrid"} & set(policies):
        if not a.model_dir:
            raise SystemExit("--model-dir is required for ml/hybrid policies")
        model_dir = Path(a.model_dir)
        meta = json.loads((model_dir / "metadata.json").read_text())
        horizon = int(meta["label_horizon"])
        model_type = meta["model_type"]
        wb = Workbench(trace=trace, dataset=build_reuse_dataset(trace, horizon))
        try:
            wb.models[meta["model_type"]] = load_scorer(model_dir)
        except ModelLoadError as exc:  # policies fall back to LRU and report why
            print(
                f"WARNING: model not usable, ML policies will fall back to LRU: {exc}",
                file=sys.stderr,
            )
        if a.precomputed:
            from cachemind.bigdata.mllib_train import load_trace_predictions

            precomputed = load_trace_predictions(model_dir, len(trace))
    measure_from = int(len(trace) * a.measure_from)
    rows = []
    for name in policies:
        pol = make_policy(
            name,
            wb,
            model_type,
            horizon,
            alpha=a.alpha,
            precomputed=precomputed if name in ("ml", "hybrid") else None,
        )
        res = simulate(
            trace.keys,
            pol,
            a.capacity,
            measure_from=measure_from,
            item_ids=trace.item_ids,
            decision_log_limit=20,
        )
        rows.append(
            {
                "policy": name,
                "capacity": a.capacity,
                "requests": res.n_requests,
                "hits": res.hits,
                "misses": res.misses,
                "hit_rate": round(res.hit_rate, 5),
                "evictions_measured": res.evictions_measured,
                "evictions_total": res.evictions,
                "wall_time_s": round(res.wall_time_s, 3),
                "fallback_evictions": res.policy_stats.get("fallback_evictions"),
            }
        )
    out = get_settings().reports_dir / "simulations"
    out.mkdir(parents=True, exist_ok=True)
    report = {"trace": trace.describe(), "measure_from": measure_from, "results": rows}
    (out / f"sim_{trace.dataset_id}_c{a.capacity}.json").write_text(
        json.dumps(report, indent=2, default=str)
    )
    _print(report)
    return 0 if np.isfinite([r["hit_rate"] for r in rows]).all() else 1


def cmd_train(a: argparse.Namespace) -> int:
    from cachemind.ml.dataset import build_reuse_dataset
    from cachemind.ml.sklearn_train import train_sklearn_models

    trace = _load_trace(a)
    ds = build_reuse_dataset(trace, a.horizon)
    reps = train_sklearn_models(ds, get_settings().models_dir, seed=a.seed)
    _print(
        [
            {
                "artifact": r["artifact"],
                "model_type": r["model_type"],
                "test": {
                    k: r["metrics"]["test@val_threshold"][k]
                    for k in ("roc_auc", "pr_auc", "f1", "brier", "positive_rate")
                },
                "parity": r["parity"],
            }
            for r in reps
        ]
    )
    return 0


def cmd_evaluate(a: argparse.Namespace) -> int:
    root = Path(a.model_dir) if a.model_dir else get_settings().models_dir
    metas = sorted(root.glob("**/metadata.json"))
    if not metas:
        raise SystemExit(f"no model artifacts under {root}")
    out = []
    for m in metas:
        meta = json.loads(m.read_text())
        parity = json.loads((m.parent / "parity.json").read_text())
        t = meta["metrics"]["test@val_threshold"]
        out.append(
            {
                "artifact": str(m.parent),
                "runtime": meta["runtime"],
                "model_type": meta["model_type"],
                "version": meta["model_version"],
                "horizon": meta["label_horizon"],
                "roc_auc": t["roc_auc"],
                "pr_auc": t["pr_auc"],
                "f1": t["f1"],
                "brier": t["brier"],
                "ece": t["ece"],
                "parity_passed": parity["passed"],
            }
        )
    _print(out)
    return 0


# --- Spark --------------------------------------------------------------------------


def _spark(name: str, **kw: Any):
    from cachemind.bigdata.spark_session import get_spark

    return get_spark(name, **kw)


def cmd_etl(a: argparse.Namespace) -> int:
    from cachemind.bigdata.etl import build_silver, ingest_bronze_files
    from cachemind.bigdata.gold import build_gold

    s = get_settings()
    spark = _spark(f"cachemind-{a.stage}")
    out: dict[str, Any] = {}
    try:
        if a.stage in ("bronze", "etl"):
            out["bronze"] = ingest_bronze_files(
                spark, s, _dataset_dir(a.dataset), include_stream_file=a.include_stream_file
            )
        if a.stage in ("silver", "etl"):
            rep = build_silver(spark, s, a.dataset, include_stream=not a.no_stream)
            out["silver"] = {
                k: rep[k]
                for k in ("silver_rows", "quarantine_by_reason", "row_accounting_ok", "seconds")
            }
            out["silver"]["verification"] = rep.get("verification", {}).get("all_ok")
        if a.stage in ("gold", "etl"):
            out["gold"] = build_gold(spark, s, a.dataset)
    finally:
        spark.stop()
    _print(out)
    ver = out.get("silver", {}).get("verification")
    return 1 if ver is False else 0


def cmd_features(a: argparse.Namespace) -> int:
    from cachemind.bigdata.features_spark import build_features

    spark = _spark("cachemind-features")
    try:
        _print(build_features(spark, get_settings(), a.dataset, a.horizon))
    finally:
        spark.stop()
    return 0


def cmd_mllib(a: argparse.Namespace) -> int:
    from cachemind.bigdata.mllib_train import train_mllib_models

    spark = _spark("cachemind-mllib")
    try:
        reps = train_mllib_models(spark, get_settings(), a.dataset, a.horizon, seed=a.seed)
    finally:
        spark.stop()
    _print(
        [
            {
                "artifact": r["artifact"],
                "model_type": r["model_type"],
                "selected_params": r["selected_params"],
                "test": {
                    k: r["metrics"]["test@val_threshold"][k]
                    for k in ("roc_auc", "pr_auc", "f1", "brier", "positive_rate")
                },
                "parity": r["parity"],
                "latency": r["latency"],
            }
            for r in reps
        ]
    )
    return 0 if all(r["parity"]["passed"] for r in reps) else 1


def cmd_rdd(a: argparse.Namespace) -> int:
    from cachemind.bigdata.rdd_demo import main

    return main(["--dataset", a.dataset])


def cmd_sql(a: argparse.Namespace) -> int:
    from cachemind.bigdata.sql_demo import main

    return main(["--dataset", a.dataset])


# --- services -----------------------------------------------------------------------


def cmd_hbase_init(a: argparse.Namespace) -> int:
    from cachemind.bigdata.hbase_store import ensure_tables

    _print(ensure_tables())
    return 0


def cmd_hbase_publish(a: argparse.Namespace) -> int:
    """Publish Gold item statistics (+ experiment summaries) to HBase as absolute values."""
    import pandas as pd
    import pyarrow.parquet as pq

    from cachemind.bigdata.hbase_store import (
        HBaseRestClient,
        publish_experiment_results,
        publish_item_statistics,
    )

    s = get_settings()
    client = HBaseRestClient()
    out: dict[str, Any] = {}
    if a.dataset:
        g = s.gold_dir
        items = pq.read_table(
            str(safe_child(g, "item_request_counts", f"dataset_id={a.dataset}"))
        ).to_pandas()
        recent = pq.read_table(
            str(safe_child(g, "recent_access_frequency", f"dataset_id={a.dataset}"))
        ).to_pandas()
        trend = pq.read_table(
            str(safe_child(g, "item_popularity_trend", f"dataset_id={a.dataset}"))
        ).to_pandas()
        df = items.merge(recent[["item_id", "requests_24h", "as_of_ms"]], on="item_id", how="left")
        if len(trend):
            df = df.merge(trend[["item_id", "trend_score"]], on="item_id", how="left")
        else:
            df["trend_score"] = None
        total = df["total_requests"].sum()
        stats = [
            {
                "item_id": r.item_id,
                "total_requests": int(r.total_requests),
                "recent_requests": None if pd.isna(r.requests_24h) else int(r.requests_24h),
                "last_access_ms": int(r.last_access.timestamp() * 1000),
                "dataset_id": a.dataset,
                "popularity_rank": int(r.popularity_rank),
                "popularity_score": round(float(r.total_requests) / total, 8),
                "trend_score": None if pd.isna(r.trend_score) else float(r.trend_score),
                "window_start_ms": int(r.first_access.timestamp() * 1000),
                "window_end_ms": int(r.last_access.timestamp() * 1000),
            }
            for r in df.itertuples()
        ]
        out["item_statistics_rows"] = publish_item_statistics(client, stats)
    if a.experiments:
        import pyarrow.parquet as pq_

        from cachemind.experiments.results import (
            CACHE_RESULT_COLUMNS,
            OPTIONAL_COLUMNS,
            cache_result_files,
        )

        files = cache_result_files(s.reports_dir)
        rows = []
        for f in files:
            names = set(pq_.read_schema(f).names)
            cols = [*CACHE_RESULT_COLUMNS, *(c for c in OPTIONAL_COLUMNS if c in names)]
            rows += pd.read_parquet(f, columns=cols).to_dict("records")
        out["cache_result_files"] = [f.parent.name for f in files]
        latest = {}
        for r in rows:  # one row per key: experiment#workload#policy#capacity#seed
            wl = str(r["workload"]).replace("@", "-at-").replace(".", "_")
            latest[(r["experiment"], wl, r["policy"], r["capacity"], r["seed"])] = {
                **r,
                "workload": wl,
            }
        out["cache_experiments_rows"] = publish_experiment_results(client, latest.values())
    _print(out)
    return 0


def cmd_hbase_verify(a: argparse.Namespace) -> int:
    from cachemind.bigdata.hbase_store import TABLES, HBaseRestClient

    client = HBaseRestClient()
    out: dict[str, Any] = {"tables": {}}
    for name in TABLES:
        sample = list(client.scan(name, limit=2))
        out["tables"][name] = {
            "rows": client.count_rows(name),
            "families": [
                c["name"] for c in (client.table_schema(name) or {}).get("ColumnSchema", [])
            ],
            "sample": sample,
        }
    _print(out)
    return 0


def cmd_health(a: argparse.Namespace) -> int:
    from cachemind.monitoring.health import system_health

    rep = system_health()
    _print(rep)
    return 0 if rep["all_required_ok"] else 2


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="cachemind", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = ap.add_subparsers(dest="command", required=True)

    def trace_args(p: argparse.ArgumentParser) -> None:
        p.add_argument("--dataset", help="Silver dataset id (Big Data path)")
        p.add_argument("--profile", default="small", choices=["tiny", "small", "medium", "large"])
        p.add_argument("--workload", default="zipf")
        p.add_argument("--seed", type=int, default=42)

    p = sub.add_parser("generate", help="generate a synthetic multi-source dataset")
    p.add_argument("--profile", default="small", choices=["tiny", "small", "medium", "large"])
    p.add_argument("--workload", default="zipf")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--dataset-id")
    p.add_argument("--anomaly-rate", type=float, default=0.005)
    p.add_argument("--overwrite", action="store_true")
    p.set_defaults(fn=cmd_generate)

    p = sub.add_parser("simulate", help="run cache policies on a trace")
    trace_args(p)
    p.add_argument(
        "--policy",
        default="classical",
        choices=["classical", "all", "fifo", "lru", "lfu", "belady", "ml", "hybrid"],
    )
    p.add_argument("--capacity", type=int, default=100)
    p.add_argument("--model-dir")
    p.add_argument(
        "--precomputed",
        action="store_true",
        help="use the Spark model's batch predictions (original runtime)",
    )
    p.add_argument("--alpha", type=float, default=0.5)
    p.add_argument(
        "--measure-from",
        type=float,
        default=0.8,
        help="fraction of the trace used as warm-up before measuring",
    )
    p.set_defaults(fn=cmd_simulate)

    p = sub.add_parser("train", help="train sklearn reuse models (lightweight profile)")
    trace_args(p)
    p.add_argument("--horizon", type=int, default=500)
    p.set_defaults(fn=cmd_train)

    p = sub.add_parser("evaluate", help="summarise saved model artifacts")
    p.add_argument("--model-dir")
    p.set_defaults(fn=cmd_evaluate)

    for stage in ("bronze", "silver", "gold", "etl"):
        p = sub.add_parser(stage, help=f"Spark batch ETL: {stage}")
        p.add_argument("--dataset", required=True)
        p.add_argument(
            "--include-stream-file",
            action="store_true",
            help="also load stream_events.jsonl as an offline replay (NOT Kafka)",
        )
        p.add_argument(
            "--no-stream", action="store_true", help="ignore Kafka Bronze when building Silver"
        )
        p.set_defaults(fn=cmd_etl, stage=stage)

    p = sub.add_parser("features", help="Spark feature engineering")
    p.add_argument("--dataset", required=True)
    p.add_argument("--horizon", type=int, default=500)
    p.set_defaults(fn=cmd_features)

    p = sub.add_parser("mllib", help="Spark MLlib training/evaluation/export")
    p.add_argument("--dataset", required=True)
    p.add_argument("--horizon", type=int, default=500)
    p.add_argument("--seed", type=int, default=42)
    p.set_defaults(fn=cmd_mllib)

    for name, fn in (("rdd", cmd_rdd), ("sql", cmd_sql)):
        p = sub.add_parser(name, help=f"Spark {name.upper()} demonstration")
        p.add_argument("--dataset", required=True)
        p.set_defaults(fn=fn)

    sub.add_parser("hbase-init", help="create HBase tables").set_defaults(fn=cmd_hbase_init)
    p = sub.add_parser("hbase-publish", help="publish Gold stats / experiments to HBase")
    p.add_argument("--dataset")
    p.add_argument("--experiments", action="store_true")
    p.set_defaults(fn=cmd_hbase_publish)
    sub.add_parser("hbase-verify", help="row counts + samples").set_defaults(fn=cmd_hbase_verify)
    sub.add_parser("health", help="service + data health").set_defaults(fn=cmd_health)

    for name, module in (
        ("produce", "cachemind.bigdata.kafka_io"),
        ("stream", "cachemind.bigdata.streaming"),
        ("experiments", "cachemind.experiments.cache_experiments"),
        ("spark-experiments", "cachemind.experiments.spark_experiments"),
        ("pipeline-demo", "cachemind.experiments.pipeline_demo"),
        ("infra", "cachemind.ops.infra"),
    ):
        p = sub.add_parser(name, help=f"see python -m {module} --help", add_help=False)
        p.set_defaults(fn=None, module=module)

    args, rest = ap.parse_known_args(argv)
    if args.fn is None:
        import importlib

        return importlib.import_module(args.module).main(rest)
    if rest:
        ap.error(f"unrecognized arguments: {' '.join(rest)}")
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())
