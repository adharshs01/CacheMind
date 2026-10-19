"""Experiment 10: Spark batch processing performance (+ optimisation comparisons).

    python -m cachemind.experiments.spark_experiments [--sizes 20000,100000,1000000]

Measures wall time of each ETL stage (Bronze, Silver, Gold, features) on datasets
of increasing size with the project's memory-conscious settings, then compares:

  A  shuffle partitions: 4 (project default) vs 200 (Spark default)
  B  metadata enrichment: broadcast hash join vs shuffle join (broadcast disabled)
  C  column pruning + partition/predicate pushdown when reading Silver

Every comparison runs the same stage on the same data; each configuration is
timed `repeats` times after one warm-up and the median is reported. Physical
plans are captured as evidence for C. Single local machine; numbers are not
cluster benchmarks.
"""

from __future__ import annotations

import argparse
import json
import time
from dataclasses import replace
from pathlib import Path
from typing import Any

import numpy as np

from cachemind.config import get_settings
from cachemind.logging_utils import get_logger, log_event

log = get_logger("cachemind.exp.spark")


def _dir_bytes(p: Path) -> int:
    return sum(f.stat().st_size for f in p.rglob("*") if f.is_file())


def _output_rows(stage: str, report: dict[str, Any]) -> int | None:
    """Rows produced by a stage, taken from the stage's own report."""
    if stage == "bronze":
        return sum(v["rows"] for v in report["sources"].values())
    if stage == "silver":
        return report["silver_rows"]
    if stage == "gold":
        return sum(t["rows"] for t in report["tables"].values())
    return report.get("rows")


def run(sizes: tuple[int, ...] = (20_000, 100_000, 1_000_000), repeats: int = 3) -> list[dict]:
    from pyspark.sql import functions as F

    from cachemind.bigdata.etl import build_silver, ingest_bronze_files, read_silver
    from cachemind.bigdata.features_spark import build_features
    from cachemind.bigdata.gold import build_gold
    from cachemind.bigdata.spark_session import get_spark
    from cachemind.data.generator import generate_dataset
    from cachemind.data.workloads import WorkloadConfig

    base = get_settings()
    s = replace(base, data_dir=base.runtime_dir / "exp10" / "data")
    spark = get_spark("cachemind-exp10")
    rows: list[dict[str, Any]] = []
    try:
        for n in sizes:
            ds = f"perf-zipf-n{n}"
            ddir = s.raw_dir / ds
            if not (ddir / "manifest.json").exists():
                t = time.time()
                cfg = WorkloadConfig(
                    workload="zipf",
                    n_events=n,
                    n_items=max(500, n // 20),
                    n_users=max(200, n // 50),
                    seed=99,
                    dataset_id=ds,
                    event_rate_per_sec=3.0,
                )
                generate_dataset(cfg, s.raw_dir, batch_split=(0.5, 0.5), overwrite=True)
                rows.append(
                    {
                        "size": n,
                        "stage": "generate (python)",
                        "seconds": time.time() - t,
                        "config": "-",
                        "repeat": 0,
                    }
                )
            raw_bytes = sum(_dir_bytes(f) for f in ddir.glob("requests_*"))
            stages = [
                ("bronze", lambda ddir=ddir: ingest_bronze_files(spark, s, ddir)),
                ("silver", lambda ds=ds: build_silver(spark, s, ds, include_stream=False)),
                ("gold", lambda ds=ds: build_gold(spark, s, ds)),
                ("features", lambda ds=ds: build_features(spark, s, ds, 500)),
            ]
            for stage, fn in stages:
                reps = repeats if n <= 100_000 else 1
                times = []
                for r in range(reps + (1 if reps > 1 else 0)):
                    t = time.perf_counter()
                    report = fn()
                    dt = time.perf_counter() - t
                    if reps > 1 and r == 0:
                        continue  # warm-up
                    times.append(dt)
                rows.append(
                    {
                        "size": n,
                        "stage": stage,
                        "config": "default (4 partitions, broadcast metadata)",
                        "seconds": float(np.median(times)),
                        "seconds_min": min(times),
                        "seconds_max": max(times),
                        "repeats": len(times),
                        "raw_input_bytes": raw_bytes,
                        "output_rows": _output_rows(stage, report),
                    }
                )
                log_event(log, "stage timed", size=n, stage=stage, seconds=rows[-1]["seconds"])

        # --- comparisons on the 100k dataset ------------------------------------------
        n_cmp = 100_000 if 100_000 in sizes else sizes[-1]
        ds = f"perf-zipf-n{n_cmp}"

        def timed(label: str, stage: str, fn, conf: dict[str, str]) -> None:
            old = {k: spark.conf.get(k) for k in conf}
            for k, v in conf.items():
                spark.conf.set(k, v)
            try:
                fn()  # warm-up
                times = []
                for _ in range(repeats):
                    t = time.perf_counter()
                    fn()
                    times.append(time.perf_counter() - t)
            finally:
                for k, v in old.items():
                    spark.conf.set(k, v)
            rows.append(
                {
                    "size": n_cmp,
                    "stage": stage,
                    "config": label,
                    "comparison": True,
                    "seconds": float(np.median(times)),
                    "seconds_min": min(times),
                    "seconds_max": max(times),
                    "repeats": repeats,
                }
            )
            log_event(log, "comparison timed", label=label, seconds=rows[-1]["seconds"])

        for parts in ("4", "200"):
            timed(
                f"A: shuffle partitions={parts}",
                "silver+gold",
                lambda: (
                    build_silver(spark, s, ds, include_stream=False),
                    build_gold(spark, s, ds),
                ),
                {"spark.sql.shuffle.partitions": parts, "spark.sql.adaptive.enabled": "false"},
            )
        timed(
            "B: broadcast metadata join",
            "silver",
            lambda: build_silver(spark, s, ds, include_stream=False),
            {"spark.sql.autoBroadcastJoinThreshold": str(32 * 1024 * 1024)},
        )
        timed(
            "B: shuffle (sort-merge) metadata join",
            "silver",
            lambda: build_silver(spark, s, ds, include_stream=False, broadcast_metadata=False),
            {"spark.sql.autoBroadcastJoinThreshold": "-1"},
        )

        # C: pruning/pushdown evidence + timing on the largest dataset
        big = f"perf-zipf-n{sizes[-1]}"

        def pruned() -> None:
            (
                read_silver(spark, s, big)
                .where(F.col("region") == "eu-west")
                .select("item_id")
                .groupBy("item_id")
                .count()
                .collect()
            )

        def full() -> None:
            # anti-pattern for comparison: materialise every column, then filter
            (
                read_silver(spark, s, big)
                .cache()
                .where(F.col("region") == "eu-west")
                .groupBy("item_id")
                .count()
                .collect()
            )
            spark.catalog.clearCache()

        plan = (
            read_silver(spark, s, big)
            .where(F.col("region") == "eu-west")
            .select("item_id")
            ._jdf.queryExecution()
            .executedPlan()
            .toString()
        )
        timed("C: pruned columns + pushed filter", "silver read", pruned, {})
        timed("C: cache all columns, then filter", "silver read", full, {})
        rows.append(
            {
                "size": sizes[-1],
                "stage": "plan evidence",
                "config": "C",
                "pushed_filters": "PushedFilters: [IsNotNull(region), "
                "EqualTo(region,eu-west)]" in plan,
                "read_schema_pruned": "ReadSchema: struct<item_id:string,region:string>" in plan,
                "plan": plan[:3000],
            }
        )
    finally:
        spark.stop()
    return rows


def main(argv: list[str] | None = None) -> int:
    from cachemind.experiments.common import save_results

    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--sizes", default="20000,100000,1000000")
    ap.add_argument("--repeats", type=int, default=3)
    args = ap.parse_args(argv)
    sizes = tuple(int(x) for x in args.sizes.split(","))
    rows = run(sizes, args.repeats)
    from cachemind.bigdata.spark_session import spark_environment

    save_results(
        "exp10_spark_performance",
        rows,
        {
            "sizes": sizes,
            "repeats": args.repeats,
            "spark": spark_environment(),
            "note": "single laptop (Apple M3, 8 GB); local[2]; medians after one warm-up; the "
            "1M-event size is timed once to bound runtime",
        },
    )
    print(
        json.dumps(
            [{k: v for k, v in r.items() if k != "plan"} for r in rows], indent=1, default=str
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
