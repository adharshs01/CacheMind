"""Spark RDD demonstration on real project data (independently executable).

    python -m cachemind.bigdata.rdd_demo --dataset <dataset_id>

Transformations: map, filter, flatMap, distinct, reduceByKey, sortByKey
                 (+ aggregateByKey, join, mapPartitions, zipWithIndex)
Actions:         count, take, collect (bounded outputs only), reduce, saveAsTextFile

Every RDD result is cross-checked against an equivalent DataFrame computation
and the comparison is recorded in the output report.
"""

from __future__ import annotations

import argparse
import json
import shutil
import time
from operator import add
from pathlib import Path
from typing import Any, cast

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from cachemind.bigdata.etl import read_silver
from cachemind.config import Settings, get_settings, safe_child


def run_rdd_demo(
    spark: SparkSession,
    settings: Settings,
    dataset_id: str,
    out_dir: Path | None = None,
    top_k: int = 10,
) -> dict[str, Any]:
    t0 = time.time()
    sc = spark.sparkContext
    out_dir = out_dir or safe_child(settings.reports_dir, "rdd", dataset_id)
    out_dir.mkdir(parents=True, exist_ok=True)
    silver = read_silver(spark, settings, dataset_id).select(
        "item_id", "user_id", "category", "region", "event_hour", "item_size_bytes"
    )

    # Base RDD of plain tuples (map #1: Row -> tuple), persisted because it is reused.
    requests = silver.rdd.map(
        lambda r: (r.item_id, r.user_id, r.category, r.region, r.event_hour, r.item_size_bytes)
    ).cache()
    report: dict[str, Any] = {
        "dataset_id": dataset_id,
        "partitions": requests.getNumPartitions(),
        "operations": {},
    }
    ops = report["operations"]

    # --- transformations ------------------------------------------------------
    pairs = requests.map(lambda r: (r[0], 1))  # map
    item_counts = pairs.reduceByKey(add)  # reduceByKey
    by_count = item_counts.map(lambda kv: (kv[1], kv[0])).sortByKey(ascending=False)  # type: ignore[call-overload]  # sortByKey; pyspark stub wrongly demands keyfunc
    eu = requests.filter(lambda r: r[3] == "eu-west")  # filter
    tokens = requests.flatMap(
        lambda r: [("category=" + r[2], 1), ("region=" + r[3], 1), (f"hour={r[4]:02d}", 1)]
    )  # flatMap
    dim_counts = tokens.reduceByKey(add)
    distinct_items = requests.map(lambda r: r[0]).distinct()  # distinct
    distinct_users = requests.map(lambda r: r[1]).distinct()

    # extra operations that make the exercise meaningful
    size_by_cat = (
        requests.map(lambda r: (r[2], r[5]))
        .aggregateByKey(
            (0, 0), lambda acc, v: (acc[0] + v, acc[1] + 1), lambda a, b: (a[0] + b[0], a[1] + b[1])
        )  # aggregateByKey
        .mapValues(lambda s: s[0] / s[1])
    )
    item_cat = requests.map(lambda r: (r[0], r[2])).distinct()
    top_with_cat = (
        item_counts.join(item_cat)  # join
        .map(lambda kv: (kv[1][0], (kv[0], kv[1][1])))
        .sortByKey(ascending=False)  # type: ignore[call-overload]
    )
    part_sizes = requests.mapPartitions(lambda it: [sum(1 for _ in it)])  # mapPartitions
    indexed = by_count.zipWithIndex()  # zipWithIndex

    # --- actions ----------------------------------------------------------------
    total = requests.count()  # count
    n_items = distinct_items.count()
    n_users = distinct_users.count()
    top = by_count.take(top_k)  # take
    region_counts = (
        requests.map(lambda r: (r[3], 1)).reduceByKey(add).collect()
    )  # collect (<=5 rows)
    category_avg = size_by_cat.collect()  # collect (<=7 rows)
    total_bytes = requests.map(lambda r: r[5]).reduce(add)  # reduce
    max_size = requests.map(lambda r: r[5]).reduce(max)
    eu_count = eu.count()
    # bounded: categories + regions + 24 hours
    dims = cast(list[tuple[str, int]], dim_counts.sortByKey().collect())
    partitions = part_sizes.collect()
    rank_of_top = indexed.take(3)
    top_cat = top_with_cat.take(5)

    save_path = out_dir / "top_items"
    if save_path.exists():
        shutil.rmtree(save_path)  # saveAsTextFile refuses to overwrite
    (
        sc.parallelize(
            [f"{item}\t{count}" for count, item in by_count.take(100)], 1
        ).saveAsTextFile(str(save_path))
    )  # saveAsTextFile

    # --- cross-checks against DataFrame equivalents --------------------------------
    df_total = silver.count()
    df_top = [
        (r["count"], r["item_id"])
        for r in silver.groupBy("item_id")
        .count()
        .orderBy(F.desc("count"), F.desc("item_id"))
        .limit(top_k)
        .collect()
    ]
    df_regions = {r["region"]: r["count"] for r in silver.groupBy("region").count().collect()}
    df_bytes = silver.agg(F.sum("item_size_bytes")).head(1)[0][0]
    df_items = silver.select("item_id").distinct().count()
    df_eu = silver.where(F.col("region") == "eu-west").count()
    top_counts_match = [c for c, _ in top] == [c for c, _ in df_top]

    ops.update(
        {
            "map+reduceByKey (requests per item)": {"distinct_keys": item_counts.count()},
            "sortByKey (top items by count)": {
                "top": [{"item_id": i, "requests": c} for c, i in top]
            },
            "filter (region == eu-west)": {"count": eu_count},
            "flatMap (dimension tokens)": {"counts": dict(dims)},
            "distinct": {"items": n_items, "users": n_users},
            "aggregateByKey (mean item size by category)": dict(sorted(category_avg)),
            "join (top items with category)": [
                {"item_id": i, "category": c, "requests": n} for n, (i, c) in top_cat
            ],
            "mapPartitions (rows per partition)": partitions,
            "zipWithIndex (rank of top items)": [
                {"rank": idx, "item_id": item, "requests": c} for (c, item), idx in rank_of_top
            ],
            "count": total,
            "take": top_k,
            "collect (regions, bounded)": dict(sorted(region_counts)),
            "reduce (total bytes, max size)": {
                "total_bytes": total_bytes,
                "max_item_size": max_size,
            },
            "saveAsTextFile": str(save_path),
        }
    )
    report["cross_checks"] = {
        "count_matches_dataframe": total == df_total,
        "distinct_items_match": n_items == df_items,
        "top_counts_match": top_counts_match,
        "regions_match": dict(region_counts) == df_regions,
        "total_bytes_match": total_bytes == df_bytes,
        "filter_matches": eu_count == df_eu,
        "partition_sizes_sum_to_count": sum(partitions) == total,
        "saved_lines": sum(1 for p in save_path.glob("part-*") for _ in p.open()),
    }
    report["all_cross_checks_ok"] = all(
        v for k, v in report["cross_checks"].items() if k != "saved_lines"
    )
    report["lineage_example"] = by_count.toDebugString().decode()[:2000]
    report["seconds"] = round(time.time() - t0, 2)
    requests.unpersist()
    (out_dir / "rdd_report.json").write_text(json.dumps(report, indent=2, default=str) + "\n")
    return report


def main(argv: list[str] | None = None) -> int:
    from cachemind.bigdata.spark_session import get_spark

    ap = argparse.ArgumentParser(description="Spark RDD demonstration")
    ap.add_argument("--dataset", required=True)
    args = ap.parse_args(argv)
    spark = get_spark("cachemind-rdd-demo")
    try:
        rep = run_rdd_demo(spark, get_settings(), args.dataset)
    finally:
        spark.stop()
    print(
        json.dumps(
            {k: rep[k] for k in ("cross_checks", "all_cross_checks_ok", "seconds")}, indent=2
        )
    )
    return 0 if rep["all_cross_checks_ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
