"""Gold layer: reusable analytical aggregates computed from canonical Silver.

Every table is a bounded snapshot for one dataset, written to
``gold/<table>/dataset_id=<d>/`` and fully reproducible from Silver with the same
code version (the ``gold_run_id`` and ``silver_run_id`` are recorded).

Silver is read once with only the needed columns (column pruning) and cached
for the duration of the job; each aggregate is a single shuffle.
"""

from __future__ import annotations

import time
from typing import Any

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from cachemind.bigdata.etl import _write_report, new_run_id, publish_dataframe, read_silver
from cachemind.config import Settings, safe_child
from cachemind.logging_utils import get_logger, log_event

log = get_logger("cachemind.gold")

GOLD_TABLES = (
    "item_request_counts",
    "category_counts",
    "region_counts",
    "hourly_totals",
    "daily_totals",
    "item_daily_counts",
    "item_popularity_trend",
    "recent_access_frequency",
    "user_group_patterns",
)


def compute_gold(silver: DataFrame, top_n: int = 1000) -> dict[str, DataFrame]:
    s = silver
    items = s.groupBy("item_id", "category").agg(
        F.count("*").alias("total_requests"),
        F.countDistinct("user_id").alias("distinct_users"),
        F.min("event_time").alias("first_access"),
        F.max("event_time").alias("last_access"),
        F.sum("item_size_bytes").alias("bytes_requested"),
    )
    # Global ranking: the item table is small (one row per item), so a single
    # ordered partition is acceptable here.
    items = items.withColumn(
        "popularity_rank",
        F.row_number().over(Window.orderBy(F.desc("total_requests"), F.asc("item_id"))),
    )

    category = s.groupBy("category").agg(
        F.count("*").alias("requests"),
        F.countDistinct("item_id").alias("distinct_items"),
        F.sum("item_size_bytes").alias("bytes_requested"),
    )
    region = s.groupBy("region").agg(
        F.count("*").alias("requests"),
        F.countDistinct("user_id").alias("distinct_users"),
        F.countDistinct("item_id").alias("distinct_items"),
    )
    hourly = (
        s.groupBy(F.date_trunc("hour", "event_time").alias("hour_start"))
        .agg(
            F.count("*").alias("requests"),
            F.countDistinct("item_id").alias("distinct_items"),
            F.countDistinct("user_id").alias("distinct_users"),
        )
        .orderBy("hour_start")
    )
    daily = (
        s.groupBy("event_date")
        .agg(
            F.count("*").alias("requests"),
            F.countDistinct("item_id").alias("distinct_items"),
            F.countDistinct("user_id").alias("distinct_users"),
            F.sum("item_size_bytes").alias("bytes_requested"),
        )
        .orderBy("event_date")
    )
    item_daily = s.groupBy("item_id", "event_date").agg(F.count("*").alias("requests"))

    # Popularity trend: last day vs the previous day for each item.
    days = s.select("event_date").distinct().orderBy(F.desc("event_date")).limit(2).collect()
    if len(days) == 2:
        last_day, prev_day = days[0][0], days[1][0]
        cur = item_daily.where(F.col("event_date") == last_day).select(
            "item_id", F.col("requests").alias("requests_last_day")
        )
        prev = item_daily.where(F.col("event_date") == prev_day).select(
            "item_id", F.col("requests").alias("requests_prev_day")
        )
        trend = (
            cur.join(prev, "item_id", "full_outer")
            .na.fill(0)
            .withColumn(
                "trend_score",
                (F.col("requests_last_day") - F.col("requests_prev_day"))
                / (F.col("requests_prev_day") + 1.0),
            )
            .withColumn(
                "rank_last_day",
                F.row_number().over(Window.orderBy(F.desc("requests_last_day"), F.asc("item_id"))),
            )
            .withColumn(
                "rank_prev_day",
                F.row_number().over(Window.orderBy(F.desc("requests_prev_day"), F.asc("item_id"))),
            )
            .withColumn("rank_change", F.col("rank_prev_day") - F.col("rank_last_day"))
            .withColumn("last_day", F.lit(last_day))
            .withColumn("prev_day", F.lit(prev_day))
            .orderBy(F.desc("requests_last_day"))
            .limit(top_n)
        )
    else:
        trend = s.sparkSession.createDataFrame(
            [],
            "item_id string, requests_last_day long, requests_prev_day long, "
            "trend_score double, rank_last_day int, rank_prev_day int, rank_change int, "
            "last_day date, prev_day date",
        )

    # Recent access frequency as of the latest event in the dataset (a bounded snapshot).
    max_t = s.agg(F.max("event_time_ms")).head(1)[0][0]
    recent = (
        s.where(F.col("event_time_ms") > max_t - 24 * 3_600_000)
        .groupBy("item_id")
        .agg(
            F.count("*").alias("requests_24h"),
            F.sum(F.when(F.col("event_time_ms") > max_t - 3_600_000, 1).otherwise(0)).alias(
                "requests_1h"
            ),
            F.max("event_time").alias("last_access"),
        )
        .withColumn("as_of_ms", F.lit(max_t))
        .orderBy(F.desc("requests_1h"), F.desc("requests_24h"), F.asc("item_id"))
        .limit(top_n)
    )

    groups = (
        s.groupBy("user_group", "category")
        .agg(F.count("*").alias("requests"))
        .withColumn(
            "share_of_group",
            F.col("requests") / F.sum("requests").over(Window.partitionBy("user_group")),
        )
    )
    return {
        "item_request_counts": items,
        "category_counts": category,
        "region_counts": region,
        "hourly_totals": hourly,
        "daily_totals": daily,
        "item_daily_counts": item_daily,
        "item_popularity_trend": trend,
        "recent_access_frequency": recent,
        "user_group_patterns": groups,
    }


def build_gold(
    spark: SparkSession, settings: Settings, dataset_id: str, top_n: int = 1000
) -> dict[str, Any]:
    t0 = time.time()
    run_id = new_run_id("gold")
    silver = (
        read_silver(spark, settings, dataset_id)
        .select(
            "item_id",
            "category",
            "user_id",
            "user_group",
            "region",
            "event_time",
            "event_time_ms",
            "event_date",
            "item_size_bytes",
            "silver_run_id",
        )
        .cache()
    )
    silver_runs = [r[0] for r in silver.select("silver_run_id").distinct().collect()]
    report: dict[str, Any] = {
        "dataset_id": dataset_id,
        "gold_run_id": run_id,
        "silver_run_ids": silver_runs,
        "tables": {},
    }
    for name, table in compute_gold(silver, top_n).items():
        final = safe_child(settings.gold_dir, name, f"dataset_id={dataset_id}")
        publish_dataframe(
            table.withColumn("gold_run_id", F.lit(run_id)),
            final,
            settings.gold_dir / "_staging",
            n_files=1,
        )
        report["tables"][name] = {
            "rows": spark.read.parquet(str(final)).count(),
            "path": str(final.relative_to(settings.data_dir)),
        }
    silver.unpersist()
    report["seconds"] = round(time.time() - t0, 2)
    _write_report(settings, f"gold_{dataset_id}", report)
    log_event(log, "gold built", dataset_id=dataset_id, run_id=run_id)
    return report


def read_gold(spark: SparkSession, settings: Settings, table: str, dataset_id: str) -> DataFrame:
    if table not in GOLD_TABLES:
        raise ValueError(f"unknown gold table {table!r}")
    return spark.read.parquet(str(safe_child(settings.gold_dir, table, f"dataset_id={dataset_id}")))
