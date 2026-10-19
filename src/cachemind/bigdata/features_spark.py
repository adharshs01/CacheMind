"""Schema-v1 reuse features computed with Spark window functions.

Equivalent to ``cachemind.ml.features.compute_features`` + ``reuse_labels``:

* ``trace_pos`` = 0-based position in the canonical order (seq, event_id);
* range frames ``rangeBetween(-(k-1), 0)`` over ``trace_pos`` partitioned by item
  count the item's requests in positions [t-k+1, t] (current request included);
* ``lag`` gives the previous request positions, ``lead`` the next one (used ONLY
  for the label, never for a feature).

Every feature column depends on rows with ``trace_pos <= t`` only; the label
uses ``lead`` by definition and is stored in a separate column.

Window functions produce the integer history (gaps, counts); the floating-point
transforms are applied by ``ml.features.feature_values`` in a pandas UDF, the same
function the online tracker calls, so both runtimes produce bit-identical values.
"""

from __future__ import annotations

import time
from typing import Any

import pandas as pd
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from cachemind.bigdata.etl import _write_report, read_silver
from cachemind.config import Settings, safe_child
from cachemind.ml.features import GAP_CAP, NUMERIC_FEATURES, feature_values

FEATURE_ID_COLUMNS = [
    "trace_pos",
    "seq",
    "event_id",
    "item_id",
    "event_time_ms",
    "item_size_bytes",
    "category",
    "region",
]


def with_trace_positions(silver: DataFrame) -> DataFrame:
    # A global ordering is inherently one partition; one row per request with
    # ~8 narrow columns is fine for traces of a few million requests on a laptop.
    w = Window.orderBy("seq", "event_id")
    return silver.select(
        "seq", "event_id", "item_id", "event_time_ms", "item_size_bytes", "category", "region"
    ).withColumn("trace_pos", (F.row_number().over(w) - 1).cast("long"))


_FEATURE_STRUCT = T.StructType([T.StructField(f, T.DoubleType()) for f in NUMERIC_FEATURES])
_HISTORY_COLUMNS = (
    "_gap1",
    "_gap2",
    "_c10",
    "_c100",
    "_c200",
    "_c1000",
    "_total",
    "_first",
    "item_size_bytes",
    "event_time_ms",
)


@F.pandas_udf(_FEATURE_STRUCT)  # type: ignore[call-overload]
def _feature_values_udf(*cols: pd.Series) -> pd.DataFrame:
    """Integer history -> feature values via ``ml.features.feature_values``.

    Window functions compute every count and gap exactly; only the final floating-point
    transforms run in Python, through the same function the online tracker uses, so
    Spark and the online path are bit-identical (the JVM's log1p/sin/cos differ from
    the C library's in the last bit for some inputs).
    """
    rows = [feature_values(*r) for r in zip(*(c.tolist() for c in cols), strict=True)]
    return pd.DataFrame(rows, columns=list(NUMERIC_FEATURES))


def compute_features_df(trace: DataFrame, horizon: int) -> DataFrame:
    n = trace.count()
    pos = F.col("trace_pos")
    w = Window.partitionBy("item_id").orderBy("trace_pos")

    def count_in(lo: int, hi: int):
        return F.count(F.lit(1)).over(w.rangeBetween(lo, hi))

    prev1 = F.lag("trace_pos", 1).over(w)
    prev2 = F.lag("trace_pos", 2).over(w)
    nxt = F.lead("trace_pos", 1).over(w)
    out = (
        trace.withColumn("_gap1", F.coalesce(pos - prev1, F.lit(GAP_CAP)))
        .withColumn("_gap2", F.coalesce(prev1 - prev2, F.lit(GAP_CAP)))
        .withColumn("_c10", count_in(-9, 0))
        .withColumn("_c100", count_in(-99, 0))
        .withColumn("_c200", count_in(-199, 0))
        .withColumn("_c1000", count_in(-999, 0))
        .withColumn("_total", F.row_number().over(w))
        .withColumn("_first", prev1.isNull())
        .withColumn("_f", _feature_values_udf(*[F.col(c) for c in _HISTORY_COLUMNS]))
        .withColumn("_next", nxt)
        .withColumn(
            "label",
            F.when(F.col("_next").isNotNull() & (F.col("_next") - pos <= horizon), 1)
            .otherwise(0)
            .cast("int"),
        )
        .withColumn("censored", F.col("_next").isNull() & (pos + horizon > n - 1))
    )
    return out.select(
        *FEATURE_ID_COLUMNS,
        *[F.col(f"_f.{f}").alias(f) for f in NUMERIC_FEATURES],
        "label",
        "censored",
    )


def build_features(
    spark: SparkSession, settings: Settings, dataset_id: str, horizon: int
) -> dict[str, Any]:
    t0 = time.time()
    silver = read_silver(spark, settings, dataset_id)
    feats = compute_features_df(with_trace_positions(silver), horizon)
    final = safe_child(settings.features_dir, f"dataset_id={dataset_id}", f"horizon={horizon}")
    from cachemind.bigdata.etl import publish_dataframe

    publish_dataframe(
        feats.repartition(4, "item_id").sortWithinPartitions("trace_pos"),
        final,
        settings.features_dir / "_staging",
    )
    stored = spark.read.parquet(str(final))
    summary = stored.agg(
        F.count("*").alias("rows"),
        F.avg("label").alias("positive_rate"),
        F.sum(F.col("censored").cast("int")).alias("censored"),
    ).head(1)[0]
    report = {
        "dataset_id": dataset_id,
        "horizon": horizon,
        "rows": summary["rows"],
        "positive_rate_all_rows": summary["positive_rate"],
        "censored_rows": summary["censored"],
        "path": str(final),
        "seconds": round(time.time() - t0, 2),
    }
    _write_report(settings, f"features_{dataset_id}_h{horizon}", report)
    return report


def features_path(settings: Settings, dataset_id: str, horizon: int):
    return safe_child(settings.features_dir, f"dataset_id={dataset_id}", f"horizon={horizon}")
