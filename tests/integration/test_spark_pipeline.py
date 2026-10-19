"""Test groups D (ETL), E (RDD), F (SQL) and Spark-side B/C (features, MLlib).

Needs a local JVM (JDK 17 in .tools) but no external services.
"""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from cachemind.config import get_settings
from cachemind.data.generator import generate_dataset
from cachemind.data.raw_formats import (
    SOURCE_API_JSON,
    SOURCE_KAFKA,
    SOURCE_LEGACY_CSV,
    validate_raw,
)
from cachemind.data.workloads import WorkloadConfig

pytestmark = pytest.mark.spark
DS = "spark-test-zipf"


@pytest.fixture(scope="module")
def settings(tmp_path_factory):
    root = tmp_path_factory.mktemp("cm")
    return replace(
        get_settings(),
        data_dir=root / "data",
        artifacts_dir=root / "artifacts",
        reports_dir=root / "reports",
    )


@pytest.fixture(scope="module")
def spark():
    from cachemind.bigdata.spark_session import get_spark

    s = get_spark("cachemind-tests")
    yield s
    s.stop()


@pytest.fixture(scope="module")
def built(spark, settings):
    """Generate a small dataset and run Bronze -> Silver -> Gold once for the module."""
    from cachemind.bigdata.etl import build_silver, ingest_bronze_files
    from cachemind.bigdata.gold import build_gold

    cfg = WorkloadConfig(
        workload="zipf",
        n_events=6_000,
        n_items=400,
        n_users=120,
        seed=5,
        dataset_id=DS,
        event_rate_per_sec=0.05,
    )  # spans ~1.4 days
    ddir = generate_dataset(cfg, settings.raw_dir, anomaly_rate=0.02)
    bronze = ingest_bronze_files(spark, settings, ddir, include_stream_file=True)
    silver = build_silver(spark, settings, DS, include_stream=False)
    gold = build_gold(spark, settings, DS)
    return {
        "dir": ddir,
        "bronze": bronze,
        "silver": silver,
        "gold": gold,
        "expected": json.loads((ddir / "expected.json").read_text()),
    }


def assert_same_counts(got: pd.Series, expected: pd.Series) -> None:
    """Exact equality of two keyed count series, independent of row order."""
    pd.testing.assert_series_equal(
        got.sort_index().astype("int64"),
        expected.sort_index().astype("int64"),
        check_names=False,
        check_index_type=False,
    )


def silver_pdf(spark, settings) -> pd.DataFrame:
    from cachemind.bigdata.etl import read_silver

    return read_silver(spark, settings, DS).toPandas()


# --- D: ETL -------------------------------------------------------------------------

FIXTURE = [
    (
        SOURCE_KAFKA,
        '{"schema_version":1,"event_id":"e1","seq":1,"dataset_id":"d","event_time":'
        '"2026-01-05T05:30:00.123+05:30","user_id":"U1","item_id":" I9 ","region":'
        '"EUW","item_size_bytes":"12"}',
    ),
    (SOURCE_KAFKA, '{"schema_version":1,"event_id":"e2","seq":2,"dataset_id":"d"'),
    (SOURCE_KAFKA, "[1,2]"),
    (SOURCE_KAFKA, '{"schema_version":2,"event_id":"e3"}'),
    (
        SOURCE_KAFKA,
        '{"schema_version":1,"event_id":"e4","seq":4,"dataset_id":"d","event_time":'
        '"2026-01-05T00:00:00Z","user_id":"u1","region":"us-east","item_size_bytes":5}',
    ),
    (
        SOURCE_KAFKA,
        '{"schema_version":1,"event_id":"e 5","seq":5,"dataset_id":"d","event_time":'
        '"2026-01-05T00:00:00Z","user_id":"u1","item_id":"i1","region":"us-east",'
        '"item_size_bytes":5}',
    ),
    (
        SOURCE_KAFKA,
        '{"schema_version":1,"event_id":"e6","seq":-6,"dataset_id":"d","event_time":'
        '"2026-01-05T00:00:00Z","user_id":"u1","item_id":"i1","region":"us-east",'
        '"item_size_bytes":5}',
    ),
    (
        SOURCE_KAFKA,
        '{"schema_version":1,"event_id":"e7","seq":7,"dataset_id":"d","event_time":'
        '"2026-01-05T00:00:00","user_id":"u1","item_id":"i1","region":"us-east",'
        '"item_size_bytes":5}',
    ),
    (
        SOURCE_KAFKA,
        '{"schema_version":1,"event_id":"e8","seq":8,"dataset_id":"d","event_time":'
        '"2026-02-30T00:00:00Z","user_id":"u1","item_id":"i1","region":"us-east",'
        '"item_size_bytes":5}',
    ),
    (
        SOURCE_KAFKA,
        '{"schema_version":1,"event_id":"e9","seq":9,"dataset_id":"d","event_time":'
        '"2026-01-05T00:00:00Z","user_id":"u1","item_id":"i 1","region":"us-east",'
        '"item_size_bytes":5}',
    ),
    (
        SOURCE_KAFKA,
        '{"schema_version":1,"event_id":"e10","seq":10,"dataset_id":"d","event_time":'
        '"2026-01-05T00:00:00Z","user_id":"u/1","item_id":"i1","region":"us-east",'
        '"item_size_bytes":5}',
    ),
    (
        SOURCE_KAFKA,
        '{"schema_version":1,"event_id":"e11","seq":11,"dataset_id":"d","event_time":'
        '"2026-01-05T00:00:00Z","user_id":"u1","item_id":"i1","region":"mars",'
        '"item_size_bytes":5}',
    ),
    (
        SOURCE_KAFKA,
        '{"schema_version":1,"event_id":"e12","seq":12,"dataset_id":"d","event_time":'
        '"2026-01-05T00:00:00Z","user_id":"u1","item_id":"i1","region":"us-east",'
        '"item_size_bytes":-5}',
    ),
    (
        SOURCE_KAFKA,
        '{"schema_version":1,"event_id":"e13","seq":13,"dataset_id":"d","event_time":'
        '"2026-01-05T00:00:00Z","user_id":"u1","item_id":"i1","region":"us-east",'
        '"item_size_bytes":"5kb"}',
    ),
    (
        SOURCE_KAFKA,
        '{"schema_version":1,"event_id":"e14","seq":14,"dataset_id":"d","event_time":'
        '"2026-01-05T00:00:00Z","user_id":"u1","item_id":"i1","region":"us-east",'
        '"item_size_bytes":99999999999}',
    ),
    (
        SOURCE_KAFKA,
        '{"schema_version":1,"event_id":"e15","seq":15,"dataset_id":"D S","event_time":'
        '"2026-01-05T00:00:00Z","user_id":"u1","item_id":"i1","region":"us-east",'
        '"item_size_bytes":5}',
    ),
    (
        SOURCE_KAFKA,
        '{"schema_version":1,"event_id":"e16","seq":12.0,"dataset_id":"d","event_time":'
        '"2026-01-05T00:00:00Z","user_id":"u1","item_id":"i1","region":"us-east",'
        '"item_size_bytes":0}',
    ),
    (SOURCE_LEGACY_CSV, "r1,1,1767571200085,U0000037,I0000292,EUW,6327"),
    (SOURCE_LEGACY_CSV, "r2,2,1767571200085,U0000037,,EUW,6327"),
    (SOURCE_LEGACY_CSV, "r3,3,2026-13-45 25:61,U0000037,I0000292,EUW,6327"),
    (SOURCE_LEGACY_CSV, "r4,4,1767571200085,U0000037,I0000292,EUW"),
    (SOURCE_LEGACY_CSV, 'r5,5,1767571200085,"U1",I0000292,EUW,6327'),
    (SOURCE_LEGACY_CSV, "r6,6,0,U0000037,I0000292,EUW,6327"),
    (
        SOURCE_API_JSON,
        '{"v":1,"id":"a1","seq":3,"dataset":"d","timestamp":'
        '"2026-01-04T16:00:00.000-08:00","user":{"id":"u1"},"item":{"id":"i2",'
        '"bytes":10},"geo":{"region":"ap-east"}}',
    ),
    (
        SOURCE_API_JSON,
        '{"v":1,"id":"a2","seq":3,"dataset":"d","timestamp":'
        '"2026-01-04T16:00:00Z","user":{"id":"u1"},"item":"notobj",'
        '"geo":{"region":"ap-east"}}',
    ),
    (
        SOURCE_API_JSON,
        '{"v":1,"id":"a3","seq":3,"dataset":"d","timestamp":"2026-01-04T16:00:00Z",'
        '"user":{"id":"u1"},"item":{"bytes":10},"geo":{"region":"ap-east"}}',
    ),
    (SOURCE_API_JSON, '{"v":1,"id":"a4","seq":3,"dataset":"d","timestamp":"2026-01-04T16:00:00Z"'),
]


def test_spark_validation_matches_python_reference(spark):
    from pyspark.sql import functions as F

    from cachemind.bigdata.etl import BRONZE_COLUMNS, map_to_wire, validate_wire

    rows = [(payload, src, "d", i) for i, (src, payload) in enumerate(FIXTURE)]
    df = spark.createDataFrame(
        rows, "raw_payload string, source string, dataset_hint string, source_line long"
    )
    for f in BRONZE_COLUMNS:
        if f.name not in df.columns:
            df = df.withColumn(f.name, F.lit(None).cast(f.dataType))
    out = (
        validate_wire(map_to_wire(df))
        .orderBy("source_line")
        .select("dq_reason", "event_time_ms", "item_id", "region")
        .collect()
    )
    python = [validate_raw(src, payload, "d") for src, payload in FIXTURE]
    for (src, payload), sp, py in zip(FIXTURE, out, python, strict=True):
        assert sp["dq_reason"] == py.reason, (src, payload, sp["dq_reason"], py.reason)
        if py.ok:
            assert sp["event_time_ms"] == py.event.event_time_ms
            assert sp["item_id"] == py.event.item_id and sp["region"] == py.event.region
    # the fixture exercises a broad set of distinct reasons
    assert len({p.reason for p in python}) >= 14


def test_etl_matches_generator_ground_truth(built):
    ver = built["silver"]["verification"]
    assert ver["all_ok"], ver
    assert built["silver"]["row_accounting_ok"]


def test_bronze_preserves_every_raw_line_with_lineage(spark, settings, built):
    from cachemind.bigdata.etl import read_bronze

    b = read_bronze(spark, settings, DS, include_stream=False).toPandas()
    exp = built["expected"]["raw_rows"]
    assert (b["source"] == SOURCE_LEGACY_CSV).sum() == exp["legacy_csv"]
    assert (b["source"] == SOURCE_API_JSON).sum() == exp["api_json"]
    assert b["source_line"].notna().all() and b["source_file"].notna().all()
    raw_csv = (built["dir"] / "requests_legacy.csv").read_text().splitlines()[1:]
    csv_rows = b[b["source"] == SOURCE_LEGACY_CSV].sort_values("source_line")
    assert csv_rows["raw_payload"].tolist() == raw_csv  # verbatim, in file order


def test_quarantine_records_are_complete(spark, settings, built):
    q = spark.read.parquet(str(settings.quarantine_dir / f"dataset_hint={DS}")).toPandas()
    assert len(q) == built["silver"]["quarantine_total"] > 0
    for col in ("raw_payload", "dq_reason", "source", "processing_ts", "silver_run_id"):
        assert q[col].notna().all(), col
    assert (q["source_line"].notna()).all()  # every rejected file record points to its line


def test_silver_rerun_is_idempotent(spark, settings, built):
    from cachemind.bigdata.etl import build_silver

    before = silver_pdf(spark, settings)
    again = build_silver(spark, settings, DS, include_stream=False)
    after = silver_pdf(spark, settings)
    assert again["silver_rows"] == len(before) == len(after)
    assert sorted(after["event_id"]) == sorted(before["event_id"])
    assert after["event_id"].is_unique and after["seq"].is_unique


def test_silver_canonical_invariants(spark, settings, built):
    s = silver_pdf(spark, settings)
    assert s["item_id"].str.match(r"^[a-z0-9][a-z0-9_-]{0,63}$").all()  # lower-cased ids
    assert s["region"].isin(["us-east", "us-west", "eu-west", "ap-south", "ap-east"]).all()
    assert (s["item_size_bytes"] > 0).all()
    assert (
        s["category"]
        .isin(["video", "image", "audio", "document", "software", "web", "unknown"])
        .all()
    )
    assert set(s["metadata_status"]) <= {"ok", "missing", "conflict"}
    # unknown category is used exactly when metadata is missing/unknown-valued
    assert (s.loc[s["metadata_status"] == "missing", "category"] == "unknown").all()


def test_timezone_normalisation_to_utc(spark, settings, built):
    s = silver_pdf(spark, settings)
    api = s[s["source"] == SOURCE_API_JSON]
    # every API event (rendered in mixed offsets) normalises to the generator's UTC instant
    from cachemind.data.generator import iter_clean_events

    truth = {
        e.event_id: e.event_time_ms
        for e in iter_clean_events(
            WorkloadConfig(
                workload="zipf",
                n_events=6_000,
                n_items=400,
                n_users=120,
                seed=5,
                dataset_id=DS,
                event_rate_per_sec=0.05,
            )
        )
    }
    assert all(truth[e] == t for e, t in zip(api["event_id"], api["event_time_ms"], strict=True))


# --- Gold -------------------------------------------------------------------------


def test_gold_aggregations_match_pandas(spark, settings, built):
    from cachemind.bigdata.gold import read_gold

    s = silver_pdf(spark, settings)
    items = read_gold(spark, settings, "item_request_counts", DS).toPandas()
    exp_items = s.groupby("item_id").size()
    got = items.set_index("item_id")["total_requests"]
    assert got.sort_index().equals(
        exp_items.sort_index().rename("total_requests").astype(got.dtype)
    )
    assert items.sort_values("popularity_rank")["total_requests"].is_monotonic_decreasing
    cat = read_gold(spark, settings, "category_counts", DS).toPandas().set_index("category")
    assert_same_counts(cat["requests"], s.groupby("category").size())
    region = read_gold(spark, settings, "region_counts", DS).toPandas().set_index("region")
    assert_same_counts(region["requests"], s.groupby("region").size())
    daily = read_gold(spark, settings, "daily_totals", DS).toPandas()
    assert daily["requests"].sum() == len(s) and len(daily) >= 2
    hourly = read_gold(spark, settings, "hourly_totals", DS).toPandas()
    assert hourly["requests"].sum() == len(s)
    trend = read_gold(spark, settings, "item_popularity_trend", DS).toPandas()
    assert len(trend) > 0  # dataset spans more than one day


# --- E: RDD -------------------------------------------------------------------------


def test_rdd_demo_all_operations_cross_checked(spark, settings, built):
    from cachemind.bigdata.rdd_demo import run_rdd_demo

    rep = run_rdd_demo(spark, settings, DS)
    assert rep["all_cross_checks_ok"], rep["cross_checks"]
    ops = rep["operations"]
    for required in (
        "map+reduceByKey (requests per item)",
        "sortByKey (top items by count)",
        "filter (region == eu-west)",
        "flatMap (dimension tokens)",
        "distinct",
        "count",
        "take",
        "collect (regions, bounded)",
        "reduce (total bytes, max size)",
        "saveAsTextFile",
    ):
        assert required in ops
    assert rep["cross_checks"]["saved_lines"] == 100


def test_rdd_primitives_on_known_data(spark):
    sc = spark.sparkContext
    rdd = sc.parallelize(["a b", "b c", "c c"], 2)
    words = rdd.flatMap(str.split)
    assert sorted(words.collect()) == ["a", "b", "b", "c", "c", "c"]
    counts = words.map(lambda w: (w, 1)).reduceByKey(lambda x, y: x + y)
    assert dict(counts.collect()) == {"a": 1, "b": 2, "c": 3}
    assert counts.map(lambda kv: (kv[1], kv[0])).sortByKey(False).first() == (3, "c")
    assert words.distinct().count() == 3
    assert words.filter(lambda w: w != "c").count() == 3
    assert words.map(len).reduce(lambda x, y: x + y) == 6
    assert words.take(2) == ["a", "b"]


# --- F: SQL -------------------------------------------------------------------------


def test_sql_queries_match_pandas(spark, settings, built):
    from cachemind.bigdata.sql_demo import load_queries, register_views

    register_views(spark, settings, DS)
    q = load_queries()
    s = silver_pdf(spark, settings)
    top = spark.sql(q["01_top_requested_items"]).toPandas()
    exp = s.groupby("item_id").size().sort_values(ascending=False)
    assert top["requests"].tolist() == exp.iloc[:20].tolist()
    cat = spark.sql(q["02_requests_by_category"]).toPandas().set_index("category")
    assert_same_counts(cat["requests"], s.groupby("category").size())
    assert cat["share"].sum() == pytest.approx(1.0, abs=1e-5)
    reg = spark.sql(q["03_requests_by_region"]).toPandas().set_index("region")
    assert_same_counts(reg["distinct_users"], s.groupby("region")["user_id"].nunique())
    hourly = spark.sql(q["04_hourly_request_stats"]).toPandas()
    exp_h = s.groupby(pd.to_datetime(s["event_time_ms"], unit="ms").dt.floor("h")).size()
    assert hourly["requests"].tolist() == exp_h.tolist()
    daily = spark.sql(q["05_daily_access_trends"]).toPandas()
    assert daily["requests"].sum() == len(s) and pd.isna(daily["prev_day_requests"].iloc[0])
    grp = spark.sql(q["06_user_group_patterns"]).toPandas()
    assert grp.groupby("user_group")["share_of_group"].sum().round(5).eq(1.0).all()
    changes = spark.sql(q["07_item_popularity_changes"]).toPandas()
    assert len(changes) <= 20 and (changes["first_half"] + changes["second_half"] >= 5).all()


# --- B/C on Spark: features + MLlib ---------------------------------------------------


def test_spark_features_equal_python_online_features(spark, settings, built):
    from cachemind.bigdata.features_spark import build_features, features_path
    from cachemind.ml.features import NUMERIC_FEATURES, compute_features, reuse_labels
    from cachemind.simulation.trace import Trace

    build_features(spark, settings, DS, 150)
    sp = spark.read.parquet(str(features_path(settings, DS, 150))).orderBy("trace_pos").toPandas()
    tr = Trace.from_silver(settings.silver_dir / "requests", DS)
    py = compute_features(tr.keys, tr.event_time_ms, tr.size_bytes)
    assert (sp["item_id"].to_numpy() == tr.item_ids[tr.keys]).all()
    # Bit-identical, not merely close: a random-forest threshold equal to a feature value
    # turns a 1-ulp difference (JVM vs C-library log1p/sin/cos) into a different branch.
    np.testing.assert_array_equal(sp[list(NUMERIC_FEATURES)].to_numpy(), py)
    label, censored = reuse_labels(tr.keys, 150)
    assert (sp["label"].to_numpy() == label).all()
    assert (sp["censored"].to_numpy() == censored).all()


def test_mllib_train_save_reload_export(spark, settings, built):
    from cachemind.bigdata.mllib_train import load_trace_predictions, train_mllib_models
    from cachemind.ml.features import compute_features
    from cachemind.ml.model_spec import load_scorer
    from cachemind.simulation.trace import Trace

    reps = train_mllib_models(spark, settings, DS, 150, seed=3)
    assert {r["model_type"] for r in reps} == {"logistic_regression", "random_forest"}
    for r in reps:
        art = Path(r["artifact"])
        assert (art / "spark_model" / "metadata").exists()
        assert r["parity"]["passed"] and r["parity"]["save_reload_max_abs_diff"] == 0.0
        assert r["selection_metric"] == "validation PR-AUC"
        scorer = load_scorer(art)
        assert scorer.metadata["runtime"] == "spark-mllib"
        p = load_trace_predictions(art, built["silver"]["silver_rows"])
        assert np.all((p >= 0) & (p <= 1))
        # online path (NumPy features + exported model) == Spark batch, on EVERY row
        tr = Trace.from_silver(settings.silver_dir / "requests", DS)
        x = compute_features(tr.keys, tr.event_time_ms, tr.size_bytes)
        online = scorer.predict_proba(x, {"category": tr.category, "region": tr.region})
        assert np.max(np.abs(online - p)) <= 1e-12, r["model_type"]
        assert r["metrics"]["test@0.5"]["roc_auc"] > 0.6  # learns something on zipf
