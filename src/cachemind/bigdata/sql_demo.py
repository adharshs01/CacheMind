"""Spark SQL analytics over registered temporary views (independently executable).

    python -m cachemind.bigdata.sql_demo --dataset <dataset_id>

Views:
  requests     canonical Silver requests for the dataset
  experiments  cache experiment results (reports/experiments/**/results.parquet),
               registered only when experiment results exist

Queries live in ``sql/*.sql`` (one file per analysis). Outputs are bounded and
saved as CSV + JSON under ``reports/sql/<dataset_id>/``.
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any, cast

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from cachemind.bigdata.etl import read_silver
from cachemind.config import REPO_ROOT, Settings, get_settings, safe_child
from cachemind.experiments.results import CACHE_RESULT_COLUMNS, cache_result_files

SQL_DIR = REPO_ROOT / "sql"
MAX_ROWS = 500


def load_queries() -> dict[str, str]:
    return {p.stem: p.read_text() for p in sorted(SQL_DIR.glob("*.sql"))}


def register_views(spark: SparkSession, settings: Settings, dataset_id: str) -> list[str]:
    read_silver(spark, settings, dataset_id).createOrReplaceTempView("requests")
    views = ["requests"]
    files = cache_result_files(settings.reports_dir)
    if files:
        # Each file is projected onto one typed column set before the union; results of
        # other shapes (Spark timings, streaming scenarios) are not cache-policy rows.
        cols = [F.col(c).cast(t).alias(c) for c, t in CACHE_RESULT_COLUMNS.items()]
        frames = [spark.read.parquet(str(f)).select(*cols) for f in files]
        view = frames[0]
        for frame in frames[1:]:
            view = view.unionByName(frame)
        view.createOrReplaceTempView("experiments")
        views.append("experiments")
    return views


def run_sql_demo(
    spark: SparkSession, settings: Settings, dataset_id: str, out_dir: Path | None = None
) -> dict[str, Any]:
    t0 = time.time()
    out_dir = out_dir or safe_child(settings.reports_dir, "sql", dataset_id)
    out_dir.mkdir(parents=True, exist_ok=True)
    views = register_views(spark, settings, dataset_id)
    report: dict[str, Any] = {"dataset_id": dataset_id, "views": views, "queries": {}}
    for name, text in load_queries().items():
        if "FROM experiments" in text and "experiments" not in views:
            report["queries"][name] = {
                "status": "skipped",
                "reason": "no experiment results yet (run `make experiments` first)",
            }
            continue
        q0 = time.time()
        pdf = cast(pd.DataFrame, spark.sql(text).limit(MAX_ROWS).toPandas())
        pdf.to_csv(out_dir / f"{name}.csv", index=False)
        report["queries"][name] = {
            "status": "ok",
            "rows": len(pdf),
            "seconds": round(time.time() - q0, 3),
            "preview": json.loads(pdf.head(10).to_json(orient="records", date_format="iso")),
        }
    report["seconds"] = round(time.time() - t0, 2)
    (out_dir / "sql_report.json").write_text(json.dumps(report, indent=2, default=str) + "\n")
    return report


def explain(spark: SparkSession, name: str) -> str:
    """Physical plan for a named query (used in performance notes)."""
    return spark.sql(load_queries()[name])._jdf.queryExecution().executedPlan().toString()


def main(argv: list[str] | None = None) -> int:
    from cachemind.bigdata.spark_session import get_spark

    ap = argparse.ArgumentParser(description="Spark SQL analytics demonstration")
    ap.add_argument("--dataset", required=True)
    args = ap.parse_args(argv)
    spark = get_spark("cachemind-sql-demo")
    try:
        rep = run_sql_demo(spark, get_settings(), args.dataset)
    finally:
        spark.stop()
    print(
        json.dumps(
            {
                k: {kk: vv for kk, vv in v.items() if kk != "preview"}
                for k, v in rep["queries"].items()
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
