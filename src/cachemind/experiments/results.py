"""Selecting cache-policy experiment results from reports/experiments/.

Experiments write results with different shapes (cache simulations, Spark timings,
streaming scenarios, sensitivity analyses). Consumers that need *cache-policy* rows
(the Spark SQL `experiments` view, HBase `cache_experiments`) select files by schema
and project them onto this fixed, typed column set instead of merging every schema.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq

# column -> Spark SQL type
CACHE_RESULT_COLUMNS: dict[str, str] = {
    "experiment": "string",
    "workload": "string",
    "policy": "string",
    "capacity": "bigint",
    "seed": "bigint",
    "hits": "bigint",
    "misses": "bigint",
    "hit_rate": "double",
    "miss_rate": "double",
    "evictions": "bigint",
    "wall_time_s": "double",
}
OPTIONAL_COLUMNS = ("run_id", "dataset_id", "horizon", "model_version", "executed_at")


def cache_result_files(reports_dir: Path) -> list[Path]:
    """results.parquet files that contain every cache-result column."""
    out = []
    for f in sorted((reports_dir / "experiments").glob("*/results.parquet")):
        names = set(pq.read_schema(f).names)
        if set(CACHE_RESULT_COLUMNS) <= names:
            out.append(f)
    return out
