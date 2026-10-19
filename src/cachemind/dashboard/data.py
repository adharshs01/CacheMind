"""Data access for the dashboard — pure functions, unit-tested, Streamlit-free.

Rules:
  * read bounded snapshots only (row limits on every table, tail of log files);
  * never start Spark; Parquet is read with pyarrow (column pruning);
  * every loader tolerates missing directories and returns an empty result;
  * provenance is explicit: callers label results LIVE / SAVED / SYNTHETIC / OFFLINE.
"""

from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.dataset as pads

from cachemind.config import Settings, get_settings, safe_child

MAX_ROWS = 5_000
LIVE_HEARTBEAT_S = 30.0
MAX_SIM_EVENTS = 200_000
MAX_SIM_CAPACITY = 50_000

PROVENANCE = {
    "live": (
        "LIVE DATA",
        f"Read from the running streaming pipeline in the last {int(LIVE_HEARTBEAT_S)} s.",
    ),
    "saved": ("SAVED EXPERIMENT", "Results stored by a completed experiment run."),
    "synthetic": ("SYNTHETIC EXAMPLE", "Computed now on a generated synthetic trace."),
    "offline": ("OFFLINE RESULT", "Batch output of the Spark pipeline (not live)."),
    "stale": ("NOT LIVE", "Last captured streaming output; the pipeline is not running."),
}


class InvalidInput(ValueError):
    pass


_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9_.-]{0,127}$")
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


def _check_name(value: str, what: str) -> str:
    """Identifiers reaching the filesystem must be plain names (no separators, no '..')."""
    if not isinstance(value, str) or not _NAME_RE.match(value) or ".." in value:
        raise InvalidInput(f"invalid {what}: {value!r}")
    return value


def validate_sim_request(n_events: int, capacity: int, policy: str) -> None:
    if not 1_000 <= n_events <= MAX_SIM_EVENTS:
        raise InvalidInput(f"trace length must be between 1,000 and {MAX_SIM_EVENTS:,}")
    if not 1 <= capacity <= MAX_SIM_CAPACITY:
        raise InvalidInput(f"capacity must be between 1 and {MAX_SIM_CAPACITY:,}")
    if policy not in {"fifo", "lru", "lfu", "belady", "ml", "hybrid"}:
        raise InvalidInput(f"unknown policy {policy!r}")


# --- experiments ----------------------------------------------------------------


def list_experiments(s: Settings | None = None) -> list[str]:
    s = s or get_settings()
    root = s.reports_dir / "experiments"
    if not root.exists():
        return []
    return sorted(p.name for p in root.iterdir() if (p / "results.parquet").exists())


def load_experiment(
    name: str, s: Settings | None = None, limit: int = MAX_ROWS
) -> tuple[pd.DataFrame, dict[str, Any]]:
    s = s or get_settings()
    d = safe_child(s.reports_dir, "experiments", _check_name(name, "experiment"))
    if not (d / "results.parquet").exists():
        return pd.DataFrame(), {}
    df = pd.read_parquet(d / "results.parquet")
    meta = json.loads((d / "meta.json").read_text()) if (d / "meta.json").exists() else {}
    return df.head(limit), meta


def hit_rate_table(df: pd.DataFrame, index: str = "workload") -> pd.DataFrame:
    """mean hit rate per (index, policy) with seed std — used by the comparison pages."""
    if df.empty:
        return df
    g = df.groupby([index, "policy"])["hit_rate"]
    out = g.mean().unstack()
    return out.reindex(columns=sorted(out.columns, key=_policy_order))


def _policy_order(p: str) -> tuple[int, str]:
    order = ["fifo", "lru", "lfu", "ml-lr", "ml-rf", "hybrid-lr", "hybrid-rf", "belady"]
    return (order.index(p) if p in order else 99, p)


def paired_delta(
    df: pd.DataFrame,
    baseline: str = "lru",
    keys: tuple[str, ...] = ("workload", "seed", "capacity"),
) -> pd.DataFrame:
    if df.empty or baseline not in set(df["policy"]):
        return pd.DataFrame()
    base = df[df["policy"] == baseline][[*keys, "hit_rate"]].rename(columns={"hit_rate": "base"})
    m = df.merge(base, on=list(keys))
    m["delta_pp"] = (m["hit_rate"] - m["base"]) * 100
    return (
        m.groupby(["workload", "policy"])["delta_pp"]
        .agg(["mean", "min", "max", "count"])
        .reset_index()
    )


# --- models ---------------------------------------------------------------------


def list_models(s: Settings | None = None) -> pd.DataFrame:
    s = s or get_settings()
    rows = []
    for root in (s.models_dir, s.artifacts_dir / "experiment_models"):
        if not root.exists():
            continue
        for meta_path in root.glob("**/metadata.json"):
            meta = json.loads(meta_path.read_text())
            parity_path = meta_path.parent / "parity.json"
            parity = json.loads(parity_path.read_text()) if parity_path.exists() else {}
            t = meta.get("metrics", {}).get("test@val_threshold", {})
            rows.append(
                {
                    "path": str(meta_path.parent),
                    "runtime": meta.get("runtime"),
                    "model_type": meta.get("model_type"),
                    "version": meta.get("model_version"),
                    "dataset": meta.get("dataset_id") or meta.get("dataset", {}).get("dataset_id"),
                    "horizon": meta.get("label_horizon"),
                    "roc_auc": t.get("roc_auc"),
                    "pr_auc": t.get("pr_auc"),
                    "f1": t.get("f1"),
                    "brier": t.get("brier"),
                    "ece": t.get("ece"),
                    "positive_rate": t.get("positive_rate"),
                    "parity_passed": parity.get("passed"),
                    "created_at": meta.get("created_at"),
                }
            )
    return pd.DataFrame(rows).sort_values("created_at", ascending=False) if rows else pd.DataFrame()


def load_model_metadata(path: str) -> dict[str, Any]:
    p = Path(path)
    return json.loads((p / "metadata.json").read_text())


# --- lakehouse (Gold / quality) -----------------------------------------------------


def list_datasets(s: Settings | None = None) -> list[str]:
    s = s or get_settings()
    base = s.silver_dir / "requests"
    if not base.exists():
        return []
    return sorted(p.name.split("=", 1)[1] for p in base.glob("dataset_id=*"))


def load_gold(
    table: str, dataset_id: str, s: Settings | None = None, limit: int = MAX_ROWS
) -> pd.DataFrame:
    s = s or get_settings()
    if table not in GOLD_TABLES:
        raise InvalidInput(f"unknown gold table {table!r}")
    d = safe_child(s.gold_dir, table, f"dataset_id={_check_name(dataset_id, 'dataset id')}")
    if not d.exists():
        return pd.DataFrame()
    return pads.dataset(str(d), format="parquet").head(limit).to_pandas()


def load_quality_report(dataset_id: str, s: Settings | None = None) -> dict[str, Any]:
    s = s or get_settings()
    p = s.lake_dir / "_reports" / f"silver_{_check_name(dataset_id, 'dataset id')}.json"
    return json.loads(p.read_text()) if p.exists() else {}


# --- streaming (live) ------------------------------------------------------------------


def tail_jsonl(path: Path, n: int = 300) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    lines = path.read_text().splitlines()[-n:]
    out = []
    for line in lines:
        try:
            out.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return out


@dataclass
class StreamSnapshot:
    provenance: str  # "live" | "stale" | "none"
    heartbeat_age_s: float | None
    progress: pd.DataFrame
    recent_events: pd.DataFrame
    status: dict[str, Any]


def streaming_snapshot(s: Settings | None = None, n_events: int = 50) -> StreamSnapshot:
    s = s or get_settings()
    rt = s.stream_runtime_dir
    status = json.loads((rt / "status.json").read_text()) if (rt / "status.json").exists() else {}
    hb = status.get("heartbeat")
    age = None
    if hb:
        from datetime import UTC, datetime

        age = (datetime.now(UTC) - datetime.fromisoformat(hb)).total_seconds()
    live = status.get("state") != "stopped" and age is not None and age < LIVE_HEARTBEAT_S
    progress = pd.DataFrame(
        [r for r in tail_jsonl(rt / "progress.jsonl") if r.get("event") == "progress"]
    )
    events = pd.DataFrame()
    base = s.lake_dir / "silver_stream" / "requests"
    if base.exists():
        batches = sorted(base.glob("batch_id=*"))
        if batches:
            events = pads.dataset(str(batches[-1]), format="parquet").head(n_events).to_pandas()
    prov = "live" if live else ("stale" if (status or not events.empty) else "none")
    return StreamSnapshot(prov, age, progress, events, status)


# --- pipeline evidence -------------------------------------------------------------------


def latest_pipeline_evidence(s: Settings | None = None) -> dict[str, Any]:
    s = s or get_settings()
    root = s.reports_dir / "pipeline"
    if not root.exists():
        return {}
    runs = sorted(root.glob("*/evidence.json"), key=lambda p: p.stat().st_mtime)
    return json.loads(runs[-1].read_text()) if runs else {}


def file_age_s(path: Path) -> float | None:
    return time.time() - path.stat().st_mtime if path.exists() else None
