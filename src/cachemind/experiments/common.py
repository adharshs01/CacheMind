"""Shared experiment machinery.

Fairness rules enforced here:
  * every policy in a comparison receives the identical ``keys`` array and capacity;
  * all policies process the whole trace (same warm-up) and are scored only on the
    TEST block ``[bounds.test_start, n)``;
  * models are trained on the TRAIN block only, hyper-parameters / hybrid alpha are
    chosen on the VALIDATION block only;
  * Belady is reported as an offline reference, never as a competitor.
"""

from __future__ import annotations

import hashlib
import json
import platform
import time
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from cachemind import __version__
from cachemind.config import REPO_ROOT, get_settings, safe_child
from cachemind.data.workloads import WorkloadConfig
from cachemind.ml.dataset import ReuseDataset, build_reuse_dataset
from cachemind.ml.model_spec import NumpyScorer, load_scorer
from cachemind.ml.predictors import OnlinePredictor, PrecomputedPredictor
from cachemind.ml.sklearn_train import train_sklearn_models
from cachemind.policies import BeladyPolicy, FIFOPolicy, LFUPolicy, LRUPolicy
from cachemind.policies.base import CachePolicy
from cachemind.policies.learned import HybridPolicy, MLPolicy
from cachemind.simulation.engine import SimulationResult, simulate
from cachemind.simulation.trace import Trace

CLASSICAL = ("fifo", "lru", "lfu")


def _source_hash() -> str:
    h = hashlib.sha256()
    for p in sorted((REPO_ROOT / "src" / "cachemind").rglob("*.py")):
        h.update(p.relative_to(REPO_ROOT).as_posix().encode())
        h.update(p.read_bytes())
    return "src-" + h.hexdigest()[:12]


# Hashed once at import, i.e. the code that is actually executing in this process
# (editing files while a long experiment runs must not change its recorded version).
_CODE_VERSION = _source_hash()


def code_version() -> str:
    """Content hash of the package source (the repo is not required to be a git repo)."""
    return _CODE_VERSION


@dataclass
class Workbench:
    """A trace plus its ML dataset and trained models for one (workload, seed, H)."""

    trace: Trace
    dataset: ReuseDataset
    models: dict[str, NumpyScorer] = field(default_factory=dict)
    model_reports: dict[str, dict[str, Any]] = field(default_factory=dict)

    @property
    def bounds(self):
        return self.dataset.bounds


def make_trace(
    workload: str, seed: int, n_events: int, n_items: int, n_users: int | None = None, **knobs: Any
) -> Trace:
    cfg = WorkloadConfig(
        workload=workload,
        n_events=n_events,
        n_items=n_items,
        n_users=n_users or max(100, n_items // 2),
        seed=seed,
        event_rate_per_sec=1.0,
        **knobs,
    )
    return Trace.from_generator(cfg)


def build_workbench(
    trace: Trace,
    horizon: int,
    seed: int,
    models_root: Path,
    model_types: tuple[str, ...] = ("logistic_regression", "random_forest"),
    max_train_rows: int = 120_000,
) -> Workbench:
    ds = build_reuse_dataset(trace, horizon)
    wb = Workbench(trace=trace, dataset=ds)
    add_models(wb, models_root, seed, model_types, max_train_rows)
    return wb


def add_models(
    wb: Workbench,
    models_root: Path,
    seed: int,
    model_types: tuple[str, ...],
    max_train_rows: int = 120_000,
) -> None:
    todo = tuple(m for m in model_types if m not in wb.models)
    if not todo:
        return
    for rep in train_sklearn_models(
        wb.dataset, models_root, seed=seed, max_train_rows=max_train_rows, model_types=todo
    ):
        wb.models[rep["model_type"]] = load_scorer(Path(rep["artifact"]))
        wb.model_reports[rep["model_type"]] = rep


def select_horizon(
    trace: Trace, capacity: int, seed: int, models_root: Path, factors: tuple[int, ...] = (2, 5, 10)
) -> tuple[Workbench, list[dict]]:
    """Pick H = factor * capacity by ML-LR hit rate on the VALIDATION block."""
    candidates: list[dict[str, Any]] = []
    for f in factors:
        h = f * capacity
        try:
            wb = build_workbench(trace, h, seed, models_root, model_types=("logistic_regression",))
        except ValueError as exc:  # e.g. split too short for this horizon
            candidates.append({"factor": f, "horizon": h, "error": str(exc)})
            continue
        r = run_policy(wb, make_policy("ml", wb, "logistic_regression", h), capacity, split="val")
        candidates.append({"factor": f, "horizon": h, "val_hit_rate": r.hit_rate, "_wb": wb})
    valid = [c for c in candidates if "_wb" in c]
    if not valid:
        raise ValueError(f"no feasible horizon for capacity {capacity}: {candidates}")
    best = max(valid, key=lambda c: c["val_hit_rate"])
    return best["_wb"], [{k: v for k, v in c.items() if k != "_wb"} for c in candidates]


def make_policy(
    name: str,
    wb: Workbench | None = None,
    model: str | None = None,
    horizon: int | None = None,
    alpha: float = 0.5,
    precomputed: np.ndarray | None = None,
) -> CachePolicy:
    if name == "fifo":
        return FIFOPolicy()
    if name == "lru":
        return LRUPolicy()
    if name == "lfu":
        return LFUPolicy()
    if name == "belady":
        return BeladyPolicy()
    if name in ("ml", "hybrid"):
        if wb is None or horizon is None:
            raise ValueError(f"{name} needs a workbench and horizon")
        if precomputed is not None:
            predictor: Any = PrecomputedPredictor(precomputed, source="spark-mllib-batch")
        else:
            scorer = wb.models.get(model or "logistic_regression")
            predictor = OnlinePredictor(
                scorer, wb.trace, load_error=None if scorer else f"model {model} missing"
            )
        if name == "ml":
            return MLPolicy(predictor, horizon)
        return HybridPolicy(predictor, horizon, alpha=alpha)
    raise ValueError(f"unknown policy {name!r}")


def run_policy(
    wb: Workbench,
    policy: CachePolicy,
    capacity: int,
    *,
    split: str = "test",
    timeseries_window: int = 0,
    measure_memory: bool = False,
) -> SimulationResult:
    b = wb.bounds
    lo, hi = {"test": (b.test_start, b.test_end), "val": (b.val_start, b.val_end), "all": (0, b.n)}[
        split
    ]
    return simulate(
        wb.trace.keys,
        policy,
        capacity,
        measure_from=lo,
        measure_to=hi,
        timeseries_window=timeseries_window,
        item_ids=wb.trace.item_ids,
        measure_memory=measure_memory,
    )


def result_row(
    exp: str,
    wb: Workbench,
    res: SimulationResult,
    *,
    workload: str,
    seed: int,
    model: str | None = None,
    horizon: int | None = None,
    alpha: float | None = None,
    runtime: str | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    ps = res.policy_stats
    lat = ps.get("inference_latency_us") or {}
    policy = (
        res.policy
        if res.policy not in ("ml", "hybrid")
        else f"{res.policy}-{'lr' if model == 'logistic_regression' else 'rf'}"
    )
    return {
        "experiment": exp,
        "workload": workload,
        "seed": seed,
        "policy": policy,
        "base_policy": res.policy,
        "model_type": model,
        "runtime": runtime,
        "capacity": res.capacity,
        "horizon": horizon,
        "alpha": alpha,
        "dataset_id": wb.trace.dataset_id,
        "n_requests_total": len(wb.trace),
        "n_distinct_items": wb.trace.n_distinct,
        "measured_requests": res.n_requests,
        "measure_from": res.measure_from,
        "hits": res.hits,
        "misses": res.misses,
        "evictions": res.evictions,
        "hit_rate": res.hit_rate,
        "miss_rate": res.miss_rate,
        "wall_time_s": res.wall_time_s,
        "cpu_time_s": res.cpu_time_s,
        "eviction_decision_us_p50": res.eviction_decision_us.get("p50"),
        "eviction_decision_us_p99": res.eviction_decision_us.get("p99"),
        "prediction_calls": ps.get("prediction_calls"),
        "inference_us_p50": lat.get("p50"),
        "inference_us_p95": lat.get("p95"),
        "fallback_evictions": ps.get("fallback_evictions"),
        "expired_evictions": ps.get("expired_evictions"),
        "model_version": (wb.model_reports.get(model or "", {}) or {}).get("model_version"),
        "peak_memory_kib": res.peak_memory_kib,
        "run_id": uuid.uuid4().hex[:12],
        "executed_at": datetime.now(UTC).isoformat(),
        **(extra or {}),
    }


def tune_hybrid_alpha(
    wb: Workbench,
    capacity: int,
    horizon: int,
    model: str,
    grid: tuple[float, ...] = (0.0, 0.25, 0.5, 0.75, 1.0),
) -> tuple[float, list]:
    """Choose alpha by hit rate on the VALIDATION block (test block never consulted)."""
    curve = []
    for a in grid:
        r = run_policy(
            wb, make_policy("hybrid", wb, model, horizon, alpha=a), capacity, split="val"
        )
        curve.append({"alpha": a, "val_hit_rate": r.hit_rate})
    best = max(curve, key=lambda c: (c["val_hit_rate"], -abs(c["alpha"] - 0.5)))
    return best["alpha"], curve


def save_results(exp: str, rows: list[dict[str, Any]], meta: dict[str, Any]) -> Path:
    out = safe_child(get_settings().reports_dir, "experiments", exp)
    out.mkdir(parents=True, exist_ok=True)
    # Nested values (dicts/lists such as per-scenario checks) are stored as JSON text:
    # Parquet cannot store e.g. an empty struct, and text keeps the schema stable.
    flat = [
        {
            k: (json.dumps(v, default=str) if isinstance(v, dict | list | tuple) else v)
            for k, v in r.items()
        }
        for r in rows
    ]
    df = pd.DataFrame(flat)
    df.to_parquet(out / "results.parquet", index=False)
    df.to_csv(out / "results.csv", index=False)
    meta = {
        **meta,
        "experiment": exp,
        "rows": len(rows),
        "code_version": code_version(),
        "cachemind_version": __version__,
        "python": platform.python_version(),
        "machine": platform.machine(),
        "platform": platform.platform(),
        "saved_at": datetime.now(UTC).isoformat(),
        "synthetic_data": True,
    }
    (out / "meta.json").write_text(json.dumps(meta, indent=2, default=str) + "\n")
    return out


def summarize(df: pd.DataFrame, by: list[str], value: str = "hit_rate") -> pd.DataFrame:
    g = df.groupby(by)[value]
    return g.agg(["mean", "std", "min", "max", "count"]).reset_index()


def paired_vs(
    df: pd.DataFrame, baseline: str, keys: list[str], value: str = "hit_rate"
) -> pd.DataFrame:
    """Per-seed paired differences policy - baseline (same trace, same capacity)."""
    base = df[df["policy"] == baseline][[*keys, value]].rename(columns={value: "base"})
    merged = df.merge(base, on=keys)
    merged["delta"] = merged[value] - merged["base"]
    out = (
        merged.groupby([k for k in keys if k != "seed"] + ["policy"])["delta"]
        .agg(["mean", "std", "min", "max", "count"])
        .reset_index()
    )
    wins = (
        merged.assign(win=merged["delta"] > 0)
        .groupby([k for k in keys if k != "seed"] + ["policy"])["win"]
        .sum()
        .reset_index()
    )
    return out.merge(wins, on=[k for k in keys if k != "seed"] + ["policy"])


class Timer:
    def __enter__(self) -> Timer:
        self.t0 = time.perf_counter()
        return self

    def __exit__(self, *exc: Any) -> None:
        self.seconds = time.perf_counter() - self.t0
