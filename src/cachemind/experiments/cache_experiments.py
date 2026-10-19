"""Cache-policy research experiments 1-9 (lightweight profile: no JVM needed).

    python -m cachemind.experiments.cache_experiments --exp all [--scale standard|quick]

1  FIFO vs LRU vs LFU                      (derived from the main grid)
2  ML-guided vs classical                   (main grid: all workloads x seeds)
3  Cache capacities                         (capacity sweep, H re-selected per capacity)
4  Workload distributions                   (derived from the main grid)
5  Sudden popularity shift                  (shift INSIDE the test block, time series)
6  Concept drift                            (drift workload, windowed hit rate + model AUC)
7  Logistic regression vs random forest     (classification metrics + cache hit rates)
8  Hybrid caching                           (alpha tuned on validation, applied to test)
9  ML inference overhead                    (repeated timing, latency breakdown, memory)

All traces are SYNTHETIC. ML models here use the scikit-learn runtime; the Spark
MLlib runtime is exercised by ``spark_experiments`` on the Big Data pipeline.
"""

from __future__ import annotations

import argparse
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.metrics import roc_auc_score

from cachemind.config import get_settings
from cachemind.data.workloads import WORKLOADS
from cachemind.experiments.common import (
    CLASSICAL,
    Workbench,
    add_models,
    make_policy,
    make_trace,
    paired_vs,
    result_row,
    run_policy,
    save_results,
    select_horizon,
    summarize,
    tune_hybrid_alpha,
)
from cachemind.logging_utils import get_logger, log_event
from cachemind.ml.features import CATEGORICAL_FEATURES

log = get_logger("cachemind.experiments")
MODELS = ("logistic_regression", "random_forest")


@dataclass(frozen=True)
class Scale:
    n_events: int
    n_items: int
    seeds: tuple[int, ...]
    capacity: int
    capacities: tuple[int, ...]
    workloads: tuple[str, ...]


SCALES = {
    "standard": Scale(100_000, 5_000, (1, 2, 3), 100, (25, 50, 100, 200, 400, 800), WORKLOADS),
    "quick": Scale(30_000, 1_500, (1,), 40, (20, 40, 80), ("zipf", "shift", "scan")),
}


def _models_root() -> Path:
    return get_settings().artifacts_dir / "experiment_models"


def evaluate_all(
    exp: str,
    wb: Workbench,
    workload: str,
    seed: int,
    capacity: int,
    horizon: int,
    alpha_by_model: dict[str, float],
    extra: dict[str, Any] | None = None,
    timeseries_window: int = 0,
    models: tuple[str, ...] = MODELS,
) -> list[dict[str, Any]]:
    rows = []
    for name in (*CLASSICAL, "belady"):
        res = run_policy(wb, make_policy(name), capacity, timeseries_window=timeseries_window)
        row = result_row(exp, wb, res, workload=workload, seed=seed, horizon=horizon, extra=extra)
        row["timeseries"] = json.dumps(res.timeseries) if timeseries_window else None
        rows.append(row)
    for model in models:
        res = run_policy(
            wb, make_policy("ml", wb, model, horizon), capacity, timeseries_window=timeseries_window
        )
        row = result_row(
            exp,
            wb,
            res,
            workload=workload,
            seed=seed,
            model=model,
            horizon=horizon,
            runtime="sklearn",
            extra=extra,
        )
        row["timeseries"] = json.dumps(res.timeseries) if timeseries_window else None
        rows.append(row)
        a = alpha_by_model[model]
        res = run_policy(
            wb,
            make_policy("hybrid", wb, model, horizon, alpha=a),
            capacity,
            timeseries_window=timeseries_window,
        )
        row = result_row(
            exp,
            wb,
            res,
            workload=workload,
            seed=seed,
            model=model,
            horizon=horizon,
            alpha=a,
            runtime="sklearn",
            extra=extra,
        )
        row["timeseries"] = json.dumps(res.timeseries) if timeseries_window else None
        rows.append(row)
    return rows


def _prepare(
    workload: str,
    seed: int,
    capacity: int,
    sc: Scale,
    models: tuple[str, ...] = MODELS,
    **knobs: Any,
) -> tuple[Workbench, dict[str, Any]]:
    trace = make_trace(workload, seed, sc.n_events, sc.n_items, **knobs)
    wb, h_candidates = select_horizon(trace, capacity, seed, _models_root())
    add_models(wb, _models_root(), seed, models)
    alphas, curves = {}, {}
    for model in models:
        alphas[model], curves[model] = tune_hybrid_alpha(wb, capacity, wb.dataset.horizon, model)
    meta = {
        "horizon_candidates": h_candidates,
        "alpha_curves": curves,
        "selected_horizon": wb.dataset.horizon,
        "selected_alpha": alphas,
    }
    return wb, meta


# --- Experiment 2 (+1, 4, 7, 8 derived) --------------------------------------------


def main_grid(sc: Scale) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    model_rows: list[dict[str, Any]] = []
    for workload in sc.workloads:
        for seed in sc.seeds:
            t0 = time.time()
            wb, meta = _prepare(workload, seed, sc.capacity, sc)
            rows += evaluate_all(
                "exp02_ml_vs_classical",
                wb,
                workload,
                seed,
                sc.capacity,
                wb.dataset.horizon,
                meta["selected_alpha"],
            )
            for model, rep in wb.model_reports.items():
                m = rep["metrics"]["test@val_threshold"]
                model_rows.append(
                    {
                        "workload": workload,
                        "seed": seed,
                        "model_type": model,
                        "horizon": wb.dataset.horizon,
                        "model_version": rep["model_version"],
                        "test_positive_rate": m["positive_rate"],
                        "roc_auc": m["roc_auc"],
                        "pr_auc": m["pr_auc"],
                        "precision": m["precision"],
                        "recall": m["recall"],
                        "f1": m["f1"],
                        "brier": m["brier"],
                        "ece": m["ece"],
                        "threshold": m["threshold"],
                        "fit_seconds": rep["fit_seconds"],
                        "selected_params": json.dumps(rep["selected_params"]),
                        "export_predict_one_us_p50": rep["latency"]["export_predict_one_us_p50"],
                        "baseline_recency_roc_auc": rep["baselines_test"]["heuristic_recency"][
                            "roc_auc"
                        ],
                        "baseline_recency_pr_auc": rep["baselines_test"]["heuristic_recency"][
                            "pr_auc"
                        ],
                        "baseline_frequency_roc_auc": rep["baselines_test"]["heuristic_frequency"][
                            "roc_auc"
                        ],
                        "baseline_frequency_pr_auc": rep["baselines_test"]["heuristic_frequency"][
                            "pr_auc"
                        ],
                        "alpha_curve": json.dumps(meta["alpha_curves"][model]),
                        "selected_alpha": meta["selected_alpha"][model],
                        "horizon_candidates": json.dumps(meta["horizon_candidates"]),
                    }
                )
            log_event(
                log,
                "main grid cell done",
                workload=workload,
                seed=seed,
                seconds=round(time.time() - t0, 1),
                horizon=wb.dataset.horizon,
            )
    return pd.DataFrame(rows), model_rows


def derive_from_grid(df: pd.DataFrame, model_rows: list[dict[str, Any]], sc: Scale) -> None:
    keys = ["workload", "seed", "capacity"]
    common = {
        "scale": sc.__dict__,
        "protocol": "train/val/test chronological split; "
        "H and alpha chosen on validation; metrics on test block only",
    }
    save_results(
        "exp02_ml_vs_classical",
        df.to_dict("records"),
        {
            **common,
            "summary": summarize(df, ["workload", "policy"]).to_dict("records"),
            "paired_vs_lru": paired_vs(df, "lru", keys).to_dict("records"),
            "paired_vs_lfu": paired_vs(df, "lfu", keys).to_dict("records"),
        },
    )
    exp1 = df[df["policy"].isin([*CLASSICAL, "belady"])].assign(experiment="exp01_classical")
    save_results(
        "exp01_classical",
        exp1.to_dict("records"),
        {**common, "summary": summarize(exp1, ["workload", "policy"]).to_dict("records")},
    )
    exp4 = df.assign(experiment="exp04_workloads")
    pivot = exp4.groupby(["workload", "policy"])["hit_rate"].mean().unstack()
    save_results(
        "exp04_workloads",
        exp4.to_dict("records"),
        {
            **common,
            "mean_hit_rate_pivot": json.loads(pivot.to_json(orient="index")),
            "best_online_policy_per_workload": {
                w: pivot.loc[w].drop(labels=["belady"]).idxmax() for w in pivot.index
            },
        },
    )
    mdf = pd.DataFrame(model_rows)
    exp7 = df[df["base_policy"] == "ml"].assign(experiment="exp07_lr_vs_rf")
    save_results(
        "exp07_lr_vs_rf",
        exp7.to_dict("records"),
        {
            **common,
            "classification_metrics": mdf.to_dict("records"),
            "classification_summary": mdf.groupby("model_type")[
                [
                    "roc_auc",
                    "pr_auc",
                    "f1",
                    "brier",
                    "ece",
                    "fit_seconds",
                    "export_predict_one_us_p50",
                ]
            ]
            .mean()
            .to_dict("index"),
        },
    )
    exp8 = df[df["base_policy"].isin(["hybrid", "ml", "lru"])].assign(experiment="exp08_hybrid")
    save_results(
        "exp08_hybrid",
        exp8.to_dict("records"),
        {
            **common,
            "alpha_selection": mdf[
                ["workload", "seed", "model_type", "selected_alpha", "alpha_curve"]
            ].to_dict("records"),
            "paired_vs_lru": paired_vs(exp8, "lru", keys).to_dict("records"),
        },
    )
    (get_settings().reports_dir / "experiments" / "model_metrics.json").write_text(
        json.dumps(model_rows, indent=2, default=str)
    )


# --- Experiment 3 -----------------------------------------------------------------


def exp03_capacity(sc: Scale, workloads: tuple[str, ...] = ("zipf", "mixed")) -> pd.DataFrame:
    rows = []
    for workload in workloads:
        for seed in sc.seeds:
            for cap in sc.capacities:
                wb, meta = _prepare(workload, seed, cap, sc, models=("logistic_regression",))
                rows += evaluate_all(
                    "exp03_capacity",
                    wb,
                    workload,
                    seed,
                    cap,
                    wb.dataset.horizon,
                    meta["selected_alpha"],
                    models=("logistic_regression",),
                )
                log_event(log, "capacity cell done", workload=workload, seed=seed, cap=cap)
    df = pd.DataFrame(rows)
    save_results(
        "exp03_capacity",
        rows,
        {
            "scale": sc.__dict__,
            "workloads": workloads,
            "summary": summarize(df, ["workload", "capacity", "policy"]).to_dict("records"),
            "paired_vs_lru": paired_vs(df, "lru", ["workload", "seed", "capacity"]).to_dict(
                "records"
            ),
        },
    )
    return df


# --- Experiments 5 & 6 --------------------------------------------------------------


def _recovery(ts: list[dict[str, Any]], change_t: int, window: int) -> dict[str, Any]:
    before = [p["hit_rate"] for p in ts if p["t_end"] <= change_t]
    after = [(p["t_end"], p["hit_rate"]) for p in ts if p["t_end"] > change_t]
    if not before or not after:
        return {}
    ref = float(np.mean(before[-5:]))
    dip = min(h for _, h in after[:10])
    recovered = next((t for t, h in after if h >= 0.9 * ref), None)
    return {
        "pre_change_hit_rate": ref,
        "min_after_change": dip,
        "requests_to_recover_90pct": (recovered - change_t) if recovered else None,
    }


def exp05_shift(sc: Scale) -> pd.DataFrame:
    rows = []
    window = max(500, sc.n_events // 100)
    for seed in sc.seeds:
        wb, meta = _prepare("shift", seed, sc.capacity, sc, shift_points=(0.9,))
        change_t = int(0.9 * sc.n_events)
        part = evaluate_all(
            "exp05_shift",
            wb,
            "shift@0.9",
            seed,
            sc.capacity,
            wb.dataset.horizon,
            meta["selected_alpha"],
            timeseries_window=window,
        )
        for r in part:
            r.update(_recovery(json.loads(r["timeseries"]), change_t, window))
            r["change_position"] = change_t
        rows += part
    df = pd.DataFrame(rows)
    save_results(
        "exp05_shift",
        rows,
        {
            "scale": sc.__dict__,
            "note": "popularity re-ranked at 90% of the trace, i.e. inside "
            "the test block; models never saw a shift during training",
            "summary": summarize(df, ["policy"]).to_dict("records"),
            "recovery": df.groupby("policy")[
                ["pre_change_hit_rate", "min_after_change", "requests_to_recover_90pct"]
            ]
            .mean()
            .reset_index()
            .to_dict("records"),
        },
    )
    return df


def exp06_drift(sc: Scale) -> pd.DataFrame:
    rows = []
    window = max(500, sc.n_events // 100)
    auc_rows = []
    for seed in sc.seeds:
        wb, meta = _prepare("drift", seed, sc.capacity, sc)
        rows += evaluate_all(
            "exp06_drift",
            wb,
            "drift",
            seed,
            sc.capacity,
            wb.dataset.horizon,
            meta["selected_alpha"],
            timeseries_window=window,
        )
        # Predictive quality over time on the test block (labels are known offline).
        ds = wb.dataset
        scorer = wb.models["logistic_regression"]
        lo, hi = ds.bounds.eligible_range("test")
        step = max(1000, (hi - lo) // 8)
        for start in range(lo, hi, step):
            pos = np.arange(start, min(hi, start + step))
            pos = pos[~ds.censored[pos]]
            xn, cats, y = ds.take(pos)
            if len(np.unique(y)) < 2:
                continue
            p = scorer.predict_proba(xn, {c: cats[c] for c in CATEGORICAL_FEATURES})
            auc_rows.append(
                {
                    "seed": seed,
                    "window_start": int(start),
                    "roc_auc": float(roc_auc_score(y, p)),
                    "positive_rate": float(y.mean()),
                }
            )
    df = pd.DataFrame(rows)
    save_results(
        "exp06_drift",
        rows,
        {
            "scale": sc.__dict__,
            "summary": summarize(df, ["policy"]).to_dict("records"),
            "lr_auc_over_test_block": auc_rows,
        },
    )
    return df


# --- Experiment 9 -----------------------------------------------------------------


def exp09_overhead(sc: Scale, repeats: int = 5) -> pd.DataFrame:
    seed = sc.seeds[0]
    wb, meta = _prepare("zipf", seed, sc.capacity, sc)
    h = wb.dataset.horizon
    configs = [
        ("lru", None),
        ("lfu", None),
        ("ml", "logistic_regression"),
        ("ml", "random_forest"),
        ("hybrid", "logistic_regression"),
    ]
    rows = []
    for name, model in configs:

        def build(name: str = name, model: str | None = model):
            return make_policy(
                name,
                wb,
                model,
                h,
                alpha=meta["selected_alpha"].get(model or "logistic_regression", 0.5),
            )

        run_policy(wb, build(), sc.capacity, split="all")  # warm-up (not recorded)
        for rep in range(repeats):
            res = run_policy(wb, build(), sc.capacity, split="all")
            ps = res.policy_stats
            n = len(wb.trace)
            row = result_row(
                "exp09_overhead",
                wb,
                res,
                workload="zipf",
                seed=seed,
                model=model,
                horizon=h,
                runtime="sklearn" if model else None,
            )
            row.update(
                {
                    "run_kind": "timing",
                    "repeat": rep,
                    "us_per_request_total": res.wall_time_s / n * 1e6,
                    "us_per_request_features": (ps.get("feature_time_ms_total") or 0) * 1e3 / n,
                    "us_per_request_inference": (ps.get("inference_time_ms_total") or 0) * 1e3 / n,
                    "eviction_decision_us_mean": res.eviction_decision_us.get("mean"),
                }
            )
            rows.append(row)
        mem = run_policy(wb, build(), sc.capacity, split="all", measure_memory=True)
        rows.append(
            {
                **result_row(
                    "exp09_overhead", wb, mem, workload="zipf", seed=seed, model=model, horizon=h
                ),
                "run_kind": "memory",
                "repeat": -1,
            }
        )
    df = pd.DataFrame(rows)
    timed = df[df["run_kind"] == "timing"]
    agg = timed.groupby("policy")[
        [
            "us_per_request_total",
            "us_per_request_features",
            "us_per_request_inference",
            "eviction_decision_us_mean",
            "eviction_decision_us_p99",
            "inference_us_p50",
            "inference_us_p95",
        ]
    ].agg(["median", "min", "max"])
    agg.columns = ["_".join(c) for c in agg.columns]
    mem = df[df["run_kind"] == "memory"].set_index("policy")["peak_memory_kib"].to_dict()
    save_results(
        "exp09_overhead",
        rows,
        {
            "scale": sc.__dict__,
            "repeats": repeats,
            "warmup_runs": 1,
            "timing": "time.perf_counter wall time of the full simulation loop / requests; "
            "inference and feature times are summed perf_counter_ns inside the predictor",
            "memory": "tracemalloc peak in a separate (slower) run; Python-heap only",
            "summary": json.loads(agg.reset_index().to_json(orient="records")),
            "peak_memory_kib": mem,
        },
    )
    return df


def exp13_horizon_sensitivity(
    sc: Scale,
    workloads: tuple[str, ...] = ("zipf", "bursty", "drift"),
    factors: tuple[int, ...] = (2, 5, 10, 20, 40),
) -> pd.DataFrame:
    """POST-HOC sensitivity of the ML policy to the label horizon H.

    The main grid selected H from {2, 5, 10} x capacity and picked the upper end on most
    workloads. This analysis reports validation AND test hit rates for a wider range so
    the reader can judge whether the pre-registered range was too narrow. It does not
    change any result of experiments 1-8.
    """
    from cachemind.experiments.common import build_workbench

    rows = []
    for workload in workloads:
        for seed in sc.seeds:
            trace = make_trace(workload, seed, sc.n_events, sc.n_items)
            for f in factors:
                h = f * sc.capacity
                try:
                    wb = build_workbench(
                        trace, h, seed, _models_root(), model_types=("logistic_regression",)
                    )
                except ValueError as exc:
                    rows.append(
                        {
                            "workload": workload,
                            "seed": seed,
                            "horizon": h,
                            "factor": f,
                            "error": str(exc),
                        }
                    )
                    continue
                pol = lambda wb=wb, h=h: make_policy("ml", wb, "logistic_regression", h)  # noqa: E731
                val = run_policy(wb, pol(), sc.capacity, split="val")
                test = run_policy(wb, pol(), sc.capacity, split="test")
                rows.append(
                    {
                        "experiment": "exp13_horizon_sensitivity",
                        "workload": workload,
                        "seed": seed,
                        "policy": "ml-lr",
                        "capacity": sc.capacity,
                        "factor": f,
                        "horizon": h,
                        "val_hit_rate": val.hit_rate,
                        "hit_rate": test.hit_rate,
                    }
                )
            log_event(log, "horizon sensitivity done", workload=workload, seed=seed)
    df = pd.DataFrame(rows)
    save_results(
        "exp13_horizon_sensitivity",
        rows,
        {
            "scale": sc.__dict__,
            "post_hoc": True,
            "note": "post-hoc analysis; main results keep the pre-registered H range {2,5,10}xC",
            "summary": df.groupby(["workload", "factor"])[["val_hit_rate", "hit_rate"]]
            .mean()
            .reset_index()
            .to_dict("records"),
        },
    )
    return df


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument(
        "--exp",
        default="all",
        help="comma list from: grid,3,5,6,9 (grid = experiments 1,2,4,7,8) or all",
    )
    ap.add_argument("--scale", default="standard", choices=sorted(SCALES))
    args = ap.parse_args(argv)
    sc = SCALES[args.scale]
    wanted = {"grid", "3", "5", "6", "9"} if args.exp == "all" else set(args.exp.split(","))
    t0 = time.time()
    if "grid" in wanted:
        df, model_rows = main_grid(sc)
        derive_from_grid(df, model_rows, sc)
    if "3" in wanted:
        exp03_capacity(sc)
    if "5" in wanted:
        exp05_shift(sc)
    if "6" in wanted:
        exp06_drift(sc)
    if "9" in wanted:
        exp09_overhead(sc)
    if "13" in wanted:
        exp13_horizon_sensitivity(sc)
    print(json.dumps({"done": sorted(wanted), "seconds": round(time.time() - t0, 1)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
