"""Lightweight-profile training with scikit-learn (no JVM required).

This is the Profile A counterpart of the Spark MLlib trainer
(``cachemind.bigdata.mllib_train``). Results are always labelled with
``runtime = "sklearn"`` — they are never presented as Spark MLlib results.

Protocol (identical for both runtimes):
  * chronological split with label-horizon purging (``ml.splits``);
  * encoders/scaler fitted on the TRAIN block only;
  * hyper-parameters chosen by validation PR-AUC;
  * decision threshold chosen on validation (best F1);
  * the test block is scored exactly once, after selection.
"""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import sklearn
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression

from cachemind.ml.dataset import ReuseDataset
from cachemind.ml.evaluation import best_f1_threshold, classification_report, heuristic_scores
from cachemind.ml.features import CATEGORICAL_FEATURES, FEATURE_SCHEMA_VERSION, NUMERIC_FEATURES
from cachemind.ml.model_spec import (
    PARITY_TOLERANCE,
    SPEC_VERSION,
    NumpyScorer,
    assemble_matrix,
    write_artifact,
)

LR_GRID = [{"C": c} for c in (0.01, 0.1, 1.0)]
RF_GRID = [{"max_depth": d, "min_samples_leaf": 20} for d in (6, 10)]
RF_TREES = 40
PARITY_ROWS = 2000


@dataclass
class FittedEncoder:
    categorical_spec: list[dict[str, Any]]
    mean: np.ndarray | None
    scale: np.ndarray | None

    @classmethod
    def fit(
        cls, numeric: np.ndarray, categories: dict[str, np.ndarray], standardize: bool
    ) -> FittedEncoder:
        cat_spec = []
        for name in CATEGORICAL_FEATURES:
            labels = sorted({str(v) for v in categories[name]})
            # unseen values at serving time map to an all-zero block
            cat_spec.append(
                {"name": name, "labels": labels, "unknown_index": -1, "size": len(labels)}
            )
        enc = cls(cat_spec, None, None)
        if standardize:
            x = enc.transform(numeric, categories)
            mean = x.mean(axis=0)
            std = x.std(axis=0)
            enc.mean = mean
            enc.scale = np.where(std > 0, 1.0 / np.where(std > 0, std, 1.0), 1.0)
        return enc

    def transform(self, numeric: np.ndarray, categories: dict[str, np.ndarray]) -> np.ndarray:
        return assemble_matrix(numeric, categories, self.categorical_spec, self.mean, self.scale)

    def spec_fields(self) -> dict[str, Any]:
        return {
            "spec_version": SPEC_VERSION,
            "feature_schema_version": FEATURE_SCHEMA_VERSION,
            "numeric": list(NUMERIC_FEATURES),
            "categorical": self.categorical_spec,
            "scaler": None
            if self.scale is None
            else {"mean": self.mean.tolist(), "scale": self.scale.tolist()},  # type: ignore[union-attr]
        }


def export_sklearn(model: Any, enc: FittedEncoder) -> dict[str, Any]:
    spec = enc.spec_fields()
    if isinstance(model, LogisticRegression):
        spec["model"] = {
            "type": "logistic_regression",
            "coef": model.coef_[0].tolist(),
            "intercept": float(model.intercept_[0]),
        }
        return spec
    if isinstance(model, RandomForestClassifier):
        pos_index = list(model.classes_).index(1)
        trees = []
        for est in model.estimators_:
            tr = est.tree_
            values = tr.value[:, 0, :]
            totals = values.sum(axis=1)
            prob1 = np.where(totals > 0, values[:, pos_index] / np.where(totals > 0, totals, 1), 0)
            trees.append(
                {
                    "feature": tr.feature.tolist(),
                    "threshold": tr.threshold.tolist(),
                    "left": tr.children_left.tolist(),
                    "right": tr.children_right.tolist(),
                    "prob1": prob1.tolist(),
                }
            )
        spec["model"] = {"type": "random_forest", "compare_dtype": "float32", "trees": trees}
        return spec
    raise TypeError(f"cannot export {type(model).__name__}")


def _fit(model_type: str, params: dict[str, Any], x: np.ndarray, y: np.ndarray, seed: int):
    if model_type == "logistic_regression":
        m = LogisticRegression(max_iter=1000, C=params["C"])
    else:
        m = RandomForestClassifier(n_estimators=RF_TREES, random_state=seed, n_jobs=2, **params)
    m.fit(x, y)
    return m


def _model_version(ds: ReuseDataset, model_type: str, params: dict[str, Any], seed: int) -> str:
    raw = json.dumps(
        [ds.version(), model_type, params, seed, "sklearn", sklearn.__version__], sort_keys=True
    )
    return "m-" + hashlib.sha256(raw.encode()).hexdigest()[:10]


def train_sklearn_models(
    ds: ReuseDataset,
    models_root: Path,
    seed: int = 42,
    max_train_rows: int = 300_000,
    model_types: tuple[str, ...] = ("logistic_regression", "random_forest"),
) -> list[dict[str, Any]]:
    train_pos = ds.rows("train", max_rows=max_train_rows, seed=seed)
    val_pos, test_pos = ds.rows("val"), ds.rows("test")
    xn_tr, c_tr, y_tr = ds.take(train_pos)
    xn_va, c_va, y_va = ds.take(val_pos)
    xn_te, c_te, y_te = ds.take(test_pos)
    if len(np.unique(y_tr)) < 2:
        raise ValueError("training block contains a single class; use a longer trace or other H")

    reports = []
    grids: dict[str, list[dict[str, Any]]] = {
        "logistic_regression": LR_GRID,
        "random_forest": RF_GRID,
    }
    for model_type in model_types:
        grid = grids[model_type]
        enc = FittedEncoder.fit(xn_tr, c_tr, standardize=model_type == "logistic_regression")
        x_tr, x_va = enc.transform(xn_tr, c_tr), enc.transform(xn_va, c_va)
        search = []
        best: tuple[dict[str, Any], Any, dict[str, Any], np.ndarray, float] | None = None
        for params in grid:
            t0 = time.perf_counter()
            m = _fit(model_type, params, x_tr, y_tr, seed)
            fit_s = time.perf_counter() - t0
            p_va = m.predict_proba(x_va)[:, 1]
            rep = classification_report(y_va, p_va)
            search.append(
                {
                    "params": params,
                    "val_pr_auc": rep["pr_auc"],
                    "val_roc_auc": rep["roc_auc"],
                    "fit_seconds": fit_s,
                }
            )
            if best is None or (rep["pr_auc"] or 0) > (best[2]["pr_auc"] or 0):
                best = (params, m, rep, p_va, fit_s)
        assert best is not None
        params, model, val_rep, p_va, fit_s = best
        threshold = best_f1_threshold(y_va, p_va)
        x_te = enc.transform(xn_te, c_te)
        t0 = time.perf_counter()
        p_te = model.predict_proba(x_te)[:, 1]
        batch_us = (time.perf_counter() - t0) / max(1, len(p_te)) * 1e6

        spec = export_sklearn(model, enc)
        scorer = NumpyScorer(spec)
        ref = test_pos[:PARITY_ROWS]
        xn_ref, c_ref, _ = ds.take(ref)
        p_native = model.predict_proba(enc.transform(xn_ref, c_ref))[:, 1]
        p_export = scorer.predict_proba(xn_ref, c_ref)
        max_diff = float(np.max(np.abs(p_native - p_export))) if len(ref) else 0.0
        # single-row latency of the exported scorer (what the cache policy pays)
        lat = []
        for i in range(min(500, len(ref))):
            row = ds.numeric[ref[i]].tolist()
            s0 = time.perf_counter_ns()
            scorer.predict_one(row, str(c_ref["category"][i]), str(c_ref["region"][i]))
            lat.append(time.perf_counter_ns() - s0)

        version = _model_version(ds, model_type, params, seed)
        out_dir = (
            models_root / ds.trace.dataset_id / f"sklearn_{model_type}_h{ds.horizon}_{version}"
        )
        metadata = {
            "model_version": version,
            "model_type": model_type,
            "runtime": "sklearn",
            "runtime_version": sklearn.__version__,
            "feature_schema_version": FEATURE_SCHEMA_VERSION,
            "label": f"reuse within next {ds.horizon} requests",
            "label_horizon": ds.horizon,
            "training_dataset_version": ds.version(),
            "dataset": ds.summary(),
            "train_rows_used": len(train_pos),
            "selected_params": params,
            "hyperparameter_search": search,
            "selection_metric": "validation PR-AUC",
            "decision_threshold_from_validation": threshold,
            "seed": seed,
            "fit_seconds": fit_s,
            "created_at": datetime.now(UTC).isoformat(),
            "metrics": {
                "validation": val_rep,
                "test@0.5": classification_report(y_te, p_te, 0.5),
                "test@val_threshold": classification_report(y_te, p_te, threshold),
            },
            "baselines_test": {
                name: classification_report(y_te, score)
                for name, score in heuristic_scores(xn_te, ds.horizon).items()
            },
            "latency": {
                "batch_predict_us_per_row_native": batch_us,
                "export_predict_one_us_p50": float(np.percentile(lat, 50) / 1e3) if lat else None,
                "export_predict_one_us_p95": float(np.percentile(lat, 95) / 1e3) if lat else None,
            },
        }
        parity = {
            "passed": max_diff <= PARITY_TOLERANCE,
            "reference_rows": len(ref),
            "max_abs_diff": max_diff,
            "tolerance": PARITY_TOLERANCE,
            "detail": "exported NumpyScorer vs sklearn predict_proba on fixed test rows",
        }
        write_artifact(out_dir, spec, metadata, parity)
        reports.append({"artifact": str(out_dir), **metadata, "parity": parity})
    return reports
