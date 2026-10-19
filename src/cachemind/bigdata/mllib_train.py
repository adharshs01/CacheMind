"""Spark MLlib training, evaluation, persistence, reload, export and batch scoring.

Pipeline (fitted on the TRAIN block only)::

    StringIndexer(category, region; handleInvalid=keep, alphabetAsc)
      -> OneHotEncoder(dropLast=False)            # unknown values get their own slot
      -> VectorAssembler(numeric + one-hot)
      -> [StandardScaler(withMean, withStd)]      # logistic regression only
      -> LogisticRegression | RandomForestClassifier

Hyper-parameters are selected on the VALIDATION block (PR-AUC). CrossValidator
is deliberately not used: its random folds would mix future rows into training.

After training the PipelineModel is saved, re-loaded from disk and used for all
predictions (so persistence is verified, not assumed). It is then exported to the
portable spec (``ml.model_spec``) and the export is checked against the reloaded
Spark model on a fixed reference set. Finally the reloaded Spark model scores the
whole trace in batch; those probabilities drive the "original runtime" cache
experiments through ``PrecomputedPredictor``.
"""

from __future__ import annotations

import glob
import hashlib
import json
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import (
    LogisticRegression,
    LogisticRegressionModel,
    RandomForestClassifier,
)
from pyspark.ml.feature import (
    OneHotEncoder,
    OneHotEncoderModel,
    StandardScaler,
    StandardScalerModel,
    StringIndexer,
    StringIndexerModel,
    VectorAssembler,
)
from pyspark.ml.functions import vector_to_array
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from cachemind.bigdata.features_spark import features_path
from cachemind.bigdata.spark_session import spark_environment
from cachemind.config import Settings, safe_child
from cachemind.logging_utils import get_logger, log_event
from cachemind.ml.evaluation import best_f1_threshold, classification_report, heuristic_scores
from cachemind.ml.features import CATEGORICAL_FEATURES, FEATURE_SCHEMA_VERSION, NUMERIC_FEATURES
from cachemind.ml.model_spec import PARITY_TOLERANCE, SPEC_VERSION, NumpyScorer, write_artifact
from cachemind.ml.splits import chronological_split

log = get_logger("cachemind.mllib")

LR_GRID = [{"regParam": r} for r in (0.0, 0.001, 0.01)]
RF_GRID = [{"maxDepth": d} for d in (6, 10)]
RF_TREES = 40
PARITY_ROWS = 2000
IDX = [f"{c}_idx" for c in CATEGORICAL_FEATURES]
OHE = [f"{c}_ohe" for c in CATEGORICAL_FEATURES]


def build_pipeline(model_type: str, params: dict[str, Any], seed: int) -> Pipeline:
    stages: list[Any] = [
        StringIndexer(
            inputCols=list(CATEGORICAL_FEATURES),
            outputCols=IDX,
            handleInvalid="keep",
            stringOrderType="alphabetAsc",
        ),
        OneHotEncoder(inputCols=IDX, outputCols=OHE, dropLast=False),
        VectorAssembler(inputCols=[*NUMERIC_FEATURES, *OHE], outputCol="features_raw"),
    ]
    if model_type == "logistic_regression":
        stages += [
            StandardScaler(
                inputCol="features_raw", outputCol="features", withMean=True, withStd=True
            ),
            LogisticRegression(
                featuresCol="features", labelCol="label", maxIter=200, elasticNetParam=0.0, **params
            ),
        ]
    elif model_type == "random_forest":
        stages.append(
            RandomForestClassifier(
                featuresCol="features_raw",
                labelCol="label",
                numTrees=RF_TREES,
                minInstancesPerNode=20,
                seed=seed,
                **params,
            )
        )
    else:
        raise ValueError(model_type)
    return Pipeline(stages=stages)


def _proba(model: PipelineModel, df: DataFrame) -> DataFrame:
    return model.transform(df).withColumn("p", vector_to_array(F.col("probability"))[1])


def _collect_labels_probs(scored: DataFrame) -> tuple[np.ndarray, np.ndarray]:
    # bounded: one split's rows, 2 columns
    pdf = cast(pd.DataFrame, scored.select("label", "p").toPandas())
    return pdf["label"].to_numpy(), pdf["p"].to_numpy()


# --- export -----------------------------------------------------------------


def export_pipeline(model: PipelineModel, saved_dir: Path, model_type: str) -> dict[str, Any]:
    indexer = cast(StringIndexerModel, model.stages[0])
    encoder = cast(OneHotEncoderModel, model.stages[1])
    labels = indexer.labelsArray
    sizes = encoder.categorySizes
    categorical = []
    for name, lab, size in zip(CATEGORICAL_FEATURES, labels, sizes, strict=True):
        if size != len(lab) + 1:
            raise RuntimeError(f"unexpected one-hot size {size} for {len(lab)} labels of {name}")
        categorical.append(
            {"name": name, "labels": list(lab), "unknown_index": len(lab), "size": size}
        )
    spec: dict[str, Any] = {
        "spec_version": SPEC_VERSION,
        "feature_schema_version": FEATURE_SCHEMA_VERSION,
        "numeric": list(NUMERIC_FEATURES),
        "categorical": categorical,
        "scaler": None,
    }
    if model_type == "logistic_regression":
        scaler = cast(StandardScalerModel, model.stages[3])
        lr = cast(LogisticRegressionModel, model.stages[4])
        std = scaler.std.toArray()
        spec["scaler"] = {
            "mean": scaler.mean.toArray().tolist(),
            "scale": np.where(std != 0, 1.0 / np.where(std != 0, std, 1), 0.0).tolist(),
        }
        spec["model"] = {
            "type": "logistic_regression",
            "coef": lr.coefficients.toArray().tolist(),
            "intercept": float(lr.intercept),
        }
    else:
        spec["model"] = {
            "type": "random_forest",
            "compare_dtype": "float64",
            "trees": _read_saved_forest(saved_dir),
        }
    return spec


def _read_saved_forest(saved_dir: Path) -> list[dict[str, Any]]:
    """Read tree structure from the *persisted* Spark model (stages/*/data/*.parquet)."""
    files = glob.glob(str(saved_dir / "stages" / "*RandomForest*" / "data" / "*.parquet"))
    if not files:
        raise FileNotFoundError(f"no saved forest data under {saved_dir}")
    rows = []
    for f in files:
        rows.extend(pq.read_table(f).to_pylist())
    trees: dict[int, list[dict[str, Any]]] = {}
    for r in rows:
        trees.setdefault(r["treeID"], []).append(r["nodeData"])
    out = []
    for tree_id in sorted(trees):
        nodes = sorted(trees[tree_id], key=lambda nd: nd["id"])
        if [nd["id"] for nd in nodes] != list(range(len(nodes))):
            raise RuntimeError(f"tree {tree_id}: node ids are not 0..n-1")
        feature, threshold, left, right, prob1 = [], [], [], [], []
        for nd in nodes:
            stats = nd["impurityStats"]
            total = sum(stats)
            prob1.append(stats[1] / total if total > 0 and len(stats) > 1 else 0.0)
            if nd["leftChild"] < 0:
                feature.append(-1)
                threshold.append(0.0)
                left.append(-1)
                right.append(-1)
                continue
            sp = nd["split"]
            lc, rc = nd["leftChild"], nd["rightChild"]
            if sp["numCategories"] < 0:  # continuous: left iff x <= threshold
                thr = sp["leftCategoriesOrThreshold"][0]
            elif sp["numCategories"] == 2 and sp["leftCategoriesOrThreshold"] in ([0.0], [1.0]):
                # binary one-hot column treated as categorical by Spark
                thr = 0.5
                if sp["leftCategoriesOrThreshold"] == [1.0]:
                    lc, rc = rc, lc  # x == 1 goes left in Spark -> swap for "x <= 0.5"
            else:
                raise NotImplementedError(f"unsupported categorical split {sp}")
            feature.append(sp["featureIndex"])
            threshold.append(thr)
            left.append(lc)
            right.append(rc)
        out.append(
            {
                "feature": feature,
                "threshold": threshold,
                "left": left,
                "right": right,
                "prob1": prob1,
            }
        )
    return out


# --- training ---------------------------------------------------------------


def train_mllib_models(
    spark: SparkSession,
    settings: Settings,
    dataset_id: str,
    horizon: int,
    seed: int = 42,
    max_train_rows: int = 300_000,
    fractions: tuple[float, float, float] = (0.6, 0.2, 0.2),
    model_types: tuple[str, ...] = ("logistic_regression", "random_forest"),
) -> list[dict[str, Any]]:
    feats = spark.read.parquet(str(features_path(settings, dataset_id, horizon))).cache()
    n = feats.count()
    bounds = chronological_split(n, horizon, fractions)

    def split_df(name: str) -> DataFrame:
        lo, hi = bounds.eligible_range(name)
        return feats.where(
            (F.col("trace_pos") >= lo) & (F.col("trace_pos") < hi) & ~F.col("censored")
        )

    train, val, test = split_df("train"), split_df("val"), split_df("test")
    n_train = train.count()
    if n_train > max_train_rows:
        train = train.sample(withReplacement=False, fraction=max_train_rows / n_train, seed=seed)
    train = train.cache()
    n_train_used = train.count()
    val, test = val.cache(), test.cache()
    dataset_version = (
        "d-"
        + hashlib.sha256(
            json.dumps([dataset_id, n, bounds.to_dict(), FEATURE_SCHEMA_VERSION]).encode()
        ).hexdigest()[:12]
    )
    split_summary = {
        "split_boundaries": bounds.to_dict(),
        "train_rows_eligible": n_train,
        "train_rows_used": n_train_used,
        "val_rows": val.count(),
        "test_rows": test.count(),
        "train_positive_rate": train.agg(F.avg("label")).head(1)[0][0],
        "val_positive_rate": val.agg(F.avg("label")).head(1)[0][0],
        "test_positive_rate": test.agg(F.avg("label")).head(1)[0][0],
    }
    reports = []
    for model_type in model_types:
        grid = LR_GRID if model_type == "logistic_regression" else RF_GRID
        search, best = [], None
        for params in grid:
            t0 = time.perf_counter()
            model = build_pipeline(model_type, params, seed).fit(train)
            fit_s = time.perf_counter() - t0
            y_va, p_va = _collect_labels_probs(_proba(model, val))
            rep = classification_report(y_va, p_va)
            search.append(
                {
                    "params": params,
                    "val_pr_auc": rep["pr_auc"],
                    "val_roc_auc": rep["roc_auc"],
                    "fit_seconds": round(fit_s, 2),
                }
            )
            log_event(
                log,
                "candidate fitted",
                model_type=model_type,
                params=params,
                val_pr_auc=rep["pr_auc"],
            )
            if best is None or (rep["pr_auc"] or 0) > (best[2]["pr_auc"] or 0):
                best = (params, model, rep, (y_va, p_va), fit_s)
        assert best is not None
        params, model, val_rep, (y_va, p_va), fit_s = best
        version = (
            "m-"
            + hashlib.sha256(
                json.dumps([dataset_version, model_type, params, seed, "spark"]).encode()
            ).hexdigest()[:10]
        )
        out_dir = settings.models_dir / dataset_id / f"spark_{model_type}_h{horizon}_{version}"
        saved = out_dir / "spark_model"
        model.write().overwrite().save(str(saved))
        reloaded = PipelineModel.load(str(saved))  # every prediction below uses the reload

        threshold = best_f1_threshold(y_va, p_va)
        t0 = time.perf_counter()
        y_te, p_te = _collect_labels_probs(_proba(reloaded, test))
        batch_us = (time.perf_counter() - t0) / max(1, len(p_te)) * 1e6
        _, p_va_reloaded = _collect_labels_probs(_proba(reloaded, val))
        reload_diff = float(np.max(np.abs(p_va_reloaded - p_va))) if len(p_va) else 0.0

        spec = export_pipeline(reloaded, saved, model_type)
        scorer = NumpyScorer(spec)
        ref = cast(
            pd.DataFrame,
            (
                _proba(reloaded, test.orderBy("trace_pos").limit(PARITY_ROWS))
                .select(*NUMERIC_FEATURES, *CATEGORICAL_FEATURES, "p")
                .toPandas()
            ),
        )
        p_export = scorer.predict_proba(
            ref[list(NUMERIC_FEATURES)].to_numpy(dtype=np.float64),
            {c: ref[c].to_numpy(dtype=object) for c in CATEGORICAL_FEATURES},
        )
        parity_diff = float(np.max(np.abs(p_export - ref["p"].to_numpy()))) if len(ref) else 0.0

        single = []
        one = test.limit(1).cache()
        one.count()
        for _ in range(10):
            s0 = time.perf_counter()
            _proba(reloaded, one).select("p").collect()
            single.append((time.perf_counter() - s0) * 1e3)
        one.unpersist()

        test_pdf = cast(pd.DataFrame, test.select(*NUMERIC_FEATURES).toPandas())
        test_numeric = test_pdf.to_numpy(dtype=np.float64)
        metadata = {
            "model_version": version,
            "model_type": model_type,
            "runtime": "spark-mllib",
            "runtime_environment": spark_environment(),
            "feature_schema_version": FEATURE_SCHEMA_VERSION,
            "label": f"reuse within next {horizon} requests",
            "label_horizon": horizon,
            "training_dataset_version": dataset_version,
            "dataset_id": dataset_id,
            "dataset": split_summary,
            "selected_params": params,
            "fixed_params": cast(
                dict[str, Any],
                {"numTrees": RF_TREES, "minInstancesPerNode": 20}
                if model_type == "random_forest"
                else {"maxIter": 200, "elasticNetParam": 0.0, "standardize": True},
            ),
            "hyperparameter_search": search,
            "selection_metric": "validation PR-AUC",
            "decision_threshold_from_validation": threshold,
            "seed": seed,
            "fit_seconds": fit_s,
            "created_at": datetime.now(UTC).isoformat(),
            "save_reload_max_abs_diff": reload_diff,
            "metrics": {
                "validation": val_rep,
                "test@0.5": classification_report(y_te, p_te, 0.5),
                "test@val_threshold": classification_report(y_te, p_te, threshold),
            },
            "baselines_test": {
                name: classification_report(y_te, sc)
                for name, sc in heuristic_scores(test_numeric, horizon).items()
            },
            "latency": {
                "spark_batch_transform_us_per_row": batch_us,
                "spark_single_row_transform_ms_median": float(np.median(single)),
            },
        }
        parity = {
            "passed": parity_diff <= PARITY_TOLERANCE and reload_diff <= 1e-12,
            "reference_rows": len(ref),
            "max_abs_diff": parity_diff,
            "tolerance": PARITY_TOLERANCE,
            "save_reload_max_abs_diff": reload_diff,
            "detail": "exported NumpyScorer vs reloaded Spark PipelineModel.transform "
            "on the first test rows by trace_pos",
        }
        write_artifact(out_dir, spec, metadata, parity)
        score_trace(spark, settings, reloaded, feats, out_dir)
        log_event(
            log,
            "model saved",
            model_type=model_type,
            version=version,
            parity_diff=parity_diff,
            out=str(out_dir),
        )
        reports.append({"artifact": str(out_dir), **metadata, "parity": parity})
    for df in (feats, train, val, test):
        df.unpersist()
    return reports


def score_trace(
    spark: SparkSession, settings: Settings, model: PipelineModel, feats: DataFrame, out_dir: Path
) -> Path:
    """Batch-score every trace position with the original Spark model."""
    path = out_dir / "trace_predictions"
    (
        _proba(model, feats)
        .select("trace_pos", "p")
        .coalesce(1)
        .write.mode("overwrite")
        .parquet(str(path))
    )
    return path


def load_trace_predictions(artifact_dir: Path, n: int) -> np.ndarray:
    table = pq.read_table(str(artifact_dir / "trace_predictions"), columns=["trace_pos", "p"])
    pos = table.column("trace_pos").to_numpy()
    p = table.column("p").to_numpy()
    out = np.full(n, np.nan)
    out[pos] = p
    if np.isnan(out).any():
        raise ValueError(f"{int(np.isnan(out).sum())} trace positions have no prediction")
    return out


def model_dir(settings: Settings, dataset_id: str) -> Path:
    return safe_child(settings.models_dir, dataset_id)
