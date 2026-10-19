"""Test group C: model training, export, persistence, loading and parity."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pytest

from cachemind.data.workloads import WorkloadConfig
from cachemind.ml.dataset import build_reuse_dataset
from cachemind.ml.features import NUMERIC_FEATURES
from cachemind.ml.model_spec import ModelLoadError, NumpyScorer, load_scorer
from cachemind.ml.sklearn_train import FittedEncoder, train_sklearn_models
from cachemind.simulation.trace import Trace


@pytest.fixture(scope="module")
def dataset():
    cfg = WorkloadConfig(workload="zipf", n_events=12_000, n_items=800, n_users=100, seed=3)
    return build_reuse_dataset(Trace.from_generator(cfg), horizon=300)


@pytest.fixture(scope="module")
def trained(dataset, tmp_path_factory):
    root = tmp_path_factory.mktemp("models")
    return dataset, train_sklearn_models(dataset, root, seed=5)


def test_training_produces_loadable_artifacts_with_passing_parity(trained):
    ds, reports = trained
    assert {r["model_type"] for r in reports} == {"logistic_regression", "random_forest"}
    for r in reports:
        assert r["parity"]["passed"], r["parity"]
        assert r["parity"]["reference_rows"] > 0
        scorer = load_scorer(Path(r["artifact"]))
        pos = ds.rows("test")[:300]
        xn, cats, _ = ds.take(pos)
        p = scorer.predict_proba(xn, cats)
        assert p.shape == (len(pos),)
        assert np.all((p >= 0) & (p <= 1)) and np.all(np.isfinite(p))
        # metadata required by the assignment
        for key in (
            "model_type",
            "model_version",
            "feature_schema_version",
            "training_dataset_version",
            "label_horizon",
            "selected_params",
            "metrics",
        ):
            assert key in r


def test_predict_one_matches_batch(trained):
    ds, reports = trained
    pos = ds.rows("test")[:50]
    xn, cats, _ = ds.take(pos)
    for r in reports:
        scorer = load_scorer(Path(r["artifact"]))
        batch = scorer.predict_proba(xn, cats)
        single = [
            scorer.predict_one(xn[i].tolist(), cats["category"][i], cats["region"][i])
            for i in range(len(pos))
        ]
        np.testing.assert_allclose(single, batch, rtol=0, atol=1e-12)


def test_training_is_deterministic(dataset, tmp_path):
    a = train_sklearn_models(dataset, tmp_path / "a", seed=11)
    b = train_sklearn_models(dataset, tmp_path / "b", seed=11)
    for ra, rb in zip(a, b, strict=True):
        spec_a = json.loads((Path(ra["artifact"]) / "export.json").read_text())
        spec_b = json.loads((Path(rb["artifact"]) / "export.json").read_text())
        assert spec_a == spec_b
        assert ra["model_version"] == rb["model_version"]


def test_test_block_is_not_used_for_selection(trained):
    _, reports = trained
    for r in reports:
        assert r["selection_metric"] == "validation PR-AUC"
        assert all("val_pr_auc" in s for s in r["hyperparameter_search"])
        assert "test" not in json.dumps(r["hyperparameter_search"])


def test_encoder_is_fitted_on_training_rows_only():
    xn = np.zeros((4, len(NUMERIC_FEATURES)))
    enc = FittedEncoder.fit(
        xn,
        {
            "category": np.array(["video", "web", "video", "web"]),
            "region": np.array(["us-east"] * 4),
        },
        standardize=False,
    )
    assert enc.categorical_spec[0]["labels"] == ["video", "web"]
    # a category first seen at test time maps to an all-zero block
    x = enc.transform(
        np.zeros((1, len(NUMERIC_FEATURES))),
        {"category": np.array(["audio"]), "region": np.array(["us-east"])},
    )
    width = len(NUMERIC_FEATURES)
    assert x[0, width : width + 2].tolist() == [0.0, 0.0]


def _artifact(tmp_path, trained):
    import shutil

    _, reports = trained
    src = Path(reports[0]["artifact"])
    dst = tmp_path / "copy"
    shutil.copytree(src, dst)
    return dst


def test_load_refuses_missing_files(tmp_path, trained):
    d = _artifact(tmp_path, trained)
    (d / "parity.json").unlink()
    with pytest.raises(ModelLoadError, match="incomplete"):
        load_scorer(d)


def test_load_refuses_corrupt_files(tmp_path, trained):
    d = _artifact(tmp_path, trained)
    (d / "export.json").write_text("{not json")
    with pytest.raises(ModelLoadError, match="corrupt"):
        load_scorer(d)


def test_load_refuses_failed_parity(tmp_path, trained):
    d = _artifact(tmp_path, trained)
    parity = json.loads((d / "parity.json").read_text())
    parity["passed"] = False
    (d / "parity.json").write_text(json.dumps(parity))
    with pytest.raises(ModelLoadError, match="parity"):
        load_scorer(d)


def test_load_refuses_feature_schema_mismatch(tmp_path, trained):
    d = _artifact(tmp_path, trained)
    spec = json.loads((d / "export.json").read_text())
    spec["feature_schema_version"] = "v0"
    (d / "export.json").write_text(json.dumps(spec))
    with pytest.raises(ModelLoadError, match="feature schema"):
        load_scorer(d)


def test_scorer_rejects_wrong_feature_width(trained):
    _, reports = trained
    scorer = load_scorer(Path(reports[0]["artifact"]))
    with pytest.raises(ValueError):
        scorer.predict_proba(
            np.zeros((1, 3)), {"category": np.array(["web"]), "region": np.array(["us-east"])}
        )


def test_forest_export_matches_sklearn_on_random_data():
    from sklearn.ensemble import RandomForestClassifier

    from cachemind.ml.sklearn_train import export_sklearn

    rng = np.random.default_rng(0)
    xn = rng.normal(size=(600, len(NUMERIC_FEATURES)))
    cats = {
        "category": rng.choice(["video", "web", "audio"], 600),
        "region": rng.choice(["us-east", "eu-west"], 600),
    }
    y = ((xn[:, 0] + (cats["category"] == "web")) > 0.3).astype(int)
    enc = FittedEncoder.fit(xn, cats, standardize=False)
    x = enc.transform(xn, cats)
    rf = RandomForestClassifier(n_estimators=15, max_depth=7, random_state=0).fit(x, y)
    scorer = NumpyScorer(export_sklearn(rf, enc))
    np.testing.assert_allclose(
        scorer.predict_proba(xn, cats), rf.predict_proba(x)[:, 1], rtol=0, atol=1e-12
    )
