"""Portable model format + a NumPy inference runtime.

Why this exists
---------------
Models are trained with Spark MLlib (the Big Data path) or scikit-learn (the
lightweight profile). Calling Spark from inside the cache simulator for every
request would cost milliseconds per prediction, so trained pipelines are
*exported* to a small JSON spec that this module evaluates with NumPy.

An exported spec is only trusted after a parity check: its predictions must
match the original runtime (``SparkPipelineModel.transform`` or sklearn
``predict_proba``) on a fixed reference dataset within ``PARITY_TOLERANCE``.
The check result is stored next to the artifact (``parity.json``) and
``load_scorer`` refuses artifacts that did not pass it.

The spec (``spec_version`` 1)::

    {
      "spec_version": 1,
      "feature_schema_version": "v1",
      "numeric": [...names in order...],
      "categorical": [{"name": "category", "labels": [...], "unknown_index": k|-1,
                       "size": m}, ...],
      "scaler": {"mean": [...], "scale": [...]} | null,   # x' = (x - mean) * scale
      "model": {"type": "logistic_regression", "coef": [...], "intercept": b}
             | {"type": "random_forest", "compare_dtype": "float64"|"float32",
                "trees": [{"feature": [...], "threshold": [...], "left": [...],
                           "right": [...], "prob1": [...]}, ...]}
    }

Assembled vector = numeric columns (in order) followed by the one-hot block of
each categorical column (in order). Tree nodes with ``left == -1`` are leaves;
``prob1`` is the leaf's class-1 probability; a sample goes left iff
``x[feature] <= threshold``. Forest probability = mean of leaf probabilities.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np

from cachemind.ml.features import CATEGORICAL_FEATURES, FEATURE_SCHEMA_VERSION, NUMERIC_FEATURES

SPEC_VERSION = 1
PARITY_TOLERANCE = 1e-6


class ModelLoadError(RuntimeError):
    """Raised when an artifact is missing, incompatible or failed its parity check."""


@dataclass
class _Forest:
    feature: np.ndarray  # (N,) int64, -1 for leaves
    threshold: np.ndarray  # (N,) float
    left: np.ndarray  # (N,) absolute node index or -1
    right: np.ndarray
    prob1: np.ndarray  # (N,)
    roots: np.ndarray  # (T,)
    depth: int
    dtype: type  # input rounding before comparison: np.float32 (sklearn) or np.float64


class NumpyScorer:
    """Evaluates an exported spec. Thread-compatible (no mutable state after init)."""

    def __init__(self, spec: dict[str, Any], metadata: dict[str, Any] | None = None) -> None:
        if spec.get("spec_version") != SPEC_VERSION:
            raise ModelLoadError(f"unsupported spec_version {spec.get('spec_version')}")
        if spec.get("feature_schema_version") != FEATURE_SCHEMA_VERSION:
            raise ModelLoadError(
                f"model feature schema {spec.get('feature_schema_version')!r} does not match "
                f"runtime schema {FEATURE_SCHEMA_VERSION!r}"
            )
        if tuple(spec["numeric"]) != NUMERIC_FEATURES:
            raise ModelLoadError("numeric feature list differs from the runtime schema")
        if tuple(c["name"] for c in spec["categorical"]) != CATEGORICAL_FEATURES:
            raise ModelLoadError("categorical feature list differs from the runtime schema")
        self.spec = spec
        self.metadata = metadata or {}
        self._n_num = len(spec["numeric"])
        self._cat_maps: list[tuple[dict[str, int], int, int]] = []
        width = self._n_num
        for c in spec["categorical"]:
            mapping = {label: i for i, label in enumerate(c["labels"])}
            self._cat_maps.append((mapping, int(c["unknown_index"]), width))
            width += int(c["size"])
        self.width = width
        sc = spec.get("scaler")
        self._mean: np.ndarray | None = None
        self._scale: np.ndarray | None = None
        if sc:
            mean = np.asarray(sc["mean"], dtype=np.float64)
            scale = np.asarray(sc["scale"], dtype=np.float64)
            if len(mean) != width or len(scale) != width:
                raise ModelLoadError("scaler width does not match assembled vector width")
            self._mean, self._scale = mean, scale
        model = spec["model"]
        self.model_type = model["type"]
        if self.model_type == "logistic_regression":
            self._coef = np.asarray(model["coef"], dtype=np.float64)
            self._intercept = float(model["intercept"])
            if len(self._coef) != width:
                raise ModelLoadError("coefficient count does not match vector width")
            # Fold the affine scaler into the weights for the single-row fast path:
            # w.((x - m) * s) + b == (w * s).x + (b - (w * s).m)
            if self._scale is not None and self._mean is not None:
                folded = self._coef * self._scale
                self._fast_coef = folded.tolist()
                self._fast_intercept = self._intercept - float(folded @ self._mean)
            else:
                self._fast_coef = self._coef.tolist()
                self._fast_intercept = self._intercept
        elif self.model_type == "random_forest":
            self._forest = self._compile_forest(model)
        else:
            raise ModelLoadError(f"unknown model type {self.model_type!r}")

    # ------------------------------------------------------------------
    def _compile_forest(self, model: dict[str, Any]) -> _Forest:
        feats, thr, left, right, prob, roots = [], [], [], [], [], []
        offset, depth = 0, 0
        for tree in model["trees"]:
            f = np.asarray(tree["feature"], dtype=np.int64)
            n = len(f)
            lft = np.asarray(tree["left"], dtype=np.int64)
            rgt = np.asarray(tree["right"], dtype=np.int64)
            leaf = lft < 0
            if np.any(f[~leaf] >= self.width) or np.any(f[~leaf] < 0):
                raise ModelLoadError("tree references a feature outside the vector")
            roots.append(offset)
            feats.append(np.where(leaf, 0, f))
            thr.append(np.where(leaf, 0.0, np.asarray(tree["threshold"], dtype=np.float64)))
            # leaves point to themselves so traversal is a fixed number of steps
            self_idx = np.arange(n) + offset
            left.append(np.where(leaf, self_idx, lft + offset))
            right.append(np.where(leaf, self_idx, rgt + offset))
            prob.append(np.asarray(tree["prob1"], dtype=np.float64))
            depth = max(depth, _tree_depth(lft, rgt))
            offset += n
        dtype = np.float32 if model.get("compare_dtype") == "float32" else np.float64
        return _Forest(
            feature=np.concatenate(feats),
            threshold=np.concatenate(thr),
            left=np.concatenate(left),
            right=np.concatenate(right),
            prob1=np.concatenate(prob),
            roots=np.asarray(roots, dtype=np.int64),
            depth=depth,
            dtype=dtype,
        )

    # ------------------------------------------------------------------
    def assemble(self, numeric: np.ndarray, categories: dict[str, np.ndarray]) -> np.ndarray:
        """Build the model input matrix (n, width) from raw features."""
        return assemble_matrix(
            numeric, categories, self.spec["categorical"], self._mean, self._scale
        )

    def predict_proba(self, numeric: np.ndarray, categories: dict[str, np.ndarray]) -> np.ndarray:
        x = self.assemble(numeric, categories)
        if self.model_type == "logistic_regression":
            z = x @ self._coef + self._intercept
            return 1.0 / (1.0 + np.exp(-z))
        return self._forest_proba(x)

    def predict_one(self, numeric: list[float], category: str, region: str) -> float:
        """Single-row fast path used by the online cache policy."""
        if self.model_type == "logistic_regression":
            z = self._fast_intercept
            coef = self._fast_coef
            for i, v in enumerate(numeric):
                z += coef[i] * v
            for (mapping, unknown, start), value in zip(
                self._cat_maps, (category, region), strict=True
            ):
                j = mapping.get(value, unknown)
                if j >= 0:
                    z += coef[start + j]
            return 1.0 / (1.0 + math.exp(-z)) if z > -700 else 0.0
        p = self.predict_proba(
            np.asarray([numeric], dtype=np.float64),
            {
                "category": np.asarray([category], dtype=object),
                "region": np.asarray([region], dtype=object),
            },
        )
        return float(p[0])

    def _forest_proba(self, x: np.ndarray) -> np.ndarray:
        fo = self._forest
        # sklearn rounds inputs to float32 but compares against float64 thresholds;
        # Spark compares doubles. Reproduce whichever the source runtime does.
        xs = x.astype(np.float32).astype(np.float64) if fo.dtype is np.float32 else x
        n = xs.shape[0]
        nodes = np.broadcast_to(fo.roots, (n, len(fo.roots))).copy()
        rows = np.arange(n)[:, None]
        for _ in range(fo.depth):
            f = fo.feature[nodes]
            go_left = xs[rows, f] <= fo.threshold[nodes]
            nodes = np.where(go_left, fo.left[nodes], fo.right[nodes])
        return fo.prob1[nodes].mean(axis=1)

    # ------------------------------------------------------------------
    def save(self, directory: Path) -> None:
        directory.mkdir(parents=True, exist_ok=True)
        (directory / "export.json").write_text(json.dumps(self.spec) + "\n")


def assemble_matrix(
    numeric: np.ndarray,
    categories: dict[str, np.ndarray],
    categorical_spec: list[dict[str, Any]],
    mean: np.ndarray | None = None,
    scale: np.ndarray | None = None,
) -> np.ndarray:
    """Numeric columns, then one one-hot block per categorical, then optional scaling.

    Shared by training (encoders fitted on the train block) and serving, so the
    two can never assemble vectors differently.
    """
    numeric = np.atleast_2d(np.asarray(numeric, dtype=np.float64))
    n, n_num = numeric.shape
    if n_num != len(NUMERIC_FEATURES):
        raise ValueError(f"expected {len(NUMERIC_FEATURES)} numeric features, got {n_num}")
    width = n_num + sum(int(c["size"]) for c in categorical_spec)
    x = np.zeros((n, width), dtype=np.float64)
    x[:, :n_num] = numeric
    start = n_num
    for c in categorical_spec:
        mapping = {label: i for i, label in enumerate(c["labels"])}
        unknown = int(c["unknown_index"])
        values = categories[c["name"]]
        idx = np.fromiter((mapping.get(v, unknown) for v in values), dtype=np.int64, count=n)
        rows = np.flatnonzero(idx >= 0)
        x[rows, start + idx[rows]] = 1.0
        start += int(c["size"])
    if scale is not None:
        x = (x - (mean if mean is not None else 0.0)) * scale
    return x


def _tree_depth(left: np.ndarray, right: np.ndarray) -> int:
    depth, frontier = 0, [0]
    while frontier:
        nxt: list[int] = []
        for node in frontier:
            if left[node] >= 0:
                nxt.extend((int(left[node]), int(right[node])))
        if nxt:
            depth += 1
        frontier = nxt
    return depth


def write_artifact(
    directory: Path, spec: dict[str, Any], metadata: dict[str, Any], parity: dict[str, Any]
) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "export.json").write_text(json.dumps(spec) + "\n")
    (directory / "metadata.json").write_text(json.dumps(metadata, indent=2, default=str) + "\n")
    (directory / "parity.json").write_text(json.dumps(parity, indent=2) + "\n")


def load_scorer(directory: Path) -> NumpyScorer:
    """Load an exported model artifact; raise ModelLoadError if it is unusable."""
    try:
        spec = json.loads((directory / "export.json").read_text())
        metadata = json.loads((directory / "metadata.json").read_text())
        parity = json.loads((directory / "parity.json").read_text())
    except FileNotFoundError as exc:
        raise ModelLoadError(f"incomplete model artifact in {directory}: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise ModelLoadError(f"corrupt model artifact in {directory}: {exc}") from exc
    if not parity.get("passed"):
        raise ModelLoadError(
            f"artifact {directory} did not pass its parity check: {parity.get('detail')}"
        )
    return NumpyScorer(spec, metadata)
