"""Supervised dataset for reuse prediction built from an ordered trace."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any

import numpy as np

from cachemind.ml.features import (
    CATEGORICAL_FEATURES,
    FEATURE_SCHEMA_VERSION,
    NUMERIC_FEATURES,
    compute_features,
    reuse_labels,
)
from cachemind.ml.splits import SplitBoundaries, assert_no_label_leakage, chronological_split
from cachemind.simulation.trace import Trace


@dataclass
class ReuseDataset:
    trace: Trace
    horizon: int
    numeric: np.ndarray  # (n, k)
    categories: dict[str, np.ndarray]
    label: np.ndarray
    censored: np.ndarray
    bounds: SplitBoundaries

    def rows(self, split: str, max_rows: int | None = None, seed: int = 0) -> np.ndarray:
        """Eligible positions of a split (optionally a deterministic subsample)."""
        lo, hi = self.bounds.eligible_range(split)
        pos = np.arange(lo, hi)
        pos = pos[~self.censored[pos]]
        if max_rows is not None and len(pos) > max_rows:
            rng = np.random.default_rng(seed)
            pos = np.sort(rng.choice(pos, size=max_rows, replace=False))
        assert_no_label_leakage(self.bounds, pos, split)
        return pos

    def take(self, pos: np.ndarray) -> tuple[np.ndarray, dict[str, np.ndarray], np.ndarray]:
        return (self.numeric[pos], {k: v[pos] for k, v in self.categories.items()}, self.label[pos])

    def version(self) -> str:
        """Content hash identifying the training data (dataset, trace, horizon, splits)."""
        h = hashlib.sha256()
        h.update(self.trace.dataset_id.encode())
        h.update(np.ascontiguousarray(self.trace.keys).tobytes())
        h.update(str(self.bounds.to_dict()).encode())
        h.update(FEATURE_SCHEMA_VERSION.encode())
        return "d-" + h.hexdigest()[:12]

    def summary(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "dataset_id": self.trace.dataset_id,
            "dataset_version": self.version(),
            "horizon": self.horizon,
            "feature_schema_version": FEATURE_SCHEMA_VERSION,
            "numeric_features": list(NUMERIC_FEATURES),
            "categorical_features": list(CATEGORICAL_FEATURES),
            "split_boundaries": self.bounds.to_dict(),
        }
        for split in ("train", "val", "test"):
            pos = self.rows(split)
            out[f"{split}_rows"] = len(pos)
            out[f"{split}_positive_rate"] = float(self.label[pos].mean()) if len(pos) else None
        return out


def build_reuse_dataset(
    trace: Trace, horizon: int, fractions: tuple[float, float, float] = (0.6, 0.2, 0.2)
) -> ReuseDataset:
    numeric = compute_features(trace.keys, trace.event_time_ms, trace.size_bytes)
    label, censored = reuse_labels(trace.keys, horizon)
    bounds = chronological_split(len(trace), horizon, fractions)
    return ReuseDataset(
        trace=trace,
        horizon=horizon,
        numeric=numeric,
        categories={"category": trace.category, "region": trace.region},
        label=label,
        censored=censored,
        bounds=bounds,
    )
