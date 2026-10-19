"""Reuse predictors consumed by the ML-guided and hybrid cache policies.

A predictor is asked, at request ``t`` of key ``k``, for the probability that
``k`` is requested again within the model's horizon. It returns ``None`` when
no valid prediction is available; policies then fall back to LRU for any
eviction that would depend on it (and count it).

* ``OnlinePredictor``      – computes schema-v1 features incrementally and scores
                             them with an exported model (NumpyScorer).
* ``PrecomputedPredictor`` – replays probabilities produced in batch by the
                             original runtime (e.g. Spark MLlib ``transform``).
                             Safe because row t of the feature table depends
                             only on positions <= t (tested).
"""

from __future__ import annotations

import math
import time
from collections import Counter
from typing import Any, Protocol

import numpy as np

from cachemind.ml.features import OnlineFeatureTracker
from cachemind.ml.model_spec import NumpyScorer
from cachemind.simulation.trace import Trace


class ReusePredictor(Protocol):
    def predict(self, t: int, key: int) -> float | None: ...

    def stats(self) -> dict[str, Any]: ...


def _valid_probability(p: Any) -> bool:
    return isinstance(p, float) and math.isfinite(p) and 0.0 <= p <= 1.0


class OnlinePredictor:
    """Feature tracking + model scoring at request time.

    ``trace`` supplies per-request context (event time, size, category,
    region) for the *current* position only.
    """

    def __init__(
        self, scorer: NumpyScorer | None, trace: Trace, load_error: str | None = None
    ) -> None:
        self.scorer = scorer
        self.load_error = load_error
        self._trace = trace
        self._tracker = OnlineFeatureTracker()
        self._times = trace.event_time_ms.tolist()
        self._sizes = trace.size_bytes.tolist()
        self._cats = trace.category.tolist()
        self._regions = trace.region.tolist()
        self.calls = 0
        self.failures: Counter[str] = Counter()
        self.feature_ns = 0
        self.inference_ns = 0
        self._lat_samples: list[int] = []

    def predict(self, t: int, key: int) -> float | None:
        t0 = time.perf_counter_ns()
        feats = self._tracker.observe(t, key, self._times[t], self._sizes[t])
        t1 = time.perf_counter_ns()
        self.feature_ns += t1 - t0
        if self.scorer is None:
            self.failures["model_unavailable"] += 1
            return None
        self.calls += 1
        try:
            p = self.scorer.predict_one(feats, self._cats[t], self._regions[t])
        except (
            ValueError,
            KeyError,
            IndexError,
            TypeError,
            FloatingPointError,
            OverflowError,
        ) as exc:
            self.failures[f"inference_error:{type(exc).__name__}"] += 1
            return None
        dt = time.perf_counter_ns() - t1
        self.inference_ns += dt
        if len(self._lat_samples) < 200_000:
            self._lat_samples.append(dt)
        if not _valid_probability(p):
            self.failures["invalid_probability"] += 1
            return None
        return p

    def stats(self) -> dict[str, Any]:
        lat = np.asarray(self._lat_samples, dtype=np.float64)
        return {
            "predictor": "online",
            "model_type": self.scorer.model_type if self.scorer else None,
            "model_version": (self.scorer.metadata.get("model_version") if self.scorer else None),
            "load_error": self.load_error,
            "prediction_calls": self.calls,
            "prediction_failures": dict(self.failures),
            "feature_time_ms_total": self.feature_ns / 1e6,
            "inference_time_ms_total": self.inference_ns / 1e6,
            "inference_latency_us": _latency_summary(lat / 1e3),
        }


class PrecomputedPredictor:
    def __init__(self, probabilities: np.ndarray, source: str = "precomputed") -> None:
        self._p = np.asarray(probabilities, dtype=np.float64)
        self._next_t = 0
        self.source = source
        self.calls = 0
        self.failures: Counter[str] = Counter()

    def predict(self, t: int, key: int) -> float | None:
        # Consumed strictly in order: index t is the current request, never later.
        if t != self._next_t:
            raise RuntimeError(f"precomputed predictions consumed out of order ({t})")
        self._next_t = t + 1
        self.calls += 1
        if t >= len(self._p):
            self.failures["missing_prediction"] += 1
            return None
        p = float(self._p[t])
        if not _valid_probability(p):
            self.failures["invalid_probability"] += 1
            return None
        return p

    def stats(self) -> dict[str, Any]:
        return {
            "predictor": self.source,
            "prediction_calls": self.calls,
            "prediction_failures": dict(self.failures),
        }


def _latency_summary(values: np.ndarray) -> dict[str, float | int]:
    if len(values) == 0:
        return {"count": 0}
    return {
        "count": len(values),
        "mean": float(values.mean()),
        "p50": float(np.percentile(values, 50)),
        "p95": float(np.percentile(values, 95)),
        "p99": float(np.percentile(values, 99)),
        "max": float(values.max()),
    }
