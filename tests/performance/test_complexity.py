"""Performance regression tests (complexity, not absolute speed).

Absolute timings depend on the machine and its load, so these tests compare the
same code at two problem sizes and use generous ratio bounds. They catch
algorithmic regressions (e.g. an O(C) scan sneaking into LRU) without being
flaky on a busy laptop. Each measurement is the best of 3 runs.
"""

from __future__ import annotations

import time

import numpy as np
import pytest

from cachemind.ml.features import CATEGORICAL_FEATURES, FEATURE_SCHEMA_VERSION, NUMERIC_FEATURES
from cachemind.ml.model_spec import SPEC_VERSION, NumpyScorer
from cachemind.policies import BeladyPolicy, FIFOPolicy, LFUPolicy, LRUPolicy
from cachemind.simulation.engine import simulate

pytestmark = pytest.mark.performance


def _trace(n: int = 120_000, items: int = 60_000, seed: int = 0) -> np.ndarray:
    rng = np.random.default_rng(seed)
    return (rng.zipf(1.05, n) % items).astype(np.int64)


def _best_seconds(fn, repeats: int = 3) -> float:
    best = float("inf")
    for _ in range(repeats):
        t0 = time.perf_counter()
        fn()
        best = min(best, time.perf_counter() - t0)
    return best


@pytest.mark.parametrize("cls", [FIFOPolicy, LRUPolicy, LFUPolicy])
def test_classical_policies_are_constant_time_in_capacity(cls):
    keys = _trace()
    small = _best_seconds(lambda: simulate(keys, cls(), 100))
    large = _best_seconds(lambda: simulate(keys, cls(), 20_000))
    # O(1) per operation: 200x more capacity must not cost anything close to 200x
    assert large < 3.0 * small, (cls.name, small, large)


def test_belady_scales_logarithmically_in_capacity():
    keys = _trace()
    small = _best_seconds(lambda: simulate(keys, BeladyPolicy(), 100))
    large = _best_seconds(lambda: simulate(keys, BeladyPolicy(), 20_000))
    assert large < 4.0 * small, (small, large)


def test_simulation_cost_is_linear_in_trace_length():
    short, long_ = _trace(60_000), _trace(240_000)
    t_short = _best_seconds(lambda: simulate(short, LRUPolicy(), 1_000))
    t_long = _best_seconds(lambda: simulate(long_, LRUPolicy(), 1_000))
    assert t_long < 8.0 * t_short  # 4x the requests; allow overhead noise, reject O(n^2)


def test_exported_lr_scorer_single_prediction_is_microseconds():
    rng = np.random.default_rng(1)
    width = len(NUMERIC_FEATURES) + 8
    spec = {
        "spec_version": SPEC_VERSION,
        "feature_schema_version": FEATURE_SCHEMA_VERSION,
        "numeric": list(NUMERIC_FEATURES),
        "categorical": [
            {"name": n, "labels": ["a", "b", "c"], "unknown_index": 3, "size": 4}
            for n in CATEGORICAL_FEATURES
        ],
        "scaler": {
            "mean": rng.normal(size=width).tolist(),
            "scale": rng.uniform(0.5, 2, size=width).tolist(),
        },
        "model": {
            "type": "logistic_regression",
            "coef": rng.normal(size=width).tolist(),
            "intercept": 0.1,
        },
    }
    scorer = NumpyScorer(spec)
    row = rng.normal(size=len(NUMERIC_FEATURES)).tolist()
    n = 20_000
    seconds = _best_seconds(lambda: [scorer.predict_one(row, "a", "b") for _ in range(n)])
    assert seconds / n < 50e-6  # well under the per-request budget of the simulator
