"""ML-guided and hybrid policies: semantics, fallback and online-ness."""

from __future__ import annotations

import math
from typing import ClassVar

import numpy as np
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from cachemind.ml.features import (
    CATEGORICAL_FEATURES,
    FEATURE_SCHEMA_VERSION,
    NUMERIC_FEATURES,
    compute_features,
)
from cachemind.ml.model_spec import SPEC_VERSION, NumpyScorer
from cachemind.ml.predictors import OnlinePredictor, PrecomputedPredictor
from cachemind.policies import LRUPolicy
from cachemind.policies.learned import HybridPolicy, MLPolicy
from cachemind.simulation.engine import simulate
from cachemind.simulation.trace import Trace


class ConstPredictor:
    def __init__(self, value):
        self.value = value
        self.calls = 0

    def predict(self, t, key):
        self.calls += 1
        return self.value

    def stats(self):
        return {"prediction_calls": self.calls}


def lr_scorer(coef_gap=-1.0, coef_cnt=0.5, intercept=2.0) -> NumpyScorer:
    coef = [0.0] * len(NUMERIC_FEATURES)
    coef[NUMERIC_FEATURES.index("f_gap1_log")] = coef_gap
    coef[NUMERIC_FEATURES.index("f_cnt100")] = coef_cnt
    spec = {
        "spec_version": SPEC_VERSION,
        "feature_schema_version": FEATURE_SCHEMA_VERSION,
        "numeric": list(NUMERIC_FEATURES),
        "categorical": [
            {"name": n, "labels": ["unknown", "us-east"], "unknown_index": -1, "size": 2}
            for n in CATEGORICAL_FEATURES
        ],
        "scaler": None,
        "model": {"type": "logistic_regression", "coef": coef + [0.0] * 4, "intercept": intercept},
    }
    return NumpyScorer(spec)


def zipf_trace(n=4000, seed=0, items=400):
    rng = np.random.default_rng(seed)
    return Trace.from_items([f"k{v}" for v in (rng.zipf(1.2, n) % items).tolist()])


def test_ml_with_constant_predictions_equals_lru():
    tr = zipf_trace()
    lru = simulate(tr.keys, LRUPolicy(), 40)
    ml = simulate(
        tr.keys,
        MLPolicy(ConstPredictor(0.5), horizon=10**9),
        40,
        check_level="full",
        full_check_every=97,
    )
    assert ml.hits == lru.hits


@pytest.mark.parametrize("bad", [None, math.nan, 1.5, -0.1])
def test_invalid_predictions_fall_back_to_lru(bad):
    tr = zipf_trace()
    lru = simulate(tr.keys, LRUPolicy(), 40)
    pred = PrecomputedPredictor(np.full(len(tr), np.nan if bad is None else bad))
    res = simulate(tr.keys, MLPolicy(pred, horizon=10**9), 40)
    assert res.hits == lru.hits
    assert res.policy_stats["fallback_evictions"] == res.evictions
    assert res.policy_stats["prediction_failures"]["invalid_probability"] == len(tr)


def test_missing_model_falls_back_and_reports():
    tr = zipf_trace()
    pred = OnlinePredictor(None, tr, load_error="artifact not found")
    res = simulate(tr.keys, MLPolicy(pred, horizon=10**9), 40)
    assert res.hits == simulate(tr.keys, LRUPolicy(), 40).hits
    assert res.policy_stats["load_error"] == "artifact not found"
    assert res.policy_stats["prediction_failures"]["model_unavailable"] == len(tr)


class _ExplodingScorer:
    model_type = "broken"
    metadata: ClassVar[dict] = {}

    def predict_one(self, *_a):
        raise ValueError("boom")


def test_inference_errors_are_counted_not_swallowed():
    tr = zipf_trace(n=500)
    pred = OnlinePredictor(_ExplodingScorer(), tr)  # type: ignore[arg-type]
    res = simulate(tr.keys, MLPolicy(pred, horizon=10**9), 10)
    assert res.policy_stats["prediction_failures"]["inference_error:ValueError"] == 500
    assert res.policy_stats["fallback_evictions"] == res.evictions


def test_expired_predictions_are_evicted_first():
    # a has a high prediction but is not requested for > H requests -> evicted before b
    keys = np.array([0, 1, 2, 2, 2, 2, 3], dtype=np.int64)
    probs = np.array([0.99, 0.10, 0.5, 0.5, 0.5, 0.5, 0.5])
    pol = MLPolicy(PrecomputedPredictor(probs), horizon=3)
    res = simulate(keys, pol, 3, item_ids=np.array(["a", "b", "c", "d"]))
    assert "a" not in res.final_cache_sample and "b" in res.final_cache_sample
    assert res.policy_stats["expired_evictions"] == 1


def test_lowest_probability_is_evicted_when_nothing_expired():
    keys = np.array([0, 1, 2, 3], dtype=np.int64)
    probs = np.array([0.9, 0.2, 0.7, 0.5])
    res = simulate(
        keys,
        MLPolicy(PrecomputedPredictor(probs), horizon=100),
        3,
        item_ids=np.array(["a", "b", "c", "d"]),
    )
    assert sorted(res.final_cache_sample) == ["a", "c", "d"]
    assert res.policy_stats["model_evictions"] == 1


def test_hybrid_alpha_zero_is_exactly_lru():
    tr = zipf_trace()
    res = simulate(tr.keys, HybridPolicy(ConstPredictor(0.3), horizon=200, alpha=0.0), 40)
    assert res.hits == simulate(tr.keys, LRUPolicy(), 40).hits


def test_one_prediction_per_request():
    tr = zipf_trace(n=1500)
    pred = ConstPredictor(0.5)
    simulate(tr.keys, MLPolicy(pred, horizon=50), 20)
    assert pred.calls == 1500


def test_precomputed_equals_online_predictions():
    tr = zipf_trace(n=3000)
    scorer = lr_scorer()
    xn = compute_features(tr.keys, tr.event_time_ms, tr.size_bytes)
    probs = scorer.predict_proba(xn, {"category": tr.category, "region": tr.region})
    for policy_cls in (MLPolicy, HybridPolicy):
        on = simulate(tr.keys, policy_cls(OnlinePredictor(scorer, tr), 100), 30)
        pre = simulate(tr.keys, policy_cls(PrecomputedPredictor(probs), 100), 30)
        assert (on.hits, on.evictions) == (pre.hits, pre.evictions)


def test_precomputed_predictor_refuses_out_of_order_access():
    pred = PrecomputedPredictor(np.array([0.1, 0.2, 0.3]))
    pred.predict(0, 1)
    with pytest.raises(RuntimeError):
        pred.predict(2, 1)


ids = st.lists(st.integers(0, 6), min_size=1, max_size=60)


@settings(max_examples=60, deadline=None)
@given(
    prefix=ids,
    suffix_a=ids,
    suffix_b=ids,
    cap=st.integers(1, 4),
    alpha=st.sampled_from([0.0, 0.5, 1.0]),
)
def test_learned_policies_are_prefix_invariant(prefix, suffix_a, suffix_b, cap, alpha):
    scorer = lr_scorer()
    n = len(prefix)
    outcomes = []
    for suffix in (suffix_a, suffix_b):
        tr = Trace.from_items([f"k{v}" for v in prefix + suffix])
        ml = simulate(tr.keys, MLPolicy(OnlinePredictor(scorer, tr), 5), cap, measure_to=n)
        hy = simulate(
            tr.keys, HybridPolicy(OnlinePredictor(scorer, tr), 5, alpha=alpha), cap, measure_to=n
        )
        outcomes.append((ml.hits, hy.hits))
    assert outcomes[0] == outcomes[1]
