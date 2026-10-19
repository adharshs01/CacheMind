"""Test group B: online features, labels, temporal splits and leakage detection."""

from __future__ import annotations

import math

import numpy as np
import pytest

from cachemind.ml.features import (
    GAP_CAP,
    NUMERIC_FEATURES,
    OnlineFeatureTracker,
    compute_features,
    reuse_labels,
)
from cachemind.ml.leakage import check_prefix_invariance
from cachemind.ml.splits import assert_no_label_leakage, chronological_split

F = {name: i for i, name in enumerate(NUMERIC_FEATURES)}
HOUR_MS = 3_600_000


def feats(keys, times=None, sizes=None):
    keys = np.asarray(keys, dtype=np.int64)
    n = len(keys)
    times = np.zeros(n, dtype=np.int64) if times is None else np.asarray(times, dtype=np.int64)
    sizes = np.full(n, 100, dtype=np.int64) if sizes is None else np.asarray(sizes)
    return compute_features(keys, times, sizes)


def brute_force_features(keys, times, sizes):
    """Direct definition of every feature by scanning the prefix (slow, obviously right)."""
    rows = []
    for t in range(len(keys)):
        hist = [p for p in range(t + 1) if keys[p] == keys[t]]  # includes t
        prev = hist[:-1]
        gap1 = t - prev[-1] if prev else GAP_CAP
        gap2 = prev[-1] - prev[-2] if len(prev) >= 2 else GAP_CAP

        def cnt(lo, hi, hist=hist):
            return sum(1 for p in hist if lo <= p <= hi)

        hour = (times[t] // HOUR_MS) % 24
        rows.append(
            [
                math.log1p(min(gap1, GAP_CAP)),
                math.log1p(min(gap2, GAP_CAP)),
                cnt(t - 9, t),
                cnt(t - 99, t),
                cnt(t - 999, t),
                math.log1p(len(hist)),
                cnt(t - 99, t) - cnt(t - 199, t - 100),
                1.0 if not prev else 0.0,
                math.log1p(sizes[t]),
                math.sin(2 * math.pi * hour / 24),
                math.cos(2 * math.pi * hour / 24),
            ]
        )
    return np.asarray(rows)


def test_hand_computed_example():
    # positions: 0:a 1:b 2:a 3:a 4:c 5:b
    x = feats([0, 1, 0, 0, 2, 1])
    a3 = x[3]
    assert a3[F["f_gap1_log"]] == pytest.approx(math.log1p(1))
    assert a3[F["f_gap2_log"]] == pytest.approx(math.log1p(2))
    assert a3[F["f_cnt10"]] == 3 and a3[F["f_cnt1000"]] == 3
    assert a3[F["f_freq_log"]] == pytest.approx(math.log1p(3))
    assert a3[F["f_first"]] == 0.0
    b5 = x[5]
    assert b5[F["f_gap1_log"]] == pytest.approx(math.log1p(4))
    assert b5[F["f_gap2_log"]] == pytest.approx(math.log1p(GAP_CAP))


def test_cold_start_defaults():
    x = feats([5])
    assert x[0, F["f_first"]] == 1.0
    assert x[0, F["f_gap1_log"]] == pytest.approx(math.log1p(GAP_CAP))
    assert x[0, F["f_gap2_log"]] == pytest.approx(math.log1p(GAP_CAP))
    assert x[0, F["f_cnt10"]] == 1.0  # the current request counts


@pytest.mark.parametrize(("second", "expected_cnt10"), [(9, 2), (10, 1)])
def test_window_boundaries_are_inclusive_of_t_minus_9(second, expected_cnt10):
    keys = [7] + [100 + i for i in range(second - 1)] + [7]
    x = feats(keys)
    assert x[second, F["f_cnt10"]] == expected_cnt10


def test_trend_uses_two_adjacent_100_windows():
    keys = [1] * 3 + list(range(10, 160)) + [1]  # item 1 at 0,1,2 then at 153
    x = feats(keys)
    t = 153
    # [t-99, t] = [54, 153] contains only t; [t-199, t-100] = [-46, 53] contains 0,1,2
    assert x[t, F["f_cnt100"]] == 1
    assert x[t, F["f_trend"]] == 1 - 3


def test_hour_of_day_is_utc_and_cyclic():
    times = [0, 6 * HOUR_MS, 12 * HOUR_MS, 24 * HOUR_MS]
    x = feats([1, 2, 3, 4], times)
    assert x[0, F["f_hour_cos"]] == pytest.approx(1.0)
    assert x[1, F["f_hour_sin"]] == pytest.approx(1.0)
    assert x[2, F["f_hour_cos"]] == pytest.approx(-1.0)
    np.testing.assert_allclose(x[3], np.r_[x[3, :9], x[0, 9:]])  # 24h == 0h


@pytest.mark.parametrize("seed", [0, 1, 2])
def test_matches_brute_force_definition(seed):
    rng = np.random.default_rng(seed)
    n = 2500
    keys = rng.zipf(1.4, n) % 60
    times = np.cumsum(rng.integers(1, 200_000, n))
    sizes = rng.integers(1, 10**9, n)
    np.testing.assert_allclose(
        compute_features(keys, times, sizes),
        brute_force_features(keys.tolist(), times, sizes),
        rtol=0,
        atol=1e-12,
    )


def test_feature_schema_width():
    assert feats([1, 2, 1]).shape == (3, len(NUMERIC_FEATURES))


def test_tracker_rejects_out_of_order_positions():
    tr = OnlineFeatureTracker()
    tr.observe(5, 1, 0, 1)
    with pytest.raises(ValueError):
        tr.observe(5, 1, 0, 1)


def test_tracker_memory_is_bounded_by_window():
    tr = OnlineFeatureTracker()
    for t in range(20_000):
        tr.observe(t, 1, 0, 1)
    state = tr._state[1]
    assert len(state.positions) - state.start <= 1000
    assert len(state.positions) < 2_100


# --- leakage ---------------------------------------------------------------


def _trace(n=3000, seed=0):
    rng = np.random.default_rng(seed)
    return (rng.zipf(1.3, n) % 300, np.arange(n, dtype=np.int64) * 1000, rng.integers(1, 10**6, n))


def test_online_features_are_prefix_invariant():
    keys, times, sizes = _trace()
    check_prefix_invariance(compute_features, keys, times, sizes).raise_if_leaky()


def test_leaky_whole_trace_popularity_feature_is_detected():
    def leaky(keys, times, sizes):
        values, counts = np.unique(keys, return_counts=True)
        total = dict(zip(values.tolist(), counts.tolist(), strict=True))
        return np.asarray([[total[k]] for k in keys.tolist()], dtype=float)

    keys, times, sizes = _trace()
    report = check_prefix_invariance(leaky, keys, times, sizes)
    assert report.leaky
    with pytest.raises(AssertionError, match="depends on the future"):
        report.raise_if_leaky()


def test_using_the_label_as_a_feature_is_detected():
    keys, times, sizes = _trace()

    def label_feature(k, _t, _s):
        return reuse_labels(k, 50)[0].astype(float)[:, None]

    assert check_prefix_invariance(label_feature, keys, times, sizes).leaky


# --- labels & splits ---------------------------------------------------------


def test_reuse_labels_and_censoring():
    keys = np.array([1, 2, 1, 3, 3, 1, 4])
    label, censored = reuse_labels(keys, horizon=2)
    # pos0 '1' next at 2 (gap 2 <= 2) -> 1 ; pos2 '1' next at 5 (gap 3) -> 0
    assert label.tolist()[:6] == [1, 0, 0, 1, 0, 0]
    # pos 5 ('1') and 6 ('4') have no next request and their horizon passes the end
    assert censored.tolist() == [False, False, False, False, False, True, True]


def test_split_eligibility_purges_label_horizon():
    b = chronological_split(1000, horizon=50, fractions=(0.6, 0.2, 0.2))
    assert (b.train_end, b.val_end) == (600, 800)
    assert b.eligible_range("train") == (0, 550)
    assert b.eligible_range("val") == (600, 750)
    assert b.eligible_range("test") == (800, 950)
    for split in ("train", "val", "test"):
        lo, hi = b.eligible_range(split)
        end = {"train": b.train_end, "val": b.val_end, "test": b.test_end}[split]
        assert hi - 1 + b.horizon < end  # the last label window stays inside the block


def test_label_leakage_guard_rejects_boundary_rows():
    b = chronological_split(1000, horizon=50)
    with pytest.raises(AssertionError, match="later block"):
        assert_no_label_leakage(b, np.array([10, 560]), "train")


def test_split_validation():
    with pytest.raises(ValueError):
        chronological_split(100, horizon=10, fractions=(0.5, 0.5, 0.5))
    with pytest.raises(ValueError):
        chronological_split(100, horizon=60)
