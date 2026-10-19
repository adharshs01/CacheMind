"""Test group A: cache algorithms and engine invariants (example-based)."""

from __future__ import annotations

import numpy as np
import pytest

from cachemind.policies import BeladyPolicy, FIFOPolicy, LFUPolicy, LRUPolicy
from cachemind.policies.base import CachePolicy
from cachemind.simulation.engine import InvariantViolation, simulate
from cachemind.simulation.trace import MalformedTraceError, Trace
from tests.reference_policies import ref_simulate

ALL = [FIFOPolicy, LRUPolicy, LFUPolicy, BeladyPolicy]


def run(items: list[str], policy: CachePolicy, capacity: int):
    trace = Trace.from_items(items)
    return simulate(trace.keys, policy, capacity, check_level="full", item_ids=trace.item_ids)


@pytest.mark.parametrize("cls", ALL)
def test_empty_trace(cls):
    res = simulate(np.array([], dtype=np.int64), cls(), 3)
    assert (res.hits, res.misses, res.evictions, res.n_requests) == (0, 0, 0, 0)
    assert res.hit_rate == 0.0


@pytest.mark.parametrize("cls", ALL)
def test_zero_capacity_everything_misses(cls):
    res = run(list("aaabab"), cls(), 0)
    assert res.hits == 0 and res.misses == 6 and res.insertions == 0 and res.evictions == 0


@pytest.mark.parametrize("cls", ALL)
def test_capacity_one(cls):
    res = run(list("aaba"), cls(), 1)
    # a miss, a hit, b miss (evict a), a miss (evict b)
    assert (res.hits, res.misses, res.evictions) == (1, 3, 2)


@pytest.mark.parametrize("cls", ALL)
def test_repeated_identical_requests(cls):
    res = run(["x"] * 50, cls(), 4)
    assert (res.hits, res.misses, res.evictions) == (49, 1, 0)


@pytest.mark.parametrize("cls", ALL)
def test_all_unique_requests(cls):
    items = [f"k{i}" for i in range(30)]
    res = run(items, cls(), 5)
    assert res.hits == 0 and res.misses == 30 and res.evictions == 25


@pytest.mark.parametrize("cls", ALL)
def test_capacity_larger_than_distinct_items(cls):
    items = list("abcabcabcd")
    res = run(items, cls(), 100)
    assert res.misses == 4  # compulsory misses only
    assert res.evictions == 0


def test_fifo_hit_does_not_change_order():
    res = run(list("abcad"), FIFOPolicy(), 3)
    assert set(res.final_cache_sample) == {"b", "c", "d"}  # 'a' was first in, despite its hit


def test_lru_hit_refreshes_recency():
    res = run(list("abcad"), LRUPolicy(), 3)
    assert set(res.final_cache_sample) == {"a", "c", "d"}  # 'b' least recently used


def test_lfu_evicts_min_frequency_with_lru_tiebreak():
    # a reaches freq 2; b and c have freq 1 and b got there first -> evict b
    res = run(list("aabcd"), LFUPolicy(), 3)
    assert set(res.final_cache_sample) == {"a", "c", "d"}


def test_lfu_counts_reset_on_eviction():
    pol = LFUPolicy()
    trace = Trace.from_items(list("aaabca"))  # cap 2: b evicted? check frequencies
    simulate(trace.keys, pol, 2)
    # 'a' is resident with 3 hits before 'b','c' traffic: freq(a) = 4 after final hit
    a = int(np.flatnonzero(trace.item_ids == "a")[0])
    assert pol.frequency(a) == 4
    pol2 = LFUPolicy()
    t2 = Trace.from_items(["a", "b", "c", "a"])  # cap 1: a evicted, re-inserted -> freq 1
    simulate(t2.keys, pol2, 1)
    a2 = int(np.flatnonzero(t2.item_ids == "a")[0])
    assert pol2.frequency(a2) == 1


def test_belady_textbook_example():
    # Classic example: capacity 3
    items = [7, 0, 1, 2, 0, 3, 0, 4, 2, 3, 0, 3, 2, 1, 2, 0, 1, 7, 0, 1]
    res = run([f"p{x}" for x in items], BeladyPolicy(), 3)
    assert res.misses == 9  # well-known optimal page-fault count for this string


@pytest.mark.parametrize("policy", ["fifo", "lru", "lfu", "belady"])
@pytest.mark.parametrize("seed", range(12))
def test_matches_naive_reference(policy, seed):
    rng = np.random.default_rng(seed)
    n_items = int(rng.integers(2, 12))
    trace = rng.integers(0, n_items, size=int(rng.integers(1, 120))).tolist()
    capacity = int(rng.integers(0, 6))
    expected = ref_simulate(trace, capacity, policy)
    cls = {"fifo": FIFOPolicy, "lru": LRUPolicy, "lfu": LFUPolicy, "belady": BeladyPolicy}[policy]
    pol = cls()
    keys = np.asarray(trace, dtype=np.int64)
    hits = []
    # replay through the engine to capture per-request outcomes
    res = simulate(keys, pol, capacity, check_level="full")
    pol2 = cls()
    pol2.reset(capacity)
    if pol2.requires_future:
        pol2.prepare_offline(keys)
    for t, k in enumerate(trace):
        h = k in pol2
        hits.append(h)
        if h:
            pol2.on_hit(t, k)
        elif capacity:
            if len(pol2) >= capacity:
                pol2.evict(pol2.choose_victim(t))
            pol2.insert(t, k)
    assert hits == expected
    assert res.hits == sum(expected)


@pytest.mark.parametrize("cls", ALL)
def test_deterministic_replay(cls):
    rng = np.random.default_rng(3)
    keys = rng.zipf(1.3, 3000) % 200
    a = simulate(keys, cls(), 20)
    b = simulate(keys, cls(), 20)
    assert (a.hits, a.misses, a.evictions) == (b.hits, b.misses, b.evictions)
    assert a.final_cache_sample == b.final_cache_sample


def test_measurement_window_counts_only_inside_window():
    keys = np.array([1, 1, 1, 2, 2, 2], dtype=np.int64)
    res = simulate(keys, LRUPolicy(), 2, measure_from=3)
    assert res.n_requests == 3 and res.misses == 1 and res.hits == 2


def test_malformed_item_ids_rejected():
    with pytest.raises(MalformedTraceError):
        Trace.from_items(["ok", "", "fine"])
    with pytest.raises(MalformedTraceError):
        Trace.from_items(["ok", None])  # type: ignore[list-item]
    with pytest.raises(MalformedTraceError):
        Trace.from_items(["UPPER case"])


def test_engine_rejects_non_integer_keys():
    with pytest.raises(ValueError):
        simulate(np.array(["a", "b"]), LRUPolicy(), 1)


def test_online_policy_cannot_receive_future():
    with pytest.raises(TypeError):
        LRUPolicy().prepare_offline(np.array([1, 2, 3]))


class _LyingPolicy(LRUPolicy):
    name = "liar"

    def choose_victim(self, t: int) -> int:
        return 10_000  # never resident


def test_engine_detects_invalid_victim():
    with pytest.raises(InvariantViolation):
        simulate(np.array([1, 2, 3], dtype=np.int64), _LyingPolicy(), 2)


class _OverfillPolicy(LRUPolicy):
    name = "overfill"

    def evict(self, key: int) -> None:
        return None  # "forgets" to evict


def test_engine_detects_capacity_violation():
    with pytest.raises(InvariantViolation):
        simulate(np.array([1, 2, 3], dtype=np.int64), _OverfillPolicy(), 2)


def test_timeseries_and_decision_log_are_bounded():
    keys = np.arange(1000, dtype=np.int64) % 37
    res = simulate(keys, LRUPolicy(), 10, timeseries_window=100, decision_log_limit=5)
    assert len(res.timeseries) == 10
    assert len(res.decision_log) == 5


def test_evictions_are_reported_for_window_and_whole_run():
    # cap 1: t0 insert, t1 evict (warm-up), t2 evict (measured), t3 evict (measured)
    res = simulate(np.array([1, 2, 1, 2], dtype=np.int64), LRUPolicy(), 1, measure_from=2)
    assert res.evictions == 3
    assert res.evictions_measured == 2
    assert res.evictions_measured <= res.misses
