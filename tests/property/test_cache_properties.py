"""Property-based tests (Hypothesis) for the cache engine and policies."""

from __future__ import annotations

import numpy as np
from hypothesis import given, settings
from hypothesis import strategies as st

from cachemind.policies import BeladyPolicy, FIFOPolicy, LFUPolicy, LRUPolicy
from cachemind.simulation.engine import simulate
from tests.reference_policies import optimal_misses, ref_simulate

ONLINE = [FIFOPolicy, LRUPolicy, LFUPolicy]
traces = st.lists(st.integers(min_value=0, max_value=7), min_size=0, max_size=80)
small_traces = st.lists(st.integers(min_value=0, max_value=3), min_size=0, max_size=10)


@settings(max_examples=150, deadline=None)
@given(trace=traces, capacity=st.integers(min_value=0, max_value=6))
def test_invariants_hold_for_all_policies(trace, capacity):
    keys = np.asarray(trace, dtype=np.int64)
    for cls in [*ONLINE, BeladyPolicy]:
        res = simulate(keys, cls(), capacity, check_level="full")  # raises on violation
        assert res.hits + res.misses == len(trace)
        assert res.evictions <= res.misses
        assert res.insertions == (res.misses if capacity else 0)


@settings(max_examples=150, deadline=None)
@given(trace=traces, capacity=st.integers(min_value=0, max_value=6))
def test_policies_match_naive_references(trace, capacity):
    keys = np.asarray(trace, dtype=np.int64)
    for name, cls in [
        ("fifo", FIFOPolicy),
        ("lru", LRUPolicy),
        ("lfu", LFUPolicy),
        ("belady", BeladyPolicy),
    ]:
        res = simulate(keys, cls(), capacity)
        assert res.hits == sum(ref_simulate(trace, capacity, name)), name


@settings(max_examples=300, deadline=None)
@given(trace=small_traces, capacity=st.integers(min_value=0, max_value=3))
def test_belady_is_optimal_by_exhaustive_search(trace, capacity):
    keys = np.asarray(trace, dtype=np.int64)
    best = optimal_misses(trace, capacity)
    assert simulate(keys, BeladyPolicy(), capacity).misses == best
    for cls in ONLINE:
        assert simulate(keys, cls(), capacity).misses >= best


@settings(max_examples=100, deadline=None)
@given(trace=traces, capacity=st.integers(min_value=0, max_value=6))
def test_belady_never_worse_than_online_policies(trace, capacity):
    keys = np.asarray(trace, dtype=np.int64)
    opt = simulate(keys, BeladyPolicy(), capacity).misses
    for cls in ONLINE:
        assert opt <= simulate(keys, cls(), capacity).misses


@settings(max_examples=100, deadline=None)
@given(
    prefix=traces, suffix_a=traces, suffix_b=traces, capacity=st.integers(min_value=1, max_value=5)
)
def test_online_policies_are_prefix_invariant(prefix, suffix_a, suffix_b, capacity):
    """An online policy's decisions on a prefix cannot depend on later requests."""
    n = len(prefix)
    for cls in ONLINE:
        ra = simulate(np.asarray(prefix + suffix_a, dtype=np.int64), cls(), capacity, measure_to=n)
        rb = simulate(np.asarray(prefix + suffix_b, dtype=np.int64), cls(), capacity, measure_to=n)
        assert (ra.hits, ra.misses) == (rb.hits, rb.misses)
