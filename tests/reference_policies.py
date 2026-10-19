"""Deliberately naive reference cache implementations used only by tests.

They are written independently of ``cachemind.policies`` (plain lists, full
scans, no clever data structures) so that agreement between the two is
meaningful evidence of correctness.
"""

from __future__ import annotations

import functools


def ref_simulate(trace: list[int], capacity: int, policy: str) -> list[bool]:
    """Return the hit/miss sequence for ``policy`` on ``trace``."""
    cache: list[int] = []  # order meaning depends on policy
    inserted_at: dict[int, int] = {}
    last_used: dict[int, int] = {}
    freq: dict[int, int] = {}
    freq_reached_at: dict[int, int] = {}
    out: list[bool] = []
    for t, k in enumerate(trace):
        if k in cache:
            out.append(True)
            last_used[k] = t
            freq[k] += 1
            freq_reached_at[k] = t
            continue
        out.append(False)
        if capacity == 0:
            continue
        if len(cache) >= capacity:
            if policy == "fifo":
                victim = min(cache, key=lambda x: inserted_at[x])
            elif policy == "lru":
                victim = min(cache, key=lambda x: last_used[x])
            elif policy == "lfu":
                victim = min(cache, key=lambda x: (freq[x], freq_reached_at[x]))
            elif policy == "belady":

                def next_use(x: int, t: int = t) -> int:
                    for j in range(t + 1, len(trace)):
                        if trace[j] == x:
                            return j
                    return len(trace)

                victim = max(cache, key=lambda x: (next_use(x), -x))
            else:
                raise ValueError(policy)
            cache.remove(victim)
            del freq[victim]
        cache.append(k)
        inserted_at[k] = t
        last_used[k] = t
        freq[k] = 1
        freq_reached_at[k] = t
    return out


def optimal_misses(trace: list[int], capacity: int) -> int:
    """Exhaustive minimum number of misses over all eviction choices (demand caching)."""

    n = len(trace)

    @functools.cache
    def best(t: int, cache: frozenset[int]) -> int:
        if t == n:
            return 0
        k = trace[t]
        if k in cache:
            return best(t + 1, cache)
        if capacity == 0:
            return 1 + best(t + 1, cache)
        if len(cache) < capacity:
            return 1 + best(t + 1, cache | {k})
        return 1 + min(best(t + 1, (cache - {v}) | {k}) for v in cache)

    return best(0, frozenset())
