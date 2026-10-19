"""Belady's MIN — the offline optimal reference for fixed-size demand caching.

On a miss with a full cache it evicts the resident key whose next request lies
farthest in the future (keys never requested again count as infinitely far).
For unit-size items with demand fetching (every miss is inserted) this minimises
the number of misses; ``tests/property/test_belady_optimality.py`` verifies that
against exhaustive search on small traces.

This policy *requires the future* and is therefore only an offline oracle. The
simulation engine hands it the full trace through ``prepare_offline``; online
policies never receive it.

Tie-breaking: among keys with the same next-use position (only possible for
"never again"), the smallest key is evicted. Any choice among such ties is
optimal; fixing one keeps runs deterministic.
"""

from __future__ import annotations

import heapq
from collections.abc import Iterable

import numpy as np

from cachemind.policies.base import CachePolicy


def next_use_positions(keys: np.ndarray) -> np.ndarray:
    """next_use[t] = smallest t' > t with keys[t'] == keys[t], or len(keys) if none."""
    n = len(keys)
    nxt = np.full(n, n, dtype=np.int64)
    last_seen: dict[int, int] = {}
    for t in range(n - 1, -1, -1):
        k = int(keys[t])
        nxt[t] = last_seen.get(k, n)
        last_seen[k] = t
    return nxt


class BeladyPolicy(CachePolicy):
    name = "belady"
    requires_future = True

    def __init__(self) -> None:
        super().__init__()
        self._next_use: np.ndarray | None = None

    def prepare_offline(self, keys: np.ndarray) -> None:
        self._next_use = next_use_positions(np.asarray(keys))

    def _reset_state(self) -> None:
        self._current: dict[int, int] = {}  # resident key -> its next-use position
        self._heap: list[tuple[int, int]] = []  # (-next_use, key), lazily invalidated

    def __contains__(self, key: object) -> bool:
        return key in self._current

    def __len__(self) -> int:
        return len(self._current)

    def _set_next(self, t: int, key: int) -> None:
        if self._next_use is None:
            raise RuntimeError("BeladyPolicy.prepare_offline() was not called")
        nu = int(self._next_use[t])
        self._current[key] = nu
        heapq.heappush(self._heap, (-nu, key))
        if len(self._heap) > 4 * len(self._current) + 1024:
            # Drop stale entries so memory stays O(capacity), not O(trace length).
            self._heap = [(-v, k) for k, v in self._current.items()]
            heapq.heapify(self._heap)

    def on_hit(self, t: int, key: int) -> None:
        self._set_next(t, key)

    def insert(self, t: int, key: int) -> None:
        self._set_next(t, key)

    def choose_victim(self, t: int) -> int:
        heap = self._heap
        while heap:
            neg_nu, key = heap[0]
            if self._current.get(key) == -neg_nu:
                return key
            heapq.heappop(heap)  # stale entry (key evicted or next-use updated)
        raise RuntimeError("choose_victim called on an empty cache")

    def evict(self, key: int) -> None:
        del self._current[key]

    def resident(self) -> Iterable[int]:
        return list(self._current)
