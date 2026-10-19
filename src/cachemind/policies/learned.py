"""ML-guided and hybrid replacement policies (online; never see future requests).

Both policies obtain one reuse prediction per request from a ``ReusePredictor``:
``p(k, t)`` = probability that key k is requested again within H requests after
its access at t. The prediction is stored with the key and refreshed on every
access, so inference cost is exactly one model call per request — independent
of cache size.

ML policy (``ml``)
    On a miss with a full cache:
      1. If the least-recently-used key's last access is more than H requests
         ago, its prediction window has expired without the predicted reuse:
         evict it (expired keys are always evicted first, oldest first).
      2. Otherwise evict the key with the lowest stored p. Ties: older last
         access first, then smaller key.
    Expired keys form a prefix of the LRU order, so step 1 is O(1); step 2 uses
    a lazily-invalidated min-heap, O(log C).

Hybrid policy (``hybrid``)
    score(k) = alpha * p_eff(k) + (1 - alpha) * exp(-(t - last(k)) / tau)
    with p_eff = 0 once the prediction window has expired. Evict the minimum
    score (ties: oldest last access). alpha = 0 is exactly LRU; alpha = 1 is the
    ML ranking. alpha/tau must be tuned on the validation block only.
    Cost O(C) per eviction (vectorised).

Fallback
    A missing/invalid prediction (model not loaded, inference error, NaN, value
    outside [0, 1], schema mismatch) is stored as "invalid". While any resident
    key has an invalid prediction, evictions use LRU. Every fallback is counted
    and reported; nothing is silently skipped.
"""

from __future__ import annotations

import heapq
import math
from collections import OrderedDict
from collections.abc import Iterable
from typing import Any

import numpy as np

from cachemind.ml.predictors import ReusePredictor
from cachemind.policies.base import CachePolicy


class MLPolicy(CachePolicy):
    name = "ml"

    def __init__(self, predictor: ReusePredictor, horizon: int) -> None:
        super().__init__()
        if horizon < 1:
            raise ValueError("horizon must be >= 1")
        self.predictor = predictor
        self.horizon = horizon

    def _reset_state(self) -> None:
        self._last: OrderedDict[int, int] = OrderedDict()  # key -> last access t (LRU order)
        self._p: dict[int, float | None] = {}
        self._heap: list[tuple[float, int, int]] = []  # (p, last_t, key)
        self._invalid = 0
        self._cur: float | None = None
        self.expired_evictions = 0
        self.model_evictions = 0
        self.fallback_evictions = 0

    def on_request(self, t: int, key: int) -> None:
        self._cur = self.predictor.predict(t, key)

    def __contains__(self, key: object) -> bool:
        return key in self._last

    def __len__(self) -> int:
        return len(self._last)

    def _record(self, t: int, key: int) -> None:
        old = self._p.get(key, 0.0)
        if key in self._p and old is None:
            self._invalid -= 1
        p = self._cur
        self._p[key] = p
        if p is None:
            self._invalid += 1
        else:
            heapq.heappush(self._heap, (p, t, key))
            if len(self._heap) > 4 * len(self._last) + 1024:
                self._heap = [(pv, self._last[k], k) for k, pv in self._p.items() if pv is not None]
                heapq.heapify(self._heap)

    def on_hit(self, t: int, key: int) -> None:
        self._last.move_to_end(key)
        self._last[key] = t
        self._record(t, key)

    def insert(self, t: int, key: int) -> None:
        self._last[key] = t
        self._record(t, key)

    def choose_victim(self, t: int) -> int:
        oldest_key, oldest_t = next(iter(self._last.items()))
        if t - oldest_t > self.horizon:
            self.expired_evictions += 1
            return oldest_key
        if self._invalid > 0:
            self.fallback_evictions += 1
            return oldest_key
        heap = self._heap
        while heap:
            p, lt, k = heap[0]
            if self._last.get(k) == lt:
                self.model_evictions += 1
                return k
            heapq.heappop(heap)
        raise RuntimeError("ML policy heap exhausted with a non-empty cache")  # pragma: no cover

    def evict(self, key: int) -> None:
        del self._last[key]
        if self._p.pop(key) is None:
            self._invalid -= 1

    def resident(self) -> Iterable[int]:
        return list(self._last)

    def stats(self) -> dict[str, Any]:
        return {
            "expired_evictions": self.expired_evictions,
            "model_evictions": self.model_evictions,
            "fallback_evictions": self.fallback_evictions,
            **self.predictor.stats(),
        }

    def describe(self) -> dict[str, Any]:
        return {**super().describe(), "horizon": self.horizon}


class HybridPolicy(CachePolicy):
    name = "hybrid"

    def __init__(
        self, predictor: ReusePredictor, horizon: int, alpha: float = 0.5, tau: float | None = None
    ) -> None:
        super().__init__()
        if not 0.0 <= alpha <= 1.0:
            raise ValueError("alpha must be in [0, 1]")
        self.predictor = predictor
        self.horizon = horizon
        self.alpha = alpha
        self.tau = float(tau if tau is not None else horizon)
        if self.tau <= 0:
            raise ValueError("tau must be positive")

    def _reset_state(self) -> None:
        c = self.capacity
        self._slot_of: dict[int, int] = {}
        self._keys = np.full(c, -1, dtype=np.int64)
        self._last = np.zeros(c, dtype=np.int64)
        self._pv = np.full(c, np.nan, dtype=np.float64)
        self._free = list(range(c - 1, -1, -1))
        self._cur: float | None = None
        self.fallback_evictions = 0
        self.model_evictions = 0

    def on_request(self, t: int, key: int) -> None:
        self._cur = self.predictor.predict(t, key)

    def __contains__(self, key: object) -> bool:
        return key in self._slot_of

    def __len__(self) -> int:
        return len(self._slot_of)

    def on_hit(self, t: int, key: int) -> None:
        s = self._slot_of[key]
        self._last[s] = t
        self._pv[s] = math.nan if self._cur is None else self._cur

    def insert(self, t: int, key: int) -> None:
        s = self._free.pop()
        self._slot_of[key] = s
        self._keys[s] = key
        self._last[s] = t
        self._pv[s] = math.nan if self._cur is None else self._cur

    def choose_victim(self, t: int) -> int:
        used = self._keys >= 0
        last = self._last[used]
        keys = self._keys[used]
        pv = self._pv[used]
        if np.isnan(pv).any():
            self.fallback_evictions += 1
            return int(keys[np.argmin(last)])
        age = (t - last).astype(np.float64)
        p_eff = np.where(age > self.horizon, 0.0, pv)
        score = self.alpha * p_eff + (1.0 - self.alpha) * np.exp(-age / self.tau)
        cand = np.flatnonzero(score == score.min())
        self.model_evictions += 1
        # last-access times are unique per key, so this tie-break is total
        return int(keys[cand[np.argmin(last[cand])]])

    def evict(self, key: int) -> None:
        s = self._slot_of.pop(key)
        self._keys[s] = -1
        self._pv[s] = np.nan
        self._free.append(s)

    def resident(self) -> Iterable[int]:
        return list(self._slot_of)

    def stats(self) -> dict[str, Any]:
        return {
            "model_evictions": self.model_evictions,
            "fallback_evictions": self.fallback_evictions,
            **self.predictor.stats(),
        }

    def describe(self) -> dict[str, Any]:
        return {**super().describe(), "horizon": self.horizon, "alpha": self.alpha, "tau": self.tau}
