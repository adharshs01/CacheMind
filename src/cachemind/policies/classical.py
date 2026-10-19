"""Classical baselines: FIFO, LRU and LFU, each O(1) per operation.

Exact semantics (these are the baselines every experiment compares against;
they must not change silently):

FIFO  Evicts the key inserted earliest. Hits do NOT change the order.
LRU   Evicts the key whose most recent request is oldest. Every hit refreshes.
LFU   In-cache LFU ("LFU with dynamic aging off"):
        * a key's frequency starts at 1 on insertion and +1 on every hit;
        * frequencies are discarded on eviction (re-inserted keys restart at 1);
        * evicts a key with the minimum frequency; ties are broken by evicting
          the key that reached that frequency least recently (i.e. LRU within the
          frequency class);
        * no frequency aging.
"""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Iterable

from cachemind.policies.base import CachePolicy


class FIFOPolicy(CachePolicy):
    name = "fifo"

    def _reset_state(self) -> None:
        self._order: OrderedDict[int, None] = OrderedDict()

    def __contains__(self, key: object) -> bool:
        return key in self._order

    def __len__(self) -> int:
        return len(self._order)

    def on_hit(self, t: int, key: int) -> None:
        return None  # insertion order is unaffected by hits

    def choose_victim(self, t: int) -> int:
        return next(iter(self._order))

    def evict(self, key: int) -> None:
        del self._order[key]

    def insert(self, t: int, key: int) -> None:
        self._order[key] = None

    def resident(self) -> Iterable[int]:
        return list(self._order)


class LRUPolicy(CachePolicy):
    name = "lru"

    def _reset_state(self) -> None:
        self._order: OrderedDict[int, None] = OrderedDict()

    def __contains__(self, key: object) -> bool:
        return key in self._order

    def __len__(self) -> int:
        return len(self._order)

    def on_hit(self, t: int, key: int) -> None:
        self._order.move_to_end(key)

    def choose_victim(self, t: int) -> int:
        return next(iter(self._order))

    def evict(self, key: int) -> None:
        del self._order[key]

    def insert(self, t: int, key: int) -> None:
        self._order[key] = None

    def resident(self) -> Iterable[int]:
        return list(self._order)


class LFUPolicy(CachePolicy):
    name = "lfu"

    def _reset_state(self) -> None:
        self._freq: dict[int, int] = {}
        self._buckets: dict[int, OrderedDict[int, None]] = {}
        self._min_freq = 0

    def __contains__(self, key: object) -> bool:
        return key in self._freq

    def __len__(self) -> int:
        return len(self._freq)

    def _detach(self, key: int, f: int) -> bool:
        """Remove ``key`` from bucket ``f``; return True if the bucket became empty."""
        bucket = self._buckets[f]
        del bucket[key]
        if bucket:
            return False
        del self._buckets[f]
        return True

    def on_hit(self, t: int, key: int) -> None:
        f = self._freq[key]
        if self._detach(key, f) and self._min_freq == f:
            self._min_freq = f + 1  # key itself moves to f + 1, so that is the new minimum
        self._freq[key] = f + 1
        self._buckets.setdefault(f + 1, OrderedDict())[key] = None

    def choose_victim(self, t: int) -> int:
        if self._min_freq not in self._buckets:
            # Only reachable after an eviction emptied the minimum bucket without a
            # following insert (the engine always inserts, so this is rare).
            self._min_freq = min(self._buckets)
        return next(iter(self._buckets[self._min_freq]))

    def evict(self, key: int) -> None:
        f = self._freq.pop(key)
        if self._detach(key, f) and self._min_freq == f:
            self._min_freq = 0  # unknown; recomputed lazily in choose_victim

    def insert(self, t: int, key: int) -> None:
        self._freq[key] = 1
        self._buckets.setdefault(1, OrderedDict())[key] = None
        self._min_freq = 1

    def resident(self) -> Iterable[int]:
        return list(self._freq)

    def frequency(self, key: int) -> int:
        return self._freq[key]
