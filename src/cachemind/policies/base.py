"""Cache policy interface.

The simulation engine (``cachemind.simulation.engine``) owns the request loop and
calls a policy through this narrow interface. Policies hold only their own
bookkeeping; the engine keeps an independent shadow residency set and checks
every policy decision against it.

Call protocol for request ``t`` of key ``k`` (keys are dense ints):

1. ``on_request(t, k)``        – always, before hit/miss handling (online state)
2. hit  -> ``on_hit(t, k)``
   miss -> if full: ``v = choose_victim(t)``; ``evict(v)``
           then ``insert(t, k)``                     (demand caching: no bypass)

Online policies never see future requests. Only a policy with
``requires_future = True`` (the Belady oracle) receives the full trace via
``prepare_offline`` — the engine refuses to pass it to anything else.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterable
from typing import Any, ClassVar

import numpy as np


class CachePolicy(ABC):
    name: ClassVar[str] = "base"
    requires_future: ClassVar[bool] = False

    def __init__(self) -> None:
        self.capacity = 0

    def reset(self, capacity: int) -> None:
        if capacity < 0:
            raise ValueError("capacity must be >= 0")
        self.capacity = capacity
        self._reset_state()

    @abstractmethod
    def _reset_state(self) -> None: ...

    def prepare_offline(self, keys: np.ndarray) -> None:  # pragma: no cover - interface
        raise TypeError(f"{self.name} is an online policy and must not receive the trace")

    def on_request(self, t: int, key: int) -> None:  # noqa: B027 - optional hook
        """Hook called for every request (hits and misses) before handling."""

    @abstractmethod
    def __contains__(self, key: object) -> bool: ...

    @abstractmethod
    def __len__(self) -> int: ...

    @abstractmethod
    def on_hit(self, t: int, key: int) -> None: ...

    @abstractmethod
    def choose_victim(self, t: int) -> int:
        """Return the key to evict. Must not change the resident set."""

    @abstractmethod
    def evict(self, key: int) -> None: ...

    @abstractmethod
    def insert(self, t: int, key: int) -> None: ...

    @abstractmethod
    def resident(self) -> Iterable[int]: ...

    def stats(self) -> dict[str, Any]:
        """Policy-specific counters reported with simulation results."""
        return {}

    def describe(self) -> dict[str, Any]:
        return {"name": self.name, "requires_future": self.requires_future}
