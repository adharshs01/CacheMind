"""Cache replacement policies."""

from cachemind.policies.base import CachePolicy
from cachemind.policies.belady import BeladyPolicy
from cachemind.policies.classical import FIFOPolicy, LFUPolicy, LRUPolicy

CLASSICAL_POLICIES: dict[str, type[CachePolicy]] = {
    "fifo": FIFOPolicy,
    "lru": LRUPolicy,
    "lfu": LFUPolicy,
}
ORACLE_POLICIES: dict[str, type[CachePolicy]] = {"belady": BeladyPolicy}

__all__ = [
    "CLASSICAL_POLICIES",
    "ORACLE_POLICIES",
    "BeladyPolicy",
    "CachePolicy",
    "FIFOPolicy",
    "LFUPolicy",
    "LRUPolicy",
]
