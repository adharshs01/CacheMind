"""Policy-independent cache simulation engine.

Invariants enforced on every request (``InvariantViolation`` is raised, never
logged-and-ignored):

* a hit reported by the policy  <=>  the key is in the engine's shadow set;
* ``0 <= len(policy) <= capacity`` after each request;
* a victim must be resident; after eviction it is gone;
* after a miss the requested key is resident (demand caching, capacity > 0);
* ``hits + misses == measured requests``.

With ``check_level="full"`` the policy's full resident set is compared with the
shadow set every ``full_check_every`` requests (used by tests).

Only policies declaring ``requires_future = True`` receive the trace via
``prepare_offline``. Everything else sees one key at a time.
"""

from __future__ import annotations

import time
import tracemalloc
from dataclasses import asdict, dataclass, field
from typing import Any, Literal

import numpy as np

from cachemind.policies.base import CachePolicy


class InvariantViolation(AssertionError):
    pass


@dataclass
class SimulationResult:
    policy: str
    capacity: int
    n_requests: int  # measured requests
    hits: int
    misses: int
    evictions: int  # whole run (warm-up + measured window)
    evictions_measured: int  # inside [measure_from, measure_to), like hits/misses
    insertions: int
    hit_rate: float
    miss_rate: float
    measure_from: int
    measure_to: int
    wall_time_s: float
    cpu_time_s: float
    eviction_decision_us: dict[str, float | int]
    timeseries: list[dict[str, float | int]]
    policy_stats: dict[str, Any]
    policy_config: dict[str, Any]
    final_cache_sample: list[str]
    decision_log: list[dict[str, Any]] = field(default_factory=list)
    peak_memory_kib: float | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def simulate(
    keys: np.ndarray,
    policy: CachePolicy,
    capacity: int,
    *,
    measure_from: int = 0,
    measure_to: int | None = None,
    check_level: Literal["cheap", "full"] = "cheap",
    full_check_every: int = 1,
    timeseries_window: int = 0,
    decision_log_limit: int = 0,
    item_ids: np.ndarray | None = None,
    measure_memory: bool = False,
) -> SimulationResult:
    keys = np.asarray(keys)
    if keys.ndim != 1:
        raise ValueError("keys must be a 1-D array")
    if keys.size and not np.issubdtype(keys.dtype, np.integer):
        raise ValueError("keys must be integer codes (use Trace to encode item ids)")
    n = len(keys)
    measure_to = n if measure_to is None else measure_to
    if not 0 <= measure_from <= measure_to <= n:
        raise ValueError(f"invalid measurement window [{measure_from}, {measure_to}) for n={n}")
    if capacity < 0:
        raise ValueError("capacity must be >= 0")

    policy.reset(capacity)
    if policy.requires_future:
        policy.prepare_offline(keys)

    shadow: set[int] = set()
    hits = misses = evictions = evictions_measured = insertions = 0
    win_hits = win_total = 0
    timeseries: list[dict[str, float | int]] = []
    decision_ns: list[int] = []
    log: list[dict[str, Any]] = []
    names = item_ids

    if measure_memory:
        tracemalloc.start()
    wall0, cpu0 = time.perf_counter(), time.process_time()
    key_list = keys.tolist()
    for t in range(n):
        k = key_list[t]
        policy.on_request(t, k)
        policy_hit = k in policy
        shadow_hit = k in shadow
        if policy_hit != shadow_hit:
            raise InvariantViolation(
                f"t={t}: policy {policy.name} reports hit={policy_hit} but key {k} "
                f"resident={shadow_hit}"
            )
        measured = measure_from <= t < measure_to
        if policy_hit:
            policy.on_hit(t, k)
            if measured:
                hits += 1
                win_hits += 1
        else:
            if measured:
                misses += 1
            if capacity > 0:
                victim = None
                if len(shadow) >= capacity:
                    t0 = time.perf_counter_ns()
                    victim = policy.choose_victim(t)
                    dt = time.perf_counter_ns() - t0
                    if victim not in shadow:
                        raise InvariantViolation(
                            f"t={t}: {policy.name} chose non-resident victim {victim}"
                        )
                    policy.evict(victim)
                    shadow.discard(victim)
                    if victim in policy:
                        raise InvariantViolation(f"t={t}: victim {victim} still resident")
                    evictions += 1
                    if measured:
                        evictions_measured += 1
                    if len(decision_ns) < 500_000:
                        decision_ns.append(dt)
                policy.insert(t, k)
                shadow.add(k)
                insertions += 1
                if k not in policy:
                    raise InvariantViolation(f"t={t}: inserted key {k} not resident")
                if decision_log_limit and len(log) < decision_log_limit and victim is not None:
                    log.append(
                        {
                            "t": t,
                            "request": _name(names, k),
                            "evicted": _name(names, victim),
                        }
                    )
        if len(policy) > capacity or len(policy) != len(shadow):
            raise InvariantViolation(
                f"t={t}: size {len(policy)} (shadow {len(shadow)}) vs capacity {capacity}"
            )
        if check_level == "full" and t % full_check_every == 0 and set(policy.resident()) != shadow:
            raise InvariantViolation(f"t={t}: resident set diverged from shadow set")
        if measured:
            win_total += 1
            if timeseries_window and win_total == timeseries_window:
                timeseries.append({"t_end": t + 1, "hit_rate": win_hits / win_total})
                win_hits = win_total = 0
    wall = time.perf_counter() - wall0
    cpu = time.process_time() - cpu0
    peak = None
    if measure_memory:
        _, peak_bytes = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        peak = peak_bytes / 1024

    total = hits + misses
    if total != measure_to - measure_from:
        raise InvariantViolation(
            f"hits+misses={total} != measured requests {measure_to - measure_from}"
        )
    lat = np.asarray(decision_ns, dtype=np.float64) / 1e3
    resident_sample = sorted(_name(names, k) for k in list(shadow)[:200])
    return SimulationResult(
        policy=policy.name,
        capacity=capacity,
        n_requests=total,
        hits=hits,
        misses=misses,
        evictions=evictions,
        evictions_measured=evictions_measured,
        insertions=insertions,
        hit_rate=hits / total if total else 0.0,
        miss_rate=misses / total if total else 0.0,
        measure_from=measure_from,
        measure_to=measure_to,
        wall_time_s=wall,
        cpu_time_s=cpu,
        eviction_decision_us=_summary(lat),
        timeseries=timeseries,
        policy_stats=policy.stats(),
        policy_config=policy.describe(),
        final_cache_sample=resident_sample,
        decision_log=log,
        peak_memory_kib=peak,
    )


def _name(names: np.ndarray | None, k: int) -> str:
    return str(names[k]) if names is not None else str(k)


def _summary(values: np.ndarray) -> dict[str, float | int]:
    if len(values) == 0:
        return {"count": 0}
    return {
        "count": len(values),
        "mean": float(values.mean()),
        "p50": float(np.percentile(values, 50)),
        "p95": float(np.percentile(values, 95)),
        "p99": float(np.percentile(values, 99)),
        "max": float(values.max()),
    }
