"""Future-leakage detection for feature functions.

``check_prefix_invariance`` runs a feature function on a trace and on the same
trace with its suffix replaced by different requests. A feature row for position
t is leakage-free only if it is identical in both runs for every t before the
cut. A function that peeks at later requests (e.g. "will this item appear
again?", whole-trace popularity, centred windows) changes those rows and is
reported.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

import numpy as np

FeatureFn = Callable[[np.ndarray, np.ndarray, np.ndarray], np.ndarray]


@dataclass(frozen=True)
class LeakageReport:
    leaky: bool
    cut: int
    first_bad_position: int | None
    max_abs_diff: float

    def raise_if_leaky(self) -> None:
        if self.leaky:
            raise AssertionError(
                f"feature function depends on the future: row {self.first_bad_position} "
                f"changed (max |diff| {self.max_abs_diff:g}) when requests after "
                f"position {self.cut} were altered"
            )


def check_prefix_invariance(
    fn: FeatureFn,
    keys: np.ndarray,
    event_time_ms: np.ndarray,
    size_bytes: np.ndarray,
    cut: int | None = None,
    seed: int = 0,
) -> LeakageReport:
    n = len(keys)
    if n < 4:
        raise ValueError("need at least 4 requests")
    cut = n // 2 if cut is None else cut
    rng = np.random.default_rng(seed)
    alt_keys = keys.copy()
    # Replace the suffix with requests that heavily re-use prefix items, which is
    # the case most likely to expose look-ahead features.
    prefix_items = np.unique(keys[:cut])
    alt_keys[cut:] = rng.choice(prefix_items, size=n - cut)
    base = np.asarray(fn(keys, event_time_ms, size_bytes))
    alt = np.asarray(fn(alt_keys, event_time_ms, size_bytes))
    diff = np.abs(base[:cut] - alt[:cut])
    if diff.ndim > 1:
        diff = diff.max(axis=1)
    bad = np.flatnonzero(diff > 1e-12)
    return LeakageReport(
        leaky=bool(len(bad)),
        cut=cut,
        first_bad_position=int(bad[0]) if len(bad) else None,
        max_abs_diff=float(diff.max()) if len(diff) else 0.0,
    )
