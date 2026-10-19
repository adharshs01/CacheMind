"""Online, leakage-free reuse features (feature schema ``v1``).

Prediction task (see docs/ML_DESIGN.md)
---------------------------------------
At the moment request ``t`` for item ``x`` has been served, predict whether ``x``
will be requested again within the next ``H`` requests, i.e. whether its next
request position lies in ``(t, t + H]``.

"Time" is the position in the canonical ordered trace (0, 1, 2, ...), so all
windows below are measured in requests, not wall-clock time.

Features of (t, x) use only requests at positions ``<= t``:

=============  ================================================================
f_gap1_log     log1p(t - previous request of x)          cold start: log1p(CAP)
f_gap2_log     log1p(gap between the two previous requests) cold start: log1p(CAP)
f_cnt10        requests of x in positions [t-9, t]   (includes the current one)
f_cnt100       requests of x in positions [t-99, t]
f_cnt1000      requests of x in positions [t-999, t]
f_freq_log     log1p(requests of x in [0, t])
f_trend        cnt in [t-99, t] minus cnt in [t-199, t-100]
f_first        1.0 if this is the first request of x, else 0.0
f_size_log     log1p(item_size_bytes)
f_hour_sin/cos hour-of-day (UTC) of the event time on the unit circle
category       item category from metadata ("unknown" when missing)
region         region of the request
=============  ================================================================

The Spark job in ``cachemind.bigdata.features_spark`` computes the same columns
with window functions; ``tests/integration/test_spark_features.py`` asserts that
both implementations agree row-for-row. No item identifier is used as a feature.
"""

from __future__ import annotations

import math
from bisect import bisect_left
from dataclasses import dataclass, field

import numpy as np

FEATURE_SCHEMA_VERSION = "v1"
GAP_CAP = 1_000_000
WINDOWS = (10, 100, 1000)
MAX_WINDOW = 1000
NUMERIC_FEATURES: tuple[str, ...] = (
    "f_gap1_log",
    "f_gap2_log",
    "f_cnt10",
    "f_cnt100",
    "f_cnt1000",
    "f_freq_log",
    "f_trend",
    "f_first",
    "f_size_log",
    "f_hour_sin",
    "f_hour_cos",
)
CATEGORICAL_FEATURES: tuple[str, ...] = ("category", "region")
_LOG_CAP = math.log1p(GAP_CAP)
_TWO_PI_OVER_24 = 2.0 * math.pi / 24.0


def feature_values(
    gap1: int,
    gap2: int,
    c10: int,
    c100: int,
    c200: int,
    c1000: int,
    total: int,
    first: bool,
    size_bytes: int,
    event_time_ms: int,
) -> list[float]:
    """Schema-v1 feature values from the integer history of one request.

    The ONLY place where floating-point transforms are applied. The online tracker and
    the Spark feature job both call it, so the two runtimes are bit-identical: JVM and
    C-library log1p/sin/cos differ in the last bit for some inputs, and a random-forest
    threshold equal to a feature value turns that 1-ulp difference into another branch.
    """
    hour = (event_time_ms // 3_600_000) % 24
    angle = hour * _TWO_PI_OVER_24
    return [
        math.log1p(min(gap1, GAP_CAP)),
        math.log1p(min(gap2, GAP_CAP)),
        float(c10),
        float(c100),
        float(c1000),
        math.log1p(total),
        float(c100 - (c200 - c100)),
        1.0 if first else 0.0,
        math.log1p(size_bytes),
        math.sin(angle),
        math.cos(angle),
    ]


@dataclass(slots=True)
class _ItemState:
    prev1: int = -1
    prev2: int = -1
    total: int = 0
    positions: list[int] = field(default_factory=list)  # ascending, may hold a dead prefix
    start: int = 0  # index of first live position (>= t - MAX_WINDOW + 1)


class OnlineFeatureTracker:
    """Incrementally maintains per-item history and emits schema-v1 features.

    Memory per item is bounded by the largest window (1000 positions).
    ``observe`` must be called with strictly increasing ``t``.
    """

    def __init__(self) -> None:
        self._state: dict[int, _ItemState] = {}
        self._last_t = -1

    def __len__(self) -> int:
        return len(self._state)

    def observe(self, t: int, key: int, event_time_ms: int, size_bytes: int) -> list[float]:
        if t <= self._last_t:
            raise ValueError(f"positions must strictly increase (got {t} after {self._last_t})")
        self._last_t = t
        st = self._state.get(key)
        if st is None:
            st = self._state[key] = _ItemState()
        first = st.prev1 < 0
        gap1 = (t - st.prev1) if not first else GAP_CAP
        gap2 = (st.prev1 - st.prev2) if st.prev2 >= 0 else GAP_CAP
        st.prev2, st.prev1 = st.prev1, t
        st.total += 1
        pos = st.positions
        pos.append(t)
        lo = bisect_left(pos, t - MAX_WINDOW + 1, st.start)
        if lo > 256 and lo > len(pos) // 2:
            del pos[:lo]
            lo = 0
        st.start = lo
        n = len(pos)
        c10 = n - bisect_left(pos, t - 9, lo)
        c100 = n - bisect_left(pos, t - 99, lo)
        c200 = n - bisect_left(pos, t - 199, lo)
        c1000 = n - lo
        return feature_values(
            gap1, gap2, c10, c100, c200, c1000, st.total, first, size_bytes, event_time_ms
        )


def compute_features(
    keys: np.ndarray, event_time_ms: np.ndarray, size_bytes: np.ndarray
) -> np.ndarray:
    """Numeric feature matrix (n, len(NUMERIC_FEATURES)) for an entire trace.

    Row t depends only on positions <= t (it is produced by the online tracker),
    so it is exactly what an online cache would have computed at that moment.
    """
    tracker = OnlineFeatureTracker()
    n = len(keys)
    out = np.empty((n, len(NUMERIC_FEATURES)), dtype=np.float64)
    k_list = keys.tolist()
    ts_list = event_time_ms.tolist()
    sz_list = size_bytes.tolist()
    for t in range(n):
        out[t] = tracker.observe(t, k_list[t], ts_list[t], sz_list[t])
    return out


def reuse_labels(keys: np.ndarray, horizon: int) -> tuple[np.ndarray, np.ndarray]:
    """Labels for the reuse task and a censoring mask.

    ``label[t] = 1`` iff the next request of keys[t] is at a position in (t, t+H].
    ``censored[t]`` is True when no later request exists *and* the horizon runs
    past the end of the trace — the true label is unknown, so such rows must be
    excluded from training and evaluation.
    """
    if horizon < 1:
        raise ValueError("horizon must be >= 1")
    n = len(keys)
    nxt = np.full(n, -1, dtype=np.int64)
    last: dict[int, int] = {}
    k_list = keys.tolist()
    for t in range(n - 1, -1, -1):
        k = k_list[t]
        nxt[t] = last.get(k, -1)
        last[k] = t
    pos = np.arange(n, dtype=np.int64)
    has_next = nxt >= 0
    label = (has_next & (nxt - pos <= horizon)).astype(np.int8)
    censored = (~has_next) & (pos + horizon > n - 1)
    return label, censored
