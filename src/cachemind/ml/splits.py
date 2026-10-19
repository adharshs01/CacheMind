"""Chronological train/validation/test splits with label-horizon purging.

The trace is cut by position into contiguous blocks::

    [0 ............ a)[a .......... b)[b ............ n)
          train            validation         test

A sample at position t has a label that looks at positions (t, t+H]. To make
sure no label peeks into a later block (and to avoid the end-of-block bias where
positives are observable but negatives are not), a sample is *eligible* only if
its whole label window lies inside its own block::

    train: t in [0, a - H)    val: t in [a, b - H)    test: t in [b, n - H)

Features use history from earlier blocks; that is legitimate (it was known at
prediction time). Preprocessing (scalers, encoders) must be fitted on the train
block only — enforced by the training code, tested in tests/unit/test_leakage.py.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass

import numpy as np


@dataclass(frozen=True)
class SplitBoundaries:
    n: int
    horizon: int
    train_start: int
    train_end: int  # exclusive, block boundary
    val_start: int
    val_end: int
    test_start: int
    test_end: int

    def eligible_range(self, split: str) -> tuple[int, int]:
        """[lo, hi) positions whose label window stays inside the block."""
        blocks = {
            "train": (self.train_start, self.train_end),
            "val": (self.val_start, self.val_end),
            "test": (self.test_start, self.test_end),
        }
        if split not in blocks:
            raise ValueError(f"unknown split {split!r}")
        lo, hi = blocks[split]
        return lo, max(lo, hi - self.horizon)

    def mask(self, split: str) -> np.ndarray:
        lo, hi = self.eligible_range(split)
        m = np.zeros(self.n, dtype=bool)
        m[lo:hi] = True
        return m

    def to_dict(self) -> dict[str, int]:
        return asdict(self)


def chronological_split(
    n: int, horizon: int, fractions: tuple[float, float, float] = (0.6, 0.2, 0.2)
) -> SplitBoundaries:
    if abs(sum(fractions) - 1.0) > 1e-9 or min(fractions) <= 0:
        raise ValueError("fractions must be positive and sum to 1")
    if horizon < 1:
        raise ValueError("horizon must be >= 1")
    a = int(n * fractions[0])
    b = a + int(n * fractions[1])
    bounds = SplitBoundaries(n, horizon, 0, a, a, b, b, n)
    for split in ("train", "val", "test"):
        lo, hi = bounds.eligible_range(split)
        if hi - lo <= 0:
            raise ValueError(
                f"split {split!r} has no eligible samples (n={n}, horizon={horizon}); "
                "use a longer trace or a shorter horizon"
            )
    return bounds


def assert_no_label_leakage(bounds: SplitBoundaries, positions: np.ndarray, split: str) -> None:
    """Raise if any sample's label window crosses the end of its block."""
    lo, hi = bounds.eligible_range(split)
    bad = (positions < lo) | (positions >= hi)
    if bad.any():
        raise AssertionError(
            f"{int(bad.sum())} {split} samples fall outside eligible range [{lo}, {hi}) "
            "- their labels would look into a later block"
        )
