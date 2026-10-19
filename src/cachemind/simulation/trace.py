"""Ordered request traces.

Replay order
------------
A trace is ordered by the key ``(seq, event_id)``. ``seq`` is assigned by the
producer of a dataset and is unique within it (Silver quarantines sequence
conflicts), so ``event_id`` only acts as a deterministic tie-breaker.

Kafka does not provide a global order across partitions, so arrival order at a
consumer is NOT used. Events always carry the producer-assigned ``seq``; that
is the documented replay order. Imported traces without a sequence number get
one assigned from (event_time_ms, source file, line number) at import time.

All policies in an experiment receive the identical ``keys`` array.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np

from cachemind.contracts import ID_RE, UNKNOWN_CATEGORY


class MalformedTraceError(ValueError):
    pass


@dataclass
class Trace:
    dataset_id: str
    keys: np.ndarray  # int64 dense codes, the only thing policies see
    item_ids: np.ndarray  # object: code -> original item id
    seq: np.ndarray
    event_time_ms: np.ndarray
    size_bytes: np.ndarray
    region: np.ndarray  # object, per request
    category: np.ndarray  # object, per request
    source: str = "unknown"
    meta: dict[str, Any] = field(default_factory=dict)

    def __len__(self) -> int:
        return len(self.keys)

    @property
    def n_distinct(self) -> int:
        return len(self.item_ids)

    def describe(self) -> dict[str, Any]:
        return {
            "dataset_id": self.dataset_id,
            "source": self.source,
            "n_requests": len(self),
            "n_distinct_items": self.n_distinct,
            "seq_min": int(self.seq.min()) if len(self) else None,
            "seq_max": int(self.seq.max()) if len(self) else None,
            **self.meta,
        }

    def slice(self, lo: int, hi: int) -> Trace:
        """Positions [lo, hi) as a new trace (codes re-densified)."""
        return Trace.from_columns(
            item_id=self.item_ids[self.keys[lo:hi]],
            seq=self.seq[lo:hi],
            event_time_ms=self.event_time_ms[lo:hi],
            size_bytes=self.size_bytes[lo:hi],
            region=self.region[lo:hi],
            category=self.category[lo:hi],
            dataset_id=self.dataset_id,
            source=f"{self.source}[{lo}:{hi}]",
            presorted=True,
        )

    @classmethod
    def from_items(cls, items: list[Any], dataset_id: str = "adhoc") -> Trace:
        """Minimal trace from a list of item ids (tests, dashboard what-if runs)."""
        n = len(items)
        return cls.from_columns(
            item_id=np.asarray(items, dtype=object),
            seq=np.arange(n, dtype=np.int64),
            event_time_ms=np.zeros(n, dtype=np.int64),
            size_bytes=np.ones(n, dtype=np.int64),
            region=np.full(n, "us-east", dtype=object),
            category=np.full(n, UNKNOWN_CATEGORY, dtype=object),
            dataset_id=dataset_id,
            source="adhoc",
            presorted=True,
        )

    @classmethod
    def from_columns(
        cls,
        *,
        item_id: np.ndarray,
        seq: np.ndarray,
        event_time_ms: np.ndarray,
        size_bytes: np.ndarray,
        region: np.ndarray,
        category: np.ndarray,
        dataset_id: str,
        source: str,
        event_id: np.ndarray | None = None,
        presorted: bool = False,
    ) -> Trace:
        item_id = np.asarray(item_id, dtype=object)
        n = len(item_id)
        for name, arr in (
            ("seq", seq),
            ("event_time_ms", event_time_ms),
            ("size_bytes", size_bytes),
            ("region", region),
            ("category", category),
        ):
            if len(arr) != n:
                raise MalformedTraceError(f"column {name} has {len(arr)} rows, expected {n}")
        for pos, v in enumerate(item_id):
            if not isinstance(v, str) or not ID_RE.match(v):
                raise MalformedTraceError(f"malformed item id {v!r} at position {pos}")
        seq = np.asarray(seq, dtype=np.int64)
        if not presorted:
            tiebreak = np.asarray(event_id, dtype=object) if event_id is not None else np.zeros(n)
            order = np.lexsort((tiebreak.astype(str), seq))
            item_id, seq = item_id[order], seq[order]
            event_time_ms = np.asarray(event_time_ms)[order]
            size_bytes = np.asarray(size_bytes)[order]
            region, category = np.asarray(region)[order], np.asarray(category)[order]
        uniques, codes = np.unique(item_id, return_inverse=True)
        return cls(
            dataset_id=dataset_id,
            keys=codes.astype(np.int64),
            item_ids=uniques,
            seq=seq,
            event_time_ms=np.asarray(event_time_ms, dtype=np.int64),
            size_bytes=np.asarray(size_bytes, dtype=np.int64),
            region=np.asarray(region, dtype=object),
            category=np.asarray(category, dtype=object),
            source=source,
        )

    @classmethod
    def from_generator(cls, config: Any) -> Trace:
        """Clean synthetic trace straight from the generator (lightweight profile)."""
        from cachemind.data.generator import iter_clean_events
        from cachemind.data.workloads import WorkloadGenerator

        gen = WorkloadGenerator(config)
        items, seqs, times, sizes, regions, cats = [], [], [], [], [], []
        for ev in iter_clean_events(config, gen):
            items.append(ev.item_id)
            seqs.append(ev.seq)
            times.append(ev.event_time_ms)
            sizes.append(ev.item_size_bytes)
            regions.append(ev.region)
            cats.append(gen.item_category_and_size(int(ev.item_id[1:]))[0])
        trace = cls.from_columns(
            item_id=np.asarray(items, dtype=object),
            seq=np.asarray(seqs),
            event_time_ms=np.asarray(times),
            size_bytes=np.asarray(sizes),
            region=np.asarray(regions, dtype=object),
            category=np.asarray(cats, dtype=object),
            dataset_id=config.resolved_dataset_id,
            source="synthetic-generator",
            presorted=True,
        )
        trace.meta["synthetic"] = True
        return trace

    @classmethod
    def from_silver(cls, silver_path: Path, dataset_id: str) -> Trace:
        """Load the canonical Silver dataset (Parquet) for one dataset_id.

        Only the needed columns are read (column pruning) and the dataset_id
        filter is pushed down to the Parquet reader.
        """
        import pyarrow.dataset as pads

        ds = pads.dataset(str(silver_path), format="parquet", partitioning="hive")
        cols = [
            "event_id",
            "seq",
            "item_id",
            "event_time_ms",
            "item_size_bytes",
            "region",
            "category",
        ]
        table = ds.to_table(columns=cols, filter=pads.field("dataset_id") == dataset_id)
        if table.num_rows == 0:
            raise MalformedTraceError(f"no Silver rows for dataset_id={dataset_id!r}")
        df = table.to_pandas()
        trace = cls.from_columns(
            item_id=df["item_id"].to_numpy(dtype=object),
            seq=df["seq"].to_numpy(),
            event_time_ms=df["event_time_ms"].to_numpy(),
            size_bytes=df["item_size_bytes"].to_numpy(),
            region=df["region"].to_numpy(dtype=object),
            category=df["category"].fillna(UNKNOWN_CATEGORY).to_numpy(dtype=object),
            event_id=df["event_id"].to_numpy(dtype=object),
            dataset_id=dataset_id,
            source=f"silver:{silver_path}",
        )
        if len(np.unique(trace.seq)) != len(trace):
            raise MalformedTraceError("Silver contains duplicate seq values for this dataset")
        return trace
