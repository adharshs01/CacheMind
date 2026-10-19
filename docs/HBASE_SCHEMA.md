# HBase schema (operational store)

HBase 2.6.7 runs in standalone mode; all access goes through the HBase REST gateway
(`cachemind.bigdata.hbase_store.HBaseRestClient`). Values are stored as UTF-8 text so
they can be read with `hbase shell` or curl. HBase serves **operational** access (point
reads/writes, small prefix scans); historical analytics read Parquet.

## Tables

### `cache_requests` — request lookup and audit

| Family | Qualifiers |
|---|---|
| `request` | event_id, item_id, user_id, event_time_ms, seq |
| `metadata` | category, region, item_size_bytes, user_group |
| `lineage` | dataset_id, source, source_file, source_line, topic, partition, offset |

Row key: `sha1(event_id)[0:4] + ":" + event_id`

* Access pattern: "show me request X" (audit, lineage checks) → single GET by event id.
* The 4-hex salt is derived from the key itself, so a lookup needs no index, and keys
  are spread uniformly across regions instead of clustering (no monotonic hot-spotting).
* Writing the same event again writes the same row → idempotent under replays.

### `item_statistics` — item popularity (wide rows)

| Family | Qualifiers |
|---|---|
| `access` | total_requests, recent_requests, last_access_ms, dataset_id |
| `popularity` | popularity_rank, popularity_score, trend_score, window_start_ms, window_end_ms |
| `windows` | one column per event-time window: qualifier = 13-digit zero-padded epoch ms of the window start, value = request count; **TTL 30 days** |

Row key: `sha1(dataset_id#item_id)[0:2] + ":" + dataset_id + "#" + item_id`

* Access pattern: "current statistics of item X in dataset D" plus its recent per-minute
  history → one GET returns both (a classic wide row; old window columns expire through
  the TTL).
* Item ids are only unique *within* a dataset (every synthetic dataset reuses `i0000001…`).
  An earlier version keyed rows by `item_id` alone; publishing a second dataset silently
  overwrote the first one's statistics. This was found while verifying HBase after the
  pipeline runs and fixed by adding the dataset to the key (the streaming window
  aggregation now groups by `(window, dataset_id, item_id)` as well). A test asserts that
  two datasets' rows for the same item id stay separate.
* Window qualifiers sort chronologically; a non-millisecond value is rejected
  (`window_column` validates the range).
* All values are **absolute** (set, never incremented), so a replayed micro-batch cannot
  double count.

### `cache_experiments` — experiment summaries

| Family | Qualifiers |
|---|---|
| `configuration` | run_id, experiment, policy, cache_capacity, dataset_version, workload, seed, horizon |
| `performance` | hits, misses, hit_rate, miss_rate, evictions, wall_time_s |
| `metadata` | model_version, execution_timestamp, code_version |

Row key: `experiment#workload#policy#capacity(8 digits)#seed(10 digits)`

* Access patterns: all rows of an experiment (`prefix experiment#`), one policy's capacity
  curve (`prefix experiment#workload#policy#`) returned in capacity order thanks to
  fixed-width numbers.
* Components are validated against `^[a-z0-9_.-]{1,64}$`, so `#` cannot appear inside a
  component and keys are unambiguous (no two tuples map to one key).

## Commands

```bash
make infra-up        # starts HBase + REST and creates the tables (idempotent)
make hbase-init      # create tables only
make hbase-publish DATASET=<id>   # Gold item stats + experiment results (absolute puts)
make hbase-verify    # row counts, column families, sample rows
.tools/hbase-2.6.7-hadoop3/bin/hbase --config .runtime/hbase/conf shell   # inspect manually
```

Operations covered by `tests/integration/test_hbase.py` against the live service: table
creation with column families, insert, read, update of a single cell, batch write, full and
prefix scans with column projection and limits, idempotent repeated writes, absolute window
counts, unambiguous keys, and a clear error for a missing table.
