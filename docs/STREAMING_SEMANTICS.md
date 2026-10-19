# Streaming delivery semantics and recovery

This document states precisely what each hop guarantees. "Exactly-once" is claimed only
where Spark/Kafka document it *and* our tests verified it.

## Hop by hop

| Hop | Mechanism | Guarantee | Verified by |
|---|---|---|---|
| Producer → Kafka | `enable.idempotence=true`, `acks=all`, bounded queue, delivery callback per message | No broker-side duplicates from librdkafka retries **within one producer session**; every message is acknowledged or counted as failed (non-zero exit). Re-running a producer re-sends messages → **logical duplicates** with identical `event_id` | `test_kafka.py` (delivery reports, duplicates keep their event id, producer restart) |
| Kafka retention | `retention.ms` = 7 days, `cleanup.policy=delete` | Kafka is **not** permanent storage; data older than retention is deleted | config |
| Kafka → Bronze (Q1) | Kafka source with checkpointed offsets + Parquet **file sink** (`_spark_metadata` commit log) | Exactly-once *for readers that read through the file-sink log* (Spark's documented guarantee for a replayable source + file sink). Each `(partition, offset)` appears once. Files of an uncommitted batch may exist on disk but are not listed in the log | Exp. 12 S2 (SIGKILL), S3 (broker restart): `bronze rows == distinct (partition, offset) == topic messages` |
| Retention vs. durability | `failOnDataLoss=true` | If offsets expire before Q1 reads them, the query **fails** rather than silently skipping data. Once in Bronze, data survives Kafka retention (Bronze is the replay source). Bronze is local disk: it is lost if the host's disk is lost — it is not "permanently durable" | config |
| Bronze → provisional Silver (Q2) | Bronze file source (reads committed files only) + `foreachBatch` | foreachBatch may run a micro-batch more than once (at-least-once execution). Outputs are idempotent: Parquet written to `batch_id=<n>` (replaced on replay), HBase rows keyed deterministically. Duplicates are de-duplicated **within** a micro-batch; duplicates in different micro-batches can both appear in the provisional streaming Silver (reported as `cross_batch_duplicate_rows`) | `test_streaming_unit.py` (replay), Exp. 12 S1 |
| Q2 → HBase `cache_requests` | REST batch put, row key = salted `event_id` | Effectively-once *state*: re-writing an event writes the same cell values to the same row; cross-batch duplicates collapse to one row | `test_hbase.py::test_repeated_writes_are_idempotent`, Exp. 12 |
| Bronze → windows (Q3) | watermark 2 min, `dropDuplicatesWithinWatermark(event_id)`, 1-minute tumbling windows, update mode, foreachBatch | Each emitted window count is the current total; HBase stores **absolute** counts per `(dataset, item, window)` — replays overwrite, never add. Events later than the watermark may be dropped from windows (counted by Spark). The watermark is global per namespace (see below) | `test_hbase.py::test_window_counts_are_absolute_not_incremented`; exact counts vs independent recount: `test_streaming_unit.py` (window tests), `test_streaming_kafka.py` |
| Bronze → canonical Silver (batch) | `build_silver` over all Bronze | Authoritative: global duplicate policy (exact / conflicting / sequence conflicts), metadata enrichment, verification against ground truth | `test_spark_pipeline.py`, pipeline demo |

## Partial-failure states (no distributed transaction exists)

Q2 writes three things for micro-batch *n*: Silver Parquet, quarantine Parquet, HBase
rows — then a manifest file **last**. There is no atomic commit across Parquet and HBase.

| Crash point | State on disk | Recovery |
|---|---|---|
| before Parquet | nothing for *n* | Spark replays *n* (not committed in the checkpoint) |
| after Parquet, before HBase | Parquet present, HBase missing, no manifest | replay rewrites Parquet (same dir) and writes HBase |
| after HBase, before manifest | both present, no manifest | replay rewrites both (idempotent) |
| after manifest, before Spark commit | everything present | replay rewrites identical outputs |
| Spark commit recorded but outputs missing (e.g. manual deletion) | committed batch without manifest | detected by `reconcile`; repaired with `reconcile --repair` (re-publish HBase from Parquet) |

`cachemind stream reconcile` compares Spark's `commits/` with manifests and output
directories and spot-checks HBase for event ids present in Parquet. `--repair`
re-publishes requests and window counts from the Parquet copies (idempotent puts).
Exp. 12 S5 injects a failure exactly between the Parquet write and HBase publication
(`CACHEMIND_FAULT_SILVER_BATCH`, test-only) and verifies the replay.

## Changing a stateful query

The window aggregation keeps state in its checkpoint, keyed by its grouping columns. Spark
cannot restart such a query after the grouping changes. When the key changed from
`(window, item_id)` to `(window, dataset_id, item_id)`, the query was renamed
`gold_windows_v2` with its own checkpoint and output directory; the new query replays from
the beginning of Bronze (Bronze is retained), and the v1 checkpoint and outputs were left
untouched. Checkpoints are never deleted to "fix" a failure.

## Late data and watermarks

See `DATA_CONTRACTS.md`. The provisional Silver flags late events relative to the
maximum event time of *earlier* micro-batches; that reference is stored per batch in
`checkpoints/silver_stream_state/` so a replay classifies the batch identically.

### One checkpoint namespace = one event-time timeline

A *namespace* is a Kafka topic plus its checkpoints, stream outputs and
`silver_stream_state`. Spark's watermark is global per query (the windows query keeps one
watermark for all datasets), and the Silver lateness reference follows the same rule.
Every synthetic dataset starts on **2026-01-05 00:00 UTC**, so two datasets streamed into
the same namespace are two overlapping timelines: whichever is streamed second and lies
behind the first one's maximum event time is late **in its entirety**, and the windows
query drops all of it (counted as `numRowsDroppedByWatermark`). This is Spark's documented
watermark semantics, not data loss in Bronze or Silver: those rows are in Bronze, in the
provisional Silver (flagged `is_late`), in HBase `cache_requests` and in the batch Silver.

Consequences (verified, see `FINAL_CORRECTNESS_AUDIT.md`):

* Independent runs must use their own namespace. The pipeline demo, the e2e test, Exp. 11
  and Exp. 12 each use a private data directory, topic and streaming status files
  (`CACHEMIND_DATA_DIR`, `CACHEMIND_KAFKA_TOPIC`, `CACHEMIND_STREAM_RUNTIME_DIR`). Before
  the audit, `make pipeline-demo` streamed into the shared namespace after the 100 k
  project dataset (whose stream portion ends 2026-01-06 07:27), so three of four demo
  datasets had 100 % of their events flagged late and 0 window rows.
* Re-sending a dataset that the namespace has already processed (e.g. running
  `make kafka-producer` twice) produces logical duplicates that are behind the watermark:
  the observed 19 912 of 20 000 late rows are exactly the replayed events more than
  2 minutes behind the dataset's own maximum; the other 88 are within the delay and are
  removed by `dropDuplicatesWithinWatermark`. Window counts are therefore not
  double-counted; zero window output is the correct result for a pure replay.
* `cachemind stream run` prints an `event_time` summary for the run (late rows per
  dataset, window input rows, rows dropped by the watermark), so an empty window output is
  never silent.

### Bronze is written as one file per micro-batch

The downstream queries read Bronze with `maxFilesPerTrigger`. When Bronze wrote one file
per Kafka partition, a downstream micro-batch could take one partition's file of Bronze
batch *n+1* before the other partitions' files of the same batch; the remaining on-time
events then looked more than 2 minutes behind the batch maximum. Measured on real Kafka
with 3 000 in-order events at 1 event/s (`max_offsets=500`, `max_files=2`): **1 604**
on-time events flagged late and **42** missing from the windows. Bronze now coalesces each
micro-batch into a single file (`ingest_ts` is the batch timestamp, so one `ingest_date`
partition and one file), so downstream micro-batches always consist of whole Bronze
batches; the same input gives 37 late flags — the documented rule applied to genuine
cross-partition progress at Bronze-batch boundaries, recomputed independently in
`tests/integration/test_streaming_kafka.py` — and 3 000 / 3 000 events in the windows.
Files written before the change keep their per-partition layout (nothing is rewritten).

### Late-event metrics

* `numRowsDroppedByWatermark` is reported per stateful operator. The windows query has
  two (aggregation listed first, `dedupeWithinWatermark` second) and late rows are dropped
  by the dedupe operator, so `progress.jsonl` now records the **sum over all operators**
  plus the per-operator values. The previous recorder read only the first operator and
  always reported 0.
* Spark judges late rows against the watermark of the *previous* micro-batch (Spark ≥ 3.5,
  multiple stateful operators), the Silver flag against the maximum of all earlier
  micro-batches. A row can therefore be flagged late in Silver and still be counted in a
  window; the converse does not happen.
* In `available-now` mode a whole backlog may fit into one micro-batch; nothing in a
  single micro-batch is "late" by definition (the pipeline demo's injected late events are
  usually in the same micro-batch as their neighbours). Exp. 12 S1 is the scenario that
  exercises late data deliberately.

### Shutdown order

`StateStore` is a JVM-wide singleton whose maintenance thread outlives
`SparkSession.stop()`. When another SparkContext started in the same JVM (the pipeline
demo runs batch stages after streaming), the thread kept the finished windows query's
providers and logged `Error managing HDFSStateStoreProvider[... gold_windows_v2/state ...]
... RpcEndpointNotFoundException: Cannot find endpoint: spark://StateStoreCoordinator`
every maintenance interval (or `SparkEnv not active` if it fired during the stop). The
committed state was unaffected (restarting on the same checkpoint works). The job now
stops queries, then the state store, then the session (`shutdown_streaming`).

### Status and health files

`status.json` / `progress.jsonl` live in `CACHEMIND_STREAM_RUNTIME_DIR` (default
`.runtime/streaming`). Records carry the streaming `run_id`; the health check reports the
current run's failed queries separately from the history of all runs, grouped by error
class. The 51 historical failures in the shared file are Exp. 12 failure injections
(45 `HBaseUnavailable`, 5 injected `RuntimeError`) and the documented v1→v2 window-schema
change (1 `StateStoreKeySchemaNotCompatible`); they were written there before experiment
runtimes were isolated and are kept as history.

## Retry and restart behaviour

* HBase REST client: bounded retries with exponential back-off (0.5, 1, 2, 4 s) on
  connection errors and 5xx; 4xx raises immediately with the response body.
* A failed streaming query is restarted from its checkpoint by the job supervisor with
  back-off 1, 2, 4, 8, 16 s (max 5 restarts, then the job exits with the last error).
* SIGINT/SIGTERM stop queries gracefully; SIGKILL is safe because of the checkpoints.
* Checkpoints are never deleted automatically and are not recreated per run.

## What is NOT claimed

* End-to-end exactly-once from producer to HBase: not claimed. Producer restarts create
  logical duplicates (removed by Silver/HBase keys, not by Kafka).
* The provisional streaming Silver is not duplicate-free across micro-batches.
* Conflicting duplicates arriving in different micro-batches are only detected by the
  batch Silver rebuild.
* Durability of local files against disk/host failure.
