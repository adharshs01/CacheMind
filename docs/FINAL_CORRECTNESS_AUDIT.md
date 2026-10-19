# CacheMind — final correctness, reliability and end-to-end verification audit

Audit date: 2026-10-19/20 (UTC). Machine: Apple M3, 8 GB RAM, macOS 26.7.1, Python 3.12.4,
Temurin JDK 17 (project-local), PySpark 4.0.4, Kafka 4.1.2 (KRaft), HBase 2.6.7, Streamlit
1.50.0. Every statement below is backed by an executed test, a stored artifact, an
inspected implementation or a reproduction whose script and output are cited. Data is
synthetic throughout.

---

## 1. Executive summary

**Verdict: READY WITH LIMITATIONS** for demonstration (details in §17).

Six genuine defects were found, reproduced, fixed and covered by regression tests. None
of them affected the canonical batch pipeline (Bronze/Silver/Gold), the ML training and
evaluation protocol, or the cache-policy results; they affected the streaming speed layer,
its metrics, run isolation, shutdown hygiene and cross-runtime model parity.

| # | Defect | Class | Effect before the fix |
|---|---|---|---|
| D1 | `make pipeline-demo` streamed into the **shared** streaming namespace although every synthetic dataset starts on 2026-01-05 and a namespace's watermark is global | bug (isolation) | 3 of 4 demo datasets: 100 % flagged late, **0** streaming-window rows |
| D2 | Bronze wrote one file per Kafka partition; downstream `maxFilesPerTrigger` could split a Bronze micro-batch | bug (pipeline-induced reordering) | on-time events flagged late (1 381 of 1 494 in one demo batch; 1 604 of 3 000 in a live reproduction) and **42 of 3 000 on-time events missing from the windows** |
| D3 | `dropped_by_watermark` read only the first stateful operator | bug (metric) | always 0, although Spark dropped rows in the dedupe operator — zero window output looked "silent" |
| D4 | Spark `StateStore` (JVM singleton) outlived `SparkSession.stop()` | lifecycle bug (shutdown order) | repeated `RpcEndpointNotFoundException … StateStoreCoordinator` on `gold_windows_v2/state`; no data effect |
| D5 | Streaming health counted errors of **all** runs (incl. failure injections) in the last 500 log lines | bug (metric/UI) | `terminated_with_error: 36` presented as health; isolated experiments wrote into the main job's files |
| D6 | Spark computed `log1p/sin/cos` features with the JVM, the online path with the C library (1-ulp differences) | bug (cross-runtime parity) | exported RF on online features ≠ Spark batch predictions on 0.66 % of rows (max Δp 0.023); 3 different evictions in 57 457 |

Investigated and found **correct** (no change): the late-flag computation itself (all 56 000
historical rows match the documented policy), the 99 629-row Silver accounting, every cache
policy, Belady optimality, the absence of future access in online/learned policies, the
absence of temporal leakage (no Gold statistic is a model feature), chronological splits,
validation-only selection, held-out test metrics, and the `WindowExec` global ordering.

---

## 2. Issues investigated

A — late events / zero streaming-window output; B — `RpcEndpointNotFoundException` /
`StateStoreCoordinator`; C — `terminated_with_error: 36`; D — `WindowExec` and
`use_container_width` warnings; plus independent audits of cache replacement (§3 of the
brief), ML correctness (§4) and Big Data correctness (§5), including the 99 629 vs 80 135
row question.

Reproduction scripts (kept outside the repository, in the session scratchpad; the
regression tests in §8 encode the same cases permanently): `repro_windows.py`,
`repro_statestore.py`, `stream_flow.py`, `verify_silver_independent.py`,
`verify_ml_independent.py`, `audit_policies.py`.

---

## 3. Issue A — late events and zero window output

### 3.1 Facts established from first principles

| Question | Finding (evidence) |
|---|---|
| How are event timestamps generated? | `WorkloadGenerator.blocks()`: a clock starting at `DEFAULT_START = 2026-01-05T00:00Z` for **every** dataset, exponential inter-arrival gaps, diurnally modulated rate `rate·(1+0.6·sin(2π(h−9)/24))`. `small` (1 ev/s): 100 000 events span 2026-01-05 00:00 → 01-06 07:27; the stream portion (last 20 %) is 01-05 21:27 → 01-06 07:27. Pipeline demo (20 000 events, 1 ev/s): stream portion 01-05 08:20 → 09:27. |
| What does event time mean? | The synthetic time of a request in its dataset's own timeline — not wall-clock time (only `kafka-producer-live` stamps "now"). Datasets are independent, overlapping timelines. |
| Kafka timestamps and order | The producer sends `stream_events.jsonl` in `seq` (= event-time) order, key `item_id`, 3 partitions; order exists only per partition. Injected "late" events are delayed by *N* messages. |
| Checkpoints / offsets | Spark tracks Kafka offsets in `checkpoints/bronze_ingest/offsets` (consumer-group offsets are not used); `startingOffsets=earliest` applies only to a new checkpoint. |
| Watermark | Silver: reference = max event time of all *earlier* micro-batches, stored per batch in `checkpoints/silver_stream_state/`; late iff `t < ref − 2 min`. Windows: `withWatermark(event_time, 2 min)` — eviction watermark of batch *N* = max event time before *N* − 2 min; late rows are filtered against the previous batch's watermark and dropped by the first stateful operator that sees them (`dedupeWithinWatermark`). Both are global per query/namespace. |
| Does checkpoint state carry over? | Yes: the windows watermark (offset log) and `silver_stream_state` persist across runs and datasets of a namespace. |
| Do unrelated datasets share state? | **Yes, in the shared namespace** (D1). Stored manifests map the 14 historical Silver batches to: demo 932e (b0–2), project dataset (b3), demo 95f1 (b4–6), demo a030 (b7–9), project dataset **re-sent** (b10, same 20 000 event ids as b3), demo 3871 (b11–13). The e2e test and Exp. 12 were already isolated. |
| Are late events identified per the documented policy? | **Yes.** Recomputing the policy from the stored outputs reproduces all 56 000 historical flags (0 mismatches) and all 28 stored state values. |
| Are windows silently suppressed? | Rows behind the watermark were dropped (Spark counted them on `dedupeWithinWatermark`, e.g. 18/18 in the last batch) while the project's metric showed 0 (D3). |
| Do windows reach HBase correctly? | Yes: in the live test, HBase `windows:<start>` cells equal the latest Parquet counts for every sampled item. (Manifest `hbase_rows` counts HBase *rows* = items, not cells.) |
| Duplicates / inconsistent aggregations on repeated runs? | The provisional streaming Silver holds the re-sent 20 000 events twice (documented as provisional); HBase rows are idempotent; window counts for the project dataset are 17 836 / 17 836 exact with **0 overcounted** after the re-send. |
| Are the late metrics correct? | Silver flags: yes. Spark drop metric as recorded: no (D3). Health error count: mixed history (D5). |
| Role of `availableNow` | It does not suppress update-mode output. It processes a backlog in bounded micro-batches; replaying hours of synthetic time in seconds is what made Bronze-batch splitting (D2) visible, and a backlog that fits in one micro-batch shows no lateness at all. |
| Is zero output correct? | For the **re-send of the same dataset**: yes — 19 912 of 20 000 replayed events are > 2 min behind the dataset's own maximum (the other 88 are within the delay and removed as duplicates); emitting nothing avoids double counting. For **independent demo datasets** in the shared namespace: correct under Spark semantics but the result of a design defect (D1). |

The 33 294 historical late flags decompose exactly as 1 382 (Bronze split, demo 932e) +
12 000 (three demo datasets behind the shared watermark) + 19 912 (re-send of the project
dataset).

### 3.2 Reproductions (expected vs actual, Spark only, `repro_windows.py`)

Hand-built Bronze files with known timestamps, real query builders, `availableNow`:

| Case | Expected | Actual |
|---|---|---|
| 720 on-time events, legacy layout (3 files per Bronze batch), `maxFilesPerTrigger=4` | 0 late | **107 late** in Silver; windows exact |
| same, one file per Bronze batch | 0 late | 0 late; 132/132 windows exact |
| dsA (later timeline) then dsB (earlier timeline), one namespace | dsB exact (if timelines were independent) | dsB 600/600 late, **0** window events, 600 dropped on `dedupeWithinWatermark`, 0 on `stateStoreSave` |
| dsA and dsB in separate namespaces | both exact | both exact, 0 late |
| one event delivered after 599 s were seen, event time 100 s | it alone late and dropped | exactly it flagged and dropped (metric 1); all 110 windows exact |

Live Kafka, same input for old and new code (`stream_flow.py`: 3 000 in-order events at
1 ev/s, `max_offsets=500`, `max_files=2`, isolated topic and data dir):

| | old code | new code |
|---|---|---|
| Bronze files per commit | 3 | **1** |
| Bronze batches split across Silver batches | 7 of 7 | **0** |
| on-time events flagged late | **1 604** | 37 (genuine cross-partition progress; equals an independent recomputation) |
| events in windows | **2 958 / 3 000** | **3 000 / 3 000** |

### 3.3 Root causes and fixes

* **D1 (isolation).** `pipeline_demo.run_pipeline` now runs inside its own namespace:
  `<data>/pipeline_runs/<run_id>/` (raw, lake, checkpoints, `_stream_runtime/`) and topic
  `cachemind.demo.<run_id>`; evidence, models and HBase rows still go to the shared
  places. Three new evidence checks recount the streaming windows from canonical Silver
  (`stream_windows_never_overcount`, `stream_windows_count_every_on_time_event`) and bound
  the late flags by the injected late events. The rule "one namespace = one timeline" is
  documented in STREAMING_SEMANTICS.md, DATA_CONTRACTS.md and the module docstring. The
  definition of "late" was deliberately **not** changed to per-dataset: Spark's watermark
  cannot be per key, and a per-dataset Silver flag would disagree with the windows.
* **D2 (Bronze layout).** `streaming.bronze_projection` coalesces each micro-batch to one
  partition (the batch timestamp makes `ingest_date` constant), so each Bronze commit is
  one file and every downstream micro-batch consists of whole Bronze batches;
  `max_offsets` is capped at `spark.sql.files.maxRecordsPerFile`.
* **D3 (metric).** `streaming.progress_record` sums `numRowsDroppedByWatermark` and state
  rows over all operators and records the per-operator values; each run also reports an
  `event_time` summary (late rows per dataset, window input rows, rows dropped).

---

## 4. Issue B — `RpcEndpointNotFoundException` / `StateStoreCoordinator`

* **Reproduction** (`repro_statestore.py`): stateful windows query → `spark.stop()` → new
  SparkSession in the same JVM, maintenance interval 15 s. Plain: `WARN StateStore: Error
  managing HDFSStateStoreProvider[id=(op=0,part=1), dir=…/checkpoints/gold_windows_v2/state/0/1]
  … Caused by: RpcEndpointNotFoundException: Cannot find endpoint:
  spark://StateStoreCoordinator@127.0.0.1:…` (8 lines); `StateStore.isMaintenanceRunning()`
  still true inside the new context. With a 3 s interval the same race shows as
  `IllegalStateException: SparkEnv not active, cannot do maintenance on StateStores`.
* **Root cause**: `StateStore` is a JVM-wide singleton; its maintenance thread and loaded
  providers outlive `SparkSession.stop()`. The pipeline demo (and the Spark test modules)
  start a new SparkContext in the same JVM, which has no `StateStoreCoordinator` endpoint.
* **Not** a data-processing failure, state corruption or resource exhaustion: restarting
  the query on the same checkpoint succeeds and reloads the state (reproduction and
  `test_stop_state_store_ends_maintenance_and_state_stays_usable`).
* **Fix**: `shutdown_streaming()` stops queries, then `StateStore.stop()`, then the session;
  `stop_state_store()` refuses while a query is active. No exception is suppressed.
* **After**: 0 occurrences in the reproduction, the Spark test suite, the live streaming
  test (asserted) and the full `make pipeline-demo` log (0 `RpcEndpointNotFound`, 0
  `WARN StateStore`).

---

## 5. Issue C — `terminated_with_error: 36`

* **What it counted**: query terminations with an exception among the **last 500 lines**
  of the shared `.runtime/streaming/progress.jsonl`. The old code reproduces exactly 36
  on the stored file.
* **What they are** (all 51 in the file, classified by the new `error_class`):
  45 `HBaseUnavailable` (Exp. 12 S4, HBase stopped on purpose), 5 `RuntimeError: injected
  fault after Parquet write` (Exp. 12 S5), 1 `StateStoreKeySchemaNotCompatible` (the
  documented v1→v2 window-key migration). None is a failure of the current run (run
  `stream-20261019T094952-4e0c77`: 0).
* **Why they were there**: `runtime_dir` was not overridable, so isolated experiments
  wrote status and progress into the main job's files.
* **Fix**: `CACHEMIND_STREAM_RUNTIME_DIR` (used by Exp. 11/12, the e2e test and the demo);
  progress records carry `run_id`; `streaming_health()` returns `current_run`
  (failed queries of the run in `status.json`) and `history` (all runs, by error class);
  the dashboard labels the history "HISTORICAL — not current health". No record was
  deleted.

---

## 6. Issue D — warnings

* **`WindowExec: No Partition Defined`** — sources: `features_spark.with_trace_positions`
  (global `row_number` over `(seq, event_id)`), three item rankings in Gold (≈ 5 k rows) and
  SQL demos 05/07 (tiny). The global position is **logically required**: features and
  labels are defined on positions in the canonical order. A scalable alternative exists
  (`sortBy` + `zipWithIndex`, range-partitioned; `monotonically_increasing_id` would be
  wrong because it is not dense). Measured cost of the current form: features stage 4.7 s
  at 1 M rows vs 72 s for Silver (Exp. 10). **Classified harmless at project scale; not
  changed** (no correctness benefit, risk to a verified path). Revisit above ~5 M rows.
* **`use_container_width`** (Streamlit 1.50): replaced by `width="stretch"` at both call
  sites; `st.dataframe`'s default is already `"stretch"`, so rendering is unchanged.
  Pages render headlessly with no deprecation warning and no exception.

---

## 7. ML audit

| Check | Result (evidence: `verify_ml_independent.py`, tests) |
|---|---|
| Prediction target | `label[t] = 1` iff the next request of the item is in `(t, t+H]`, censored when no later request exists and `t+H` passes the end. Spark labels/censoring == NumPy on all 99 629 rows. |
| Features available at eviction time | Every feature uses positions ≤ t; prefix-invariance check on the real trace: not leaky. On the real trace with the real exported LR and RF, ML and hybrid decisions before position 60 000 are **identical** when every later request is replaced (34–35 k decisions each). |
| Temporal leakage through Gold aggregates | **None.** No Gold table or whole-trace statistic is read by feature, training or policy code (`grep`: Gold is used only by `build_gold`, `hbase-publish`, the demo's HBase check and the dashboard). `category` comes from static item metadata. |
| Splits / purging | Chronological 60/20/20; a sample is eligible only if its label window stays inside its block. Stored metadata boundaries == `chronological_split(n, H)`. |
| Preprocessing | StringIndexer/OHE/scaler fitted on the train block only (pipeline `.fit(train)`; sklearn encoder fitted on train rows). |
| Hyper-parameters | Selected by validation PR-AUC; the stored choice is the best validation candidate for both models. |
| Test metrics | Recomputed from the stored Spark trace predictions on the eligible, uncensored test rows (19 427): ROC-AUC 0.94221 (LR) / 0.93745 (RF), PR-AUC 0.94745 / 0.94169 — identical to the metadata to every digit. |
| Exporter parity | Export vs reloaded Spark on Spark features: ≤ 4.4e-16 (LR), ≤ 3.3e-16 (RF), all rows. |
| Online-path parity | **Defect D6** (below). |
| Fallbacks, loading, schema checks | Unit tests: missing/invalid/NaN/out-of-range predictions fall back to LRU and are counted; `load_scorer` refuses failed-parity or wrong-schema artifacts. |
| Inference overhead | Unchanged; Exp. 9 numbers stand (LR ≈ 1–3 µs, RF ≈ 34–38 µs per request online). |

**D6 — cross-runtime feature bits.** Spark computed `log1p/sin/cos` in the JVM, the online
tracker with the C library. On the project dataset 20 653 rows differ by 1 ulp; on 655 rows
(0.66 %) a differing value equals a random-forest threshold (thresholds are data values),
so Spark goes left and the online path right: max |Δp| 0.023, and the RF ML policy made
57 457 vs 57 454 evictions (capacity 100). LR was unaffected. The stored parity check
missed it because it scores *Spark-computed* features with both runtimes. A comparison
tolerance cannot fix it: hour-of-day sin/cos contain mathematically equal pairs (e.g. hours
1 and 11) that differ by 1 ulp and on which the forest splits. **Fix**: one function,
`ml.features.feature_values`, applies all floating-point transforms; Spark computes the
integer history with window functions and calls it through a pandas UDF. **After** (isolated
rebuild of the project dataset's features and models in a scratch copy): online vs Spark
max |Δp| 3.9e-16 (LR) / 3.3e-16 (RF) on all rows; identical hits and evictions at
capacities 50/100/200 for both models; test ROC-AUC/PR-AUC and selected hyper-parameters
identical to the stored models. Feature stage time 3.96 s → 4.98 s (100 k rows).

---

## 8. Big Data audit

* **Row accounting (99 629 vs 80 135).** Independent pure-Python re-derivation
  (`validate_raw` reference path + an independent implementation of the duplicate policy,
  no Spark) from the raw files and the Kafka Bronze rows visible to the stored Silver run:
  **39 803 legacy CSV + 39 826 API JSON + 20 000 Kafka = 99 629** rows; the 80 135 batch
  rows split into 79 629 valid + 506 quarantined. Event-id sets equal, 0 payload-hash
  mismatches, 0 kept-source mismatches, quarantine counts identical per (reason, source),
  `seq` unique. The "extra" 19 494 rows are the 20 000 Kafka events minus the 506
  quarantined batch rows.
* **Re-sent events.** The 09:45 re-send added 20 000 Kafka rows to Bronze *after* that
  Silver run. A rebuild keeps the first copy by lineage order (lower Kafka offsets) and
  quarantines the re-sent copies as `exact_duplicate` (identical payload).
* **Schemas, validation order, quarantine, exact/conflicting/cross-source duplicates,
  sequence conflicts, time-zone normalisation, Gold vs pandas, idempotent re-runs**:
  covered by the Spark suite (13 tests in `test_spark_pipeline.py`) — all pass.
* **Kafka partitions/offsets, at-least-once, idempotent sinks, restart, partial failures,
  late data, duplicates**: live tests (§10) and Exp. 12 scenarios.
* **HBase**: row keys salted `dataset#item`; absolute window counts (replays overwrite);
  verified live against Parquet.
* **Batch vs streaming**: the canonical Silver contains every streamed event exactly once
  (demo check); streaming windows equal an independent recount from canonical Silver
  (demo and live test).

---

## 9. Cache-policy audit

Independent checks (`audit_policies.py`) in addition to the existing 201 unit/property tests:

| Check | Cases | Mismatches |
|---|---|---|
| ML and hybrid vs naive scan references written from the documented rules (expired-first, `(p, last, key)` ties, hybrid score, α ∈ {0, .25, .5, 1}), random traces (n ≤ 60), coarse predictions (many ties), capacities 0–6, random measurement windows, full invariant checks | 6 000 | 0 |
| Belady misses vs an independent DP optimum over cache states (n ≤ 11, capacity 0–4) | 1 500 | 0 |
| FIFO/LRU/LFU vs naive references | 4 500 | 0 |
| No future access on the real trace (ML and hybrid, LR and RF) | 4 configurations | 0 |

Existing tests additionally cover: empty traces, capacity 0 and 1, capacity > distinct
items, repeated requests, malformed item ids (rejected), non-integer keys (rejected),
deterministic replay, warm-up vs measured windows (`evictions_measured`), engine invariant
violations, online policies refused the future, hybrid α = 0 ≡ LRU, constant predictions ≡
LRU, prefix invariance of online and learned policies, exhaustive Belady optimality.

---

## 10. Files changed and why

| File | Change |
|---|---|
| `src/cachemind/bigdata/streaming.py` | D2 `bronze_projection` (one file per micro-batch, `max_offsets` cap); D3 `progress_record` (sum over operators), `run_id` on records, per-run `event_time` summary (bounded, replay-safe totals), late rows per dataset in Silver manifests; D4 `stop_state_store`, `shutdown_streaming`; runtime dir from settings; docstring (one namespace = one timeline) |
| `src/cachemind/config.py` | `stream_runtime_override` / `stream_runtime_dir` (`CACHEMIND_STREAM_RUNTIME_DIR`) |
| `src/cachemind/monitoring/health.py` | D5 current run vs history, `error_class`, timezone-safe timestamps, tolerant of a half-written last line |
| `src/cachemind/experiments/pipeline_demo.py` | D1 isolated namespace per run, `stream_window_check`, 3 new evidence checks, isolation recorded in evidence |
| `src/cachemind/experiments/streaming_experiments.py` | D5 per-scenario streaming runtime dir; S1 drains two Bronze files per Silver trigger (see §12) |
| `src/cachemind/ml/features.py` | D6 `feature_values` (the single place of floating-point transforms) |
| `src/cachemind/bigdata/features_spark.py` | D6 integer history in Spark, values through a pandas UDF calling `feature_values` |
| `src/cachemind/dashboard/views.py`, `data.py` | current run vs history on the health page; `width="stretch"`; runtime dir from settings |
| tests | see §11 |
| docs | this file; STREAMING_SEMANTICS, DATA_CONTRACTS, ML_DESIGN, ARCHITECTURE, TEST_REPORT, ENVIRONMENT_AUDIT, FINAL_REPORT, DEMO_GUIDE, README |

No dependency was added or upgraded; no checkpoint, dataset, model or evidence file was
deleted or rewritten by the fixes.

---

## 11. Tests added

Tests that encode a defect fail on the old behaviour (value in the last column, observed
before the fix); "semantics pin" tests document correct Spark behaviour that the fixes rely on.

| Test | Encodes | Old behaviour |
|---|---|---|
| `test_streaming_unit.py::test_windows_equal_independent_recount_and_only_the_late_event_is_dropped` | exact window counts on a tiny trace; only the late event dropped and counted; idempotent restart | (semantics pin) |
| `…::test_one_namespace_is_one_event_time_timeline` | earlier timeline in a shared namespace is dropped and *counted*; separate namespaces exact | (semantics pin) |
| `…::test_splitting_a_bronze_batch_across_micro_batches_inflates_late_flags` | why Bronze must be whole batches | 107 false late flags |
| `…::test_bronze_projection_is_one_partition_per_micro_batch` | D2 | 3 partitions |
| `…::test_stop_state_store_ends_maintenance_and_state_stays_usable`, `…_refuses_while_a_query_is_active` | D4 | maintenance thread outlives the session |
| `tests/integration/test_streaming_kafka.py` (live Kafka + HBase, own process) | D2 end to end: 1 file per commit, no split, flags == recomputed policy, windows == recount (0 dropped), HBase cells == Parquet, idempotent restart, no `RpcEndpointNotFound` | same flow measured on the old code (`stream_flow.py`): 3 files per commit, 7/7 split, 1 604 false late, 2 958/3 000 counted |
| `tests/unit/test_streaming_health.py` (6 tests) | D3 drop sum (real Spark operator order), D5 current vs history, error classes on real messages, demo isolation env, demo window check | 0 for 18 dropped rows; 36 mixed count |
| `test_spark_pipeline.py::test_spark_features_equal_python_online_features` (tightened to bitwise) | D6 | 2 032 of 65 054 values differ |
| `test_spark_pipeline.py::test_mllib_train_save_reload_export` (online path vs Spark batch on every row) | D6 | RF max |Δp| 0.0073 |
| `test_pipeline.py` (e2e) | requires isolation and the 3 new checks | — |

---

## 12. Tests executed and results (final code)

| Suite | Command | Result |
|---|---|---|
| Unit + property | `make test` | **207 passed** (201 before + 6) |
| Spark (pipeline + streaming writers/windows) | `make test-spark` | **24 passed** (18 before + 6) |
| Integration with live Kafka + HBase + Spark | `make integration-test` | **41 passed, 1 error** — the error was `test_kafka.py::test_producer_restart_reuses_event_ids` at fixture setup (`Broker transport failure`) while the Kafka JVM was frozen (GC log: 55.6 s and 56.3 s pauses, missed controller heartbeats; see ENVIRONMENT_AUDIT). Re-run of `test_kafka.py` alone: **5 passed** |
| Live streaming regression | `pytest tests/integration/test_streaming_kafka.py` | **1 passed** (also inside the integration run) |
| End-to-end pipeline (isolated tmp) | `pytest tests/end_to_end/test_pipeline.py -m e2e` | **1 passed** |
| Failure injection (6 scenarios, live services) | `pytest tests/end_to_end/test_failure_recovery.py -m e2e` | first run on the new code: S2–S6 passed, S1 `late_events_flagged` failed (root cause below); after restoring S1's micro-batch grouping: **1 passed — all 6 scenarios** (342 s). S1 then flagged 31 late events, identical to the stored Exp. 12 run |
| Pipeline demo | `make pipeline-demo` | **19/19 checks** — `reports/pipeline/e2e-20261019T182336-69d9/evidence.json` |
| Lint / format / types | `make lint`, `make typecheck` | Ruff clean, 80 files formatted; mypy: no issues in 51 source files |
| Independent audits | scripts in §2 | all 0 mismatches (§§ 7–9) |

Self-review of the diff found two further issues, fixed before the final runs: the per-run
late-row summary kept one entry per micro-batch (unbounded in a long continuous run; now
running totals plus the last batch, replay-safe — asserted in
`test_late_events_flagged_against_previous_batches_and_stable_on_replay`), and the health
check would crash on a half-written last line of `progress.jsonl` (now skipped — test fails
without the fix). After these, the final code was re-run: `make test` 207 passed,
`make test-spark` 24 passed, live streaming test 1 passed, e2e pipeline test 1 passed,
lint/types clean.

**Compatibility with the existing shared checkpoints.** A byte-for-byte copy of
`data/checkpoints` and the streaming outputs was resumed with the new code against the
shared topic (read only): all three queries restarted from the historical checkpoints
without error, the topic's end offsets equalled the committed offsets, no batch was
written, and the copy was byte-identical afterwards.

Problems found while verifying the fixes (both root-caused, neither weakened a check):

* **Exp. 12 S1 flagged 0 late events after the Bronze fix** (first failure-injection run on
  the new code: S2–S6 passed, S1's `late_events_flagged` failed). S1 drains with
  `max_offsets=2000, max_files=6`; with three files per Bronze batch that meant two Silver
  micro-batches (Bronze 0+1, then 2) and the delayed events in the second were late
  relative to the first. With one file per batch the whole backlog became one Silver
  micro-batch, in which nothing is late by definition. S1 now drains with two files per
  trigger, which reproduces the original grouping exactly; the check is unchanged.
* **The new live streaming test failed inside the full suite** with `Failed to find data
  source: kafka`: an earlier test module had started the JVM without the Kafka connector
  (`spark.jars.packages` only applies at JVM launch). The test now runs the streaming job
  in its own process, like `make streaming-once`.

---

## 13. Before vs after (key numbers)

| Measure | Before | After |
|---|---|---|
| Demo streaming windows (events counted / streamed) | 0 / 4 000 for 3 of 4 demos | 4 000 / 4 000; 3 234 / 3 234 windows exact |
| On-time events flagged late (live, 3 000 in-order at 1 ev/s) | 1 604 | 37 (= documented rule on whole batches) |
| On-time events missing from windows (same) | 42 | 0 |
| Recorded `dropped_by_watermark` when Spark dropped 18 | 0 | 18 |
| `RpcEndpointNotFound` in an integrated run | repeated | 0 |
| Health "errors" | 36 (mixed history) | current run 0; history 51 by class |
| RF online vs Spark batch, max |Δp| on the project dataset | 0.023 (655 rows) | 3.3e-16 (rebuilt in isolation) |
| RF evictions online vs batch (capacity 100) | 57 457 vs 57 454 | identical |

---

## 14. Streaming-window output verification

* Tiny traces with hand-computed expectations: exact (5 reproduction cases, 3 tests).
* Live Kafka, 3 000 events: windows == recount from Silver for every key, 0 dropped,
  HBase cells == Parquet.
* Pipeline demo: windows == recount from canonical Silver, 3 234/3 234 exact, 0
  overcounted; Spark input 4 048 = 4 000 events + 35 duplicates + 13 malformed.
* Historical shared namespace: project dataset 17 836/17 836 windows exact, 0 overcounted;
  demo 932e 3 233/3 234 (one late event dropped); the three later demos 0 (D1).

---

## 15. Integrity of historical artifacts

SHA-256 manifest of `data/`, `artifacts/`, `reports/` taken before any change (2 461 files)
and compared after all runs: **0 removed, 3 changed, 273 added.** The 3 changed files are
`reports/experiments/e2e_pipeline/{results.parquet,results.csv,meta.json}`, replaced by the
audit's `make pipeline-demo` run — by design that table holds the latest demo; every earlier
demo's simulation results remain in its `reports/pipeline/<run>/evidence.json`. The 273
added files are that demo run (`data/pipeline_runs/e2e-20261019T182336-69d9/`, its models,
its evidence). The shared checkpoints, streaming outputs, Silver/Gold, features, project
models and experiment results are byte-identical. All other test and reproduction runs
used temporary or scratch directories and private Kafka topics (`cachemind.test.*`,
`cachemind.audit.*`, `cachemind.demo.*`, `cachemind.exp.*`; 7-day retention, left in place).
HBase received rows only for new, uniquely named datasets.

The stored Spark models of `zipf-n100000-s42` predate D6 and were left unchanged (online vs
batch decisions can differ on ≈ 0.7 % of RF predictions); `make spark-mllib` rebuilds them
on bit-identical features.

---

## 16. Remaining limitations

* Docker Compose (Profile C on a larger machine): syntax-validated only, **never run**.
* Single node only; no multi-broker Kafka, distributed HBase or Spark cluster.
* All data synthetic; no production traffic.
* The shared default namespace still holds one timeline: streaming a second dataset whose
  timeline lies behind it into the default topic makes it late and absent from the
  windows (documented; use a separate namespace). The historical shared outputs from
  before the fix are unchanged.
* Pre-fix Bronze files keep their per-partition layout; a *new* stateful query replaying
  them from the beginning would see the old skew.
* With the pipeline demo's backlog in one micro-batch, the demo itself shows no late flags;
  Exp. 12 S1 exercises late data.
* The Kafka JVM on this 8 GB laptop freezes for ~55 s under memory pressure; timing-
  sensitive Kafka tests can fail at setup there (one occurrence in this audit).
* `WindowExec` global ordering is single-partition by design; fine up to the measured 1 M
  rows, not intended for the `large` profile on this machine.

---

## 17. Resource usage and Mac safety

Available memory was checked before every heavy step (1.9–2.8 GB available, 24–35 %);
an unrelated Neo4j server was running and was left alone. Kafka (384 MB heap) and HBase
(512 MB + 192 MB REST) were started only through `make infra-up` (memory guard passed);
Spark ran with `local[1–2]` and a 768 MB–1 GB driver; all suites ran sequentially, never two
Spark applications at once (except the Exp. 12 subprocesses, which run one streaming job
at a time by design). Swap grew from 0.55 to 1.13 GB during the 15-minute integration run;
the Kafka JVM showed two ~56 s freezes (no crash). No system setting, swap, SIP, kernel or
global Java configuration was touched. Services were stopped with `make infra-down` at the
end (§18); afterwards no Kafka/HBase/Spark process remained and 3.3 GB were available.

---

## 18. Commands to reproduce

```bash
make setup                                   # once
make test                                    # 207 unit/property tests, no JVM
make test-spark                              # 24 Spark tests (local JVM)
make infra-up && make infra-health           # Kafka + HBase (memory-guarded)
make integration-test                        # live Kafka/HBase/Spark (42 tests)
CACHEMIND_REQUIRE_INFRA=1 .venv/bin/python -m pytest tests/integration/test_streaming_kafka.py -q
CACHEMIND_REQUIRE_INFRA=1 .venv/bin/python -m pytest tests/end_to_end/test_pipeline.py -q -m e2e
CACHEMIND_REQUIRE_INFRA=1 .venv/bin/python -m pytest tests/end_to_end/test_failure_recovery.py -q -m e2e   # ~15 min, restarts services
make pipeline-demo                           # isolated run, 19 checks, evidence in reports/pipeline/
make infra-down
make dashboard                               # http://127.0.0.1:8501
```

---

## 19. Is CacheMind demonstration-ready?

**Yes, with limitations.** The batch pipeline, the ML protocol and the cache-policy results
were verified independently and are unchanged. The streaming layer now isolates runs,
keeps Bronze batches whole, reports late and dropped rows truthfully and shuts down
cleanly, and the exported model reproduces the Spark model exactly. It is not production
ready: single node, no security, synthetic data, Docker deployment not executed.
