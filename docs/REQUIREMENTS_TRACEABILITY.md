# Requirements traceability matrix

Columns: requirement → implementation → input data → command → expected output →
automated test → **actual verification result** → documentation. "Verified" means the test
or command was executed in this environment and passed; see [TEST_REPORT.md](TEST_REPORT.md)
for the exact runs. Dataset ids: `zipf-n100000-s42` (project dataset), per-test temporary
datasets otherwise.

## A. Multiple data sources

| Req | Implementation | Data | Command | Expected | Test | Result | Docs |
|---|---|---|---|---|---|---|---|
| Historical CSV logs | `data/generator.py` (legacy CSV), `bigdata/etl.py::ingest_bronze_files` | `requests_legacy.csv` | `make generate-data spark-etl` | Bronze rows = CSV data lines, verbatim | `test_bronze_preserves_every_raw_line_with_lineage` | **Verified** – Spark test + project run: 40 067 CSV rows in Bronze = raw lines | DATA_CONTRACTS |
| Historical JSON logs | nested API JSON writer, `map_to_wire` | `requests_api.jsonl` | same | parsed + normalised (mixed offsets → UTC) | `test_timezone_normalisation_to_utc` | **Verified** – every API event normalised to the generator's UTC instant | DATA_CONTRACTS |
| Real-time Kafka JSON | `bigdata/kafka_io.py`, streaming Q1 | `stream_events.jsonl` → topic | `make kafka-producer streaming-once` | every message delivered, in Bronze once | `tests/integration/test_kafka.py`, Exp. 12 | **Verified** – Kafka tests 5/5; project run: 20 000 events produced, 20 000 in Silver | STREAMING_SEMANTICS |
| Item metadata joins | `load_item_metadata`, `load_user_metadata`, broadcast joins | `items.csv`, `users.csv` | `make spark-etl` | category/user_group enriched; missing → `unknown`, flagged | `test_silver_canonical_invariants` | **Verified** – Silver invariants test; project run: 1 640 rows with missing metadata flagged, 2 326 → `unknown` category | DATA_CONTRACTS |

## B. Big Data characteristics

| Req | Where | Evidence | Result |
|---|---|---|---|
| Large volume | generator profiles up to 5 M events (streamed, never fully in memory); Spark runs at 1 M events | Exp. 10 | **Verified** – Exp. 10 processed 1 M events (Silver 72 s on the laptop) |
| High-dimensional features | 11 numeric + 2 categorical features (≈ 25-dim assembled vector) — meaningful, no padding columns | `ml/features.py` | implemented |
| Semi-structured | nested API JSON, Kafka JSON, malformed/corrupt records | `map_to_wire`, quarantine | **Verified** – malformed/nested-type records quarantined (parity test) |
| Time series | event-time hourly/daily Gold tables, event-time windows | `gold.py`, Q3 | **Verified** – Gold hourly (32 rows) and daily tables; streaming windows |
| Streaming | Kafka + Structured Streaming | streaming.py | **Verified** – Kafka tests 5/5; project run: 20 000 events produced, 20 000 in Silver |
| Multiple related datasets | requests ⋈ items ⋈ users | Silver | **Verified** – Silver invariants test; project run: 1 640 rows with missing metadata flagged, 2 326 → `unknown` category |

## C. ETL pipeline

| Step | Implementation | Test | Result |
|---|---|---|---|
| Extraction / ingestion | `ingest_bronze_files` (textFile + zipWithIndex → line lineage), Q1 | `test_bronze_preserves_every_raw_line_with_lineage` | **Verified** – Spark suite 24/24 (final audit run); project run `verification.all_ok = true`, row accounting holds |
| Schema validation, missing values, malformed records | `validate_wire` (12 ordered rules) | `test_spark_validation_matches_python_reference` | **Verified** – Spark suite 24/24 (final audit run); project run `verification.all_ok = true`, row accounting holds |
| Duplicate detection (exact / conflicting / sequence) | `deduplicate` | `test_etl_matches_generator_ground_truth` (expected counts per reason) | **Verified** – Spark suite 24/24 (final audit run); project run `verification.all_ok = true`, row accounting holds |
| Timestamp normalisation | ISO with offset → UTC ms | `test_timezone_normalisation_to_utc` | **Verified** – Spark suite 24/24 (final audit run); project run `verification.all_ok = true`, row accounting holds |
| Standardisation / mapping | lower-casing ids, region code map, category aliases | `test_silver_canonical_invariants` | **Verified** – Spark suite 24/24 (final audit run); project run `verification.all_ok = true`, row accounting holds |
| Integration | metadata joins | same | **Verified** – Spark suite 24/24 (final audit run); project run `verification.all_ok = true`, row accounting holds |
| Filtering / sorting / grouping / aggregation | Gold, SQL, RDD | `test_gold_aggregations_match_pandas`, SQL tests | **Verified** – Spark suite 24/24 (final audit run); project run `verification.all_ok = true`, row accounting holds |
| Feature engineering | `features_spark.py` | `test_spark_features_equal_python_online_features` | **Verified** – Spark suite 24/24 (final audit run); project run `verification.all_ok = true`, row accounting holds |
| Loading into storage | Parquet (staging + rename), HBase | `test_silver_rerun_is_idempotent`, HBase tests | **Verified** – Spark suite 24/24 (final audit run); project run `verification.all_ok = true`, row accounting holds |
| Before/after examples | DATA_CONTRACTS §Before/after | – | documented |
| Automated DQ checks | DQ report + `verify_against_expected` + row accounting | `test_etl_matches_generator_ground_truth` | **Verified** – Spark suite 24/24 (final audit run); project run `verification.all_ok = true`, row accounting holds |

## D/E. Spark RDD transformations and actions

| Op | Where | Test | Result |
|---|---|---|---|
| map, filter, flatMap, distinct, reduceByKey, sortByKey (+ aggregateByKey, join, mapPartitions, zipWithIndex) | `bigdata/rdd_demo.py` on Silver | `test_rdd_demo_all_operations_cross_checked`, `test_rdd_primitives_on_known_data` | **Verified** – Spark suite; project run: all 7 cross-checks true, 100 lines saved |
| count, take, collect (bounded), reduce, saveAsTextFile | same | same (saved 100 lines) | **Verified** – Spark suite; project run: all 7 cross-checks true, 100 lines saved |

Command: `make spark-rdd DATASET=…` (independently executable module).

## F. Spark SQL

| Query | File | Test | Result |
|---|---|---|---|
| Top requested items | `sql/01_top_requested_items.sql` | `test_sql_queries_match_pandas` | **Verified** – `test_sql_queries_match_pandas`; project run: 8/8 queries OK |
| Requests by category / region | `02`, `03` | same | **Verified** – `test_sql_queries_match_pandas`; project run: 8/8 queries OK |
| Hourly statistics / daily trends | `04`, `05` | same | **Verified** – `test_sql_queries_match_pandas`; project run: 8/8 queries OK |
| User-group patterns | `06` | same | **Verified** – `test_sql_queries_match_pandas`; project run: 8/8 queries OK |
| Item popularity changes | `07` | same | **Verified** – `test_sql_queries_match_pandas`; project run: 8/8 queries OK |
| Cache experiment summaries | `08` (view over experiment results) | executed by `make spark-sql` after experiments | **Verified** – project run: 371 summary rows over the experiment results |

## G. Spark MLlib

| Req | Implementation | Test | Result |
|---|---|---|---|
| Feature transformations, assembly, preparation | StringIndexer → OneHotEncoder → VectorAssembler → StandardScaler | `test_mllib_train_save_reload_export` | **Verified** – Spark suite; project run: LR/RF trained, reloaded (Δ = 0), parity ≤ 2.8e-16 |
| Training LR + RF, evaluation | `mllib_train.train_mllib_models` (validation selection, test metrics) | same | **Verified** – Spark suite; project run: LR/RF trained, reloaded (Δ = 0), parity ≤ 2.8e-16 |
| Saved model, loading, prediction | `PipelineModel.save/load`; predictions from the reloaded model; batch scoring | same (`save_reload_max_abs_diff == 0`) | **Verified** – Spark suite; project run: LR/RF trained, reloaded (Δ = 0), parity ≤ 2.8e-16 |
| Export validated against Spark | `export_pipeline` + parity check | same (`parity.passed`) | **Verified** – Spark suite; project run: LR/RF trained, reloaded (Δ = 0), parity ≤ 2.8e-16 |

## H. Real-time streaming

| Req | Implementation | Test / evidence | Result |
|---|---|---|---|
| Live producer, topic | `kafka_io.produce`, `ops.infra.ensure_topic` | `test_kafka.py` | **Verified** – Kafka tests 5/5; project run: 20 000 events produced, 20 000 in Silver |
| Live consumer, parsing, validation, transformation | Q1/Q2 | Exp. 11/12, pipeline demo | **Verified** – Exp. 11 (throughput/latency) and Exp. 12 (6 failure scenarios) on live services; `test_streaming_unit.py` 11/11, live `test_streaming_kafka.py`; e2e tests passed |
| Event-time windows, watermark | Q3 | `test_streaming_unit.py` (exact window counts, namespace = timeline), `test_streaming_kafka.py` (live: windows == recount, HBase cells), Exp. 12 S1 | **Verified** – final audit: windows equal independent recounts (tiny traces, live Kafka, pipeline demo 3 234/3 234); drops reported per run; see FINAL_CORRECTNESS_AUDIT §3 |
| Checkpointing, restart, persistent outputs | checkpoints, file sink, manifests | Exp. 12 S2/S3/S6 | **Verified** – Exp. 11 (throughput/latency) and Exp. 12 (6 failure scenarios) on live services; `test_streaming_unit.py` 11/11, live `test_streaming_kafka.py`; e2e tests passed |

## I. Wide-column database (HBase)

| Req | Implementation | Test | Result |
|---|---|---|---|
| Table creation, column families | `ensure_tables` | `test_tables_have_expected_column_families` | **Verified** – HBase tests 12/12 against live HBase 2.6.7 |
| Row-key design | `request_row_key`, `item_row_key`, `experiment_row_key` | `test_row_keys_are_unambiguous`, `test_experiment_prefix_scan` | **Verified** – HBase tests 12/12 against live HBase 2.6.7 |
| Insert / read / update / scan / batch write / verify | `HBaseRestClient` | `tests/integration/test_hbase.py` (12 tests) | **Verified** – HBase tests 12/12 against live HBase 2.6.7 |

## J/K. Analytics and pipeline demonstration

| Req | Implementation | Evidence | Result |
|---|---|---|---|
| Historical analytics | Gold + SQL + dashboard page 6 | `reports/sql/…`, dashboard | **Verified** – Gold/SQL/RDD reports, experiments 1–10 & 13 stored, dashboard pages render (AppTest) |
| Streaming analytics | progress listener, windows, dashboard page 2 | `.runtime/streaming/progress.jsonl` | **Verified** – Gold/SQL/RDD reports, experiments 1–10 & 13 stored, dashboard pages render (AppTest) |
| ML model analytics | metrics + calibration + baselines, dashboard page 5 | model `metadata.json` | **Verified** – Gold/SQL/RDD reports, experiments 1–10 & 13 stored, dashboard pages render (AppTest) |
| Cache performance analytics | experiments 1–12, dashboard pages 4/8 | `reports/experiments` | **Verified** – Gold/SQL/RDD reports, experiments 1–10 & 13 stored, dashboard pages render (AppTest) |
| End-to-end demonstration with stored evidence | `experiments/pipeline_demo.py` | `reports/pipeline/<run>/evidence.json`, `tests/end_to_end/test_pipeline.py` | **Verified** – E2E test passed (twice); `make pipeline-demo`: 19/19 checks (`reports/pipeline/e2e-20261019T182336-69d9/evidence.json`, final audit) |

## Cross-cutting

| Req | Where | Test | Result |
|---|---|---|---|
| Cache invariants (hits+misses, capacity, resident hits/evictions) | `simulation/engine.py` | property tests | **Verified** – unit + property tests (300 exhaustive Belady cases) |
| Belady optimality | `policies/belady.py` | exhaustive search (300 random traces) | **Verified** – unit + property tests (300 exhaustive Belady cases) |
| No future access by online policies | engine contract | prefix-invariance property tests | **Verified** – unit + property tests (300 exhaustive Belady cases) |
| Leakage prevention | `ml/splits.py`, `ml/leakage.py` | `test_features.py` | **Verified** – unit tests incl. deliberate leakage detection |
| Fallback on model failure | `ml/predictors.py`, `policies/learned.py` | `test_learned_policies.py` | **Verified** – unit tests incl. deliberate leakage detection |
| Dashboard (8 pages, provenance labels, unavailable backends, input validation) | `dashboard/` | `test_dashboard.py` (AppTest renders all pages) | **Verified** – 20 dashboard tests; HTTP health OK, bound to 127.0.0.1 |
| Security (localhost binding, no secrets, path validation, safe subprocess) | configs, `safe_child`, `_check_name` | `test_path_traversal_is_rejected` | **Verified** – 20 dashboard tests; HTTP health OK, bound to 127.0.0.1 |
