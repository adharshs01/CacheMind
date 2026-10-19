# Architecture

## 1. Logical view

```
  Historical sources                      Real-time
  requests_legacy.csv (CSV)               Kafka producer (kafka_io.py)
  requests_api.jsonl  (nested JSON)              |
  items.csv / users.csv (metadata)               v
          |                               Kafka topic cachemind.requests.v1 (3 partitions)
          v                                      |
  Spark batch Bronze ingest (etl.py)      Spark Structured Streaming (streaming.py)
          |                                Q1 bronze_ingest: Kafka -> Parquet file sink
          v                                      |
          +--------------> BRONZE (raw + lineage) <-+
                              |               \
                 batch Silver |                \ Q2 silver_stream (provisional) -> HBase cache_requests
                 (etl.py)     v                 \ Q3 gold_windows (event-time windows) -> HBase item_statistics
                           SILVER (canonical, validated, de-duplicated) + QUARANTINE
                              |                        |
                Gold (gold.py)|                        | features (features_spark.py)
                              v                        v
                            GOLD                ML FEATURES (+ labels)
                              |                        |
               HBase item_statistics          Spark MLlib LR/RF (mllib_train.py)
                              |                        |  save -> reload -> export (spec JSON) -> parity check
                              +-----------+------------+
                                          v
                          CACHE EXPERIMENT ENGINE (simulation/, policies/)
                          trace = Silver ordered by (seq, event_id)
                                          |
                     results Parquet (reports/experiments) + HBase cache_experiments
                                          |
                                    STREAMLIT UI (dashboard/)
```

The Python cache engine is separate from the distributed analytics: it reads a bounded
trace (columns pruned, dataset filter pushed down) from Silver Parquet, and loads
models through the exported spec — never through a live Spark session.

## 2. Physical layout

All paths are under `CACHEMIND_DATA_DIR` (default `<repo>/data`, git-ignored).

| Path | Written by | Format / partitioning | Notes |
|---|---|---|---|
| `raw/<dataset>/` | generator | CSV, JSONL, JSON manifests | Immutable inputs + ground truth (`expected.json`) |
| `lake/bronze/batch/source=<s>/dataset_hint=<d>/` | `etl.ingest_bronze_files` | Parquet (snappy) | Verbatim lines, file + line lineage; replaced per (source, dataset) |
| `lake/bronze/stream/ingest_date=<date>/` | Q1 (file sink) | Parquet + `_spark_metadata` log | Raw Kafka value + topic/partition/offset/timestamp; one file per micro-batch (since the correctness audit) |
| `lake/silver/requests/dataset_id=<d>/event_date=<date>/` | `etl.build_silver` | Parquet, sorted by `seq` | Canonical requests (authoritative) |
| `lake/quarantine/dataset_hint=<d>/` | `etl.build_silver` | Parquet | Every rejected record + reason + lineage |
| `lake/silver_stream/requests/batch_id=<n>/` | Q2 | Parquet | Provisional streaming Silver (speed layer) |
| `lake/quarantine_stream/batch_id=<n>/` | Q2 | Parquet | Streaming rejects |
| `lake/gold/<table>/dataset_id=<d>/` | `gold.build_gold` | Parquet (1 file each) | 9 bounded aggregate tables |
| `lake/gold_stream/item_windows_v2/batch_id=<n>/` | Q3 | Parquet | Event-time window counts per (dataset, item) |
| `lake/features/dataset_id=<d>/horizon=<H>/` | `features_spark.build_features` | Parquet | Features + label + censoring flag |
| `lake/_stream_manifests/<query>/batch_<n>.json` | Q2/Q3 | JSON | Written last: micro-batch completion marker |
| `lake/_reports/*.json` | ETL jobs | JSON | Data-quality and verification reports |
| `checkpoints/{bronze_ingest,silver_stream,gold_windows_v2}` | Spark | Spark checkpoint | Never deleted automatically; the stateful window query is versioned (see STREAMING_SEMANTICS) |
| `checkpoints/silver_stream_state/` | Q2 | JSON per batch | Lateness reference per batch (replay-stable) |
| `<repo>/artifacts/models/<dataset>/<runtime>_<model>_h<H>_<version>/` | trainers | `spark_model/`, `export.json`, `metadata.json`, `parity.json`, `trace_predictions/` | Versioned model artifacts |
| `<repo>/reports/experiments/<exp>/` | experiment runners | `results.parquet/csv`, `meta.json` | Saved experiments read by the dashboard |
| `<repo>/reports/pipeline/<run>/evidence.json` | `pipeline_demo` | JSON | End-to-end evidence |
| `<repo>/.runtime/{kafka,hbase}/` | `ops.infra` | service data, logs, pids | Native service state |
| `<repo>/.runtime/streaming/{status.json,progress.jsonl}` | streaming job | JSON | Heartbeat + per-batch progress for health/UI (`CACHEMIND_STREAM_RUNTIME_DIR` overrides; isolated runs use their own) |
| `pipeline_runs/<run_id>/` (under the data dir) | `pipeline_demo` | full private data dir: raw, lake, checkpoints, `_stream_runtime/` | One isolated streaming namespace per demo run (topic `cachemind.demo.<run_id>`) |

Every batch stage writes to a staging directory and renames it into place, so readers see
either the old or the new complete version, and re-running a stage replaces exactly that
dataset's partitions (idempotent; replayable from Bronze).

## 3. Jobs

| Job | Entry point | Input → output |
|---|---|---|
| Generator | `cachemind generate` | config → `raw/<dataset>/` |
| Bronze (batch) | `cachemind bronze` | raw files → `bronze/batch` |
| Silver | `cachemind silver` | all Bronze for the dataset (batch + Kafka) → `silver`, `quarantine`, DQ report |
| Gold | `cachemind gold` | Silver → `gold/*` |
| Features | `cachemind features` | Silver → `features` |
| MLlib | `cachemind mllib` | features → model artifacts (+ batch predictions) |
| RDD / SQL demos | `cachemind rdd` / `cachemind sql` | Silver → `reports/rdd`, `reports/sql` |
| Producer | `cachemind produce` | `stream_events.jsonl` or live generator → Kafka |
| Streaming | `cachemind stream run` | Kafka → Bronze → provisional Silver / windows → HBase |
| Reconcile | `cachemind stream reconcile [--repair]` | checkpoints + outputs + HBase → consistency report |
| HBase publish | `cachemind hbase-publish` | Gold + experiment results → HBase |
| Experiments | `python -m cachemind.experiments.*` | traces/models → `reports/experiments` |
| Pipeline demo | `cachemind pipeline-demo` | everything above, with evidence |

## 4. Execution profiles

| Profile | What runs | Needs | Command |
|---|---|---|---|
| **A – lightweight** | generator, simulator, all policies, scikit-learn models, experiments 1–9, dashboard on saved results | Python only | `make test demo experiments dashboard` |
| **B – component integration** | Spark ETL/RDD/SQL/MLlib (local JVM), then Kafka, then HBase, one at a time | JDK 17 in `.tools` | `make test-spark`, `make infra-up`, `make integration-test` |
| **C – full end-to-end** | Kafka + Spark Structured Streaming + HBase + MLlib + simulator + UI together | ~3 GB free RAM natively, or a Docker host with ~6 GB for the VM | `make infra-up pipeline-demo e2e-test` (native) or `infra/docker/docker-compose.yml` |

Profile A is clearly labelled in the UI ("SYNTHETIC EXAMPLE", "SAVED EXPERIMENT") and does
not claim to verify the Big Data stack.

## 5. Concurrency and memory

* One SparkSession per process (`get_spark`), `local[2]`, 768 MB–1 GB driver,
  4 shuffle partitions, UI off, adaptive execution on.
* The dashboard never creates a Spark session; it reads bounded Parquet snapshots
  with pyarrow and caches health checks for 10 s.
* `cachemind.ops.infra` refuses to start JVM services under memory pressure.
* Streaming micro-batches are bounded: `maxOffsetsPerTrigger` (default 5000) on Kafka,
  `maxFilesPerTrigger` (default 20) on the Bronze file source.
