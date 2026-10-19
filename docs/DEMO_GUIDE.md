# Demonstration guide (for presenting the project)

Times are what the steps took on the 8 GB development MacBook. Close other heavy apps
first; keep the laptop plugged in (on battery with the display off, macOS throttled the
Kafka broker during development).

## 0. Before the demo (once)

```bash
make setup fetch-tools                      # ~5 min, downloads are checksum-verified
make generate-data PROFILE=small            # 1–2 s → data/raw/zipf-n100000-s42/
make spark-etl DATASET=zipf-n100000-s42     # ~75 s
make experiments                            # ~45–60 min, results saved for the dashboard
```

## 1. Lightweight profile — algorithms and ML (no JVM) — 5 min

| Show | Command | What to point out |
|---|---|---|
| tests | `make test` | 207 tests incl. exhaustive Belady optimality and leakage detection |
| all policies on one trace | `make demo` | hit/miss per policy; Belady is the upper bound; ML vs LRU/LFU |
| dashboard | `make dashboard` → *Cache simulator* | change workload/capacity/policy; eviction decisions; labelled SYNTHETIC EXAMPLE |

## 2. Big Data batch — Spark (requirements C–G) — 5 min

| Requirement | Command | Evidence |
|---|---|---|
| ETL + data quality | `make spark-etl DATASET=zipf-n100000-s42` | `data/lake/_reports/silver_zipf-n100000-s42.json`: quarantine per reason, `verification.all_ok: true`, row accounting |
| RDD | `make spark-rdd DATASET=…` | `reports/rdd/…/rdd_report.json`: every operation + cross-checks |
| Spark SQL | `make spark-sql DATASET=…` | `reports/sql/…/*.csv` (8 queries, files in `sql/`) |
| MLlib | `make spark-mllib DATASET=… HORIZON=500` | `artifacts/models/…/metadata.json` (metrics, versions), `parity.json` |
| dashboard | *Big Data analytics*, *ML model evaluation* pages | Gold tables, quarantine chart, calibration, baselines |

## 3. Real-time + HBase (requirements H, I) — 5 min

```bash
make infra-up && make infra-health                  # Kafka + HBase (≈ 1 min)
make kafka-producer DATASET=zipf-n100000-s42        # 20 000 events, delivery report
make streaming-once                                 # Kafka → Bronze → Silver/HBase → windows
make reconcile                                      # every micro-batch complete?
make hbase-verify                                   # row counts, families, sample rows
```
Show the dashboard *System health* and *Live request monitoring* pages (they say NOT LIVE
unless `make streaming` is running; start it plus `make kafka-producer-live` to show live data).

What to point out (and what not to be surprised by):

* `make streaming-once` prints an `event_time` block: late rows per dataset, window input
  rows and rows dropped by the watermark. The default namespace (topic
  `cachemind.requests.v1` + `data/checkpoints`) is **one event-time timeline**. Running
  `make kafka-producer` a second time re-sends the same 20 000 events: they are flagged
  late (19 912 — all but the last 2 minutes) and the windows emit nothing, which is
  correct (no double counting). Streaming a *different* dataset into the default
  namespace after this one also makes it entirely late, because every synthetic dataset
  starts on 2026-01-05; use a fresh namespace for that (see STREAMING_SEMANTICS.md).
* *System health* shows the **current run** (its failed queries — expect 0) separately
  from the **history** of all recorded runs. The history (51) consists of deliberate
  failure injections from Exp. 12 and the documented window-schema migration; it is not
  current health.

## 4. End-to-end with evidence (requirement K) — 2 min

```bash
make pipeline-demo      # 12 stages, 19 checks → reports/pipeline/<run>/evidence.json (~1 min)
```
Each run streams into its own namespace: `data/pipeline_runs/<run>/` (raw data, lake,
checkpoints, streaming status) and Kafka topic `cachemind.demo.<run>`; evidence, models,
HBase rows and the `e2e_pipeline` result table go to the usual shared places (the table
holds the latest demo; earlier demos remain in their `evidence.json`).
Open the evidence file: stage timings, Kafka delivery counts, ground-truth verification,
`stream_event_time` (window counts vs an independent recount from canonical Silver: every
streamed event counted, no window overcounted), MLlib parity, identical decisions from the
exported and the Spark model, and one event traced from its Kafka offset to its HBase row.

## 5. Results to discuss

* `docs/EXPERIMENTS.md` — where ML helps (7 of 10 workloads) and where it does not (scan,
  hot set, uniform), the post-shift dip, capacity, overhead, Spark tuning, streaming
  latency, failure recovery.
* `docs/RESEARCH_ALIGNMENT.md` — what is and is not taken from the LRB paper.
* `docs/TEST_REPORT.md` — 29 failures found, root-caused and fixed during testing.
* `docs/FINAL_CORRECTNESS_AUDIT.md` — the independent correctness audit: what was wrong
  (streaming namespaces, Bronze layout, drop metric, shutdown order, cross-runtime feature
  bits, health history), how it was proven and fixed, and what was verified correct.

## Shut down

```bash
make infra-down
```
