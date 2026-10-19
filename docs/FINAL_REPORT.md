# CacheMind — final engineering report

_Status as of the end of the build session. Every claim below points to an executed test,
command or stored result; anything not executed is listed under "Not verified"._

## 1. What was built

| Layer | Delivered |
|---|---|
| Data | deterministic generator for 10 workload shapes (zipf, uniform, bursty, periodic, shift, drift, hot set, scan, cold start, mixed); three raw formats (legacy CSV, nested API JSON, Kafka JSON) + item/user metadata; injected anomalies with ground truth |
| Big Data | Spark batch Bronze/Silver/Gold with quarantine and DQ verification; Spark RDD and SQL demonstrations; Spark window features; Spark MLlib LR + RF with save/reload/export; Kafka producer/consumer; Spark Structured Streaming (3 checkpointed queries, watermark windows); HBase wide-column store via REST; reconciliation and repair |
| Algorithms | FIFO, LRU, LFU (O(1)), Belady MIN (offline oracle, O(log C)), ML-guided and hybrid policies with fallback; simulation engine with invariant checks |
| ML | leakage-safe features/labels/splits, scikit-learn and Spark MLlib trainers, portable NumPy runtime with parity checks |
| Experiments | 13 experiments with stored results and a generated results document |
| UI | Streamlit dashboard, 8 pages, provenance labels |
| Ops | native service manager with memory guard, health checks, Makefile, CLI, Docker Compose |

## 2. Verification summary

All of the following were executed in this environment on the final code, after the
independent correctness audit ([FINAL_CORRECTNESS_AUDIT.md](FINAL_CORRECTNESS_AUDIT.md);
commands and counts: [TEST_REPORT.md](TEST_REPORT.md) §0):

| What | Evidence |
|---|---|
| Cache algorithms: invariants, naive-reference agreement, exhaustive Belady optimality, prefix-invariance (no future access) | 207 unit/property tests pass; audit: 12 000 further reference cases, DP-optimal Belady, no-future check on the real trace — 0 mismatches |
| ML: leakage guards, purged chronological splits, validation-only selection, export parity, fallbacks; no Gold statistic used as a feature | same; audit recomputed the stored test metrics exactly; Spark and online features now bit-identical |
| ETL vs ground truth (Spark and an independent pure-Python reference) | Spark suite 24/24; audit re-derived the 99 629-row Silver exactly |
| Real Kafka, real HBase, streaming event time | integration suite 41 + 1 environment error (Kafka file re-run 5/5); new live event-time test passes |
| Full pipeline Kafka → Spark Structured Streaming → Silver/Gold → HBase → MLlib → simulator | e2e test passed; `make pipeline-demo` 19/19 checks (isolated namespace, windows recounted from Silver) |
| Failure recovery (6 scenarios on live services) | e2e failure test passed on the final code (6/6 scenarios); Exp. 12 stored results |
| Static checks | Ruff clean, mypy clean (51 files) |

Not verified: the Docker Compose stack (never run), anything multi-node. The Kafka test
file is flaky on this laptop when the broker freezes (passed in 4 of 6 executions; every
failure coincided with a logged stall).

## 3. Research findings (synthetic data)

See [EXPERIMENTS.md](EXPERIMENTS.md) and [EXPERIMENT_RESULTS.md](EXPERIMENT_RESULTS.md).

* The learned policy beats LRU on 9 of 10 workloads and the best classical policy on 7 of
  10 (+2.8 to +13.1 pp), consistently across seeds and capacities 25–800.
* It does not help on scan-heavy (−1.1 pp vs LFU), hot-set (tie) and uniform (no signal)
  workloads, and it degrades more than LRU right after an unseen popularity shift.
* A large gap to Belady remains.
* Logistic regression is as good as a 40-tree random forest for this task and ~40× cheaper
  per prediction (≈ 3 µs vs ≈ 38 µs per request in total).

**Big Data pipeline (measured on the 8 GB laptop, native Kafka 4.1.2 / Spark 4.0.4 / HBase 2.6.7)**

* Batch ETL is verified against generator ground truth on every run: for the 100k-event
  project dataset, Silver = 99 629 rows, 506 quarantined, all 20 000 Kafka events present,
  checksum and per-reason quarantine counts identical to the ground truth.
* Spark Silver processes 1 M events in 72 s; using 4 instead of 200 shuffle partitions is
  5.5× faster, broadcasting metadata 1.4× faster, and column pruning + predicate pushdown
  turns a 3.3 s read into 0.2 s (Exp. 10).
* Streaming: Kafka append → durable Bronze p50 1.8 s / p95 3.1 s at 400 events/s with a 3 s
  trigger; HBase REST puts limit the Silver path to ≈ 1 900 rows/s (Exp. 11).
* Six failure scenarios (duplicates/malformed/late, SIGKILL, broker restart, HBase outage,
  injected partial write, idempotent re-drain) all end with every Kafka offset exactly once
  in Bronze, every event in Silver and HBase, and no incomplete micro-batch (Exp. 12).
* The exported NumPy model and the original Spark MLlib batch predictions produce
  identical cache decisions (pipeline demo; on the 100 k dataset after the audit's
  bit-identical-feature fix); a single-row prediction through Spark takes ≈ 62 ms versus
  ≈ 1 µs for the exported LR — the reason the export exists.

## 4. Limitations

**Research validity**
* All data is synthetic. The workload shapes are realistic in form, but no claim is made
  about production CDN traffic. Unit-size items and object hit ratio only (LRB optimises
  byte miss ratio for variable sizes).
* CacheMind adapts rather than reproduces LRB (classification within *H* instead of
  regression on time-to-next-request; fewer features; per-request training samples; LR/RF
  instead of GBM; no online retraining) — see RESEARCH_ALIGNMENT.md.
* The pre-registered *H* range was slightly too narrow for stationary workloads
  (≤ 0.9 pp left on the table, Exp. 13).
* After an unseen popularity shift the learned policy temporarily performs worse than LRU
  (Exp. 5): models are trained once and are not retrained online.
* Logistic regression is poorly calibrated on the test block (ECE ≈ 0.29) because the
  cumulative-frequency feature grows with trace age and, in ≈ 28-hour traces, the test block
  contains unseen hours of day (Exp. 14). Ranking (what eviction uses) is good, but the
  hybrid's blend relies on probabilities.

**Engineering**
* Single-node everything: one Kafka broker (replication factor 1), standalone HBase on
  the local filesystem, Spark `local[2]`. No security (no TLS/SASL/auth) — development
  only, all ports bound to 127.0.0.1.
* No distributed transaction between Parquet and HBase; consistency relies on idempotent
  writes, manifests and `reconcile --repair` (STREAMING_SEMANTICS.md).
* The provisional streaming Silver can contain cross-micro-batch duplicates; the batch
  Silver rebuild is authoritative.
* A streaming namespace (topic + checkpoints) is one event-time timeline: the watermark is
  global, and every synthetic dataset starts on 2026-01-05, so a second dataset streamed
  into the same namespace behind the first is late and absent from the windows. Demos,
  tests and experiments use their own namespaces (STREAMING_SEMANTICS.md).
* The stored Spark models of the 100 k dataset predate the feature-parity fix; their online
  and batch decisions can differ on ≈ 0.7 % of random-forest predictions until
  `make spark-mllib` is re-run.
* Docker Compose (Profile C on a bigger machine) was syntax-validated but **never run**.
* On the 8 GB development Mac, JVM stalls of 20–57 s were observed under memory pressure
  and once after the laptop slept; a Kafka broker briefly fenced itself both times and
  recovered. Timing-sensitive tests can be flaky there.
* Spotlight indexing (`corespotlightd`, up to ~1.4 GB RSS and 170 % CPU) competed with the
  workloads during the session; the `.metadata_never_index` markers did not stop it.
* No optional importer for public traces was built (the brief marked it optional).

## 5. Recommended next steps

1. Add the project folder to *System Settings → Siri & Spotlight → Spotlight Privacy*
   (a user decision; not changed by us) and re-run Exp. 9 on an idle machine.
2. Run `infra/docker/docker-compose.yml` on a 16 GB+ machine or a small Linux VM and record
   the results in TEST_REPORT.md (the stack has not been executed in containers yet).
3. Widen the *H* grid (e.g. 5–40× capacity) and add a regression target on
   log(time-to-next-request), as LRB does; compare good-decision ratio as well as hit rate.
4. Periodic retraining on a sliding window (batch, scheduled) to address the
   post-shift dip; measure recovery with Exp. 5.
5. Variable-size objects and byte hit ratio; an importer for a public trace (e.g. a
   Wikipedia CDN trace) to test beyond synthetic data.
6. Replace the cumulative count with a decayed/windowed one (stationary) and bucket or
   drop hour-of-day for short traces; or recalibrate on the validation block before
   using probabilities in the hybrid (Exp. 14).
