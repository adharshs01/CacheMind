# Experiments: design, protocol and findings

All numbers quoted here come from [EXPERIMENT_RESULTS.md](EXPERIMENT_RESULTS.md), which is
generated from the stored outputs in `reports/experiments/` by
`python -m cachemind.experiments.report`. All traces are **synthetic**.

## Protocol (applies to every cache experiment)

1. **Identical inputs.** Every policy receives the same `keys` array (the trace ordered by
   `(seq, event_id)`) and the same capacity (in items; unit-size items).
2. **Chronological split.** Positions 0–60 % train, 60–80 % validation, 80–100 % test.
   Labels whose horizon crosses a block boundary are purged.
3. **Same warm-up.** Every policy processes the whole trace; hits/misses are counted only on
   the test block. Belady sees the whole trace (offline oracle) and is reported as a
   reference.
4. **Selection on validation only.** For the ML policies: label horizon
   *H* ∈ {2, 5, 10} × capacity (by validation hit rate of ML-LR), model hyper-parameters
   (validation PR-AUC), hybrid α ∈ {0, 0.25, 0.5, 0.75, 1} (validation hit rate). The test
   block is evaluated once per configuration.
5. **Seeds.** Three seeds per configuration (traces and models differ per seed); we report
   mean, standard deviation, and **paired** per-seed differences (same trace, same capacity).
6. **Runtime.** Experiments 1–9 use scikit-learn models evaluated through the exported
   NumPy scorer (Profile A). The Spark MLlib runtime is exercised on the Big Data pipeline
   (pipeline demo), where exported and Spark-batch predictions are compared.

## Experiments

| # | Question | Design | Output |
|---|---|---|---|
| 1 | FIFO vs LRU vs LFU | 10 workloads × 3 seeds, capacity 100 (2 % of a 5 000-item catalogue), 100 000 requests | `exp01_classical` |
| 2 | ML-guided vs classical | same grid; ML-LR, ML-RF, hybrid-LR, hybrid-RF, Belady | `exp02_ml_vs_classical` |
| 3 | Capacity | zipf + mixed, capacities 25…800, *H* re-selected per capacity, LR only | `exp03_capacity` |
| 4 | Workload distributions | per-workload view of the grid; best online policy per workload | `exp04_workloads` |
| 5 | Sudden popularity shift | complete re-ranking at 90 % of the trace (inside the test block — never seen in training); windowed hit rate, dip and recovery | `exp05_shift` |
| 6 | Concept drift | drift workload; windowed hit rate; LR ROC-AUC on successive test windows | `exp06_drift` |
| 7 | LR vs RF | classification metrics (ROC-AUC, PR-AUC, F1, Brier, ECE), non-ML baselines, cache hit rates | `exp07_lr_vs_rf` |
| 8 | Hybrid | α chosen on validation; test hit rate vs ML and LRU | `exp08_hybrid` |
| 9 | ML overhead | zipf, 5 timed runs after 1 warm-up; µs per request split into features / inference / eviction decision; tracemalloc peak in a separate run | `exp09_overhead` |
| 10 | Spark batch performance | Bronze/Silver/Gold/features at 20k, 100k, 1M events; shuffle partitions 4 vs 200; broadcast vs shuffle join; column pruning + pushdown with plan evidence | `exp10_spark_performance` |
| 11 | Streaming throughput | backlog drain + live run at a fixed producer rate; Kafka→Bronze latency from real timestamps | `exp11_streaming_throughput` |
| 12 | Failure recovery | duplicates/malformed/late, SIGKILL of the streaming job, Kafka restart, HBase outage, injected partial failure, idempotent re-drain — each verified on the data | `exp12_failure_recovery` |

## Findings — cache experiments (1–9)

Scale: 100 000-request traces, 5 000-item catalogue, capacity 100 unless stated, 3 seeds.
"pp" = percentage points of hit rate on the test block.

### 1. Classical baselines behave as theory predicts
* LRU beats FIFO on every skewed workload (e.g. zipf 0.305 vs 0.263).
* In-cache LFU (no aging) is the best classical policy on stationary skew (zipf 0.417,
  bursty 0.419, periodic 0.387, hot set 0.807, scan 0.229) but **collapses** whenever
  popularity moves: shift 0.031, drift 0.014, cold start 0.025, mixed 0.037. Frequencies
  accumulated by items that are no longer popular keep them resident forever.
* On the uniform workload every online policy is at the theoretical ≈ 2 % (capacity /
  catalogue); only the offline Belady (0.185) does better.
* Sanity check that came out of the data: FIFO/LRU/Belady give *identical* numbers on
  `zipf` and `shift` because, after the 50 % shift, the test block is an exact relabelling
  of the zipf block — label-invariant policies must score the same.

### 2. ML-guided vs classical (the main question)
* ML-LR beats LRU on 9 of 10 workloads (+1.2 to +14.0 pp mean; every seed positive; the
  exception is `uniform`) and
  beats the **best** classical policy on 7 of 10: zipf +2.8, bursty +4.8, periodic +5.2,
  mixed +9.0, cold start +9.0, drift +10.4, shift +13.1 pp (min–max across seeds all
  positive).
* It does **not** help on: `scan` (−1.1 pp vs LFU, consistent across seeds), `hot_set`
  (−0.01 pp, a tie with LFU) and `uniform` (−0.16 pp vs best; models score ROC-AUC 0.50
  there — there is nothing to learn).
* The remaining gap to Belady is large: e.g. zipf 0.445 vs 0.525, cold start 0.231 vs 0.349.
* ML-RF is within about ±1.1 pp of ML-LR everywhere: better on cold start (+1.1 pp) and
  drift (+0.2), worse on zipf (−1.1), periodic (−1.0) and bursty (−0.9).

### 3. Capacity
On zipf and mixed the ML advantage holds at every capacity from 25 to 800 items:
vs the best classical policy +1.8 → +3.3 pp (zipf, growing with capacity) and
+5.6 → +9.6 pp (mixed). The advantage over LRU shrinks as the cache grows (zipf +14.4 pp at
25 → +9.4 pp at 800) because every policy approaches Belady at large capacities.

### 4. Workload distributions
Best online policy per workload: ML (LR or RF) on 7 workloads, LFU on scan and hot set,
LFU/ML/RF within noise on uniform. No single classical policy is best everywhere; LFU is
best or worst depending on whether popularity moves.

### 5. Sudden shift inside the test block (never seen in training)
Over the whole test block ML still wins (ML-LR 0.403, ML-RF 0.417, LRU 0.304,
LFU 0.224). But **immediately after the shift ML-LR drops below LRU** (windowed minimum
0.213 vs 0.275) and needs ≈ 7 000 requests to recover 90 % of its pre-shift hit rate,
versus ≈ 1 000 for LRU and ≈ 2 700 for ML-RF; LFU never recovers. Frequency-type features
(cumulative and 1 000-request counts) keep stale popularity for a while — a real weakness
of a model trained once.

### 6. Concept drift
On the gradual-drift workload ML keeps a +10.4 pp lead over the best classical policy, and
the LR model's ROC-AUC stays between 0.886 and 0.904 across the whole test block. The
features describe *relative* recency/frequency rather than item identities, so the learned
rule transfers as items rise and fall. (A faster or qualitatively different drift could
behave differently; not tested.)

### 7. Logistic regression vs random forest
Classification quality is close: e.g. zipf ROC-AUC 0.937 (LR) vs 0.930 (RF), cold start
0.821 vs 0.842. Both beat the non-ML recency baseline (zipf 0.890) and frequency
baseline (0.877) on most workloads; on `hot_set` they only tie the frequency baseline
(0.996) and on `mixed`/`scan` the margin over the recency baseline is small (≤ 0.004).
Both are at chance on uniform (0.49–0.50).
Better classification does not always mean a better cache: RF's higher AUC on cold start
gives +1.1 pp hit rate, but on zipf LR is better in both. RF costs ~35 µs per prediction
vs ~1 µs for LR (Exp. 9).

### 8. Hybrid
Validation chose α between 0.75 and 1 on almost every workload (pure ML or nearly), so the
hybrid adds little: it is within ±0.3 pp of the corresponding ML policy everywhere. A
blend with recency did not improve on the learned ranking in these traces.

### 9. ML inference overhead (median of 5 runs, µs per request, zipf, capacity 100)
LRU 0.59, LFU 0.78, ML-LR 3.10 (of which ≈ 0.9 features, ≈ 0.8 inference), ML-RF 38.3
(≈ 35 inference), hybrid-LR 8.5 (dominated by its O(C) eviction scan). Run-to-run spread is
small (e.g. ML-LR 3.06–3.45, ML-RF 38.1–38.6). Peak Python heap allocated during a
simulation (tracemalloc, includes engine bookkeeping): LRU ≈ 6.1 MiB, LFU ≈ 5.8 MiB,
ML/hybrid ≈ 12.7 MiB (adds the per-item feature tracker). Measured on the development laptop
with Spotlight indexing in the background (≈ 1 core); see EXPERIMENT_RESULTS.md for the
min/max spread.

### 13. Post-hoc: sensitivity to the label horizon H (does not change results 1–8)
Because the largest pre-registered *H* (10 × capacity) won on 8 of 10 workloads, ML-LR was
re-evaluated with *H* ∈ {2, 5, 10, 20, 40} × capacity (3 seeds, capacity 100):

| workload | 10× (test) | 20× (test) | 40× (test) | observation |
|---|---|---|---|---|
| zipf | 0.4448 | 0.4522 | 0.4535 | +0.9 pp available beyond the searched range |
| bursty | 0.4664 | 0.4738 | 0.4739 | +0.7 pp available |
| drift | 0.4050 | 0.3873 | 0.3503 | 10× is the optimum; larger *H* hurts |

Validation ranks the horizons in the same order as the test block on all three workloads
(absolute differences ≤ 0.6 pp), so choosing *H* on validation is reliable; the pre-registered range was slightly too narrow for stationary
workloads (cost < 1 pp) and correct for drift. A wider grid is recommended for future runs.

### Caveats that apply to all of the above
* Synthetic traces; unit-size items; object hit ratio (not byte hit ratio).
* *H* was selected from {2, 5, 10} × capacity and the largest value won on 8 of 10
  workloads — the search range may have been too narrow (see Experiment 13).
* Models are trained once per trace; no online retraining.
* The recorded `code_version` of this run was computed when results were saved, after
  purely cosmetic lint edits made during the run (the hash is now taken at process start).

## Findings — Big Data experiments (10–11)

### 10. Spark batch performance (single laptop, `local[2]`, 1 GB driver)
| events | Bronze | Silver | Gold | features |
|---|---|---|---|---|
| 20 000 | 0.8 s | 3.5 s | 2.2 s | 0.4 s |
| 100 000 | 1.4 s | 10.1 s | 3.0 s | 0.8 s |
| 1 000 000 | 3.7 s | 72.2 s | 7.5 s | 4.7 s |

(20k/100k: median of 3 runs after a warm-up; 1M: one run.) Silver dominates: it parses
three formats, applies 12 validation rules, runs two window-based duplicate checks and
two joins. Cost grows roughly linearly from 100k to 1M (×7.2 for ×10 data).

Controlled comparisons on 100k events (median of 3 after warm-up):
* **Shuffle partitions 4 vs 200** (Spark's default; AQE off for the comparison): Silver+Gold
  9.4 s vs 51.9 s — 5.5× faster. 200 tiny tasks per shuffle are pure overhead on 2 cores.
* **Broadcast vs shuffle (sort-merge) metadata join**: 7.5 s vs 10.2 s for Silver — the item
  and user tables are small, broadcasting avoids shuffling the request data.
* **Column pruning + predicate pushdown**: reading only the needed columns with the filter
  pushed into the Parquet scan took 0.20 s vs 3.3 s for caching all columns first (the
  anti-pattern). The physical plan shows `PushedFilters: [IsNotNull(region),
  EqualTo(region,eu-west)]` and a pruned `ReadSchema`.

### 11. Kafka → Spark streaming throughput and latency (native Kafka 4.1.2, Spark 4.0.4, HBase 2.6.7, one laptop)
* Producer: 20 000 events in 0.6 s (≈ 35 000 events/s, idempotent, `acks=all`).
* Backlog drain (available-now, 2 000 offsets per micro-batch): 20 000 events through
  Bronze → provisional Silver + HBase → windows + HBase in 36 s including JVM start-up
  (≈ 550 events/s end to end). Per query, median processed rate: Bronze ≈ 7 500 rows/s,
  Silver+HBase ≈ 1 900 rows/s (HBase REST puts are the bottleneck), windows ≈ 2 400 rows/s.
* Live run at 400 events/s for 45 s with a 3 s processing-time trigger: Kafka append →
  micro-batch start p50 1.5 s / p95 2.9 s; Kafka append → durable Bronze file p50 1.8 s /
  p95 3.1 s / max 4.2 s (measured on the 18 000 live rows from real timestamps).
* Verification after the run: Bronze holds every Kafka offset exactly once, every event
  reached the streaming Silver and HBase, reconcile reports no incomplete micro-batch.
* An earlier run of this experiment reported latency over all rows including the backlog;
  that was a measurement error (backlog drain delay is not live latency) and was fixed
  before the results above were produced.

### 12. Failure recovery (infrastructure; every scenario verified on the data afterwards)
| scenario | what was done | outcome |
|---|---|---|
| duplicates / malformed / late | 4 000 events + 119 duplicate sends + 55 truncated JSON + 78 events held back by 3 000 messages (≈ 150 s of event time) | Bronze = all 4 174 Kafka offsets exactly once; 4 000 distinct events in Silver and HBase; 55 `malformed_json` + 118 `exact_duplicate` quarantined; 1 duplicate crossed a micro-batch boundary (kept twice in the provisional Silver, once in HBase — documented behaviour); 31 of the 78 delayed events were flagged late (the others were within the 2-minute allowance relative to earlier micro-batches) |
| Spark process killed | `SIGKILL` of the streaming process group after ≥ 1 000 Bronze rows, 3 000 more events, restart | 6 000 offsets exactly once, 6 000 events in Silver and HBase, reconcile consistent |
| Kafka broker restart | broker stopped for 15 s while the query ran, restarted, 2 000 more events | no query restart was needed (the Kafka source retried); 4 000 offsets exactly once |
| HBase unavailable | HBase stopped while events arrived, restarted ~20 s later | Silver and window queries failed and were restarted from their checkpoints (2 times each in the stored run, 3 in an earlier identical run); afterwards all 3 000 events present in HBase |
| partial output failure | injected failure after the Parquet write and before HBase/manifest of micro-batch 0 | the inconsistent window was observed (Parquet present, manifest absent); the restart replayed the batch; HBase complete, reconcile consistent |
| re-drain without new data | available-now run with no new offsets | Bronze and Silver row counts unchanged (3 000 / 3 000) |

Since the final correctness audit, Bronze writes one file per micro-batch; the
duplicates/malformed/late scenario therefore drains with two files per Silver trigger,
which reproduces the micro-batch grouping above (Bronze batches 0+1, then 2). Re-run on the
final code (`tests/end_to_end/test_failure_recovery.py`): all six scenarios passed, and the
first scenario again flagged 31 late events with 4 174 offsets exactly once in Bronze.

### 14. Post-hoc: why is logistic regression poorly calibrated?
LR ranks well (ROC-AUC ≈ 0.94) but over-predicts reuse on the test block (zipf: mean
prediction 0.84 vs positive rate 0.56, ECE 0.29); RF is calibrated (ECE ≈ 0.1). Masking one
feature at a time (zipf and bursty, 3 seeds, capacity 100, *H* = 1 000):

| variant | ECE zipf | ROC-AUC zipf | ML-LR hit rate zipf | ECE bursty | ML-LR hit rate bursty |
|---|---|---|---|---|---|
| all features | 0.287 | 0.937 | 0.445 | 0.275 | 0.467 |
| without `f_freq_log` | 0.062 | 0.914 | 0.420 | 0.063 | 0.443 |
| without hour features | 0.094 | 0.934 | 0.440 | 0.093 | 0.463 |
| without `f_cnt1000` | 0.296 | 0.937 | 0.445 | 0.275 | 0.467 |

Two causes: (1) `f_freq_log` counts requests since the start of the trace, so it grows with
trace age — 7.1 % of test rows lie above the training 99th percentile and a linear model
extrapolates; (2) these ≈ 28-hour traces put hours of day into the test block that the
training block never saw. Removing the cumulative count fixes calibration but costs
≈ 2.4 pp hit rate — the feature carries real ranking signal, only its scale drifts. A
stationary replacement (decayed or windowed count, as LRB's EDCs) or recalibration on the
validation block are the appropriate fixes (not done here). The cache policies rank by
probability, so their hit rates are affected only through ranking, not calibration.
