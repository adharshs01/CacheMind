# ML design: reuse prediction for cache replacement

## 1. Task

At the moment request *t* for item *x* has been served, predict whether *x* will be
requested again within the next *H* requests (positions *t+1 … t+H*). Binary
classification; *H* is configurable and chosen on validation data from
{2, 5, 10} × cache capacity. Time is measured in requests (positions of the canonical
trace ordered by `(seq, event_id)`).

## 2. Features (schema `v1`)

All features of row *t* are computed from positions ≤ *t* only.

| Feature | Definition | Cold start |
|---|---|---|
| `f_gap1_log` | log1p(*t* − previous request of *x*) | log1p(10⁶) |
| `f_gap2_log` | log1p(gap between the two previous requests) | log1p(10⁶) |
| `f_cnt10`, `f_cnt100`, `f_cnt1000` | requests of *x* in [*t*−k+1, *t*] (current included) | 1 |
| `f_freq_log` | log1p(requests of *x* in [0, *t*]) | log1p(1) |
| `f_trend` | count in [*t*−99, *t*] − count in [*t*−199, *t*−100] | 1 |
| `f_first` | 1 if first request of *x* | 1 |
| `f_size_log` | log1p(item size) | — (size is required) |
| `f_hour_sin`, `f_hour_cos` | UTC hour of day on the unit circle | — |
| `category` | item category; `unknown` if metadata missing/invalid | `unknown` |
| `region` | request region | — |

No item or user identifier is a feature (prevents memorising ids). Missing metadata
maps to an explicit `unknown` category; categories first seen at serving time map to the
encoder's unknown slot (Spark: dedicated one-hot slot; scikit-learn: all-zero block).

The integer history is implemented twice:
* `ml/features.py` — an online tracker (bounded memory per item: the 1000-request window);
* `bigdata/features_spark.py` — Spark window functions (`lag`, `lead`,
  `rangeBetween` frames over `trace_pos` partitioned by item).

Both hand the integers (gaps, counts, total, first flag, size, event time) to ONE function,
`ml.features.feature_values`, which applies every floating-point transform (`log1p`,
`sin`, `cos`); Spark calls it through a pandas UDF. The two runtimes are therefore
**bit-identical**. Until the correctness audit, Spark used the JVM's `log1p/sin/cos`,
which differ from the C library's in the last bit for some inputs (20 653 of 99 629 rows
of the project dataset). Because random-forest thresholds are data values, 655 rows (0.66 %)
fell exactly on a split threshold on one side and 1 ulp past it on the other, so the
exported model on online features disagreed with the Spark batch predictions by up to
0.023 and made 3 different evictions out of 57 457 (capacity 100). Note that hour-of-day
sin/cos contain mathematically equal pairs (e.g. hours 1 and 11) that differ by 1 ulp, so
a comparison tolerance cannot fix this; only identical arithmetic can.

`test_spark_features_equal_python_online_features` asserts row-for-row **bitwise**
equality (previously `atol 1e-12`, which hid the difference) and identical
labels/censoring. `test_matches_brute_force_definition` checks the tracker against a
direct O(n²) definition.

## 3. Labels, censoring and splits

* `label[t] = 1` iff the next request of *x* is in (*t*, *t+H*].
* A row is *censored* when no later request exists and *t+H* runs past the end of the
  trace; censored rows are excluded.
* Chronological split 60 / 20 / 20 by position. A row is eligible for a block only if its
  whole label window lies inside the block:
  `train t ∈ [0, a−H)`, `val t ∈ [a, b−H)`, `test t ∈ [b, n−H)`.
  This purges rows whose labels would look into a later block and removes the
  end-of-block bias (positives observable, negatives not).
* Features may use history from earlier blocks — that information existed at prediction
  time.

## 4. Leakage prevention (tested)

| Risk | Control | Test |
|---|---|---|
| feature uses future requests | prefix-invariance check: features of the prefix must not change when the suffix is replaced | `test_online_features_are_prefix_invariant`; a deliberately leaky "whole-trace popularity" feature and "label as feature" are both **detected** |
| label window crosses a split | eligibility ranges + `assert_no_label_leakage` | `test_label_leakage_guard_rejects_boundary_rows` |
| preprocessing fitted on all data | encoders/scaler fitted on train rows only | `test_encoder_is_fitted_on_training_rows_only`; Spark pipeline `.fit(train)` |
| tuning on test | hyper-parameters, threshold, H and hybrid α chosen on validation; test scored once | `test_test_block_is_not_used_for_selection` |
| random record splits | not used anywhere (no `CrossValidator`, no `randomSplit`) | code review |
| online policy reads the future | engine passes the trace only to `requires_future` policies; prefix-invariance property tests for FIFO/LRU/LFU/ML/hybrid | `test_online_policies_are_prefix_invariant`, `test_learned_policies_are_prefix_invariant` |

## 5. Models

| Runtime | Models | Pipeline |
|---|---|---|
| Spark MLlib (Big Data path) | LogisticRegression (regParam ∈ {0, 0.001, 0.01}), RandomForestClassifier (40 trees, maxDepth ∈ {6, 10}, minInstancesPerNode 20) | StringIndexer(keep) → OneHotEncoder(dropLast=False) → VectorAssembler → [StandardScaler] → model |
| scikit-learn (lightweight profile) | LogisticRegression (C ∈ {0.01, 0.1, 1}), RandomForest (40 trees, max_depth ∈ {6, 10}, min_samples_leaf 20) | same assembled vector via shared `assemble_matrix` |

Selection metric: validation PR-AUC. Decision threshold: best F1 on validation.
Reported on test: precision, recall, F1 (at 0.5 and at the validation threshold), ROC-AUC,
PR-AUC, Brier score, log loss, expected calibration error with a 10-bin calibration table,
positive rate (class balance), predicted-positive rate. Two non-ML baselines are evaluated
on the same rows: a recency estimate `1 − exp(−H / gap)` and a frequency estimate
`1 − exp(−H · cnt1000 / 1000)`.

## 6. Artifacts, export and parity

Each model is saved as a versioned directory:
`<runtime>_<model>_h<H>_<version>/` with `metadata.json` (model type/version, feature
schema version, training dataset version, label horizon, configuration, search results,
all metrics, latency), `export.json` (portable spec), `parity.json`, and for Spark the
saved `spark_model/` plus `trace_predictions/`.

Spark models are **saved, re-loaded from disk, and every reported prediction comes from
the reloaded model** (save/reload max |Δp| is recorded; observed 0.0). The reloaded
pipeline is exported to a JSON spec evaluated by NumPy (`NumpyScorer`): logistic
regression coefficients + scaler, or the random-forest trees read from the *persisted*
Spark model files (`stages/*/data/*.parquet`). Binary one-hot columns that Spark treats
as categorical splits are converted to equivalent threshold splits.

`load_scorer` refuses an artifact unless its parity check passed (|p_export − p_spark| ≤
1e-6 on the first 2000 test rows; observed ≤ 2.3e-16) and its feature schema matches the
runtime. That check scores Spark-computed features with both runtimes, so it verifies the
*exporter*; `test_mllib_train_save_reload_export` additionally checks the *online path*
(NumPy features + export) against the Spark batch predictions on every row (≤ 1e-12).
Two inference paths are therefore available and compared:

* **original runtime** — Spark MLlib batch-scores the whole trace
  (`trace_predictions/`), replayed in order by `PrecomputedPredictor`;
* **exported runtime** — `OnlinePredictor` computes features online and scores them with
  the exported spec (~1 µs per call for LR, ~34 µs for RF).

The pipeline demo checks that both paths produce identical cache decisions. Spark models
trained before the audit (e.g. `artifacts/models/zipf-n100000-s42/spark_*`) were trained
on JVM-computed features and are kept unchanged; their online and Spark-batch decisions
can differ on ≈ 0.7 % of random-forest predictions (logistic regression: identical).
Re-running `make spark-mllib` rebuilds them on bit-identical features; doing so in an
isolated copy reproduced identical metrics and identical decisions (audit §7).

## 7. ML-guided and hybrid policies

One prediction per request (hit or miss), stored with the item:

* **ML**: on eviction, if the LRU-oldest item's prediction window has expired
  (*t* − last access > *H*) evict it; otherwise evict the item with the lowest stored
  probability (ties: older access, then smaller key). O(1) + O(log C) with a lazily
  invalidated heap.
* **Hybrid**: score = α·p_eff + (1−α)·exp(−age/τ), τ = H, p_eff = 0 once expired;
  α = 0 is exactly LRU (tested), α tuned on validation.
* **Fallback**: missing model, inference exception, NaN or out-of-range probability, or a
  schema mismatch → the prediction is marked invalid and evictions use LRU while any
  resident item has an invalid prediction. Every fallback is counted
  (`fallback_evictions`, `prediction_failures`) — tested for each failure mode.

## 8. Known limitations

* Classification within *H* discards information that a regression on time-to-next-request
  would keep (LRB found regression better).
* Training rows are sampled per request (biased toward popular items).
* Models are trained once per trace; no online retraining (see the drift experiment).
* Unit-size items and object hit ratio only; no byte-weighted objective.
