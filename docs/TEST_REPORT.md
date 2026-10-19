# Test report

Environment: Apple M3, 8 GB RAM, macOS 26.7.1, Python 3.12.4, Temurin JDK 17.0.20.1,
PySpark 4.0.4, Kafka 4.1.2 (native KRaft), HBase 2.6.7 (native standalone). Dates are UTC,
2026-10-18/19.

**Rule used for this report:** a test counts as passed only if it was executed here and
passed. Skipped tests are listed as not verified.

## 0. Final correctness audit — re-verification on the final code (2026-10-19/20)

The independent audit (`FINAL_CORRECTNESS_AUDIT.md`) fixed six defects (items 30–35 below)
and re-ran everything on the resulting code. These results supersede §1.

| Suite | Command | Result |
|---|---|---|
| Unit + property | `make test` | **207 passed** (201 + 6 new: watermark-drop metric, health current-run vs history, error classes, demo isolation, demo window check, runtime-dir override) |
| Spark (no services) | `make test-spark` | **24 passed** (18 + 6 new event-time tests; feature parity tightened to bitwise; online-path vs Spark-batch parity on every row) |
| Integration, live Kafka + HBase + Spark | `make integration-test` | **41 passed, 1 error** (fixture setup of `test_kafka.py::test_producer_restart_reuses_event_ids`: `Broker transport failure` during a logged 55–56 s Kafka JVM freeze); `test_kafka.py` re-run alone: **5 passed** |
| Live streaming event-time test (new) | `pytest tests/integration/test_streaming_kafka.py` | **1 passed** (also passed inside the integration run) |
| End-to-end pipeline (isolated) | `pytest tests/end_to_end/test_pipeline.py -m e2e` | **1 passed** |
| Failure injection, 6 scenarios | `pytest tests/end_to_end/test_failure_recovery.py -m e2e` | first run: S1 failed (item 36), S2–S6 passed; after the fix: **1 passed — all 6 scenarios** (342 s) |
| Final code (after two self-review fixes) | `make test`, `make test-spark`, live streaming test, e2e test | 207, 24, 1, 1 passed; lint/types clean |
| Resume of the existing shared checkpoints (on a copy) | `cachemind stream run --available-now --no-hbase` | all 3 queries resume; nothing new to process; copy byte-identical |
| Pipeline demo | `make pipeline-demo` | **19/19 checks** (`reports/pipeline/e2e-20261019T182336-69d9/evidence.json`) |
| Lint / types | `make lint`, `make typecheck` | Ruff clean (80 files); mypy clean (51 files) |
| Independent audits (scripts, not part of the suite) | see audit §§ 7–9 | Silver re-derivation exact; ML metrics recomputed exactly; 12 000 policy cases + DP Belady + real-trace no-future check: 0 mismatches |

## 1. Summary (runs at the end of the build session, 2026-10-19 — superseded by §0)

| Suite | Command | Result |
|---|---|---|
| Unit + property (groups A, B, C, D-Python, L) | `make test` | **201 passed** |
| Performance regression (complexity) | `pytest tests/performance` | **6 passed** |
| Spark (groups D, E, F, Spark-side B/C, streaming writers) | part of `make integration-test` | **18 passed** |
| HBase integration (group I, live HBase 2.6.7) | part of `make integration-test` | **12 passed** |
| Kafka integration (group G, live Kafka 4.1.2) | `CACHEMIND_REQUIRE_INFRA=1 pytest tests/integration/test_kafka.py` | **5 passed** on re-run (see note) |
| End-to-end pipeline (group J) | `make e2e-test` → `test_pipeline.py` | **passed** |
| Failure injection on live services (group K) | `make e2e-test` → `test_failure_recovery.py` (6 scenarios) | **passed** |
| Pipeline demo with stored evidence | `make pipeline-demo` | **16/16 checks** (`reports/pipeline/e2e-20261019T062909-a030/evidence.json`) |
| Lint / format | `make lint` | clean (77 files) |
| Types | `make typecheck` | no issues in 51 source files (dashboard excluded from mypy) |
| Docker Compose | `make compose-config` | valid; **stack not run** |

Unit/property coverage of the algorithmic and ETL modules: contracts 94 %, generator 93 %,
raw formats 87 %, workloads 92 %, features 97 %, model export 92 %, splits 86 %, policies
92–100 %, engine 87 %, trace 72 %. (Package total 44 %: the Spark, Kafka, HBase, dashboard and
experiment modules are exercised by the integration/e2e suites and experiment runs instead.)

**Note on the Kafka file.** In the second final sweep the integration suite reported 33
passed, 1 failed, 1 error — both in `test_kafka.py`, while the broker was frozen (see §2,
items 8, 21, 29). The file was re-run immediately with the machine awake: 5/5 passed. Over
the whole session the Kafka file passed in 4 of 6 executions; every failure coincided with
a logged broker stall. This is reported as environment-induced flakiness, not hidden.

**Environment during the final runs.** Spotlight (`corespotlightd`) used up to 177 % CPU,
`duetexpertd` ~95 %, the laptop ran on battery, and the Kafka JVM showed recurring
whole-process freezes of ~50–57 s roughly every 3 minutes (GC log pauses of 55.9 s and
56.4 s; broker log silent for 52 s with no GC pause). Not system sleep (power log), not
GC alone; correlated with memory compression of the JVM (RSS ≈ 140 MB for a 384 MB heap)
and heavy background I/O. Root cause not determined. Suites that normally take 1–6 min
took 12–16 min.

## 2. Failures found during testing and their root causes

Every failure below was reproduced, root-caused and fixed; no assertion was weakened.

| # | Failure | Root cause | Fix |
|---|---|---|---|
| 1 | Generator: duplicate metadata pass | metadata writer re-ran the whole trace | reuse the generator state |
| 2 | Spark/Python validation disagreement risk (numbers vs strings, nested objects) | Spark reads JSON tokens into StringType as literal text | Python validation rewritten to operate on the JSON text of each field; nested non-objects treated as malformed in both |
| 3 | `min_event_time` shown 5 h 30 m late | PySpark `collect()` converts timestamps to the process time zone | `TZ=UTC` + `-Duser.timezone=UTC` in Spark processes |
| 4 | SciPy `OptimizeWarning: iprint` | pip resolved SciPy 1.18 for scikit-learn 1.6 | pinned SciPy 1.15.3 |
| 5 | Heuristic frequency baseline ROC-AUC 0.5 | clipping made it constant | Poisson-rate estimates |
| 6 | HBase REST could not reach master | macOS hostname resolves to an external IP | `hbase.master.hostname` / `hbase.unsafe.regionserver.hostname=localhost`, IPv4 |
| 7 | HBase master exited | two hostname properties are mutually exclusive | removed the reverse-DNS flag |
| 8 | Kafka consumer-restart test failed (0 messages) | broker fenced after a ~57 s JVM stall (GC log `Real=57.63s`, `User=0.03s`) under memory pressure; LeaveGroup timed out | dev `broker.session.timeout.ms=30000`; the second consumer waits for the rebalance (assertions unchanged) |
| 9 | Window repair wrote 16-digit qualifiers | Spark timestamps read back as ns, divided as if µs | explicit `timestamp[ms]` cast; `window_column` now rejects non-ms values |
| 10 | Overhead experiment could not be saved | mixed int/str column | typed `run_kind` column |
| 11 | Dashboard path test | `dataset_id=../../x` stayed inside the base dir (not an escape) but was accepted | explicit identifier validation in all loaders |
| 12 | Gold/SQL pandas comparisons | Series compared with different row order | order-independent exact comparison helper |
| 13 | Simulator reported whole-run evictions next to window-only hits/misses | inconsistent window | added `evictions_measured` (window) and kept `evictions` (whole run); test added |
| 14 | Experiment 10 crashed after the Bronze stage | counted Silver rows before Silver existed | each stage records rows from its own report |
| 15 | E2E test: `hbase-publish` `TypeError: float(None)` | single-day dataset → empty trend table → `None`, but the NaN check only caught NaN | `pd.isna` for optional fields |
| 16 | `make spark-sql` / `make hbase-publish` failed on the project dataset | the `experiments` view merged every results file (incompatible schemas); publish expected cache columns in all files | shared schema-based selection of cache-policy results + typed projection |
| 17 | HBase `item_statistics` rows overwritten across datasets | key was `item_id`, but item ids repeat across datasets | key = salted `dataset_id#item_id`; windows grouped by dataset too; test asserts separation |
| 18 | Streaming window query refused to restart (`STATE_STORE_KEY_SCHEMA_NOT_COMPATIBLE`) after 17 | stateful aggregation key changed | versioned query/checkpoint/output (`gold_windows_v2`), replay from Bronze; old checkpoint kept; schema check **not** disabled |
| 19 | Pipeline demo: Kafka health "Unknown partition" | laptop slept ~7 h; on wake the broker was fenced for a few seconds | broker recovered by itself; re-run passed; long jobs now run under `caffeinate -i` |
| 20 | Exp. 11 reported live latency over backlog rows too | latency statistics included the 20 000 pre-produced rows | latency computed only on rows appended during the live phase (by Kafka timestamp); re-run |
| 21 | Exp. 12 crashed creating a scenario topic (`TimeoutError`) | broker fenced repeatedly (~every 3 min) while on battery with the display off; create request timed out | `ensure_topic` retries and verifies against cluster metadata; long runs under `caffeinate -dim` |
| 22 | Exp. 12 crashed in cleanup (`PermissionError` from `killpg`) | macOS returns EPERM for an empty process group | tolerate EPERM/ESRCH, then verify no group member survives |
| 23 | Exp. 12 results not saved (`Cannot write struct ... with no child field`) | an empty dict column | nested values stored as JSON text |
| 24 | `infra down hbase` failed mid-scenario | `hbase-daemon.sh stop` exceeded the 120 s subprocess timeout and the exception escaped | bounded wait, then SIGTERM/SIGKILL and verification |
| 25 | HBase master aborted during Exp. 12 | ZooKeeper WAL fsync took 34 s (disk starved) → 10 s ZK session expired → master abort | dev-only ZK settings: session 90 s (incl. `zookeeper.session.timeout.localHBaseCluster`), `forceSync=no`; negotiated timeout verified as 90 000 ms |
| 26 | Exp. 12 scenario 1 did not exercise late data (0 flagged) | events were delayed ≈ 75 s of event time, within the 2-minute allowance — correct behaviour but no evidence | delay raised to ≈ 150 s; explicit check `late_events_flagged`; 31 of 78 delayed events flagged |
| 27 | Final sweep: failure-recovery e2e test errored in `infra down hbase` | after SIGTERM timed out, the HBase JVM needed > 10 s to die even after SIGKILL (blocked in I/O on a starved disk) | `_terminate` waits up to 60 s after SIGKILL and treats zombies as exited |
| 28 | Final sweep: pipeline e2e test failed ("HBase REST not reachable") | cascade — the failed recovery test above had left HBase stopped | the failure-scenario runner restores Kafka and HBase in `finally` |
| 29 | Final sweep: Kafka consumer-restart test failed again (second consumer got 0 messages) | JVM stalls of up to ~56 s (GC log `55908 ms`) during the sweep delayed the group rebalance beyond the 30 s wait | rebalance wait raised to 90 s; the assertions (exactly the remaining 120 messages) are unchanged |
| 30 | (audit, D1) Demo datasets 100 % late, 0 streaming-window rows | `make pipeline-demo` streamed into the shared namespace; all synthetic datasets start 2026-01-05 and the watermark is global per namespace | each demo run gets its own data dir, topic and streaming status; 3 new evidence checks recount the windows |
| 31 | (audit, D2) On-time events flagged late / missing from windows | Bronze wrote one file per Kafka partition; `maxFilesPerTrigger` split Bronze batches | Bronze coalesced to one file per micro-batch; live test asserts whole batches and exact windows |
| 32 | (audit, D3) `dropped_by_watermark` always 0 | recorder read only the first stateful operator; drops happen in `dedupeWithinWatermark` | sum over all operators; per-run `event_time` summary |
| 33 | (audit, D4) `RpcEndpointNotFoundException … StateStoreCoordinator` | JVM-wide `StateStore` outlived `SparkSession.stop()` | stop queries → state store → session |
| 34 | (audit, D5) `terminated_with_error: 36` shown as health | mixed history of all runs (failure injections) in the last 500 lines | per-run health vs history by error class; isolated runtime dirs |
| 35 | (audit, D6) RF online decisions ≠ Spark batch (0.66 % of predictions) | JVM vs C-library `log1p/sin/cos` differ by 1 ulp; RF thresholds equal data values | one Python function for all float feature transforms in both runtimes; bitwise parity test |
| 36 | (audit) Exp. 12 S1 flagged 0 late events after fix 31 | the scenario relied on three Bronze files per batch to get two Silver micro-batches; with one file per batch the whole backlog was one micro-batch (nothing is late within one) | S1 drains with two files per trigger, reproducing the original grouping; the `late_events_flagged` check is unchanged |
| 37 | (audit) New live streaming test: `Failed to find data source: kafka` inside the full suite | an earlier test had started the JVM without the Kafka connector | the test runs the streaming job as its own process (as `make streaming-once`) |

## 3. Not verified in this environment

| Item | Why not | What is needed | How to verify |
|---|---|---|---|
| Docker Compose stack (`infra/docker/docker-compose.yml`) | Docker daemon not running; starting Docker Desktop's VM on this 8 GB machine was judged unsafe | Docker host with ≈ 6 GB for the VM (16 GB machine or Linux VM) | `docker compose -f infra/docker/docker-compose.yml up -d kafka hbase`, then `... run --rm app python -m cachemind pipeline-demo`; expect `all_checks_passed: true` |
| Multi-broker Kafka / distributed HBase / Spark cluster behaviour (replication, region splits, executor loss) | single-node development setup by design | a real cluster | not in scope of the provided tests |
| Production traffic | no production traces available | a public CDN trace importer | out of scope (optional in the brief) |
| Overhead numbers on an idle machine | Spotlight indexing used 1–2 cores throughout the session | exclude the folder from Spotlight (user setting) | `make benchmark` |

Skipped tests: none in the final runs below — every infrastructure test was executed with
`CACHEMIND_REQUIRE_INFRA=1`, under which an unreachable service is a failure, not a skip.
