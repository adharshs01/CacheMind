# Environment audit

Audit performed on 2026-10-18 before any implementation, then updated with what
was learned while bringing services up. Every value below was observed on the
development machine; nothing is assumed.

## 1. Machine

| Item | Finding |
|---|---|
| OS | macOS 26.7.1 (build 25G241) |
| CPU | Apple M3, 8 cores, arm64 |
| RAM | 8 GB unified memory; 2 GB encrypted swap already ~0.8 GB used before the project started |
| Free memory during work | 1.7 – 2.4 GB "available" (psutil), 23 – 30 % |
| Disk | 460 GB volume, 287 GB free |
| Largest memory users (not ours) | `corespotlightd` 0.8 → 1.4 GB, VS Code helpers ~0.9 GB total, Neo4j and MongoDB running in the background |
| Ports already in use | 7474 / 7687 (Neo4j), 27017 (MongoDB). 9092, 9093, 2181, 8080, 8501, 16000, 16010, 16020 were free |

## 2. Software found

| Tool | Found | Used? |
|---|---|---|
| Python | 3.13.3 (python.org), 3.13.16 (Homebrew), 3.12.4 (MacPorts), 3.12.7 (Anaconda) | **3.12.4 (MacPorts) → project `.venv`** |
| Java | Homebrew OpenJDK 27 (default), OpenJDK 21 (keg-only) | **Neither** – see §3 |
| Docker | Docker Desktop 28.4.0 + Compose v2.39.2 installed, **daemon not running** | Compose file validated only |
| Git | 2.51.0 | – |
| Homebrew / conda | present | not used (no system-wide installs) |
| Spark / Kafka / HBase | not installed | fetched project-locally |

The MacPorts Python ships without a CA bundle for `urllib`; `scripts/fetch_tools.py`
therefore verifies TLS with `certifi` or the macOS system bundle `/etc/ssl/cert.pem`
(verification is never disabled).

## 3. Compatibility matrix (tested)

| Component | Version | Why this version |
|---|---|---|
| Python | 3.12.4 | Supported by every library below; avoids being first on 3.13 |
| JDK | **Eclipse Temurin 17.0.20.1+1** (project-local, `.tools/`) | The only JDK that *all three* JVM components document: Spark 4.0 supports 17/21; HBase 2.6 documents 17 (JDK 21 not listed); Kafka 4.x supports 17+. The machine's default Java 27 is unsupported by Spark/HBase |
| Apache Spark / PySpark | 4.0.4 | Mature 4.0 line (4.0.0 May 2025), latest patch; Scala 2.13; Python 3.9+ |
| Spark Kafka connector | `org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.4` | Must equal the Spark version and Scala binary version exactly; resolved via Ivy into `.runtime/ivy` (pulls kafka-clients 3.9.1) |
| Apache Kafka | 4.1.2 (Scala 2.13), KRaft mode | Released 2026-03; 4.1 is a mature line; ZooKeeper-free |
| Apache HBase | 2.6.7 (hadoop3 build), standalone mode | 2.6 line, JDK 17 documented; standalone runs master + RS + ZK in one JVM |
| confluent-kafka (librdkafka) | 2.11.1 | Idempotent producer, delivery reports, cp312 macOS arm64 wheel |
| NumPy / pandas / PyArrow | 2.2.6 / 2.2.3 / 18.1.0 | Versions PySpark 4.0 supports (pandas 3.0 deliberately avoided) |
| scikit-learn / SciPy | 1.6.1 / **1.15.3** | pip first resolved SciPy 1.18.1, which made scikit-learn 1.6.1 emit `OptimizeWarning: Unknown solver options: iprint`; pinned to a contemporary SciPy and verified warning-free |
| Streamlit | 1.50.0 | `st.navigation` multipage + `AppTest` for headless tests |
| psutil | 7.0.0 | Memory guard; PySpark spill support |
| pytest / Hypothesis / Ruff / mypy | 8.3.5 / 6.131.0 / 0.12.12 / 1.15.0 | Quality tooling |
| Docker images (Profile C) | `apache/kafka:4.1.2` (official Apache image); `eclipse-temurin:17-jre-noble` base for HBase and app images | Maintained, documented images; HBase tarball SHA-512-verified at build time |

`pip check` reports no broken requirements. The resolved environment is frozen in
`requirements.lock` (57 packages).

## 4. Downloads and integrity

`scripts/fetch_tools.py` downloads into `.tools/` only and verifies before extracting:

| Artifact | Source | Check |
|---|---|---|
| Temurin JDK 17 (macOS aarch64) | Adoptium API → GitHub release | SHA-256 from the Adoptium API |
| `kafka_2.13-4.1.2.tgz` | downloads.apache.org | SHA-512 from the Apache `.sha512` file |
| `hbase-2.6.7-hadoop3-bin.tar.gz` | downloads.apache.org | SHA-512 from the Apache `.sha512` file |

Extraction uses Python's `tarfile` `data` filter, which rejects path traversal.
Total: ~0.9 GB extracted plus 0.7 GB of cached archives (deletable).

## 5. Resource estimate and decisions

| Process | Configured heap | Observed RSS |
|---|---|---|
| Kafka broker (KRaft) | 320–384 MB | ~380 MB |
| HBase standalone master | 384–512 MB | ~550–700 MB |
| HBase REST gateway | 128–192 MB | ~200 MB |
| Spark driver (local[2]) | 768 MB – 1 GB | ~1.0–1.3 GB incl. Python workers |
| Streamlit dashboard | – | ~150–250 MB |

Running the full stack therefore needs roughly 2.5–3 GB *on top of* what the OS and
editor already use. On this machine that is at the limit of available memory.

Decisions taken because of that:

1. **Native processes, not Docker, on this laptop.** Docker Desktop's VM would reserve
   several GB permanently. Kafka and HBase run natively from `.tools/` with capped heaps;
   Docker Compose remains the reproducible deployment for a larger machine.
2. **Memory guard.** `cachemind.ops.infra` refuses to start a JVM when available memory is
   below `CACHEMIND_MIN_FREE_MEMORY_PCT` (default 20 %) or below 1.5× the requested heap.
3. **Sequential verification.** Services are stopped when not needed; Spark jobs, the
   experiment suite and service tests were run one after another, not concurrently.
4. **Spotlight exclusion markers.** `.metadata_never_index` files are placed in the
   generated directories so macOS indexing does not chase thousands of new files.
5. **Localhost only.** Every listener (Kafka, HBase ZK/master/REST, Spark driver,
   Streamlit) binds to 127.0.0.1.

## 6. Issues encountered and how they were resolved

| Issue | Evidence | Resolution |
|---|---|---|
| HBase master advertised itself as `190.180.164.237:16000` | Master log: `host.name=190.180.164.237`; REST gateway "Connection refused" | The Mac's hostname resolves through DNS to a non-local IP. Set `hbase.master.hostname` and `hbase.unsafe.regionserver.hostname` to `localhost` plus `-Djava.net.preferIPv4Stack=true` |
| HBase refused to start after that change | `...hostname.disable.master.reversedns and ...hostname are mutually exclusive` | Removed the reverse-DNS flag |
| Kafka broker fenced mid-test, consumer group unloaded | `Cancelled in-flight BROKER_HEARTBEAT`; GC log `Pause Young ... 57624.702ms`, `User=0.03s Sys=0.03s Real=57.63s` (twice, ~57.5 s each) | The Kafka JVM was blocked for ~57 s with almost no CPU — consistent with its pages being swapped/compressed under memory pressure (the exact cause could not be proven). Dev config now uses `broker.session.timeout.ms=30000`; the issue is recorded as an environment limitation |
| Spark JVM once took >60 s to exit (shutdown-hook `rm -rf` timeout) | `ShutdownHookManager: Exception while deleting Spark temp dir` | Not reproducible in four further runs; `make clean-runtime` removes leftover temp dirs |
| PySpark printed timestamps 5 h 30 m off | Silver report `min_event_time 05:30` for data starting `00:00Z` | `collect()` converts to the *Python process* zone. Spark jobs now pin `TZ=UTC` and `-Duser.timezone=UTC` |
| HBase master aborted mid-experiment | ZooKeeper log: `fsync-ing the write ahead log ... took 34404ms`; master ZK session (10 s) expired | dev-only: `hbase.zookeeper.property.forceSync=no`, ZK sessions up to 120 s, `zookeeper.session.timeout.localHBaseCluster=90000` (negotiated 90 s verified in the log) |
| Recurring ~50–57 s freezes of the Kafka JVM, ≈ every 3 min, late in the session | GC pauses 55.9 s / 56.4 s; broker log silent 52 s with no GC pause; no sleep in the power log | not resolved; correlated with memory compression (broker RSS ≈ 140 MB for a 384 MB heap), Spotlight at up to 177 % CPU and running on battery. Long jobs were run under `caffeinate -dim`; timing-sensitive Kafka tests are flaky in this state |
| Laptop slept during runs | power log: `Entering Sleep state due to 'Idle Sleep'`, `Wake from Deep Idle` | broker fenced on wake and recovered by itself; jobs re-run |
| Reference arXiv id in the brief is wrong | arXiv:1803.02376 is an astrophysics paper (Brightman et al., Nature Astronomy 2018) | The intended paper (Song et al., NSDI 2020) is cited correctly in `RESEARCH_ALIGNMENT.md` |

### Added by the final correctness audit (2026-10-19/20)

| Issue | Evidence | Resolution |
|---|---|---|
| Kafka JVM freezes recurred during the integration suite | GC log `Pause Young … 55552.557ms` and `… 56338.586ms` on a 384 MB heap; `Unable to send a heartbeat because the RPC got timed out`; swap 0.55 → 1.13 GB; one test errored at setup with `Broker transport failure` | environment limitation (as above); the Kafka file re-run alone passed 5/5. A Neo4j server (not ours) was running throughout and was left alone |
| `RpcEndpointNotFoundException … StateStoreCoordinator` after streaming | reproduced: Spark's `StateStore` is a JVM singleton whose maintenance thread outlives `SparkSession.stop()`; a later SparkContext in the same JVM has no coordinator | streaming shutdown now stops queries → state store → session (`FINAL_CORRECTNESS_AUDIT.md` §4) |
| Spark Kafka source unavailable in a process whose JVM was started without the connector | `Failed to find data source: kafka` when a test ran streaming after other Spark tests in the same pytest process; `spark.jars.packages` only applies at JVM launch | the live streaming test runs the job as its own process, like `make streaming-once` |
| JVM vs C-library math differ in the last bit | `log1p`, `sin`, `cos` differ by 1 ulp on 20 653 of 99 629 feature rows | all floating-point feature transforms run through one Python function in both runtimes (audit §7) |
| Streamlit 1.50 deprecates `use_container_width` | deprecation notice in the dashboard | `width="stretch"` (same rendering) |

## 7. Risks that remain

* Full-stack runs on this 8 GB machine can stall a JVM for tens of seconds (§6). Tests that
  depend on timing (Kafka consumer-group rebalances, topic creation) can therefore be
  flaky here; observed again during the final correctness audit.
* The Docker Compose deployment is **syntax-validated only** (`docker compose config`);
  it was not run because the Docker daemon was not running and starting its VM on this
  machine was judged unsafe.
* All data is synthetic.
