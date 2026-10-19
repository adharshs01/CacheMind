# CacheMind

**Learning-augmented cache replacement using Machine Learning and real-time Big Data analytics.**

A cache keeps a small subset of items; when it is full, a replacement policy decides what to
evict. CacheMind learns — from request history only — the probability that an item is
requested again within the next *H* requests, and evicts the item least likely to be reused.
Around that idea it builds a complete, verifiable Big Data pipeline:

```
CSV + JSON logs + metadata ─► Spark batch ETL ─┐
Kafka producer ─► Kafka ─► Spark Structured Streaming ─► Bronze ─► Silver ─► Gold ─► HBase
                                                              └─► Spark features ─► Spark MLlib (LR, RF)
                                     cache experiment engine (FIFO · LRU · LFU · ML · hybrid · Belady)
                                                   └─► results (Parquet + HBase) ─► Streamlit dashboard
```

All data is **synthetic** (10 deterministic workload shapes). Results are not production
measurements. The research inspiration is LRB (Song et al., NSDI 2020); CacheMind is an
adaptation, not a reproduction — see [docs/RESEARCH_ALIGNMENT.md](docs/RESEARCH_ALIGNMENT.md).

## Verification status (summary)

See [docs/TEST_REPORT.md](docs/TEST_REPORT.md) for exact commands, counts and evidence.

| Area | Status |
|---|---|
| Cache algorithms, invariants, Belady optimality (exhaustive), leakage guards, ETL rules | verified – 207 unit/property tests |
| Spark ETL / RDD / SQL / features / MLlib / streaming event-time windows on a local JVM | verified – 24 Spark tests + project-dataset run |
| Kafka 4.1.2 (KRaft) and HBase 2.6.7 integration | verified against live native services (integration suite, incl. live event-time test) |
| Full pipeline Kafka → Spark streaming → Silver/Gold → HBase → MLlib → simulator | verified – e2e test + `make pipeline-demo` (19/19 checks, isolated namespace, stored evidence) |
| Failure recovery (kill, broker restart, HBase outage, partial write, duplicates, late data) | verified – 6 scenarios on live services (re-run on the final code) |
| Independent correctness audit (streaming semantics, ML leakage, data accounting, policies) | 6 defects fixed with regression tests — see [docs/FINAL_CORRECTNESS_AUDIT.md](docs/FINAL_CORRECTNESS_AUDIT.md) |
| Docker Compose deployment (Profile C on a larger machine) | **syntax-validated only, not run** (no Docker daemon on the dev machine) |

## Quick start

Requirements: macOS or Linux, Python **3.12**, ~3 GB free disk. No system-wide installs: the
JDK, Kafka and HBase are downloaded into `.tools/` and checksum-verified.

```bash
make setup          # .venv + pinned dependencies (requirements.lock)
make test           # unit + property tests, no JVM needed
make demo           # Profile A: synthetic trace, train a model, compare all policies
make dashboard      # http://127.0.0.1:8501
```

Big Data components (Profiles B and C):

```bash
make fetch-tools                             # JDK 17, Kafka 4.1.2, HBase 2.6.7 into .tools/ (SHA verified)
make generate-data PROFILE=small             # dataset zipf-n100000-s42 (CSV + JSON + stream + metadata)
make spark-etl spark-rdd spark-sql DATASET=zipf-n100000-s42
make spark-mllib DATASET=zipf-n100000-s42 HORIZON=500
make infra-up && make infra-health           # native Kafka + HBase (memory-guarded)
make kafka-producer DATASET=zipf-n100000-s42 # stream portion -> Kafka
make streaming-once                          # Kafka -> Bronze -> Silver/HBase -> windows
make spark-etl DATASET=zipf-n100000-s42      # rebuild canonical Silver incl. Kafka records
make pipeline-demo                           # everything end to end in its own namespace, with stored evidence
make infra-down
```

## Execution profiles

| Profile | Purpose | Runs | Memory |
|---|---|---|---|
| **A – lightweight** | development on an 8 GB laptop | simulator, all policies, scikit-learn models, experiments 1–9, dashboard on saved results | < 1 GB |
| **B – component integration** | verify each Big Data component | Spark (local JVM), then Kafka, then HBase, one at a time | ≈ 1–1.5 GB per component |
| **C – full end-to-end** | the real pipeline | Kafka + Spark streaming + HBase + MLlib + simulator + UI | ≈ 3 GB natively; ≈ 6 GB Docker VM (`infra/docker/docker-compose.yml`) |

The dashboard labels every number as **LIVE DATA**, **SAVED EXPERIMENT**, **SYNTHETIC EXAMPLE**,
**OFFLINE RESULT** or **NOT LIVE** and shows unavailable services as unavailable.

## Commands

`make help` lists everything. The CLI behind the targets is `python -m cachemind <command>`:

| Area | Make targets |
|---|---|
| Environment | `setup`, `fetch-tools`, `lock` |
| Quality | `test`, `test-spark`, `integration-test`, `e2e-test`, `failure-test`, `coverage`, `lint`, `typecheck` |
| Data / ML (no JVM) | `generate-data`, `simulate`, `train`, `evaluate`, `demo`, `experiments`, `benchmark` |
| Spark | `spark-etl`, `spark-rdd`, `spark-sql`, `spark-features`, `spark-mllib`, `spark-experiments` |
| Services | `infra-up`, `infra-health`, `infra-status`, `infra-down`, `hbase-init`, `hbase-publish`, `hbase-verify` |
| Streaming | `kafka-producer`, `kafka-producer-live`, `streaming`, `streaming-once`, `reconcile`, `streaming-experiments`, `pipeline-demo` |
| UI | `dashboard` |
| Deployment | `compose-config` |

Targets that need Kafka/HBase check them and fail with a clear message if they are down.

## Configuration

Copy `.env.example` to `.env` (git-ignored) to change paths, Spark memory, Kafka/HBase
endpoints, service heaps and the memory guard. No credentials are used or required.

## Repository map

```
src/cachemind/
  contracts.py            canonical event schema + validation rules
  data/                   workload generator, raw formats, dataset writer
  policies/               FIFO, LRU, LFU, Belady, ML-guided, hybrid
  simulation/             trace loading, simulation engine with invariant checks
  ml/                     features, labels, splits, leakage checks, models, export, predictors
  bigdata/                Spark session, ETL, Gold, features, MLlib, RDD, SQL, Kafka, streaming, HBase
  experiments/            experiments 1-12, pipeline demo, results report
  monitoring/ ops/        health checks, native service manager
  dashboard/              Streamlit app (8 pages)
sql/                      Spark SQL queries
tests/                    unit, property, integration, end_to_end
infra/native/ infra/docker/   service configs, Docker Compose deployment
docs/                     documentation (below)
scripts/fetch_tools.py    verified project-local JDK / Kafka / HBase download
```

## Documentation

| Document | Contents |
|---|---|
| [ARCHITECTURE](docs/ARCHITECTURE.md) | logical + physical architecture, storage layout, jobs, profiles |
| [ENVIRONMENT_AUDIT](docs/ENVIRONMENT_AUDIT.md) | machine audit, compatibility matrix, issues met and resolved |
| [DATA_CONTRACTS](docs/DATA_CONTRACTS.md) | schemas, validation order, duplicate/late-data policy, quarantine, before/after |
| [STREAMING_SEMANTICS](docs/STREAMING_SEMANTICS.md) | delivery guarantees per hop, partial failures, recovery, what is not claimed |
| [HBASE_SCHEMA](docs/HBASE_SCHEMA.md) | tables, column families, row-key design and access patterns |
| [ML_DESIGN](docs/ML_DESIGN.md) | prediction task, features, leakage prevention, models, export/parity, policies |
| [RESEARCH_ALIGNMENT](docs/RESEARCH_ALIGNMENT.md) | comparison with the LRB paper |
| [EXPERIMENTS](docs/EXPERIMENTS.md) | experiment design and findings |
| [EXPERIMENT_RESULTS](docs/EXPERIMENT_RESULTS.md) | all result tables, generated from stored outputs |
| [REQUIREMENTS_TRACEABILITY](docs/REQUIREMENTS_TRACEABILITY.md) | requirement → code → data → command → test → result |
| [TEST_REPORT](docs/TEST_REPORT.md) | what was executed, results, failures investigated, what was not verified |
| [FINAL_CORRECTNESS_AUDIT](docs/FINAL_CORRECTNESS_AUDIT.md) | independent audit: defects found, reproductions, fixes, evidence, readiness verdict |
| [FINAL_REPORT](docs/FINAL_REPORT.md) | engineering report: results, limitations, next steps |
| [DEMO_GUIDE](docs/DEMO_GUIDE.md) | step-by-step presentation script with timings |

## Safety notes

Development-only stack: no authentication anywhere, so every service binds to 127.0.0.1.
Do not expose these ports. No secrets are stored in the repository.
