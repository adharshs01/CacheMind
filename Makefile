# CacheMind task runner (macOS / zsh compatible; GNU make 3.81+).
# Every target that needs Kafka/HBase checks them first and fails loudly.

SHELL      := /bin/bash
PY         := .venv/bin/python
PIP        := .venv/bin/pip
CM         := $(PY) -m cachemind
DATASET    ?= zipf-n100000-s42
PROFILE    ?= small
WORKLOAD   ?= zipf
SEED       ?= 42
HORIZON    ?= 500
CAPACITY   ?= 100
PYTEST     := $(PY) -m pytest -p no:cacheprovider

.DEFAULT_GOAL := help
.PHONY: help setup fetch-tools lock lint typecheck test test-spark integration-test e2e-test \
        failure-test coverage demo generate-data simulate train evaluate spark-etl spark-rdd \
        spark-sql spark-features spark-mllib infra-up infra-health infra-status infra-down \
        kafka-producer kafka-producer-live streaming streaming-once reconcile hbase-init \
        hbase-verify hbase-publish dashboard experiments spark-experiments \
        streaming-experiments benchmark pipeline-demo compose-config clean-runtime

help: ## list targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'

# --- environment -------------------------------------------------------------------------
setup: ## create .venv (Python 3.12) and install pinned dependencies
	@test -x .venv/bin/python || python3.12 -m venv .venv
	$(PIP) install --upgrade "pip==25.2"
	@if [ -f requirements.lock ]; then $(PIP) install -r requirements.lock; else $(PIP) install -e ".[dev]"; fi
	$(PIP) install -e . --no-deps
	$(PY) -m pip check

fetch-tools: ## download + checksum-verify JDK 17, Kafka 4.1.2, HBase 2.6.7 into .tools/
	$(PY) scripts/fetch_tools.py

lock: ## freeze the tested environment into requirements.lock
	$(PIP) freeze --exclude-editable | grep -v '^cachemind' > requirements.lock

# --- quality -----------------------------------------------------------------------------
lint: ## ruff lint + format check
	$(PY) -m ruff check src tests scripts
	$(PY) -m ruff format --check src tests scripts

typecheck: ## mypy on the package
	$(PY) -m mypy

test: ## unit + property tests (no JVM, no services)
	$(PYTEST) tests/unit tests/property -q

test-spark: ## Spark tests (local JVM; no services)
	$(PYTEST) tests/integration/test_spark_pipeline.py tests/integration/test_streaming_unit.py -q

integration-test: ## Kafka + HBase + Spark integration tests (services REQUIRED)
	CACHEMIND_REQUIRE_INFRA=1 $(PYTEST) tests/integration -q

e2e-test: ## full pipeline test (services REQUIRED)
	CACHEMIND_REQUIRE_INFRA=1 $(PYTEST) tests/end_to_end -q -m e2e

failure-test: ## failure-injection scenarios (services REQUIRED; slow)
	CACHEMIND_REQUIRE_INFRA=1 $(PY) -m cachemind.experiments.streaming_experiments --exp 12

coverage: ## unit/property coverage report
	$(PYTEST) tests/unit tests/property -q --cov=cachemind --cov-report=term-missing

# --- lightweight profile (A) ---------------------------------------------------------------
demo: ## Profile A demo: synthetic trace -> all policies incl. ML (no JVM)
	$(CM) train --profile tiny --workload $(WORKLOAD) --seed $(SEED) --horizon 200
	$(CM) simulate --profile tiny --workload $(WORKLOAD) --seed $(SEED) --capacity 40 --policy all \
	  --model-dir "$$(ls -d artifacts/models/$(WORKLOAD)-n5000-s$(SEED)/sklearn_logistic_regression_* | tail -1)"

generate-data: ## generate dataset (PROFILE, WORKLOAD, SEED)
	$(CM) generate --profile $(PROFILE) --workload $(WORKLOAD) --seed $(SEED) --overwrite

simulate: ## classical policies on a synthetic trace (or DATASET from Silver with SOURCE=silver)
	@if [ "$(SOURCE)" = "silver" ]; then $(CM) simulate --dataset $(DATASET) --capacity $(CAPACITY); \
	else $(CM) simulate --profile $(PROFILE) --workload $(WORKLOAD) --seed $(SEED) --capacity $(CAPACITY); fi

train: ## scikit-learn models (lightweight profile)
	$(CM) train --profile $(PROFILE) --workload $(WORKLOAD) --seed $(SEED) --horizon $(HORIZON)

evaluate: ## summarise every saved model artifact
	$(CM) evaluate

experiments: ## research experiments 1-9 (standard scale, ~45 min)
	$(PY) -m cachemind.experiments.cache_experiments --scale standard

benchmark: ## experiment 9 only (policy/ML overhead)
	$(PY) -m cachemind.experiments.cache_experiments --scale standard --exp 9

# --- Spark (profile B) -------------------------------------------------------------------------
spark-etl: ## Bronze -> Silver -> Gold for DATASET (historical files)
	$(CM) etl --dataset $(DATASET)

spark-rdd: ## RDD transformations/actions demo
	$(CM) rdd --dataset $(DATASET)

spark-sql: ## Spark SQL analytics
	$(CM) sql --dataset $(DATASET)

spark-features: ## Spark window features + labels
	$(CM) features --dataset $(DATASET) --horizon $(HORIZON)

spark-mllib: spark-features ## Spark MLlib LR + RF (train/eval/save/reload/export)
	$(CM) mllib --dataset $(DATASET) --horizon $(HORIZON) --seed $(SEED)

spark-experiments: ## experiment 10 (Spark performance)
	$(PY) -m cachemind.experiments.spark_experiments

# --- services (profiles B/C) ---------------------------------------------------------------------
infra-up: ## start Kafka + HBase natively (memory-guarded)
	$(PY) -m cachemind.ops.infra up all

infra-health: ## real connectivity checks (non-zero exit if a service is down)
	$(PY) -m cachemind.ops.infra health

infra-status: ## pids, RSS and ports of local services
	$(PY) -m cachemind.ops.infra status

infra-down: ## stop HBase + Kafka (data is kept)
	$(PY) -m cachemind.ops.infra down all

kafka-producer: ## send DATASET's stream events to Kafka
	$(CM) produce --dataset $(DATASET) --rate 200

kafka-producer-live: ## live events stamped 'now' (Ctrl-C to stop)
	$(CM) produce --live --rate 50

streaming: ## continuous Structured Streaming job (Ctrl-C stops gracefully)
	$(CM) stream run

streaming-once: ## process everything available, then stop
	$(CM) stream run --available-now

reconcile: ## verify streaming outputs (add REPAIR=1 to repair HBase from Parquet)
	$(CM) stream reconcile $(if $(REPAIR),--repair,)

hbase-init: ## create HBase tables
	$(CM) hbase-init

hbase-publish: ## publish Gold item stats + experiment results to HBase
	$(CM) hbase-publish --dataset $(DATASET) --experiments

hbase-verify: ## row counts + sample rows of every table
	$(CM) hbase-verify

streaming-experiments: ## experiments 11 + 12 (services REQUIRED)
	$(PY) -m cachemind.experiments.streaming_experiments --exp 11,12

pipeline-demo: ## Profile C: whole pipeline with stored evidence (services REQUIRED)
	$(CM) pipeline-demo --events 20000 --capacity 100

dashboard: ## Streamlit UI on http://127.0.0.1:8501
	$(PY) -m streamlit run src/cachemind/dashboard/app.py

compose-config: ## validate docker-compose.yml (Profile C deployment)
	docker compose -f infra/docker/docker-compose.yml config --quiet && echo "compose file valid"

clean-runtime: ## remove Spark temp/ivy scratch (keeps service data, checkpoints, datasets)
	rm -rf .runtime/spark-tmp .runtime/spark-warehouse
