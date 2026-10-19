"""Central, environment-driven configuration.

All paths default to locations inside the repository so that the project is
self-contained. Every value can be overridden with an environment variable
(see ``.env.example``). A ``.env`` file in the repo root is read if present;
real environment variables take precedence over it.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


def _load_dotenv(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def _env(name: str, default: str, dotenv: dict[str, str]) -> str:
    return os.environ.get(name) or dotenv.get(name) or default


def _resolve_java_home(dotenv: dict[str, str]) -> str | None:
    explicit = os.environ.get("JAVA_HOME") or dotenv.get("JAVA_HOME")
    manifest = REPO_ROOT / ".tools" / "tools.json"
    if manifest.exists():
        jdk_dir = json.loads(manifest.read_text()).get("jdk")
        if jdk_dir:
            candidate = REPO_ROOT / jdk_dir / "Contents" / "Home"
            if not candidate.exists():  # Linux layout
                candidate = REPO_ROOT / jdk_dir
            # The project-local JDK 17 is preferred over an arbitrary system
            # JAVA_HOME (e.g. Java 27), which Spark 4.0 / HBase 2.6 do not support.
            if candidate.exists() and not os.environ.get("CACHEMIND_RESPECT_JAVA_HOME"):
                return str(candidate)
    return explicit


@dataclass(frozen=True)
class Settings:
    repo_root: Path
    data_dir: Path
    artifacts_dir: Path
    reports_dir: Path
    runtime_dir: Path
    java_home: str | None
    spark_master: str
    spark_driver_memory: str
    spark_shuffle_partitions: int
    kafka_bootstrap: str
    kafka_topic: str
    hbase_rest_url: str
    min_free_memory_pct: int
    stream_runtime_override: Path | None = None

    @property
    def stream_runtime_dir(self) -> Path:
        """Streaming status.json / progress.jsonl.

        Isolated runs (tests, experiments, the pipeline demo) point this at their own
        directory so they never overwrite or add to the main job's health history.
        """
        return self.stream_runtime_override or self.runtime_dir / "streaming"

    # Lakehouse layout ---------------------------------------------------
    @property
    def raw_dir(self) -> Path:
        return self.data_dir / "raw"

    @property
    def lake_dir(self) -> Path:
        return self.data_dir / "lake"

    @property
    def bronze_dir(self) -> Path:
        return self.lake_dir / "bronze"

    @property
    def silver_dir(self) -> Path:
        return self.lake_dir / "silver"

    @property
    def gold_dir(self) -> Path:
        return self.lake_dir / "gold"

    @property
    def quarantine_dir(self) -> Path:
        return self.lake_dir / "quarantine"

    @property
    def features_dir(self) -> Path:
        return self.lake_dir / "features"

    @property
    def checkpoint_dir(self) -> Path:
        return self.data_dir / "checkpoints"

    @property
    def models_dir(self) -> Path:
        return self.artifacts_dir / "models"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    dotenv = _load_dotenv(REPO_ROOT / ".env")
    data_dir = Path(_env("CACHEMIND_DATA_DIR", str(REPO_ROOT / "data"), dotenv))
    stream_runtime = _env("CACHEMIND_STREAM_RUNTIME_DIR", "", dotenv)
    return Settings(
        repo_root=REPO_ROOT,
        data_dir=data_dir,
        artifacts_dir=Path(_env("CACHEMIND_ARTIFACTS_DIR", str(REPO_ROOT / "artifacts"), dotenv)),
        reports_dir=Path(_env("CACHEMIND_REPORTS_DIR", str(REPO_ROOT / "reports"), dotenv)),
        runtime_dir=REPO_ROOT / ".runtime",
        java_home=_resolve_java_home(dotenv),
        spark_master=_env("CACHEMIND_SPARK_MASTER", "local[2]", dotenv),
        spark_driver_memory=_env("CACHEMIND_SPARK_DRIVER_MEMORY", "1g", dotenv),
        spark_shuffle_partitions=int(_env("CACHEMIND_SPARK_SHUFFLE_PARTITIONS", "4", dotenv)),
        kafka_bootstrap=_env("CACHEMIND_KAFKA_BOOTSTRAP", "127.0.0.1:9092", dotenv),
        kafka_topic=_env("CACHEMIND_KAFKA_TOPIC", "cachemind.requests.v1", dotenv),
        hbase_rest_url=_env("CACHEMIND_HBASE_REST_URL", "http://127.0.0.1:8080", dotenv),
        min_free_memory_pct=int(_env("CACHEMIND_MIN_FREE_MEMORY_PCT", "20", dotenv)),
        stream_runtime_override=Path(stream_runtime) if stream_runtime else None,
    )


def safe_child(base: Path, *parts: str) -> Path:
    """Join ``parts`` under ``base`` and refuse paths that escape it.

    Used wherever a path component comes from user input (CLI args, dashboard).
    """
    base_resolved = base.resolve()
    candidate = base_resolved.joinpath(*parts).resolve()
    if candidate != base_resolved and base_resolved not in candidate.parents:
        raise ValueError(f"path {candidate} escapes base directory {base_resolved}")
    return candidate
