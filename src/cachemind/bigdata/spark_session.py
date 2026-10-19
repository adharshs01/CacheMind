"""Memory-conscious SparkSession factory.

Defaults target an 8 GB laptop: ``local[2]``, 1 GB driver heap, 4 shuffle
partitions, UI disabled, UTC session time zone, and every scratch directory
inside ``.runtime/`` so nothing is written outside the project. All values can
be overridden through environment variables (see ``cachemind.config``).

Create ONE session per process and reuse it (``get_spark`` caches it). The
dashboard never creates a Spark session.
"""

from __future__ import annotations

import os
import sys
import time
from typing import Any

from cachemind.config import get_settings

KAFKA_CONNECTOR_GROUP = "org.apache.spark"
KAFKA_CONNECTOR_ARTIFACT = "spark-sql-kafka-0-10_2.13"


def kafka_connector_coordinate() -> str:
    """Connector version must equal the Spark version exactly (Scala 2.13 build)."""
    import pyspark

    return f"{KAFKA_CONNECTOR_GROUP}:{KAFKA_CONNECTOR_ARTIFACT}:{pyspark.__version__}"


def get_spark(
    app_name: str = "cachemind", *, kafka: bool = False, extra_conf: dict[str, Any] | None = None
):
    from pyspark.sql import SparkSession

    s = get_settings()
    if s.java_home:
        os.environ["JAVA_HOME"] = s.java_home
    # PySpark converts collected timestamps to the *Python process* local zone,
    # regardless of spark.sql.session.timeZone. Pin the job process (and JVM) to
    # UTC so that every timestamp a job prints or reports is UTC.
    os.environ["TZ"] = "UTC"
    time.tzset()
    runtime = s.runtime_dir
    for sub in ("spark-tmp", "spark-warehouse", "ivy", "spark-events"):
        (runtime / sub).mkdir(parents=True, exist_ok=True)
    # The Python workers must use the same interpreter as the driver.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    conf: dict[str, Any] = {
        "spark.master": s.spark_master,
        "spark.driver.memory": s.spark_driver_memory,
        "spark.sql.shuffle.partitions": str(s.spark_shuffle_partitions),
        "spark.default.parallelism": str(s.spark_shuffle_partitions),
        "spark.sql.session.timeZone": "UTC",
        "spark.driver.bindAddress": "127.0.0.1",
        "spark.driver.host": "127.0.0.1",
        "spark.ui.enabled": os.environ.get("CACHEMIND_SPARK_UI", "false"),
        "spark.ui.showConsoleProgress": "false",
        "spark.local.dir": str(runtime / "spark-tmp"),
        "spark.sql.warehouse.dir": str(runtime / "spark-warehouse"),
        "spark.jars.ivy": str(runtime / "ivy"),
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.sources.partitionOverwriteMode": "dynamic",
        "spark.sql.parquet.compression.codec": "snappy",
        "spark.sql.files.maxRecordsPerFile": "1000000",
        "spark.sql.execution.arrow.pyspark.enabled": "true",
        "spark.sql.autoBroadcastJoinThreshold": str(32 * 1024 * 1024),
        "spark.python.worker.reuse": "true",
        "spark.sql.streaming.stateStore.providerClass": (
            "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider"
        ),
        "spark.driver.extraJavaOptions": (
            "-XX:+UseG1GC -Duser.timezone=UTC -Dio.netty.tryReflectionSetAccessible=true"
        ),
    }
    if kafka:
        conf["spark.jars.packages"] = kafka_connector_coordinate()
    conf.update(extra_conf or {})

    builder = SparkSession.builder.appName(app_name)
    for key, value in conf.items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.environ.get("CACHEMIND_SPARK_LOG_LEVEL", "WARN"))
    return spark


def spark_environment() -> dict[str, Any]:
    """Versions/config recorded in run metadata."""
    import pyspark

    s = get_settings()
    return {
        "pyspark": pyspark.__version__,
        "java_home": s.java_home,
        "master": s.spark_master,
        "driver_memory": s.spark_driver_memory,
        "shuffle_partitions": s.spark_shuffle_partitions,
    }
