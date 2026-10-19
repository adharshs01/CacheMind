"""Experiments 11 (Kafka/Spark streaming throughput) and 12 (failure recovery).

    python -m cachemind.experiments.streaming_experiments --exp 11,12

Requires Kafka + HBase. Each scenario runs in isolation: its own data directory,
Kafka topic, Spark checkpoints, streaming status/progress files and dataset id.
Streaming runs as a separate OS process so it can be killed. After every scenario the
outputs are *verified*:

  * Bronze (file-sink log) holds every Kafka (partition, offset) exactly once;
  * every clean event reached the streaming Silver and HBase;
  * reconcile() reports no incomplete micro-batch.

Measured latencies are real timestamps from the pipeline (Kafka append time vs
the Spark micro-batch timestamp that wrote the Bronze row) — never simulated.
"""

from __future__ import annotations

import argparse
import contextlib
import glob
import json
import os
import signal
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any

import numpy as np
import psutil
import pyarrow.dataset as pads

from cachemind.bigdata.streaming import WINDOWS_QUERY
from cachemind.config import REPO_ROOT, get_settings
from cachemind.logging_utils import get_logger, log_event

log = get_logger("cachemind.exp.streaming")
PY = sys.executable


def _pgid(pid: int) -> int | None:
    try:
        return os.getpgid(pid)
    except (ProcessLookupError, PermissionError):
        return None


class Scenario:
    def __init__(self, name: str, events: int, root: Path) -> None:
        self.name = name
        self.tag = f"{name}-{uuid.uuid4().hex[:6]}"
        self.data_dir = root / self.tag / "data"
        self.stream_runtime = root / self.tag / "stream_runtime"
        self.topic = f"cachemind.exp.{self.tag}"
        self.dataset_id = f"exp-{self.tag}"
        self.env = {
            **os.environ,
            "CACHEMIND_DATA_DIR": str(self.data_dir),
            "CACHEMIND_KAFKA_TOPIC": self.topic,
            # deliberate failures must not land in the main job's health history
            "CACHEMIND_STREAM_RUNTIME_DIR": str(self.stream_runtime),
            "CACHEMIND_SPARK_DRIVER_MEMORY": os.environ.get(
                "CACHEMIND_SPARK_DRIVER_MEMORY", "768m"
            ),
            "PYTHONUNBUFFERED": "1",
        }
        self.logs = root / self.tag / "logs"
        self.logs.mkdir(parents=True, exist_ok=True)
        self._stream: subprocess.Popen | None = None
        self.notes: list[str] = []
        from cachemind.data.generator import generate_dataset
        from cachemind.data.workloads import WorkloadConfig
        from cachemind.ops.infra import ensure_topic

        cfg = WorkloadConfig(
            workload="zipf",
            n_events=events,
            n_items=1_000,
            n_users=300,
            seed=17,
            dataset_id=self.dataset_id,
            event_rate_per_sec=20.0,
        )
        generate_dataset(cfg, self.data_dir / "raw", batch_split=(0.0, 0.0))  # all streamed
        ensure_topic(self.topic, partitions=3)

    # --- processes ------------------------------------------------------------------
    def produce(self, n: int, skip: int = 0, rate: float = 0.0, **inject: float) -> dict:
        args = [
            PY,
            "-m",
            "cachemind.bigdata.kafka_io",
            "--dataset",
            self.dataset_id,
            "--max-events",
            str(n),
            "--skip",
            str(skip),
            "--rate",
            str(rate),
        ]
        for k, v in inject.items():
            args += [f"--{k.replace('_', '-')}", str(v)]
        out = subprocess.run(
            args, env=self.env, capture_output=True, text=True, check=False, cwd=REPO_ROOT
        )
        if out.returncode != 0:
            raise RuntimeError(f"producer failed: {out.stderr[-2000:]}")
        return json.loads(out.stdout)

    def start_stream(
        self, extra_env: dict[str, str] | None = None, trigger_seconds: int = 3
    ) -> None:
        log_file = (self.logs / f"stream-{int(time.time())}.log").open("w")
        previous_run = self.status().get("run_id")
        self._stream = subprocess.Popen(
            [
                PY,
                "-m",
                "cachemind.bigdata.streaming",
                "run",
                "--trigger-seconds",
                str(trigger_seconds),
                "--max-offsets",
                "2000",
                "--max-files",
                "6",
            ],
            env={**self.env, **(extra_env or {})},
            stdout=log_file,
            stderr=subprocess.STDOUT,
            cwd=REPO_ROOT,
            start_new_session=True,
        )
        self._wait_status(
            lambda st: st.get("run_id") != previous_run and st.get("heartbeat") is not None,
            240,
            "streaming start",
        )

    def stop_stream(self, kill: bool = False, timeout: float = 120) -> int | None:
        if self._stream is None:
            return None
        if kill:
            os.killpg(self._stream.pid, signal.SIGKILL)  # abrupt: no graceful shutdown
        else:
            self._stream.send_signal(signal.SIGINT)
        try:
            self._stream.wait(timeout)
        except subprocess.TimeoutExpired:
            os.killpg(self._stream.pid, signal.SIGKILL)
            self._stream.wait(30)
        rc = self._stream.returncode
        # The Spark JVM is a child in the same process group (start_new_session=True);
        # make sure nothing from the group survives to hold the checkpoint. Once the group
        # is empty macOS may answer EPERM instead of ESRCH, so verify explicitly.
        pgid = self._stream.pid
        with contextlib.suppress(ProcessLookupError, PermissionError):
            os.killpg(pgid, signal.SIGKILL)
        survivors = [p for p in psutil.process_iter(["pid"]) if _pgid(p.info["pid"]) == pgid]
        for proc in survivors:
            with contextlib.suppress(psutil.NoSuchProcess):
                proc.kill()
        self._stream = None
        return rc

    def drain(
        self, timeout: float = 600, extra_env: dict[str, str] | None = None, max_files: int = 6
    ) -> dict:
        out = subprocess.run(
            [
                PY,
                "-m",
                "cachemind.bigdata.streaming",
                "run",
                "--available-now",
                "--max-offsets",
                "2000",
                "--max-files",
                str(max_files),
            ],
            env={**self.env, **(extra_env or {})},
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=REPO_ROOT,
            check=False,
        )
        (self.logs / f"drain-{int(time.time())}.log").write_text(out.stdout + out.stderr)
        if out.returncode != 0:
            raise RuntimeError(f"drain failed ({out.returncode}): {out.stderr[-3000:]}")
        return json.loads(out.stdout[out.stdout.index("{") :])

    def reconcile(self, repair: bool = False) -> dict:
        args = [PY, "-m", "cachemind.bigdata.streaming", "reconcile"] + (
            ["--repair"] if repair else []
        )
        out = subprocess.run(
            args, env=self.env, capture_output=True, text=True, cwd=REPO_ROOT, check=False
        )
        return json.loads(out.stdout[out.stdout.index("{") :])

    def status(self) -> dict:
        p = self.stream_runtime / "status.json"
        return json.loads(p.read_text()) if p.exists() else {}

    def _wait_status(self, pred, timeout: float, what: str) -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self._stream is not None and self._stream.poll() is not None:
                raise RuntimeError(f"streaming process exited during {what}")
            st = self.status()
            if st.get("run_id") and pred(st):
                return
            time.sleep(1)
        raise TimeoutError(f"timed out waiting for {what}")

    def wait_bronze(self, n: int, timeout: float = 240) -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.bronze()["rows"] >= n:
                return
            time.sleep(2)
        raise TimeoutError(f"bronze did not reach {n} rows")

    # --- verification -------------------------------------------------------------------
    def bronze(self, since_ms: float | None = None) -> dict[str, Any]:
        """Bronze as Spark sees it (files listed in the file-sink log) + measured latencies.

        * to micro-batch start: ingest_ts (Spark's batch timestamp) - Kafka append time;
        * to durable file: Parquet file modification time - Kafka append time
          (when the row was on disk; the log commit follows immediately).
        Timestamps are cast explicitly to milliseconds (pyarrow may read them as ns).
        Latency statistics use only rows appended to Kafka at or after ``since_ms``
        (e.g. the live phase), while row counts always cover the whole topic.
        """
        import pyarrow as pa

        base = self.data_dir / "lake" / "bronze" / "stream"
        listed = set()
        for f in sorted(glob.glob(str(base / "_spark_metadata" / "*"))):
            for line in Path(f).read_text().splitlines()[1:]:
                if line.startswith("{"):
                    listed.add(json.loads(line)["path"].replace("file://", ""))
        if not listed:
            return {"rows": 0, "distinct_offsets": 0, "files": 0}
        pairs: list[tuple[int, int]] = []
        to_batch: list[np.ndarray] = []
        to_file: list[np.ndarray] = []
        for path in sorted(listed):
            t = pads.dataset(path, format="parquet").to_table(
                columns=["kafka_partition", "kafka_offset", "kafka_timestamp", "ingest_ts"]
            )

            def ms(col: str, t: Any = t) -> np.ndarray:
                return t.column(col).cast(pa.timestamp("ms")).cast(pa.int64()).to_numpy()

            pairs += list(
                zip(
                    t.column("kafka_partition").to_pylist(),
                    t.column("kafka_offset").to_pylist(),
                    strict=True,
                )
            )
            kts = ms("kafka_timestamp")
            keep = kts >= since_ms if since_ms is not None else np.ones(len(kts), dtype=bool)
            to_batch.append((ms("ingest_ts") - kts)[keep])
            to_file.append((Path(path).stat().st_mtime * 1000.0 - kts)[keep])
        lb, lf = np.concatenate(to_batch), np.concatenate(to_file)
        if len(lb) == 0:
            return {
                "rows": len(pairs),
                "distinct_offsets": len(set(pairs)),
                "files": len(listed),
                "latency_rows": 0,
            }
        return {
            "rows": len(pairs),
            "distinct_offsets": len(set(pairs)),
            "files": len(listed),
            "latency_rows": len(lb),
            "kafka_to_batch_start_ms_p50": float(np.percentile(lb, 50)),
            "kafka_to_batch_start_ms_p95": float(np.percentile(lb, 95)),
            "kafka_to_bronze_file_ms_p50": float(np.percentile(lf, 50)),
            "kafka_to_bronze_file_ms_p95": float(np.percentile(lf, 95)),
            "kafka_to_bronze_file_ms_max": float(lf.max()),
        }

    def silver(self) -> dict[str, Any]:
        base = self.data_dir / "lake" / "silver_stream" / "requests"
        if not base.exists():
            return {"rows": 0, "distinct_event_ids": 0}
        t = pads.dataset(str(base), format="parquet", partitioning="hive").to_table(
            columns=["event_id", "is_late"]
        )
        ids = t.column("event_id").to_pylist()
        return {
            "rows": len(ids),
            "distinct_event_ids": len(set(ids)),
            "cross_batch_duplicate_rows": len(ids) - len(set(ids)),
            "late_flagged": int(sum(t.column("is_late").to_pylist())),
            "_ids": set(ids),
        }

    def quarantine(self) -> dict[str, int]:
        base = self.data_dir / "lake" / "quarantine_stream"
        if not base.exists():
            return {}
        t = pads.dataset(str(base), format="parquet", partitioning="hive").to_table(
            columns=["dq_reason"]
        )
        out: dict[str, int] = {}
        for r in t.column("dq_reason").to_pylist():
            out[r] = out.get(r, 0) + 1
        return out

    def hbase_missing(self, ids: set[str]) -> int:
        from cachemind.bigdata.hbase_store import HBaseRestClient, request_row_key

        c = HBaseRestClient()
        return sum(1 for e in ids if c.get_row("cache_requests", request_row_key(e)) is None)

    def topic_messages(self) -> int:
        from cachemind.bigdata.kafka_io import topic_end_offsets

        return sum(topic_end_offsets(self.topic).values())

    def verify(self, clean_events: int) -> dict[str, Any]:
        b, s = self.bronze(), self.silver()
        ids = s.pop("_ids")
        msgs = self.topic_messages()
        rec = self.reconcile()
        missing = self.hbase_missing(ids)
        checks = {
            "bronze_exactly_once": b["rows"] == b["distinct_offsets"] == msgs,
            "all_clean_events_in_silver": s["distinct_event_ids"] == clean_events,
            "all_events_in_hbase": missing == 0,
            "reconcile_consistent": bool(rec.get("consistent")),
        }
        return {
            "topic_messages": msgs,
            "bronze": b,
            "silver": s,
            "quarantine": self.quarantine(),
            "hbase_missing": missing,
            "reconcile": {k: rec.get(k) for k in ("silver_stream", WINDOWS_QUERY, "consistent")},
            "checks": checks,
            "passed": all(checks.values()),
        }


def _infra(action: str, service: str) -> None:
    out = subprocess.run(
        [PY, "-m", "cachemind.ops.infra", action, service],
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
        check=False,
    )
    if out.returncode != 0:
        raise RuntimeError(f"infra {action} {service} failed: {out.stderr[-2000:]}")


# --- Experiment 11 ------------------------------------------------------------------------


def exp11_throughput(
    root: Path, backlog: int = 20_000, live_rate: float = 400.0, live_seconds: int = 45
) -> list[dict[str, Any]]:
    rows = []
    sc = Scenario("throughput", backlog + int(live_rate * live_seconds) + 1_000, root)
    t0 = time.time()
    rep = sc.produce(backlog, rate=0)
    rows.append(
        {
            "phase": "produce_backlog",
            "events": rep["sent"],
            "seconds": rep["elapsed_s"],
            "events_per_s": rep["achieved_rate_per_s"],
        }
    )
    t0 = time.time()
    sc.drain()
    drain_s = time.time() - t0
    prog = [
        json.loads(x)
        for x in (sc.stream_runtime / "progress.jsonl").read_text().splitlines()
        if '"progress"' in x
    ]
    prog = [p for p in prog if p.get("ts", "") >= time.strftime("%Y-%m-%dT%H:%M", time.gmtime(t0))]
    for q in ("bronze_ingest", "silver_stream", WINDOWS_QUERY):
        pq = [p for p in prog if p["name"] == q and p.get("input_rows")]
        if pq:
            rows.append(
                {
                    "phase": f"drain_backlog:{q}",
                    "batches": len(pq),
                    "rows": sum(p["input_rows"] for p in pq),
                    "processed_rows_per_s_median": float(
                        np.median([p["processed_rows_per_s"] for p in pq])
                    ),
                    "batch_duration_ms_p50": float(np.median([p["batch_duration_ms"] for p in pq])),
                    "batch_duration_ms_max": float(max(p["batch_duration_ms"] for p in pq)),
                }
            )
    rows.append(
        {
            "phase": "drain_backlog:total",
            "events": backlog,
            "seconds": drain_s,
            "events_per_s": backlog / drain_s,
            "note": "includes Spark/JVM start-up and three sequential available-now queries",
        }
    )
    # live phase: producer at a fixed rate while the continuous job runs
    sc.start_stream(trigger_seconds=3)
    try:
        before = sc.bronze()["rows"]
        live_start_ms = time.time() * 1000.0
        rep = sc.produce(int(live_rate * live_seconds), skip=backlog, rate=live_rate)
        sc.wait_bronze(before + rep["sent"])
        time.sleep(8)
    finally:
        sc.stop_stream()
    b = sc.bronze(since_ms=live_start_ms)
    rows.append(
        {
            "phase": "live",
            "target_rate": live_rate,
            "events": rep["sent"],
            "achieved_produce_rate": rep["achieved_rate_per_s"],
            "kafka_to_batch_start_ms_p50": b["kafka_to_batch_start_ms_p50"],
            "kafka_to_batch_start_ms_p95": b["kafka_to_batch_start_ms_p95"],
            "kafka_to_bronze_file_ms_p50": b["kafka_to_bronze_file_ms_p50"],
            "kafka_to_bronze_file_ms_p95": b["kafka_to_bronze_file_ms_p95"],
            "kafka_to_bronze_file_ms_max": b["kafka_to_bronze_file_ms_max"],
            "latency_rows": b["latency_rows"],
            "note": "latency over rows appended to Kafka during the live phase only; "
            "processing-time trigger every 3 s",
        }
    )
    v = sc.verify(clean_events=backlog + rep["sent"])
    rows.append({"phase": "verification", **{k: v[k] for k in ("checks", "passed")}})
    return rows


# --- Experiment 12 ------------------------------------------------------------------------


def _scenario_result(
    name: str, sc: Scenario, clean: int, expectations: str, extra: dict | None = None
) -> dict[str, Any]:
    v = sc.verify(clean_events=clean)
    st = sc.status()
    res = {
        "scenario": name,
        "expectation": expectations,
        "passed": v["passed"],
        "checks": v["checks"],
        "topic_messages": v["topic_messages"],
        "bronze_rows": v["bronze"]["rows"],
        "silver_distinct": v["silver"]["distinct_event_ids"],
        "silver_cross_batch_duplicates": v["silver"]["cross_batch_duplicate_rows"],
        "late_flagged": v["silver"]["late_flagged"],
        "quarantine": v["quarantine"],
        "query_restarts": st.get("restarts", {}),
        "notes": sc.notes,
        **(extra or {}),
    }
    log_event(log, "scenario finished", scenario=name, passed=res["passed"])
    return res


def exp12_failures(root: Path) -> list[dict[str, Any]]:
    try:
        return _exp12_scenarios(root)
    finally:
        # Whatever happened, leave both services running so later work (and later
        # tests) never start against infrastructure a failed scenario left stopped.
        _infra("up", "kafka")
        _infra("up", "hbase")


def _exp12_scenarios(root: Path) -> list[dict[str, Any]]:
    results = []

    # S1 duplicates + malformed + late events
    sc = Scenario("s1-bad-data", 4_000, root)
    # Late events are held back by 3 000 messages ≈ 150 s of event time at 20 events/s,
    # i.e. beyond the 2-minute allowed lateness, so they must be flagged (a smaller delay
    # would correctly NOT count as late).
    rep = sc.produce(
        4_000, rate=0, duplicate_rate=0.03, malformed_rate=0.01, late_rate=0.02, late_by=3000
    )
    # Lateness is defined against EARLIER micro-batches, so the delayed events must reach
    # Silver in a later micro-batch than the events they lag. Bronze writes one file per
    # micro-batch (2 000 offsets), so two files per Silver trigger reproduce the original
    # grouping (Bronze batches 0+1, then 2); with more files the whole backlog would be
    # one Silver micro-batch, in which nothing is late by definition.
    sc.drain(max_files=2)
    s1 = _scenario_result(
        "duplicate_malformed_late",
        sc,
        4_000,
        "malformed -> quarantine; duplicates never create extra HBase rows; late events "
        "kept in Silver and flagged",
        {"injected": rep["injected"]},
    )
    s1["checks"]["late_events_flagged"] = s1["late_flagged"] > 0
    s1["checks"]["malformed_quarantined"] = s1["quarantine"].get("malformed_json", 0) == rep[
        "injected"
    ].get("malformed", 0)
    s1["passed"] = all(s1["checks"].values())
    results.append(s1)

    # S2 abrupt kill of the streaming process mid-run, then restart
    sc = Scenario("s2-kill", 6_000, root)
    sc.start_stream()
    sc.produce(3_000, rate=600)
    sc.wait_bronze(1_000)
    sc.stop_stream(kill=True)
    sc.notes.append("streaming process SIGKILLed after >=1000 Bronze rows")
    sc.produce(3_000, skip=3_000, rate=0)
    sc.drain()
    results.append(
        _scenario_result(
            "spark_process_killed",
            sc,
            6_000,
            "restart from checkpoint: Bronze exactly-once, no loss",
        )
    )

    # S3 Kafka broker restart while streaming
    sc = Scenario("s3-kafka-restart", 4_000, root)
    sc.start_stream()
    sc.produce(2_000, rate=0)
    sc.wait_bronze(2_000)
    _infra("down", "kafka")
    sc.notes.append("Kafka broker stopped while the streaming query was running")
    time.sleep(15)
    _infra("up", "kafka")
    sc.produce(2_000, skip=2_000, rate=0)
    try:
        sc.wait_bronze(4_000, timeout=300)
    finally:
        sc.stop_stream()
    continuous = sc.status()  # captured before the drain overwrites status.json
    sc.drain()
    results.append(
        _scenario_result(
            "kafka_broker_restart",
            sc,
            4_000,
            "query survives/restarts; no loss or duplication",
            {
                "continuous_run_restarts": continuous.get("restarts", {}),
                "continuous_run_last_errors": {
                    k: ((v or {}).get("last_error") or "")[:300]
                    for k, v in continuous.get("queries", {}).items()
                },
            },
        )
    )

    # S4 HBase unavailable while streaming
    sc = Scenario("s4-hbase-down", 3_000, root)
    sc.start_stream()
    sc.produce(1_000, rate=0)
    sc.wait_bronze(1_000)
    _infra("down", "hbase")
    sc.notes.append("HBase stopped; micro-batches fail at HBase publication")
    sc.produce(2_000, skip=1_000, rate=0)
    time.sleep(20)
    _infra("up", "hbase")
    time.sleep(25)
    sc.stop_stream()
    continuous = sc.status()
    sc.drain()
    results.append(
        _scenario_result(
            "hbase_unavailable",
            sc,
            3_000,
            "failed batches are replayed after HBase returns; idempotent puts leave no gaps",
            {
                "continuous_run_restarts": continuous.get("restarts", {}),
                "continuous_run_last_errors": {
                    k: ((v or {}).get("last_error") or "")[:300]
                    for k, v in continuous.get("queries", {}).items()
                },
            },
        )
    )

    # S5 injected partial failure (Parquet written, HBase/manifest not)
    sc = Scenario("s5-partial", 3_000, root)
    sc.produce(3_000, rate=0)
    fault_failed = False
    try:
        sc.drain(extra_env={"CACHEMIND_FAULT_SILVER_BATCH": "0"})
        sc.notes.append("UNEXPECTED: drain succeeded despite injected fault")
    except RuntimeError:
        fault_failed = True
        sc.notes.append("first drain failed at the injected fault (expected)")
    ckpt = sc.data_dir / "checkpoints"
    results_extra = {
        "fault_triggered": fault_failed,
        "fault_marker_exists": (ckpt / "fault_injected_batch_0").exists(),
        "parquet_written_before_restart": (
            sc.data_dir / "lake/silver_stream/requests/batch_id=0000000000"
        ).exists(),
        "manifest_written_before_restart": (
            sc.data_dir / "lake/_stream_manifests/silver_stream/batch_0000000000.json"
        ).exists(),
    }
    sc.drain()  # restart without the fault: the failed batch is replayed
    results.append(
        _scenario_result(
            "partial_output_failure",
            sc,
            3_000,
            "batch replayed after restart; reconcile consistent",
            results_extra,
        )
    )

    # S6 idempotent re-drain: no new data -> nothing changes
    before = sc.bronze()["rows"], sc.silver()["rows"]
    sc.drain()
    after = sc.bronze()["rows"], sc.silver()["rows"]
    results.append(
        {
            "scenario": "redrain_without_new_data",
            "passed": before == after,
            "checks": {"unchanged": before == after},
            "before": before,
            "after": after,
            "expectation": "a restart with no new offsets writes nothing",
        }
    )
    return results


def main(argv: list[str] | None = None) -> int:
    from cachemind.experiments.common import save_results

    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--exp", default="11,12")
    args = ap.parse_args(argv)
    root = get_settings().runtime_dir / "exp_streaming"
    root.mkdir(parents=True, exist_ok=True)
    ok = True
    if "11" in args.exp.split(","):
        rows = exp11_throughput(root)
        save_results(
            "exp11_streaming_throughput",
            rows,
            {
                "infrastructure": "native Kafka 4.1.2 (KRaft, 1 broker) + Spark 4.0.4 local[2] + "
                "HBase 2.6.7 standalone, all on one 8 GB laptop"
            },
        )
        ok &= rows[-1]["passed"]
    if "12" in args.exp.split(","):
        os.environ.pop("CACHEMIND_FAULT_SILVER_BATCH", None)
        rows = exp12_failures(root)
        save_results("exp12_failure_recovery", rows, {"scenarios": len(rows)})
        ok &= all(r["passed"] for r in rows)
    print(json.dumps({"ok": ok}))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
