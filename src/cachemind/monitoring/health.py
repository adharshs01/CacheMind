"""System health: real connectivity checks + data freshness, never fabricated.

Each check returns ``{"ok": bool | None, ...}`` where ``None`` means "not
applicable / not configured". Unreachable services report ``ok: False`` with the
error text. The dashboard renders these as-is.
"""

from __future__ import annotations

import json
import re
import socket
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from cachemind.config import get_settings


def _port_open(hostport: str, timeout: float = 0.5) -> bool:
    host, _, port = hostport.rpartition(":")
    try:
        with socket.create_connection((host or "127.0.0.1", int(port)), timeout=timeout):
            return True
    except OSError:
        return False


def kafka_health(timeout: float = 3.0) -> dict[str, Any]:
    s = get_settings()
    if not _port_open(s.kafka_bootstrap):
        return {"ok": False, "error": f"no broker listening at {s.kafka_bootstrap}"}
    try:
        from confluent_kafka.admin import AdminClient

        md = AdminClient({"bootstrap.servers": s.kafka_bootstrap}).list_topics(timeout=timeout)
        topic = md.topics.get(s.kafka_topic)
        out: dict[str, Any] = {
            "ok": True,
            "brokers": len(md.brokers),
            "topic": s.kafka_topic,
            "topic_exists": topic is not None,
        }
        if topic is not None:
            from cachemind.bigdata.kafka_io import topic_end_offsets

            ends = topic_end_offsets()
            out.update(
                {
                    "partitions": len(topic.partitions),
                    "end_offsets": ends,
                    "total_messages": sum(ends.values()),
                }
            )
        return out
    except Exception as exc:  # noqa: BLE001 - surfaced verbatim to the operator
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


def hbase_health() -> dict[str, Any]:
    from cachemind.bigdata.hbase_store import TABLES, HBaseError, HBaseRestClient

    s = get_settings()
    hostport = s.hbase_rest_url.split("://", 1)[-1]
    if not _port_open(hostport):
        return {"ok": False, "error": f"no REST gateway at {s.hbase_rest_url}"}
    try:
        c = HBaseRestClient(retries=0, timeout=3)
        ver = c.version()
        st = c.cluster_status()
        tables = set(c.list_tables())
        return {
            "ok": bool(st.get("LiveNodes")),
            "version": ver,
            "live_nodes": len(st.get("LiveNodes", [])),
            "dead_nodes": len(st.get("DeadNodes", [])),
            "regions": st.get("regions"),
            "tables": {t: t in tables for t in TABLES},
        }
    except HBaseError as exc:
        return {"ok": False, "error": str(exc)}


def _age_seconds(ts: str | None) -> float | None:
    if not ts:
        return None
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except ValueError:
        return None
    return round((datetime.now(UTC) - dt).total_seconds(), 1)


_EXCEPTION_LINE = re.compile(r"^\s*([A-Za-z_][\w.$]*):\s")


def error_class(text: str) -> str:
    """Most specific exception class in a query-termination message.

    For Python foreachBatch failures Spark wraps the traceback in a Py4JException;
    the last ``Name: message`` line of the traceback is the real error.
    """
    names = [
        m.group(1)
        for line in text.splitlines()
        if (m := _EXCEPTION_LINE.match(line))
        and not m.group(1).startswith("py4j.")
        and ("." in m.group(1) or m.group(1).endswith(("Error", "Exception")))
    ]
    return names[-1].rsplit(".", 1)[-1] if names else "unknown"


def _timestamp(ts: str | None) -> datetime | None:
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00")) if ts else None
    except ValueError:
        return None
    return dt.replace(tzinfo=UTC) if dt is not None and dt.tzinfo is None else dt


def streaming_health() -> dict[str, Any]:
    """Health of the CURRENT streaming run, kept apart from the error history.

    ``progress.jsonl`` holds every run that used this runtime directory. Before stream
    runtimes were isolated, failure-injection experiments (HBase stopped on purpose,
    injected partial failures) also wrote here, so a whole-file error count is history,
    not current health. Records carry ``run_id`` since then; older records are assigned
    to the current run only if they are not older than its start.
    """
    s = get_settings()
    rt = s.stream_runtime_dir
    status_file = rt / "status.json"
    if not status_file.exists():
        return {"ok": None, "detail": "streaming job has never run"}
    status = json.loads(status_file.read_text())
    run_id = status.get("run_id")
    started = _timestamp(status.get("started_at"))

    def in_current_run(rec: dict[str, Any]) -> bool:
        if rec.get("run_id"):
            return bool(rec["run_id"] == run_id)
        ts = _timestamp(rec.get("ts"))
        return started is not None and ts is not None and ts >= started

    last: dict[str, Any] = {}
    current_errors: list[dict[str, Any]] = []
    history: dict[str, int] = {}
    files = [rt / "progress.jsonl.1", rt / "progress.jsonl"]
    for path in files:
        if not path.exists():
            continue
        for line in path.read_text().splitlines():
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue  # a line being appended concurrently
            current = in_current_run(rec)
            if rec.get("event") == "progress" and current:
                last[rec["name"]] = rec
            elif rec.get("event") == "terminated" and rec.get("exception"):
                cls = error_class(rec["exception"])
                history[cls] = history.get(cls, 0) + 1
                if current:
                    current_errors.append({"ts": rec.get("ts"), "error": cls})
    heartbeat_age = _age_seconds(status.get("heartbeat"))
    running = status.get("state") != "stopped" and heartbeat_age is not None and heartbeat_age < 30
    return {
        "ok": running if status.get("mode") == "continuous" else None,
        "state": "running" if running else status.get("state", "unknown"),
        "mode": status.get("mode"),
        "heartbeat_age_s": heartbeat_age,
        "restarts": status.get("restarts", {}),
        "current_run": {
            "run_id": run_id,
            "started_at": status.get("started_at"),
            "stopped_at": status.get("stopped_at"),
            "queries_terminated_with_error": len(current_errors),
            "errors": current_errors[-20:],
            "event_time": status.get("event_time"),
        },
        "history": {
            "scope": f"all runs recorded in {rt.name}/progress.jsonl[.1]",
            "queries_terminated_with_error": sum(history.values()),
            "by_error_class": dict(sorted(history.items(), key=lambda kv: -kv[1])),
        },
        "last_batches": {
            k: {
                "batch_id": v.get("batch_id"),
                "ts": v.get("ts"),
                "input_rows": v.get("input_rows"),
                "processed_rows_per_s": v.get("processed_rows_per_s"),
                "watermark": v.get("watermark"),
                "dropped_by_watermark": v.get("dropped_by_watermark"),
            }
            for k, v in last.items()
        },
    }


def _newest_mtime(path: Path, pattern: str = "**/*.parquet") -> float | None:
    if not path.exists():
        return None
    newest = None
    for p in path.glob(pattern):
        m = p.stat().st_mtime
        newest = m if newest is None or m > newest else newest
    return newest


def data_freshness() -> dict[str, Any]:
    s = get_settings()
    now = time.time()
    out: dict[str, Any] = {}
    for name, path in (
        ("bronze_stream", s.bronze_dir / "stream"),
        ("silver_stream", s.lake_dir / "silver_stream"),
        ("silver_batch", s.silver_dir),
        ("gold_batch", s.gold_dir),
    ):
        m = _newest_mtime(path)
        out[name] = {
            "last_write_age_s": round(now - m, 1) if m else None,
            "last_write": datetime.fromtimestamp(m, UTC).isoformat() if m else None,
        }
    reports = s.lake_dir / "_reports"
    out["quarantine_counts"] = {}
    if reports.exists():
        for rp in sorted(reports.glob("silver_*.json")):
            rep = json.loads(rp.read_text())
            out["quarantine_counts"][rep.get("dataset_id", rp.stem)] = rep.get("quarantine_total")
    return out


def system_health(required: tuple[str, ...] = ("kafka", "hbase")) -> dict[str, Any]:
    from cachemind.ops.infra import memory_status

    report: dict[str, Any] = {
        "checked_at": datetime.now(UTC).isoformat(),
        "memory": memory_status(),
        "kafka": kafka_health(),
        "hbase": hbase_health(),
        "spark_streaming": streaming_health(),
        "data": data_freshness(),
    }
    report["all_required_ok"] = all(report[r]["ok"] for r in required)
    return report
