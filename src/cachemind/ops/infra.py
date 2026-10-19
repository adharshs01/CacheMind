"""Start/stop/inspect the native (non-Docker) Kafka and HBase services.

    python -m cachemind.ops.infra up kafka|hbase|all
    python -m cachemind.ops.infra down kafka|hbase|all
    python -m cachemind.ops.infra status
    python -m cachemind.ops.infra health

Binaries come from ``.tools/`` (checksum-verified by scripts/fetch_tools.py);
state, logs and pid files live in ``.runtime/<service>/``. Every listener binds
to 127.0.0.1. Heaps are capped (CACHEMIND_KAFKA_HEAP, CACHEMIND_HBASE_HEAP,
CACHEMIND_HBASE_REST_HEAP) and a service refuses to start when free memory is
below CACHEMIND_MIN_FREE_MEMORY_PCT — this machine has 8 GB and a history of
freezing under memory pressure.

Nothing here deletes data. ``reset`` (which wipes a service's runtime data) is
a separate, explicit command that only touches ``.runtime/<service>/data``.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import shutil
import signal
import socket
import string
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import psutil

from cachemind.config import REPO_ROOT, get_settings

KAFKA_PORT = 9092
KAFKA_CONTROLLER_PORT = 9093
HBASE_ZK_PORT = 2181
HBASE_UI_PORT = 16010
HBASE_REST_PORT = 8080


class InfraError(RuntimeError):
    pass


def _tools() -> dict[str, Path]:
    manifest = REPO_ROOT / ".tools" / "tools.json"
    if not manifest.exists():
        raise InfraError("tools not installed: run `make fetch-tools` (scripts/fetch_tools.py)")
    return {k: REPO_ROOT / v for k, v in json.loads(manifest.read_text()).items()}


def _env_value(name: str, default: str) -> str:
    from cachemind.config import _load_dotenv

    return os.environ.get(name) or _load_dotenv(REPO_ROOT / ".env").get(name) or default


def port_open(port: int, host: str = "127.0.0.1", timeout: float = 0.5) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def memory_status() -> dict[str, Any]:
    vm = psutil.virtual_memory()
    return {
        "total_gb": round(vm.total / 2**30, 2),
        "available_gb": round(vm.available / 2**30, 2),
        "available_pct": round(100 * vm.available / vm.total, 1),
        "swap_used_gb": round(psutil.swap_memory().used / 2**30, 2),
    }


def _heap_mb(spec: str) -> int:
    spec = spec.strip().lower()
    return int(spec[:-1]) * 1024 if spec.endswith("g") else int(spec.rstrip("m"))


def memory_guard(service: str, heap: str) -> None:
    s = get_settings()
    mem = memory_status()
    need_gb = _heap_mb(heap) * 1.5 / 1024  # heap + metaspace/threads/native overhead
    if mem["available_pct"] < s.min_free_memory_pct or mem["available_gb"] < need_gb:
        raise InfraError(
            f"refusing to start {service}: {mem['available_gb']} GB ({mem['available_pct']}%) "
            f"available, need >= {need_gb:.1f} GB and >= {s.min_free_memory_pct}% free. "
            "Stop other services/apps or lower the heap (see .env.example)."
        )


def _render(template: Path, dest: Path, values: dict[str, Any]) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(
        string.Template(template.read_text()).substitute({k: str(v) for k, v in values.items()})
    )


def _wait(predicate, timeout: float, what: str, log_file: Path | None = None) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return
        time.sleep(1.0)
    tail = ""
    if log_file and log_file.exists():
        tail = "\n".join(log_file.read_text(errors="replace").splitlines()[-25:])
    raise InfraError(f"timed out after {timeout:.0f}s waiting for {what}\n{tail}")


def _pid_alive(pid_file: Path) -> int | None:
    if not pid_file.exists():
        return None
    try:
        pid = int(pid_file.read_text().strip())
    except ValueError:
        return None
    return pid if psutil.pid_exists(pid) else None


def _gone(proc: psutil.Process) -> bool:
    try:
        return proc.status() == psutil.STATUS_ZOMBIE  # exited, waiting to be reaped
    except psutil.NoSuchProcess:
        return True


def _terminate(pid: int, timeout: float = 45.0, kill_timeout: float = 60.0) -> None:
    """SIGTERM, then SIGKILL. A process blocked in uninterruptible I/O (observed on this
    laptop when the disk was starved) can take tens of seconds to die even after SIGKILL,
    so the second wait is long; a zombie counts as terminated."""
    try:
        proc = psutil.Process(pid)
    except psutil.NoSuchProcess:
        return
    proc.send_signal(signal.SIGTERM)  # Kafka/HBase handle SIGTERM with an orderly shutdown
    try:
        proc.wait(timeout)
        return
    except psutil.TimeoutExpired:
        pass
    with contextlib.suppress(psutil.NoSuchProcess):
        proc.kill()
    deadline = time.time() + kill_timeout
    while time.time() < deadline:
        if _gone(proc):
            return
        time.sleep(1.0)
    raise InfraError(f"process {pid} did not exit within {kill_timeout:.0f}s of SIGKILL")


@dataclass
class ServiceDirs:
    root: Path

    @property
    def data(self) -> Path:
        return self.root / "data"

    @property
    def logs(self) -> Path:
        return self.root / "logs"

    @property
    def conf(self) -> Path:
        return self.root / "conf"

    @property
    def pid(self) -> Path:
        return self.root / "pids"


def _dirs(service: str) -> ServiceDirs:
    d = ServiceDirs(get_settings().runtime_dir / service)
    for p in (d.data, d.logs, d.conf, d.pid):
        p.mkdir(parents=True, exist_ok=True)
    return d


def _java_env() -> dict[str, str]:
    env = dict(os.environ)
    java_home = get_settings().java_home
    if not java_home:
        raise InfraError("no JDK 17 found: run `make fetch-tools`")
    env["JAVA_HOME"] = java_home
    env["PATH"] = f"{java_home}/bin:{env.get('PATH', '')}"
    return env


# --- Kafka -------------------------------------------------------------------


def kafka_up() -> dict[str, Any]:
    d = _dirs("kafka")
    if _pid_alive(d.pid / "kafka.pid"):
        return {"service": "kafka", "status": "already running"}
    if port_open(KAFKA_PORT):
        raise InfraError(f"port {KAFKA_PORT} is in use by another process")
    heap = _env_value("CACHEMIND_KAFKA_HEAP", "384m")
    memory_guard("kafka", heap)
    home = _tools()["kafka"]
    conf = d.conf / "server.properties"
    _render(
        REPO_ROOT / "infra/native/kafka/server.properties",
        conf,
        {
            "KAFKA_DATA_DIR": d.data,
            "KAFKA_PORT": KAFKA_PORT,
            "KAFKA_CONTROLLER_PORT": KAFKA_CONTROLLER_PORT,
        },
    )
    env = _java_env()
    env.update(
        {
            "KAFKA_HEAP_OPTS": f"-Xms{heap} -Xmx{heap}",
            "LOG_DIR": str(d.logs),
            "KAFKA_JVM_PERFORMANCE_OPTS": "-server -XX:+UseG1GC -XX:MaxGCPauseMillis=50 "
            "-XX:+ExplicitGCInvokesConcurrent",
        }
    )
    if not (d.data / "meta.properties").exists():
        subprocess.run(
            [
                str(home / "bin/kafka-storage.sh"),
                "format",
                "--standalone",
                "-t",
                _kafka_cluster_id(home, env, d.root / "cluster.id"),
                "-c",
                str(conf),
            ],
            env=env,
            check=True,
            capture_output=True,
            text=True,
        )
    log_file = d.logs / "kafka-server.out"
    with log_file.open("ab") as out:
        proc = subprocess.Popen(
            [str(home / "bin/kafka-server-start.sh"), str(conf)],
            env=env,
            stdout=out,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    (d.pid / "kafka.pid").write_text(str(proc.pid))
    _wait(lambda: port_open(KAFKA_PORT) or proc.poll() is not None, 90, "Kafka port", log_file)
    if proc.poll() is not None:
        raise InfraError(f"Kafka exited with code {proc.returncode}; see {log_file}")
    _wait(_kafka_metadata_ready, 60, "Kafka metadata", log_file)
    return {"service": "kafka", "status": "started", "pid": proc.pid, "heap": heap}


def _kafka_cluster_id(home: Path, env: dict[str, str], cache: Path) -> str:
    """Cluster id generated once by Kafka's own tool and reused for re-formats."""
    if cache.exists():
        return cache.read_text().strip()
    res = subprocess.run(
        [str(home / "bin/kafka-storage.sh"), "random-uuid"],
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )
    cid = res.stdout.strip().splitlines()[-1]
    cache.write_text(cid)
    return cid


def _kafka_metadata_ready() -> bool:
    from confluent_kafka.admin import AdminClient

    try:
        md = AdminClient({"bootstrap.servers": get_settings().kafka_bootstrap}).list_topics(
            timeout=3
        )
        return len(md.brokers) >= 1
    except Exception:  # noqa: BLE001 - readiness probe: any failure means "not yet"
        return False


def ensure_topic(
    topic: str | None = None, partitions: int = 3, attempts: int = 4, wait_s: float = 60.0
) -> dict[str, Any]:
    """Create the request topic if missing (idempotent).

    A create request can time out on the client while the broker still completes it
    (observed when the broker was briefly fenced after the laptop slept), so the
    outcome is verified against the cluster metadata instead of trusting the timeout.
    """
    from concurrent.futures import TimeoutError as FutureTimeout

    from confluent_kafka import KafkaException
    from confluent_kafka.admin import AdminClient, NewTopic

    s = get_settings()
    topic = topic or s.kafka_topic
    admin = AdminClient({"bootstrap.servers": s.kafka_bootstrap})
    last_error: Exception | None = None
    for attempt in range(attempts):
        existing = admin.list_topics(timeout=15).topics
        if topic in existing and existing[topic].partitions:
            return {
                "topic": topic,
                "created": attempt > 0,
                "partitions": len(existing[topic].partitions),
            }
        futures = admin.create_topics(
            [
                NewTopic(
                    topic,
                    num_partitions=partitions,
                    replication_factor=1,
                    config={
                        "retention.ms": str(7 * 24 * 3600 * 1000),
                        "cleanup.policy": "delete",
                        "min.insync.replicas": "1",
                    },
                )
            ]
        )
        try:
            futures[topic].result(timeout=wait_s)
            return {"topic": topic, "created": True, "partitions": partitions}
        except (FutureTimeout, KafkaException) as exc:
            last_error = exc  # verify on the next loop iteration whether it exists
            time.sleep(2 * (attempt + 1))
    raise InfraError(
        f"could not create Kafka topic {topic!r} after {attempts} attempts: "
        f"{type(last_error).__name__}: {last_error}"
    )


def kafka_down() -> dict[str, Any]:
    d = _dirs("kafka")
    pid = _pid_alive(d.pid / "kafka.pid")
    if not pid:
        return {"service": "kafka", "status": "not running"}
    _terminate(pid)
    (d.pid / "kafka.pid").unlink(missing_ok=True)
    return {"service": "kafka", "status": "stopped", "pid": pid}


# --- HBase -------------------------------------------------------------------


def _hbase_env(d: ServiceDirs, heap: str) -> dict[str, str]:
    env = _java_env()
    env.update(
        {
            "HBASE_CONF_DIR": str(d.conf),
            "HBASE_LOG_DIR": str(d.logs),
            "HBASE_PID_DIR": str(d.pid),
            "HBASE_HEAPSIZE": heap,
            "HBASE_IDENT_STRING": "cachemind",
            "HBASE_MANAGES_ZK": "true",
            "HBASE_DISABLE_HADOOP_CLASSPATH_LOOKUP": "true",
            # "localhost" must resolve to 127.0.0.1, matching the IPv4 bind address
            "HBASE_OPTS": "-Djava.net.preferIPv4Stack=true -XX:+UseG1GC",
        }
    )
    return env


def hbase_up() -> dict[str, Any]:
    d = _dirs("hbase")
    home = _tools()["hbase"]
    heap = _env_value("CACHEMIND_HBASE_HEAP", "512m")
    rest_heap = _env_value("CACHEMIND_HBASE_REST_HEAP", "192m")
    master_pid = d.pid / "hbase-cachemind-master.pid"
    rest_pid = d.pid / "hbase-cachemind-rest.pid"
    started = []
    if not _pid_alive(master_pid):
        for port in (HBASE_ZK_PORT, 16000, 16020, HBASE_UI_PORT):
            if port_open(port):
                raise InfraError(f"port {port} is in use by another process")
        memory_guard("hbase", heap)
        _render(
            REPO_ROOT / "infra/native/hbase/hbase-site.xml",
            d.conf / "hbase-site.xml",
            {
                "HBASE_DATA_DIR": d.data,
                "HBASE_ZK_PORT": HBASE_ZK_PORT,
                "HBASE_UI_PORT": HBASE_UI_PORT,
                "HBASE_REST_PORT": HBASE_REST_PORT,
            },
        )
        shutil.copy(
            REPO_ROOT / "infra/native/hbase/log4j2.properties", d.conf / "log4j2.properties"
        )
        (d.conf / "hbase-env.sh").write_text("# intentionally empty: env is set by cachemind\n")
        (d.conf / "regionservers").write_text("127.0.0.1\n")
        env = _hbase_env(d, heap)
        subprocess.run(
            [str(home / "bin/hbase-daemon.sh"), "--config", str(d.conf), "start", "master"],
            env=env,
            check=True,
            capture_output=True,
            text=True,
        )
        log_file = next(iter(sorted(d.logs.glob("hbase-cachemind-master-*.log"))), None)

        def master_up() -> bool:
            if not _pid_alive(master_pid):
                raise InfraError(f"HBase master exited during startup; see {log_file}")
            return port_open(HBASE_ZK_PORT) and port_open(16000) and _master_registered(log_file)

        _wait(master_up, 180, "HBase master initialisation", log_file)
        started.append("master")
    if not _pid_alive(rest_pid):
        if port_open(HBASE_REST_PORT):
            raise InfraError(f"port {HBASE_REST_PORT} is in use by another process")
        memory_guard("hbase-rest", rest_heap)
        env = _hbase_env(d, heap)
        env["HBASE_REST_OPTS"] = f"-Xmx{rest_heap} -Xms64m"
        subprocess.run(
            [
                str(home / "bin/hbase-daemon.sh"),
                "--config",
                str(d.conf),
                "start",
                "rest",
                "-p",
                str(HBASE_REST_PORT),
            ],
            env=env,
            check=True,
            capture_output=True,
            text=True,
        )
        log_file = next(iter(sorted(d.logs.glob("hbase-cachemind-rest-*.log"))), None)
        _wait(_hbase_rest_ready, 120, "HBase REST gateway + cluster", log_file)
        started.append("rest")
    return {
        "service": "hbase",
        "status": "started" if started else "already running",
        "started": started,
        "heap": heap,
        "rest_heap": rest_heap,
    }


def _master_registered(log_file: Path | None) -> bool:
    if log_file is None or not log_file.exists():
        return False
    text = log_file.read_text(errors="replace")
    return "Master has completed initialization" in text


def _hbase_rest_ready() -> bool:
    import requests

    try:
        r = requests.get(
            f"{get_settings().hbase_rest_url}/status/cluster",
            headers={"Accept": "application/json"},
            timeout=3,
        )
        return r.status_code == 200 and r.json().get("LiveNodes")
    except (requests.RequestException, ValueError):
        return False


def hbase_down() -> dict[str, Any]:
    d = _dirs("hbase")
    home = _tools()["hbase"]
    env = _hbase_env(d, "256m")
    stopped = []
    for daemon in ("rest", "master"):
        pid = _pid_alive(d.pid / f"hbase-cachemind-{daemon}.pid")
        if not pid:
            continue
        # The script waits for an orderly exit; if that takes too long, signals follow.
        with contextlib.suppress(subprocess.TimeoutExpired):
            subprocess.run(
                [str(home / "bin/hbase-daemon.sh"), "--config", str(d.conf), "stop", daemon],
                env=env,
                capture_output=True,
                text=True,
                timeout=90,
                check=False,
            )
        if psutil.pid_exists(pid):
            _terminate(pid)  # SIGTERM, then SIGKILL after a grace period; raises if stuck
        stopped.append(daemon)
    return {
        "service": "hbase",
        "status": "stopped" if stopped else "not running",
        "stopped": stopped,
    }


# --- status / CLI -------------------------------------------------------------


def status() -> dict[str, Any]:
    s = get_settings()
    out: dict[str, Any] = {"memory": memory_status(), "services": {}}
    rt = s.runtime_dir
    for name, pid_file, ports in (
        ("kafka", rt / "kafka/pids/kafka.pid", [KAFKA_PORT, KAFKA_CONTROLLER_PORT]),
        (
            "hbase-master",
            rt / "hbase/pids/hbase-cachemind-master.pid",
            [HBASE_ZK_PORT, 16000, HBASE_UI_PORT],
        ),
        ("hbase-rest", rt / "hbase/pids/hbase-cachemind-rest.pid", [HBASE_REST_PORT]),
    ):
        pid = _pid_alive(pid_file)
        rss = None
        if pid:
            try:
                rss = round(psutil.Process(pid).memory_info().rss / 2**20)
            except psutil.NoSuchProcess:
                pid = None
        out["services"][name] = {
            "pid": pid,
            "rss_mb": rss,
            "ports": {p: port_open(p) for p in ports},
        }
    return out


def reset(service: str) -> dict[str, Any]:
    """Wipe a STOPPED service's runtime data dir (project-generated state only)."""
    d = ServiceDirs(get_settings().runtime_dir / service)
    if service not in ("kafka", "hbase"):
        raise InfraError("reset accepts kafka or hbase")
    st = status()["services"]
    if (service == "kafka" and st["kafka"]["pid"]) or (
        service == "hbase" and (st["hbase-master"]["pid"] or st["hbase-rest"]["pid"])
    ):
        raise InfraError(f"stop {service} before resetting it")
    if d.data.exists():
        shutil.rmtree(d.data)
    if service == "kafka":
        (d.root / "cluster.id").unlink(missing_ok=True)
    return {"service": service, "reset": str(d.data)}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("action", choices=["up", "down", "status", "health", "topic", "reset"])
    ap.add_argument("service", nargs="?", default="all", choices=["kafka", "hbase", "all"])
    args = ap.parse_args(argv)
    try:
        if args.action == "up":
            res = []
            if args.service in ("kafka", "all"):
                res.append(kafka_up())
                res.append(ensure_topic())
            if args.service in ("hbase", "all"):
                res.append(hbase_up())
                from cachemind.bigdata.hbase_store import ensure_tables

                res.append(ensure_tables())
            print(json.dumps(res, indent=2, default=str))
        elif args.action == "down":
            res = []
            if args.service in ("hbase", "all"):
                res.append(hbase_down())
            if args.service in ("kafka", "all"):
                res.append(kafka_down())
            print(json.dumps(res, indent=2))
        elif args.action == "status":
            print(json.dumps(status(), indent=2))
        elif args.action == "health":
            from cachemind.monitoring.health import system_health

            report = system_health()
            print(json.dumps(report, indent=2, default=str))
            return 0 if report["all_required_ok"] else 2
        elif args.action == "topic":
            print(json.dumps(ensure_topic(), indent=2))
        elif args.action == "reset":
            if args.service == "all":
                raise InfraError("reset needs an explicit service (kafka or hbase)")
            print(json.dumps(reset(args.service), indent=2))
    except InfraError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
