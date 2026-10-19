"""Shared fixtures and infrastructure gating.

Integration tests that need Kafka / HBase / a JVM are marked. When a service is
unreachable they are *skipped with an explicit reason* during ordinary runs, but
when ``CACHEMIND_REQUIRE_INFRA=1`` (set by ``make integration-test`` /
``make e2e-test``) an unreachable service is a test FAILURE. That way a skipped
integration test can never be mistaken for a passing one in a verification run.
"""

from __future__ import annotations

import os
import socket
from pathlib import Path

import pytest

REQUIRE_INFRA = os.environ.get("CACHEMIND_REQUIRE_INFRA") == "1"


def _port_open(host: str, port: int, timeout: float = 1.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def require_service(name: str, host: str, port: int) -> None:
    if _port_open(host, port):
        return
    msg = f"{name} not reachable at {host}:{port}"
    if REQUIRE_INFRA:
        pytest.fail(f"{msg} (CACHEMIND_REQUIRE_INFRA=1: unavailable infrastructure is a failure)")
    pytest.skip(f"{msg} - start it with `make infra-up` (NOT verified in this run)")


@pytest.fixture
def tmp_data_dir(tmp_path: Path) -> Path:
    d = tmp_path / "data"
    d.mkdir()
    return d
