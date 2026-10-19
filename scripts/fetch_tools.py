#!/usr/bin/env python3
"""Download and verify project-local JVM tooling (JDK 17, Kafka, HBase).

Everything is written under ``<repo>/.tools``; nothing is installed system-wide.
Every archive is verified against the publisher's checksum *before* extraction,
and extraction uses the ``data`` tarfile filter to reject path traversal.

Usage:
    python scripts/fetch_tools.py            # fetch everything that is missing
    python scripts/fetch_tools.py --only kafka
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import ssl
import sys
import tarfile
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
TOOLS = REPO / ".tools"
DOWNLOADS = TOOLS / "downloads"

# Pinned versions. See docs/ENVIRONMENT_AUDIT.md for the compatibility matrix.
JDK_FEATURE_VERSION = 17
KAFKA_VERSION = "4.1.2"
KAFKA_SCALA = "2.13"
HBASE_VERSION = "2.6.7"


@dataclass(frozen=True)
class Artifact:
    name: str
    url: str
    algorithm: str  # "sha256" | "sha512"
    expected_digest: str
    extracted_dir: str  # top-level directory inside the archive


def _ssl_context() -> ssl.SSLContext:
    """A *verifying* TLS context. Some Python builds ship without a CA bundle,
    so fall back to certifi or the macOS system bundle; never disable checks."""
    candidates: list[str] = []
    try:
        import certifi

        candidates.append(certifi.where())
    except ImportError:
        pass
    candidates.append("/etc/ssl/cert.pem")
    for cafile in candidates:
        if Path(cafile).exists():
            return ssl.create_default_context(cafile=cafile)
    return ssl.create_default_context()


_CTX = _ssl_context()


def _open(url: str, timeout: int):
    req = urllib.request.Request(url, headers={"User-Agent": "cachemind-fetch-tools/1.0"})
    return urllib.request.urlopen(req, timeout=timeout, context=_CTX)


def _http_get(url: str, timeout: int = 60) -> bytes:
    with _open(url, timeout) as resp:
        return resp.read()


def _parse_apache_sha512(text: str) -> str:
    """Parse Apache's GPG-style ``file: HEX HEX ...`` checksum format."""
    _, _, digest_part = text.partition(":")
    digest = re.sub(r"[^0-9A-Fa-f]", "", digest_part).lower()
    if len(digest) != 128:
        raise ValueError(f"unexpected sha512 file contents: {text[:200]!r}")
    return digest


def jdk_artifact() -> Artifact:
    api = (
        f"https://api.adoptium.net/v3/assets/latest/{JDK_FEATURE_VERSION}/hotspot"
        "?architecture=aarch64&image_type=jdk&os=mac&vendor=eclipse"
    )
    meta = json.loads(_http_get(api))[0]
    pkg = meta["binary"]["package"]
    return Artifact(
        name="jdk",
        url=pkg["link"],
        algorithm="sha256",
        expected_digest=pkg["checksum"].lower(),
        extracted_dir=meta["release_name"],
    )


def kafka_artifact() -> Artifact:
    fname = f"kafka_{KAFKA_SCALA}-{KAFKA_VERSION}.tgz"
    base = f"https://downloads.apache.org/kafka/{KAFKA_VERSION}/{fname}"
    digest = _parse_apache_sha512(_http_get(base + ".sha512").decode())
    return Artifact("kafka", base, "sha512", digest, f"kafka_{KAFKA_SCALA}-{KAFKA_VERSION}")


def hbase_artifact() -> Artifact:
    fname = f"hbase-{HBASE_VERSION}-hadoop3-bin.tar.gz"
    base = f"https://downloads.apache.org/hbase/{HBASE_VERSION}/{fname}"
    digest = _parse_apache_sha512(_http_get(base + ".sha512").decode())
    return Artifact("hbase", base, "sha512", digest, f"hbase-{HBASE_VERSION}-hadoop3")


def download(art: Artifact) -> Path:
    DOWNLOADS.mkdir(parents=True, exist_ok=True)
    target = DOWNLOADS / Path(urllib.parse.urlparse(art.url).path).name
    if target.exists() and _digest(target, art.algorithm) == art.expected_digest:
        print(f"[{art.name}] cached archive verified: {target.name}")
        return target
    tmp = target.with_suffix(target.suffix + ".part")
    print(f"[{art.name}] downloading {art.url}")
    with _open(art.url, 120) as resp, tmp.open("wb") as fh:
        while chunk := resp.read(1 << 20):
            fh.write(chunk)
    actual = _digest(tmp, art.algorithm)
    if actual != art.expected_digest:
        tmp.unlink(missing_ok=True)
        raise RuntimeError(
            f"[{art.name}] {art.algorithm} mismatch: expected {art.expected_digest}, got {actual}"
        )
    tmp.rename(target)
    print(f"[{art.name}] {art.algorithm} verified")
    return target


def _digest(path: Path, algorithm: str) -> str:
    h = hashlib.new(algorithm)
    with path.open("rb") as fh:
        while chunk := fh.read(1 << 20):
            h.update(chunk)
    return h.hexdigest()


def extract(archive: Path, art: Artifact) -> Path:
    dest = TOOLS / art.extracted_dir
    if dest.exists():
        print(f"[{art.name}] already extracted: {dest.relative_to(REPO)}")
        return dest
    with tarfile.open(archive) as tf:
        tf.extractall(TOOLS, filter="data")
    if not dest.exists():
        raise RuntimeError(f"[{art.name}] expected directory {dest} after extraction")
    print(f"[{art.name}] extracted to {dest.relative_to(REPO)}")
    return dest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--only", choices=["jdk", "kafka", "hbase"], action="append")
    args = parser.parse_args()
    wanted = args.only or ["jdk", "kafka", "hbase"]
    builders = {"jdk": jdk_artifact, "kafka": kafka_artifact, "hbase": hbase_artifact}
    resolved: dict[str, str] = {}
    for name in wanted:
        art = builders[name]()
        path = extract(download(art), art)
        resolved[name] = str(path.relative_to(REPO))
    manifest = TOOLS / "tools.json"
    existing = json.loads(manifest.read_text()) if manifest.exists() else {}
    existing.update(resolved)
    manifest.write_text(json.dumps(existing, indent=2) + "\n")
    print(f"tool manifest: {manifest.relative_to(REPO)} -> {existing}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
