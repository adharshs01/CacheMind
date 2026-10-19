"""Apache HBase operational store, accessed through the HBase REST gateway.

Why REST: it ships with HBase, needs no extra native client or Thrift build,
and works identically from the laptop and from Docker. All cells are written as
UTF-8 text so they can be inspected with ``hbase shell`` or curl.

Tables, column families and row keys
------------------------------------
cache_requests   request:{event_id,item_id,user_id,event_time_ms,seq}
                 metadata:{category,region,item_size_bytes,user_group}
                 lineage:{source,source_file,source_line,topic,partition,offset,dataset_id}
    row key  = sha1(event_id)[:4] + ":" + event_id
    Point lookup by event id (audit). The 4-hex salt spreads otherwise clustered
    ids across regions; it is derived from the key itself, so lookups need no
    secondary index. Re-writing an event writes the same key -> idempotent.

item_statistics  access:{total_requests,recent_requests,last_access_ms,dataset_id}
                 popularity:{popularity_rank,popularity_score,trend_score,window_start_ms,
                             window_end_ms}
                 windows:<13-digit window_start_ms> = request count in that event-time window
    row key  = sha1(dataset_id#item_id)[:2] + ":" + dataset_id + "#" + item_id
    (item ids are only unique within a dataset). One WIDE row per (dataset, item):
    a point lookup returns current statistics and the item's recent per-window
    counts together; the windows family has a TTL so old
    columns expire automatically. All values are ABSOLUTE (set, never
    incremented), so replayed micro-batches cannot double count.

cache_experiments configuration:{run_id,experiment,policy,cache_capacity,dataset_version,
                                 workload,seed,horizon}
                  performance:{hits,misses,hit_rate,miss_rate,evictions,wall_time_s}
                  metadata:{model_version,execution_timestamp,code_version}
    row key  = experiment#workload#policy#capacity(8 digits)#seed(10 digits)
    Prefix scans return an experiment, or an experiment+workload+policy series,
    in capacity order. Components are validated to exclude '#', so keys are
    unambiguous.

HBase is the OPERATIONAL store (point reads/writes, small scans). Large
historical scans and analytics use the Parquet lakehouse instead.
"""

from __future__ import annotations

import base64
import time
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote

import requests

from cachemind.config import get_settings
from cachemind.contracts import key_component, salt_prefix
from cachemind.logging_utils import get_logger, log_event

log = get_logger("cachemind.hbase")

DAY = 24 * 3600

TABLES: dict[str, dict[str, dict[str, Any]]] = {
    "cache_requests": {
        "request": {"VERSIONS": "1"},
        "metadata": {"VERSIONS": "1"},
        "lineage": {"VERSIONS": "1"},
    },
    "item_statistics": {
        "access": {"VERSIONS": "1"},
        "popularity": {"VERSIONS": "1"},
        "windows": {"VERSIONS": "1", "TTL": str(30 * DAY)},
    },
    "cache_experiments": {
        "configuration": {"VERSIONS": "1"},
        "performance": {"VERSIONS": "1"},
        "metadata": {"VERSIONS": "1"},
    },
}


class HBaseError(RuntimeError):
    pass


class HBaseUnavailable(HBaseError):
    pass


def _b64(s: str | bytes) -> str:
    return base64.b64encode(s.encode() if isinstance(s, str) else s).decode()


def _unb64(s: str) -> str:
    return base64.b64decode(s).decode()


def _next_prefix(prefix: str) -> str:
    """Smallest key greater than every key starting with ``prefix``."""
    b = bytearray(prefix.encode())
    while b and b[-1] == 0xFF:
        b.pop()
    if not b:
        return ""
    b[-1] += 1
    return b.decode("latin-1")


@dataclass
class HBaseRestClient:
    base_url: str = ""
    timeout: float = 10.0
    retries: int = 4
    backoff: float = 0.5
    table_prefix: str = ""

    def __post_init__(self) -> None:
        self.base_url = (self.base_url or get_settings().hbase_rest_url).rstrip("/")
        self._session = requests.Session()
        self._session.headers.update({"Accept": "application/json"})

    def table(self, name: str) -> str:
        return f"{self.table_prefix}{name}"

    # --- transport with bounded retries ------------------------------------------
    def _request(
        self,
        method: str,
        path: str,
        *,
        ok: tuple[int, ...] = (200,),
        allow_404: bool = False,
        **kw: Any,
    ) -> requests.Response:
        url = f"{self.base_url}{path}"
        last_exc: Exception | None = None
        for attempt in range(self.retries + 1):
            try:
                resp = self._session.request(method, url, timeout=self.timeout, **kw)
            except (requests.ConnectionError, requests.Timeout) as exc:
                last_exc = exc
            else:
                if resp.status_code in ok or (allow_404 and resp.status_code == 404):
                    return resp
                if resp.status_code < 500:
                    raise HBaseError(f"{method} {path} -> {resp.status_code}: {resp.text[:300]}")
                last_exc = HBaseError(f"{method} {path} -> {resp.status_code}: {resp.text[:300]}")
            if attempt < self.retries:
                time.sleep(self.backoff * 2**attempt)
        if isinstance(last_exc, requests.RequestException):
            raise HBaseUnavailable(f"HBase REST unreachable at {self.base_url}: {last_exc}")
        raise HBaseError(str(last_exc))

    # --- cluster / schema ---------------------------------------------------------
    def version(self) -> dict[str, Any]:
        return self._request("GET", "/version/cluster").json()

    def cluster_status(self) -> dict[str, Any]:
        return self._request("GET", "/status/cluster").json()

    def list_tables(self) -> list[str]:
        body = self._request("GET", "/").json()
        return [t["name"] for t in body.get("table", [])]

    def create_table(self, name: str, families: dict[str, dict[str, Any]]) -> None:
        schema = {
            "name": self.table(name),
            "ColumnSchema": [{"name": cf, **attrs} for cf, attrs in families.items()],
        }
        self._request(
            "PUT",
            f"/{quote(self.table(name), safe='')}/schema",
            ok=(200, 201),
            json=schema,
            headers={"Content-Type": "application/json"},
        )

    def table_schema(self, name: str) -> dict[str, Any] | None:
        resp = self._request("GET", f"/{quote(self.table(name), safe='')}/schema", allow_404=True)
        return None if resp.status_code == 404 else resp.json()

    def delete_table(self, name: str) -> None:
        self._request(
            "DELETE", f"/{quote(self.table(name), safe='')}/schema", ok=(200,), allow_404=True
        )

    # --- data ----------------------------------------------------------------------
    def put_rows(self, name: str, rows: dict[str, dict[str, Any]], chunk: int = 500) -> int:
        """Batch write. ``rows`` maps row key -> {"family:qualifier": value}."""
        items = list(rows.items())
        for i in range(0, len(items), chunk):
            cellset = {
                "Row": [
                    {
                        "key": _b64(rk),
                        "Cell": [
                            {"column": _b64(col), "$": _b64(str(val))}
                            for col, val in cells.items()
                            if val is not None
                        ],
                    }
                    for rk, cells in items[i : i + chunk]
                ]
            }
            self._request(
                "PUT",
                f"/{quote(self.table(name), safe='')}/batch",
                ok=(200, 201),
                json=cellset,
                headers={"Content-Type": "application/json"},
            )
        return len(items)

    def get_row(self, name: str, row_key: str) -> dict[str, str] | None:
        resp = self._request(
            "GET", f"/{quote(self.table(name), safe='')}/{quote(row_key, safe='')}", allow_404=True
        )
        if resp.status_code == 404:
            return None
        rows = resp.json().get("Row", [])
        return _cells(rows[0]) if rows else None

    def delete_row(self, name: str, row_key: str) -> None:
        self._request(
            "DELETE",
            f"/{quote(self.table(name), safe='')}/{quote(row_key, safe='')}",
            allow_404=True,
        )

    def scan(
        self,
        name: str,
        *,
        prefix: str | None = None,
        start_row: str | None = None,
        end_row: str | None = None,
        columns: list[str] | None = None,
        batch: int = 500,
        limit: int | None = None,
    ) -> Iterator[tuple[str, dict[str, str]]]:
        spec: dict[str, Any] = {"batch": batch}
        if prefix is not None:
            start_row, end_row = prefix, _next_prefix(prefix)
        if start_row:
            spec["startRow"] = _b64(start_row)
        if end_row:
            spec["endRow"] = _b64(end_row)
        if columns:
            spec["column"] = [_b64(c) for c in columns]
        resp = self._request(
            "PUT",
            f"/{quote(self.table(name), safe='')}/scanner",
            ok=(201,),
            json=spec,
            headers={"Content-Type": "application/json"},
        )
        location = resp.headers["Location"]
        path = (
            location[len(self.base_url) :]
            if location.startswith(self.base_url)
            else "/" + location.split("/", 3)[-1]
        )
        emitted = 0
        current_key, current_cells = None, {}
        try:
            while True:
                r = self._request("GET", path, ok=(200, 204))
                if r.status_code == 204:
                    break
                for row in r.json().get("Row", []):
                    key = _unb64(row["key"])
                    if key != current_key and current_key is not None:
                        yield current_key, current_cells
                        emitted += 1
                        if limit is not None and emitted >= limit:
                            return
                        current_cells = {}
                    current_key = key
                    current_cells.update(_cells(row))
            if current_key is not None and (limit is None or emitted < limit):
                yield current_key, current_cells
        finally:
            try:
                self._request("DELETE", path, ok=(200,), allow_404=True)
            except HBaseError as exc:  # scanner lease expires on its own; record it
                log_event(log, "scanner delete failed", error=str(exc))

    def count_rows(self, name: str, prefix: str | None = None) -> int:
        return sum(1 for _ in self.scan(name, prefix=prefix, batch=1000))


def _cells(row: dict[str, Any]) -> dict[str, str]:
    return {_unb64(c["column"]): _unb64(c["$"]) for c in row.get("Cell", [])}


# --- schema management ---------------------------------------------------------------


def ensure_tables(client: HBaseRestClient | None = None) -> dict[str, Any]:
    client = client or HBaseRestClient()
    existing = set(client.list_tables())
    result: dict[str, Any] = {}
    for name, families in TABLES.items():
        full = client.table(name)
        if full not in existing:
            client.create_table(name, families)
            result[full] = "created"
        else:
            result[full] = "exists"
        schema = client.table_schema(name) or {}
        have = {c["name"] for c in schema.get("ColumnSchema", [])}
        missing = set(families) - have
        if missing:
            raise HBaseError(f"table {full} lacks column families {sorted(missing)}")
    return result


# --- row-key design ---------------------------------------------------------------


def request_row_key(event_id: str) -> str:
    return f"{salt_prefix(event_id, 4)}:{event_id}"


def item_row_key(dataset_id: str, item_id: str) -> str:
    """Item ids are only unique within a dataset, so the dataset is part of the key."""
    identity = f"{key_component(dataset_id)}#{key_component(item_id)}"
    return f"{salt_prefix(identity, 2)}:{identity}"


def experiment_row_key(
    experiment: str, workload: str, policy: str, capacity: int, seed: int
) -> str:
    if not (0 <= capacity < 10**8 and 0 <= seed < 10**10):
        raise ValueError("capacity/seed out of range for fixed-width key encoding")
    return "#".join(
        [
            key_component(experiment),
            key_component(workload),
            key_component(policy),
            f"{capacity:08d}",
            f"{seed:010d}",
        ]
    )


def window_column(window_start_ms: int) -> str:
    """Qualifier = zero-padded epoch MILLISECONDS (13 digits, sorts chronologically)."""
    if not 0 <= window_start_ms < 10**13:
        raise ValueError(f"window start {window_start_ms} is not epoch milliseconds")
    return f"windows:{window_start_ms:013d}"


# --- record -> cells mappings ----------------------------------------------------


def request_cells(r: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    return request_row_key(r["event_id"]), {
        "request:event_id": r["event_id"],
        "request:item_id": r["item_id"],
        "request:user_id": r["user_id"],
        "request:event_time_ms": r["event_time_ms"],
        "request:seq": r["seq"],
        "metadata:category": r.get("category"),
        "metadata:region": r["region"],
        "metadata:item_size_bytes": r["item_size_bytes"],
        "metadata:user_group": r.get("user_group"),
        "lineage:dataset_id": r["dataset_id"],
        "lineage:source": r.get("source"),
        "lineage:source_file": r.get("source_file"),
        "lineage:source_line": r.get("source_line"),
        "lineage:topic": r.get("kafka_topic"),
        "lineage:partition": r.get("kafka_partition"),
        "lineage:offset": r.get("kafka_offset"),
    }


def publish_requests(client: HBaseRestClient, records: Iterable[dict[str, Any]]) -> int:
    rows = dict(request_cells(r) for r in records)
    return client.put_rows("cache_requests", rows) if rows else 0


def publish_item_windows(
    client: HBaseRestClient, windows: Iterable[tuple[str, str, int, int]]
) -> int:
    """(dataset_id, item_id, window_start_ms, count) -> absolute per-window counts."""
    rows: dict[str, dict[str, Any]] = {}
    for dataset_id, item_id, start_ms, count in windows:
        key = item_row_key(dataset_id, item_id)
        rows.setdefault(key, {})[window_column(int(start_ms))] = int(count)
    return client.put_rows("item_statistics", rows) if rows else 0


def publish_item_statistics(client: HBaseRestClient, stats: Iterable[dict[str, Any]]) -> int:
    rows = {}
    for s in stats:
        rows[item_row_key(s["dataset_id"], s["item_id"])] = {
            "access:total_requests": s["total_requests"],
            "access:recent_requests": s.get("recent_requests"),
            "access:last_access_ms": s.get("last_access_ms"),
            "access:dataset_id": s.get("dataset_id"),
            "popularity:popularity_rank": s.get("popularity_rank"),
            "popularity:popularity_score": s.get("popularity_score"),
            "popularity:trend_score": s.get("trend_score"),
            "popularity:window_start_ms": s.get("window_start_ms"),
            "popularity:window_end_ms": s.get("window_end_ms"),
        }
    return client.put_rows("item_statistics", rows) if rows else 0


def publish_experiment_results(client: HBaseRestClient, results: Iterable[dict[str, Any]]) -> int:
    rows = {}
    for r in results:
        key = experiment_row_key(
            r["experiment"], r["workload"], r["policy"], int(r["capacity"]), int(r["seed"])
        )
        rows[key] = {
            "configuration:run_id": r["run_id"],
            "configuration:experiment": r["experiment"],
            "configuration:policy": r["policy"],
            "configuration:cache_capacity": r["capacity"],
            "configuration:dataset_version": r.get("dataset_id"),
            "configuration:workload": r["workload"],
            "configuration:seed": r["seed"],
            "configuration:horizon": r.get("horizon"),
            "performance:hits": r["hits"],
            "performance:misses": r["misses"],
            "performance:hit_rate": r["hit_rate"],
            "performance:miss_rate": r["miss_rate"],
            "performance:evictions": r["evictions"],
            "performance:wall_time_s": r["wall_time_s"],
            "metadata:model_version": r.get("model_version"),
            "metadata:execution_timestamp": r.get("executed_at"),
            "metadata:code_version": r.get("code_version"),
        }
    return client.put_rows("cache_experiments", rows) if rows else 0
