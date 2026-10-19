"""Canonical event contract shared by the generator, Kafka, Spark ETL and the simulator.

The canonical (Silver) request record is::

    event_id         str   logical event identity, stable across retries/replays
    seq              int   per-dataset sequence number; unique & strictly ordered
    dataset_id       str   which generated/imported trace the event belongs to
    event_time_ms    int   event time, epoch milliseconds, UTC
    user_id          str   normalised lowercase identifier
    item_id          str   normalised lowercase identifier
    region           str   one of REGIONS
    item_size_bytes  int   0 < size <= MAX_ITEM_SIZE_BYTES
    schema_version   int   SCHEMA_VERSION

Validation is deliberately implemented twice: here in pure Python (used by the
lightweight profile, the Kafka consumer and unit tests) and as Spark column
expressions in ``cachemind.bigdata.etl``. ``tests/integration/test_spark_etl.py``
checks the two implementations agree on a fixture that triggers every rule.
The *order* of the checks is part of the contract: the first failing rule is
the reported reason.
"""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any

SCHEMA_VERSION = 1

CATEGORIES: tuple[str, ...] = ("video", "image", "audio", "document", "software", "web")
UNKNOWN_CATEGORY = "unknown"
REGIONS: tuple[str, ...] = ("us-east", "us-west", "eu-west", "ap-south", "ap-east")
USER_GROUPS: tuple[str, ...] = ("free", "premium", "enterprise")

# Legacy CSV logs use short region codes; this is the documented mapping.
REGION_ALIASES: dict[str, str] = {
    "use": "us-east",
    "usw": "us-west",
    "euw": "eu-west",
    "aps": "ap-south",
    "ape": "ap-east",
    "us_east": "us-east",
    "us_west": "us-west",
    "eu_west": "eu-west",
    "ap_south": "ap-south",
    "ap_east": "ap-east",
}
REGION_TO_LEGACY_CODE = {
    "us-east": "USE",
    "us-west": "USW",
    "eu-west": "EUW",
    "ap-south": "APS",
    "ap-east": "APE",
}

# Dirty category spellings seen in metadata, mapped to canonical values.
CATEGORY_ALIASES: dict[str, str] = {
    "vid": "video",
    "videos": "video",
    "img": "image",
    "images": "image",
    "doc": "document",
    "docs": "document",
    "app": "software",
    "apps": "software",
    "html": "web",
}

ID_PATTERN = r"^[a-z0-9][a-z0-9_-]{0,63}$"
ID_RE = re.compile(ID_PATTERN)
EVENT_ID_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$"
EVENT_ID_RE = re.compile(EVENT_ID_PATTERN)

MAX_ITEM_SIZE_BYTES = 10 * 1024**3  # 10 GiB
MIN_EVENT_TIME_MS = int(datetime(2000, 1, 1, tzinfo=UTC).timestamp() * 1000)
MAX_EVENT_TIME_MS = int(datetime(2100, 1, 1, tzinfo=UTC).timestamp() * 1000)

REQUIRED_FIELDS: tuple[str, ...] = (
    "event_id",
    "seq",
    "dataset_id",
    "event_time",
    "user_id",
    "item_id",
    "region",
    "item_size_bytes",
)

# Stable namespace so event IDs derived from (dataset_id, seq) never change.
EVENT_NAMESPACE = uuid.UUID("6f1c2a10-3c55-4a3e-9d0b-7a1f0c9e2b11")


class Reason:
    """Quarantine reason codes (stable strings, used in reports and tests)."""

    MALFORMED_JSON = "malformed_json"
    MALFORMED_CSV = "malformed_csv"
    UNSUPPORTED_SCHEMA = "unsupported_schema_version"
    MISSING_FIELD = "missing_field"  # suffixed with ":<field>"
    INVALID_EVENT_ID = "invalid_event_id"
    INVALID_DATASET_ID = "invalid_dataset_id"
    INVALID_SEQ = "invalid_seq"
    INVALID_TIMESTAMP = "invalid_timestamp"
    INVALID_ITEM_ID = "invalid_item_id"
    INVALID_USER_ID = "invalid_user_id"
    INVALID_REGION = "invalid_region"
    INVALID_SIZE = "invalid_size"
    NON_POSITIVE_SIZE = "non_positive_size"
    EXACT_DUPLICATE = "exact_duplicate"
    CONFLICTING_DUPLICATE = "conflicting_duplicate"
    SEQUENCE_CONFLICT = "sequence_conflict"


def make_event_id(dataset_id: str, seq: int) -> str:
    """Deterministic, collision-resistant logical event id (UUIDv5)."""
    return str(uuid.uuid5(EVENT_NAMESPACE, f"{dataset_id}/{seq}"))


@dataclass(frozen=True, slots=True)
class CanonicalEvent:
    event_id: str
    seq: int
    dataset_id: str
    event_time_ms: int
    user_id: str
    item_id: str
    region: str
    item_size_bytes: int
    schema_version: int = SCHEMA_VERSION

    def to_wire(self) -> dict[str, Any]:
        """Kafka / JSON representation (ISO-8601 UTC timestamp with millis)."""
        return {
            "schema_version": self.schema_version,
            "event_id": self.event_id,
            "seq": self.seq,
            "dataset_id": self.dataset_id,
            "event_time": format_iso_ms(self.event_time_ms),
            "user_id": self.user_id,
            "item_id": self.item_id,
            "region": self.region,
            "item_size_bytes": self.item_size_bytes,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_wire(), separators=(",", ":"), sort_keys=True)

    def payload_hash(self) -> str:
        """Hash of the canonical payload; used to tell exact from conflicting duplicates."""
        return canonical_payload_hash(
            self.seq,
            self.dataset_id,
            self.event_time_ms,
            self.user_id,
            self.item_id,
            self.region,
            self.item_size_bytes,
        )

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def canonical_payload_hash(
    seq: int,
    dataset_id: str,
    event_time_ms: int,
    user_id: str,
    item_id: str,
    region: str,
    item_size_bytes: int,
) -> str:
    # Spark computes the same value with sha2(concat_ws('|', ...), 256).
    joined = "|".join(
        str(v) for v in (seq, dataset_id, event_time_ms, user_id, item_id, region, item_size_bytes)
    )
    return hashlib.sha256(joined.encode()).hexdigest()


def format_iso_ms(epoch_ms: int) -> str:
    dt = datetime.fromtimestamp(epoch_ms / 1000, tz=UTC)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{epoch_ms % 1000:03d}Z"


_ISO_RE = re.compile(
    r"^(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?"
    r"(Z|[+-]\d{2}:\d{2})$"
)


def parse_event_time_ms(value: Any) -> int | None:
    """Parse an ISO-8601 timestamp *with* an explicit offset (or Z) into UTC epoch ms.

    Naive timestamps are rejected: guessing a timezone silently is exactly the
    kind of normalisation error that corrupts time-series analytics.
    Integers are interpreted as epoch milliseconds (the legacy CSV format).
    """
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if MIN_EVENT_TIME_MS <= value < MAX_EVENT_TIME_MS else None
    if not isinstance(value, str):
        return None
    m = _ISO_RE.match(value.strip())
    if not m:
        return None
    year, month, day, hh, mm, ss, frac, tz = m.groups()
    try:
        dt = datetime.fromisoformat(
            f"{year}-{month}-{day}T{hh}:{mm}:{ss}{'+00:00' if tz == 'Z' else tz}"
        )
    except ValueError:
        return None
    millis = int((frac or "0").ljust(3, "0")[:3])
    ms = int(dt.timestamp()) * 1000 + millis
    return ms if MIN_EVENT_TIME_MS <= ms < MAX_EVENT_TIME_MS else None


def json_text(value: Any) -> str | None:
    """Text form of a JSON scalar, matching how Spark reads any JSON token into a
    StringType column (numbers/booleans become their literal text)."""
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    return json.dumps(value, separators=(",", ":"))


def normalize_id(value: Any) -> str | None:
    text = json_text(value)
    if text is None:
        return None
    v = text.strip().lower()
    return v if ID_RE.match(v) else None


def normalize_region(value: Any) -> str | None:
    text = json_text(value)
    if text is None:
        return None
    v = text.strip().lower()
    v = REGION_ALIASES.get(v, v)
    return v if v in REGIONS else None


def normalize_category(value: Any) -> str:
    """Metadata category normalisation. Unknown values map to ``unknown`` (counted, not dropped)."""
    if not isinstance(value, str):
        return UNKNOWN_CATEGORY
    v = value.strip().lower()
    v = CATEGORY_ALIASES.get(v, v)
    return v if v in CATEGORIES else UNKNOWN_CATEGORY


_INT_TEXT_RE = re.compile(r"^-?\d{1,19}$")
_LONG_MIN, _LONG_MAX = -(2**63), 2**63 - 1


def parse_long_text(text: str | None) -> int | None:
    """Parse an integer the way Spark's ``try_cast(trim(x) AS BIGINT)`` does after
    our regex guard: digits only, must fit in a signed 64-bit long."""
    if text is None:
        return None
    t = text.strip()
    if not _INT_TEXT_RE.match(t):
        return None
    v = int(t)
    return v if _LONG_MIN <= v <= _LONG_MAX else None


@dataclass(frozen=True, slots=True)
class ValidationResult:
    event: CanonicalEvent | None
    reason: str | None

    @property
    def ok(self) -> bool:
        return self.event is not None


def validate_wire_record(record: Any) -> ValidationResult:
    """Validate one wire-format (Kafka/canonical JSON) record.

    Every field is first reduced to its JSON text (as Spark does with a
    StringType schema). Check order (first failure wins), mirrored in Spark:
    schema_version, required fields, event_id, dataset_id, seq, event_time,
    item_id, user_id, region, item_size_bytes.
    """
    if not isinstance(record, dict):
        return ValidationResult(None, Reason.MALFORMED_JSON)
    text = {k: json_text(record.get(k)) for k in (*REQUIRED_FIELDS, "schema_version")}
    if parse_long_text(text["schema_version"]) != SCHEMA_VERSION:
        return ValidationResult(None, Reason.UNSUPPORTED_SCHEMA)
    for name in REQUIRED_FIELDS:
        value = text[name]
        if value is None or value.strip() == "":
            return ValidationResult(None, f"{Reason.MISSING_FIELD}:{name}")
    event_id = text["event_id"].strip()  # type: ignore[union-attr]
    if not EVENT_ID_RE.match(event_id):
        return ValidationResult(None, Reason.INVALID_EVENT_ID)
    dataset_id = normalize_id(text["dataset_id"])
    if dataset_id is None:
        return ValidationResult(None, Reason.INVALID_DATASET_ID)
    seq = parse_long_text(text["seq"])
    if seq is None or seq < 0:
        return ValidationResult(None, Reason.INVALID_SEQ)
    ts = parse_event_time_ms(text["event_time"])
    if ts is None:
        return ValidationResult(None, Reason.INVALID_TIMESTAMP)
    item_id = normalize_id(text["item_id"])
    if item_id is None:
        return ValidationResult(None, Reason.INVALID_ITEM_ID)
    user_id = normalize_id(text["user_id"])
    if user_id is None:
        return ValidationResult(None, Reason.INVALID_USER_ID)
    region = normalize_region(text["region"])
    if region is None:
        return ValidationResult(None, Reason.INVALID_REGION)
    size_text = text["item_size_bytes"].strip()  # type: ignore[union-attr]
    if re.fullmatch(r"-\d+", size_text) or re.fullmatch(r"0+", size_text):
        return ValidationResult(None, Reason.NON_POSITIVE_SIZE)
    size = parse_long_text(size_text)
    if size is None or size > MAX_ITEM_SIZE_BYTES:
        return ValidationResult(None, Reason.INVALID_SIZE)
    return ValidationResult(
        CanonicalEvent(
            event_id=event_id,
            seq=seq,
            dataset_id=dataset_id,
            event_time_ms=ts,
            user_id=user_id,
            item_id=item_id,
            region=region,
            item_size_bytes=size,
        ),
        None,
    )


def validate_wire_json(line: str | bytes) -> ValidationResult:
    try:
        record = json.loads(line)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return ValidationResult(None, Reason.MALFORMED_JSON)
    return validate_wire_record(record)


# --- HBase / storage key helpers ---------------------------------------------

_KEY_COMPONENT_RE = re.compile(r"^[a-z0-9_.-]{1,64}$")


def key_component(value: str) -> str:
    """Validate a component used inside a composite row key.

    Components may not contain the ``#`` / ``:`` delimiters, which makes composite
    keys unambiguous (no two component tuples map to the same key).
    """
    if not isinstance(value, str) or not _KEY_COMPONENT_RE.match(value):
        raise ValueError(f"invalid row-key component: {value!r}")
    return value


def salt_prefix(identity: str, width: int = 4) -> str:
    """Fixed-width hash prefix that spreads otherwise-monotonic keys across regions."""
    return hashlib.sha1(identity.encode(), usedforsecurity=False).hexdigest()[:width]
