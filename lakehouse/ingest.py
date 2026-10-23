"""Bronze ingestion: validate a micro-batch and append it to Iceberg.

This module knows nothing about Kafka. It takes a list of ``Message`` objects
(raw bytes plus where they came from) so the same code is used by the Kafka
consumer, the local demo, and the tests.

Delivery guarantee: at-least-once. The Kafka consumer commits offsets only
after the Iceberg commit succeeds, so a crash in between replays the batch.
Replays (and producer retries) land in bronze as duplicates and are removed in
the dbt staging layer by ``event_id``.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone

import pyarrow as pa
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import DayTransform
from pyiceberg.types import (
    IntegerType,
    LongType,
    NestedField,
    StringType,
    TimestamptzType,
)

from .contracts import validate

NAMESPACE = "bronze"
EVENTS_TABLE = f"{NAMESPACE}.events"
REJECTED_TABLE = f"{NAMESPACE}.events_rejected"

EVENTS_SCHEMA = Schema(
    NestedField(1, "event_id", StringType(), required=True),
    NestedField(2, "event_type", StringType(), required=True),
    NestedField(3, "event_ts", TimestamptzType(), required=True, doc="When the event happened (event time)"),
    NestedField(4, "user_id", StringType(), required=True),
    NestedField(5, "session_id", StringType(), required=True),
    NestedField(6, "schema_version", IntegerType(), required=True),
    NestedField(7, "payload", StringType(), required=True, doc="Event-specific fields as JSON"),
    NestedField(8, "ingested_at", TimestamptzType(), required=True, doc="When the lakehouse received it"),
    NestedField(9, "source_partition", IntegerType(), required=False),
    NestedField(10, "source_offset", LongType(), required=False),
)
EVENTS_SPEC = PartitionSpec(
    PartitionField(source_id=3, field_id=1000, transform=DayTransform(), name="event_day")
)

REJECTED_SCHEMA = Schema(
    NestedField(1, "raw", StringType(), required=True, doc="The message exactly as received"),
    NestedField(2, "errors", StringType(), required=True, doc="Contract violations, one per line"),
    NestedField(3, "event_type", StringType(), required=False),
    NestedField(4, "ingested_at", TimestamptzType(), required=True),
    NestedField(5, "source_partition", IntegerType(), required=False),
    NestedField(6, "source_offset", LongType(), required=False),
)
REJECTED_SPEC = PartitionSpec(
    PartitionField(source_id=4, field_id=1000, transform=DayTransform(), name="ingested_day")
)


@dataclass
class Message:
    value: bytes | str
    partition: int | None = None
    offset: int | None = None
    received_at: datetime | None = None  # defaults to the batch's ingestion time


@dataclass
class BatchResult:
    valid_rows: list[dict] = field(default_factory=list)
    rejected_rows: list[dict] = field(default_factory=list)

    @property
    def counts(self) -> dict[str, int]:
        return {"valid": len(self.valid_rows), "rejected": len(self.rejected_rows)}


def ensure_tables(catalog) -> None:
    try:
        catalog.create_namespace(NAMESPACE)
    except NamespaceAlreadyExistsError:
        pass
    for name, schema, spec in (
        (EVENTS_TABLE, EVENTS_SCHEMA, EVENTS_SPEC),
        (REJECTED_TABLE, REJECTED_SCHEMA, REJECTED_SPEC),
    ):
        try:
            catalog.load_table(name)
        except NoSuchTableError:
            catalog.create_table(name, schema=schema, partition_spec=spec)


def split_batch(messages: list[Message], now: datetime | None = None) -> BatchResult:
    """Validate each message against its contract and sort it into valid or rejected."""
    now = now or datetime.now(timezone.utc)
    result = BatchResult()
    for m in messages:
        ingested_at = m.received_at or now
        raw = m.value.decode("utf-8", errors="replace") if isinstance(m.value, bytes) else m.value
        try:
            event = json.loads(raw)
        except json.JSONDecodeError as exc:
            errors, event = [f"invalid JSON: {exc.msg}"], None
        else:
            errors = validate(event)

        if errors:
            result.rejected_rows.append({
                "raw": raw,
                "errors": "\n".join(errors),
                "event_type": event.get("event_type") if isinstance(event, dict) else None,
                "ingested_at": ingested_at,
                "source_partition": m.partition,
                "source_offset": m.offset,
            })
            continue

        result.valid_rows.append({
            "event_id": event["event_id"],
            "event_type": event["event_type"],
            "event_ts": datetime.fromisoformat(event["event_ts"].replace("Z", "+00:00")),
            "user_id": event["user_id"],
            "session_id": event["session_id"],
            "schema_version": event["schema_version"],
            "payload": json.dumps(event["payload"], separators=(",", ":"), sort_keys=True),
            "ingested_at": ingested_at,
            "source_partition": m.partition,
            "source_offset": m.offset,
        })
    return result


class BronzeWriter:
    """Appends validated batches to the bronze Iceberg tables."""

    def __init__(self, catalog):
        ensure_tables(catalog)
        self.events = catalog.load_table(EVENTS_TABLE)
        self.rejected = catalog.load_table(REJECTED_TABLE)
        self._events_arrow = schema_to_pyarrow(EVENTS_SCHEMA)
        self._rejected_arrow = schema_to_pyarrow(REJECTED_SCHEMA)

    def write(self, result: BatchResult) -> None:
        # One Iceberg commit per table per batch: readers see the whole batch or none of it.
        if result.valid_rows:
            self.events.append(pa.Table.from_pylist(result.valid_rows, schema=self._events_arrow))
        if result.rejected_rows:
            self.rejected.append(pa.Table.from_pylist(result.rejected_rows, schema=self._rejected_arrow))


def process(messages: list[Message], writer: BronzeWriter, now: datetime | None = None) -> BatchResult:
    result = split_batch(messages, now)
    writer.write(result)
    return result
