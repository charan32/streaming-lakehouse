"""Run the ingestion path without Docker or Kafka.

Generates a deterministic batch of events, feeds them through the same
validation and Iceberg writer the Kafka consumer uses, and prints a summary.
Used by ``make local``, the integration test, and CI.

    python -m lakehouse.local_run --sessions 3000
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta

from . import settings
from .generator import generate_events
from .ingest import BronzeWriter, Message, process


def run(sessions: int, seed: int, batch_size: int) -> dict:
    writer = BronzeWriter(settings.catalog())
    totals = {"valid": 0, "rejected": 0, "batches": 0}
    batch: list[Message] = []
    clock: datetime | None = None
    for offset, event in enumerate(generate_events(sessions, seed=seed)):
        # Simulated arrival time: the stream's clock is the newest event time seen so
        # far, plus a couple of seconds of transit. Late events then show their delay.
        ts = _event_time(event)
        if ts and (clock is None or ts > clock):
            clock = ts
        received = clock + timedelta(seconds=2) if clock else None
        batch.append(Message(json.dumps(event), partition=0, offset=offset, received_at=received))
        if len(batch) == batch_size:
            _flush(batch, writer, totals)
            batch = []
    if batch:
        _flush(batch, writer, totals)
    return totals


def _event_time(event: dict) -> datetime | None:
    try:
        return datetime.fromisoformat(str(event.get("event_ts")).replace("Z", "+00:00"))
    except ValueError:
        return None


def _flush(batch: list[Message], writer: BronzeWriter, totals: dict) -> None:
    counts = process(batch, writer).counts
    totals["valid"] += counts["valid"]
    totals["rejected"] += counts["rejected"]
    totals["batches"] += 1


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--sessions", type=int, default=3000)
    p.add_argument("--seed", type=int, default=7)
    p.add_argument("--batch-size", type=int, default=1000)
    args = p.parse_args()
    print(json.dumps(run(args.sessions, args.seed, args.batch_size)))


if __name__ == "__main__":
    main()
