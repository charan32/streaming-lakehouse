"""Kafka -> Iceberg bronze ingestor.

    python -m lakehouse.consume

Reads ``events.raw`` in micro-batches (up to ``--max-batch`` messages or
``--max-wait`` seconds), validates each event against its data contract,
appends valid events to ``bronze.events`` and rejected ones to
``bronze.events_rejected`` plus the ``events.dlq`` topic, then commits offsets.

Offsets are committed only after the Iceberg commit succeeds (at-least-once).
"""

from __future__ import annotations

import argparse
import json
import logging
import signal
import time

from . import settings
from .ingest import BronzeWriter, Message, process

log = logging.getLogger("ingestor")


def _connect_writer(retries: int = 30) -> BronzeWriter:
    # The catalog database and object store may still be starting up.
    for attempt in range(1, retries + 1):
        try:
            return BronzeWriter(settings.catalog())
        except Exception as exc:  # noqa: BLE001 - log and retry any startup failure
            log.warning("catalog not ready (%s), retry %d/%d", exc, attempt, retries)
            time.sleep(2)
    raise SystemExit("could not reach the Iceberg catalog")


def main() -> None:
    p = argparse.ArgumentParser(description="Kafka to Iceberg bronze ingestor.")
    p.add_argument("--max-batch", type=int, default=2000)
    p.add_argument("--max-wait", type=float, default=10.0, help="seconds to wait for a batch to fill")
    args = p.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")

    from confluent_kafka import Consumer, Producer

    writer = _connect_writer()
    consumer = Consumer({
        "bootstrap.servers": settings.KAFKA_BOOTSTRAP,
        "group.id": settings.CONSUMER_GROUP,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    })
    dlq = Producer({"bootstrap.servers": settings.KAFKA_BOOTSTRAP, "acks": "all"})
    consumer.subscribe([settings.RAW_TOPIC])

    stopping = False

    def stop(*_):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    log.info("consuming %s from %s", settings.RAW_TOPIC, settings.KAFKA_BOOTSTRAP)
    try:
        run_loop(consumer, dlq, writer, args.max_batch, args.max_wait, lambda: stopping)
    finally:
        consumer.close()


def run_loop(consumer, dlq, writer: BronzeWriter, max_batch: int, max_wait: float, should_stop) -> int:
    """Consume, validate, write, then commit. Returns the number of batches committed."""
    batches = 0
    while not should_stop():
        msgs = consumer.consume(num_messages=max_batch, timeout=max_wait)
        batch = []
        for m in msgs:
            if m.error():
                log.warning("kafka error: %s", m.error())
                continue
            batch.append(Message(m.value(), m.partition(), m.offset()))
        if not batch:
            continue

        started = time.monotonic()
        result = process(batch, writer)          # 1. Iceberg commit
        for row in result.rejected_rows:         # 2. dead-letter the rejects
            dlq.produce(settings.DLQ_TOPIC, value=json.dumps({
                "raw": row["raw"],
                "errors": row["errors"].split("\n"),
                "source_partition": row["source_partition"],
                "source_offset": row["source_offset"],
            }))
        dlq.flush(10)
        consumer.commit(asynchronous=False)      # 3. only now move the offsets
        batches += 1
        log.info("batch committed: %d valid, %d rejected in %.2fs",
                 len(result.valid_rows), len(result.rejected_rows), time.monotonic() - started)
    return batches


if __name__ == "__main__":
    main()
