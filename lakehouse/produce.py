"""Stream synthetic e-commerce events into Kafka.

    python -m lakehouse.produce --rate 25

Events are keyed by ``user_id`` so all of a shopper's events land in the same
partition and keep their order.
"""

from __future__ import annotations

import argparse
import json
import logging
import signal
import time
from datetime import datetime, timezone

from . import settings
from .generator import generate_events

log = logging.getLogger("producer")


def main() -> None:
    p = argparse.ArgumentParser(description="Stream synthetic e-commerce events into Kafka.")
    p.add_argument("--rate", type=float, default=25.0, help="events per second")
    p.add_argument("--sessions", type=int, default=10_000_000, help="stop after this many sessions")
    p.add_argument("--seed", type=int, default=None, help="fix the random seed for a repeatable stream")
    args = p.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")

    from confluent_kafka import Producer

    producer = Producer({
        "bootstrap.servers": settings.KAFKA_BOOTSTRAP,
        "enable.idempotence": True,   # broker drops retried duplicates of the same send
        "acks": "all",
        "linger.ms": 50,
        "compression.type": "zstd",
    })

    stopping = False

    def stop(*_):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    seed = args.seed if args.seed is not None else int(time.time())
    events = generate_events(args.sessions, seed=seed, now_fn=lambda: datetime.now(timezone.utc))
    interval = 1.0 / args.rate if args.rate > 0 else 0
    sent = 0
    next_at = time.monotonic()
    for event in events:
        if stopping:
            break
        key = event.get("user_id") or "unknown"
        producer.produce(settings.RAW_TOPIC, key=key, value=json.dumps(event))
        producer.poll(0)
        sent += 1
        if sent % 500 == 0:
            log.info("sent %d events", sent)
        next_at += interval
        delay = next_at - time.monotonic()
        if delay > 0:
            time.sleep(delay)
    producer.flush(10)
    log.info("stopped after %d events", sent)


if __name__ == "__main__":
    main()
