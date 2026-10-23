import json
from datetime import datetime, timezone

from lakehouse.consume import run_loop
from lakehouse.generator import generate_events
from lakehouse.ingest import EVENTS_TABLE, REJECTED_TABLE, BronzeWriter, Message, process, split_batch

NOW = datetime(2026, 3, 1, 12, 0, tzinfo=timezone.utc)


def messages(n_sessions=40, **kw):
    return [Message(json.dumps(e), 0, i) for i, e in enumerate(generate_events(n_sessions, seed=2, **kw))]


def test_split_routes_bad_json_and_contract_breaks_to_rejected():
    batch = messages(bad_rate=0, dup_rate=0) + [Message(b"{not json", 0, 999)]
    good = json.loads(batch[0].value)
    good.pop("user_id")
    batch.append(Message(json.dumps(good), 1, 5))

    result = split_batch(batch, now=NOW)

    assert len(result.valid_rows) == len(batch) - 2
    reasons = [r["errors"] for r in result.rejected_rows]
    assert reasons[0].startswith("invalid JSON")
    assert reasons[1] == "user_id: is required"
    assert result.rejected_rows[1]["source_partition"] == 1
    assert result.rejected_rows[1]["source_offset"] == 5


def test_valid_rows_are_typed_for_iceberg():
    row = split_batch(messages(1, bad_rate=0), now=NOW).valid_rows[0]
    assert isinstance(row["event_ts"], datetime) and row["event_ts"].tzinfo is not None
    assert row["ingested_at"] == NOW
    assert json.loads(row["payload"])  # payload stored as compact JSON text


def test_process_appends_one_snapshot_per_table(local_catalog):
    writer = BronzeWriter(local_catalog)
    batch = messages(bad_rate=0.2)
    result = process(batch, writer, now=NOW)

    events = local_catalog.load_table(EVENTS_TABLE)
    rejected = local_catalog.load_table(REJECTED_TABLE)
    assert events.scan().to_arrow().num_rows == len(result.valid_rows)
    assert rejected.scan().to_arrow().num_rows == len(result.rejected_rows) > 0
    assert len(events.metadata.snapshots) == 1


def test_replayed_batch_is_kept_in_bronze_for_dbt_to_deduplicate(local_catalog):
    writer = BronzeWriter(local_catalog)
    batch = messages(bad_rate=0, dup_rate=0)
    process(batch, writer, now=NOW)
    process(batch, writer, now=NOW)  # consumer crashed before committing offsets

    table = local_catalog.load_table(EVENTS_TABLE).scan().to_arrow()
    ids = table.column("event_id").to_pylist()
    assert len(ids) == 2 * len(set(ids))


class FakeKafkaMessage:
    def __init__(self, value, partition, offset, error=None):
        self._v, self._p, self._o, self._e = value, partition, offset, error

    def value(self): return self._v
    def partition(self): return self._p
    def offset(self): return self._o
    def error(self): return self._e


class FakeConsumer:
    def __init__(self, batches, fail_on_write=False):
        self.batches = list(batches)
        self.commits = 0

    def consume(self, num_messages, timeout):
        return self.batches.pop(0) if self.batches else []

    def commit(self, asynchronous):
        self.commits += 1


class FakeProducer:
    def __init__(self):
        self.sent = []

    def produce(self, topic, value):
        self.sent.append((topic, json.loads(value)))

    def flush(self, timeout):
        return 0


def kafka_batch(msgs):
    return [FakeKafkaMessage(m.value.encode(), m.partition, m.offset) for m in msgs]


def test_consumer_loop_writes_dead_letters_then_commits(local_catalog):
    msgs = messages(bad_rate=0.3)
    consumer = FakeConsumer([kafka_batch(msgs[:50]), kafka_batch(msgs[50:])])
    dlq = FakeProducer()
    rounds = iter(range(3))

    committed = run_loop(consumer, dlq, BronzeWriter(local_catalog), 1000, 0.1,
                         should_stop=lambda: next(rounds, 99) >= 2)

    assert committed == consumer.commits == 2
    assert dlq.sent and all(topic == "events.dlq" for topic, _ in dlq.sent)
    total = local_catalog.load_table(EVENTS_TABLE).scan().to_arrow().num_rows + len(dlq.sent)
    assert total == len(msgs)


def test_consumer_does_not_commit_when_iceberg_write_fails(local_catalog):
    class BrokenWriter:
        def write(self, result):
            raise IOError("object store unavailable")

    consumer = FakeConsumer([kafka_batch(messages(5))])
    try:
        run_loop(consumer, FakeProducer(), BrokenWriter(), 1000, 0.1, should_stop=lambda: False)
    except IOError:
        pass
    assert consumer.commits == 0  # offsets stay put, so the batch is re-read after restart
