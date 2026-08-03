"""run_consumer tests. No real Kafka: confluent_kafka.Consumer and
DLQProducer are both faked. These tests exist to pin down the correctness
guarantees consumer.py is built around, not just the happy path:

- a valid message is processed then committed
- a malformed message is DLQ'd then committed (never reaches process_tick)
- a processing exception is NOT swallowed, and leaves the offset uncommitted
- Kafka-level errors (including partition EOF) don't call process_tick or
  commit anything

Faked Consumer.poll() raises _QueueExhausted once its queue is empty
instead of blocking/returning None forever, which is what a real consumer
does -- that's deliberate: it gives each test a deterministic point to stop
at instead of needing a timeout-based loop.
"""

from __future__ import annotations

import json

import pytest

from streaming import consumer as consumer_module
from streaming.consumer import run_consumer


class _QueueExhausted(Exception):
    """Raised by the fake consumer once there's nothing left to poll."""


class _FakeMessage:
    def __init__(
        self, topic="market-ticks", partition=0, offset=0, key=b"AAPL", value=b"", error=None
    ):
        self._topic = topic
        self._partition = partition
        self._offset = offset
        self._key = key
        self._value = value
        self._error = error

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def key(self):
        return self._key

    def value(self):
        return self._value

    def error(self):
        return self._error


class _FakeKafkaError:
    def __init__(self, code):
        self._code = code

    def code(self):
        return self._code

    def __str__(self):
        return f"fake kafka error {self._code}"


class _FakeConsumer:
    def __init__(self, config, queue):
        self.config = config
        self.subscribed = None
        self.committed = []
        self.closed = False
        self._queue = list(queue)

    def subscribe(self, topics):
        self.subscribed = topics

    def poll(self, timeout):
        if not self._queue:
            raise _QueueExhausted
        return self._queue.pop(0)

    def commit(self, message=None, asynchronous=True):
        self.committed.append(message)

    def close(self):
        self.closed = True


class _FakeDLQ:
    def __init__(self, bootstrap_servers=None):
        self.sent = []
        self.closed = False

    def send(self, original, error):
        self.sent.append((original, error))

    def close(self):
        self.closed = True


def _valid_trade_bytes(symbol="AAPL"):
    payload = {
        "type": "trade",
        "symbol": symbol,
        "timestamp": "2024-01-02T15:30:00+00:00",
        "price": 100.0,
        "size": 1,
        "exchange": "V",
        "trade_id": 1,
        "conditions": [],
        "tape": "C",
    }
    return json.dumps(payload).encode("utf-8")


def _wire(monkeypatch, queue):
    fake_consumer = _FakeConsumer(config=None, queue=queue)
    monkeypatch.setattr(
        consumer_module, "Consumer", lambda config: _set_config(fake_consumer, config)
    )

    dlq_holder = []

    def _make_dlq(bootstrap_servers=None):
        d = _FakeDLQ(bootstrap_servers)
        dlq_holder.append(d)
        return d

    monkeypatch.setattr(consumer_module, "DLQProducer", _make_dlq)
    return fake_consumer, dlq_holder


def _set_config(fake_consumer, config):
    fake_consumer.config = config
    return fake_consumer


def test_valid_message_processed_then_committed(monkeypatch):
    msg = _FakeMessage(offset=5, value=_valid_trade_bytes())
    fake_consumer, dlq_holder = _wire(monkeypatch, [msg])

    processed = []
    with pytest.raises(_QueueExhausted):
        run_consumer(processed.append, bootstrap_servers="localhost:9092")

    assert [t["symbol"] for t in processed] == ["AAPL"]
    assert fake_consumer.committed == [msg]
    assert dlq_holder[0].sent == []


def test_malformed_message_dlqd_then_committed_without_processing(monkeypatch):
    msg = _FakeMessage(offset=7, value=b"{not json")
    fake_consumer, dlq_holder = _wire(monkeypatch, [msg])

    processed = []
    with pytest.raises(_QueueExhausted):
        run_consumer(processed.append, bootstrap_servers="localhost:9092")

    assert processed == []
    assert fake_consumer.committed == [msg]
    [(sent_msg, error)] = dlq_holder[0].sent
    assert sent_msg is msg
    assert "not valid json" in str(error)


def test_processing_exception_propagates_and_offset_stays_uncommitted(monkeypatch):
    msg = _FakeMessage(offset=9, value=_valid_trade_bytes())
    fake_consumer, dlq_holder = _wire(monkeypatch, [msg])

    def _boom(tick):
        raise RuntimeError("processing bug")

    with pytest.raises(RuntimeError, match="processing bug"):
        run_consumer(_boom, bootstrap_servers="localhost:9092")

    assert fake_consumer.committed == []
    assert dlq_holder[0].sent == []
    # cleanup still ran despite the crash
    assert fake_consumer.closed is True
    assert dlq_holder[0].closed is True


def test_partition_eof_is_skipped_without_side_effects(monkeypatch):
    eof_msg = _FakeMessage(error=_FakeKafkaError(consumer_module.KafkaError._PARTITION_EOF))
    fake_consumer, dlq_holder = _wire(monkeypatch, [eof_msg])

    processed = []
    with pytest.raises(_QueueExhausted):
        run_consumer(processed.append, bootstrap_servers="localhost:9092")

    assert processed == []
    assert fake_consumer.committed == []
    assert dlq_holder[0].sent == []


def test_kafka_error_is_logged_and_skipped(monkeypatch, caplog):
    error_msg = _FakeMessage(error=_FakeKafkaError(-1))
    fake_consumer, dlq_holder = _wire(monkeypatch, [error_msg])

    processed = []
    with caplog.at_level("ERROR"), pytest.raises(_QueueExhausted):
        run_consumer(processed.append, bootstrap_servers="localhost:9092")

    assert processed == []
    assert fake_consumer.committed == []
    assert dlq_holder[0].sent == []
    assert "consumer error" in caplog.text


def test_none_message_is_skipped(monkeypatch):
    valid = _FakeMessage(offset=1, value=_valid_trade_bytes())
    _wire(monkeypatch, [None, valid])

    processed = []
    with pytest.raises(_QueueExhausted):
        run_consumer(processed.append, bootstrap_servers="localhost:9092")

    assert [t["symbol"] for t in processed] == ["AAPL"]


def test_disables_auto_commit_and_reads_from_earliest(monkeypatch):
    fake_consumer, _ = _wire(monkeypatch, [])
    with pytest.raises(_QueueExhausted):
        run_consumer(lambda tick: None, bootstrap_servers="localhost:9092")

    assert fake_consumer.config["enable.auto.commit"] is False
    assert fake_consumer.config["auto.offset.reset"] == "earliest"


def test_default_topic_and_group_id(monkeypatch):
    fake_consumer, _ = _wire(monkeypatch, [])
    monkeypatch.delenv("KAFKA_CONSUMER_GROUP", raising=False)
    with pytest.raises(_QueueExhausted):
        run_consumer(lambda tick: None, bootstrap_servers="localhost:9092")

    assert fake_consumer.subscribed == [consumer_module.MARKET_TICKS_TOPIC]
    assert fake_consumer.config["group.id"] == consumer_module.DEFAULT_GROUP_ID


def test_group_id_env_override(monkeypatch):
    fake_consumer, _ = _wire(monkeypatch, [])
    monkeypatch.setenv("KAFKA_CONSUMER_GROUP", "custom-group")
    with pytest.raises(_QueueExhausted):
        run_consumer(lambda tick: None, bootstrap_servers="localhost:9092")

    assert fake_consumer.config["group.id"] == "custom-group"
