"""DLQProducer tests. No real Kafka: confluent_kafka.Producer is
monkeypatched, same pattern as tests/ingestion/test_producer.py.
"""

from __future__ import annotations

import base64
import json

import pytest

from streaming import dlq as dlq_module
from streaming.dlq import DLQProducer


class _FakeKafkaProducer:
    def __init__(self, config):
        self.config = config
        self.produced = []
        self.flush_remaining = 0
        self.flush_calls = []

    def produce(self, topic, key=None, value=None, callback=None):
        self.produced.append({"topic": topic, "key": key, "value": value})

    def flush(self, timeout):
        self.flush_calls.append(timeout)
        return self.flush_remaining


class _FakeMessage:
    def __init__(self, topic, partition, offset, key, value):
        self._topic = topic
        self._partition = partition
        self._offset = offset
        self._key = key
        self._value = value

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


def _make_dlq(monkeypatch) -> DLQProducer:
    monkeypatch.setattr(dlq_module, "Producer", lambda config: _FakeKafkaProducer(config))
    return DLQProducer(bootstrap_servers="localhost:9092")


def test_configures_idempotence_and_acks(monkeypatch):
    d = _make_dlq(monkeypatch)
    assert d._producer.config["enable.idempotence"] is True
    assert d._producer.config["acks"] == "all"


def test_send_publishes_envelope_with_original_metadata(monkeypatch):
    d = _make_dlq(monkeypatch)
    original = _FakeMessage("market-ticks", 3, 42, b"AAPL", b'{"bad": "json"')

    d.send(original, ValueError("not valid json"))

    [sent] = d._producer.produced
    assert sent["topic"] == dlq_module.DLQ_TOPIC == "market-ticks-dlq"
    assert sent["key"] == b"AAPL"

    envelope = json.loads(sent["value"])
    assert envelope["original_topic"] == "market-ticks"
    assert envelope["original_partition"] == 3
    assert envelope["original_offset"] == 42
    assert envelope["original_key"] == "AAPL"
    assert envelope["error"] == "ValueError: not valid json"
    assert base64.b64decode(envelope["raw_value_b64"]) == b'{"bad": "json"'
    assert "failed_at" in envelope


def test_send_with_no_key(monkeypatch):
    d = _make_dlq(monkeypatch)
    original = _FakeMessage("market-ticks", 0, 1, None, b"garbage")

    d.send(original, ValueError("boom"))

    [sent] = d._producer.produced
    assert sent["key"] is None
    envelope = json.loads(sent["value"])
    assert envelope["original_key"] is None


def test_send_raises_when_flush_incomplete(monkeypatch):
    d = _make_dlq(monkeypatch)
    d._producer.flush_remaining = 1
    original = _FakeMessage("market-ticks", 0, 1, b"AAPL", b"x")

    with pytest.raises(RuntimeError, match="did not complete"):
        d.send(original, ValueError("boom"))


def test_close_flushes(monkeypatch):
    d = _make_dlq(monkeypatch)
    d.close()
    assert d._producer.flush_calls == [10.0]
