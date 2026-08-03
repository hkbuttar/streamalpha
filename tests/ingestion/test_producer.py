"""TickProducer tests. No real Kafka: confluent_kafka.Producer is
monkeypatched with a fake that records what it was called with.
"""

from __future__ import annotations

import json

from ingestion import producer as producer_module
from ingestion.producer import TickProducer


class _FakeKafkaProducer:
    def __init__(self, config):
        self.config = config
        self.produced = []
        self.poll_calls = 0
        self.flush_calls = []
        self.flush_remaining = 0

    def produce(self, topic, key=None, value=None, callback=None):
        self.produced.append({"topic": topic, "key": key, "value": value, "callback": callback})

    def poll(self, timeout):
        self.poll_calls += 1

    def flush(self, timeout):
        self.flush_calls.append(timeout)
        return self.flush_remaining


def _make_producer(monkeypatch, **kwargs) -> TickProducer:
    monkeypatch.setattr(producer_module, "Producer", lambda config: _FakeKafkaProducer(config))
    return TickProducer(bootstrap_servers="localhost:9092", **kwargs)


def test_configures_idempotence_and_acks(monkeypatch):
    monkeypatch.setattr(producer_module, "Producer", lambda config: _FakeKafkaProducer(config))
    tp = TickProducer(bootstrap_servers="localhost:9092")
    assert tp._producer.config["enable.idempotence"] is True
    assert tp._producer.config["acks"] == "all"
    assert tp._producer.config["bootstrap.servers"] == "localhost:9092"


def test_reads_bootstrap_servers_from_env(monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "broker:9092")
    monkeypatch.setattr(producer_module, "Producer", lambda config: _FakeKafkaProducer(config))
    tp = TickProducer()
    assert tp._producer.config["bootstrap.servers"] == "broker:9092"


def test_publish_keys_by_symbol_and_serializes_payload(monkeypatch):
    tp = _make_producer(monkeypatch)
    tp.publish("AAPL", {"type": "trade", "price": 231.5})

    [sent] = tp._producer.produced
    assert sent["topic"] == producer_module.MARKET_TICKS_TOPIC == "market-ticks"
    assert sent["key"] == b"AAPL"
    assert json.loads(sent["value"]) == {"type": "trade", "price": 231.5}


def test_publish_polls_to_service_delivery_callbacks(monkeypatch):
    tp = _make_producer(monkeypatch)
    tp.publish("AAPL", {"type": "trade"})
    tp.publish("MSFT", {"type": "trade"})
    assert tp._producer.poll_calls == 2


def test_delivery_callback_logs_error(caplog):
    class _Msg:
        def key(self):
            return b"AAPL"

    with caplog.at_level("ERROR"):
        TickProducer._delivery_callback(Exception("boom"), _Msg())
    assert "delivery failed" in caplog.text


def test_delivery_callback_silent_on_success(caplog):
    class _Msg:
        def key(self):
            return b"AAPL"

    with caplog.at_level("ERROR"):
        TickProducer._delivery_callback(None, _Msg())
    assert caplog.text == ""


def test_flush_warns_when_messages_remain_undelivered(monkeypatch, caplog):
    tp = _make_producer(monkeypatch)
    tp._producer.flush_remaining = 3

    with caplog.at_level("WARNING"):
        tp.flush(timeout=1.0)

    assert tp._producer.flush_calls == [1.0]
    assert "3 messages still undelivered" in caplog.text


def test_flush_silent_when_fully_delivered(monkeypatch, caplog):
    tp = _make_producer(monkeypatch)

    with caplog.at_level("WARNING"):
        tp.flush()

    assert caplog.text == ""
