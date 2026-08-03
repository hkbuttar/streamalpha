"""AnomalyProducer tests. No real Kafka: confluent_kafka.Producer is
monkeypatched, same pattern as test_producer.py and test_dlq.py.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from streaming import anomaly_producer as anomaly_producer_module
from streaming.anomaly_producer import AnomalyProducer
from streaming.models import RegimeChange, VolumeAnomaly

WINDOW_START = datetime(2024, 1, 2, 15, 30, tzinfo=UTC)
WINDOW_END = datetime(2024, 1, 2, 15, 30, 10, tzinfo=UTC)


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


def _make_producer(monkeypatch) -> AnomalyProducer:
    monkeypatch.setattr(
        anomaly_producer_module, "Producer", lambda config: _FakeKafkaProducer(config)
    )
    return AnomalyProducer(bootstrap_servers="localhost:9092")


def _volume_event(**overrides):
    defaults = dict(
        symbol="AAPL",
        window_start=WINDOW_START,
        window_end=WINDOW_END,
        volume=50000.0,
        trade_count=42,
        anomaly_score=0.99,
        score_mean=0.5,
        score_std=0.1,
    )
    defaults.update(overrides)
    return VolumeAnomaly(**defaults)


def _regime_event(**overrides):
    defaults = dict(
        symbol="AAPL",
        window_start=WINDOW_START,
        window_end=WINDOW_END,
        realized_volatility=0.3,
        changepoint_probability=0.87,
    )
    defaults.update(overrides)
    return RegimeChange(**defaults)


def test_configures_idempotence_and_acks(monkeypatch):
    p = _make_producer(monkeypatch)
    assert p._producer.config["enable.idempotence"] is True
    assert p._producer.config["acks"] == "all"


def test_volume_anomaly_publishes_to_volume_anomalies_topic(monkeypatch):
    p = _make_producer(monkeypatch)
    p.publish(_volume_event())

    [sent] = p._producer.produced
    assert sent["topic"] == "volume-anomalies"
    assert sent["key"] == b"AAPL"
    payload = json.loads(sent["value"])
    assert payload["symbol"] == "AAPL"
    assert payload["volume"] == 50000.0
    assert payload["trade_count"] == 42
    assert payload["anomaly_score"] == 0.99
    assert payload["window_start"] == WINDOW_START.isoformat()


def test_regime_change_publishes_to_regime_changes_topic(monkeypatch):
    p = _make_producer(monkeypatch)
    p.publish(_regime_event())

    [sent] = p._producer.produced
    assert sent["topic"] == "regime-changes"
    assert sent["key"] == b"AAPL"
    payload = json.loads(sent["value"])
    assert payload["realized_volatility"] == 0.3
    assert payload["changepoint_probability"] == 0.87


def test_publish_flushes_synchronously(monkeypatch):
    p = _make_producer(monkeypatch)
    p.publish(_volume_event())
    assert p._producer.flush_calls == [10.0]


def test_publish_raises_when_flush_incomplete(monkeypatch):
    p = _make_producer(monkeypatch)
    p._producer.flush_remaining = 1
    with pytest.raises(RuntimeError, match="did not complete"):
        p.publish(_volume_event())


def test_publish_rejects_unknown_event_type(monkeypatch):
    p = _make_producer(monkeypatch)
    with pytest.raises(TypeError, match="unknown anomaly event type"):
        p.publish(object())


def test_close_flushes(monkeypatch):
    p = _make_producer(monkeypatch)
    p.close()
    assert p._producer.flush_calls == [10.0]
