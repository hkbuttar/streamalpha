"""run_sink tests. No real Kafka/Postgres: Consumer and the db module
functions are faked, same pattern as tests/streaming/test_consumer.py.
"""

from __future__ import annotations

import json

import pytest

from storage import sink as sink_module
from storage.sink import run_sink


class _QueueExhausted(Exception):
    """Raised by the fake consumer once there's nothing left to poll."""


class _FakeMessage:
    def __init__(self, topic, partition=0, offset=0, key=b"AAPL", value=b"", error=None):
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


class _FakeConnection:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


def _volume_anomaly_bytes(symbol="AAPL"):
    payload = {
        "symbol": symbol,
        "window_start": "2024-01-02T15:30:00+00:00",
        "window_end": "2024-01-02T15:30:10+00:00",
        "volume": 50000.0,
        "trade_count": 10,
        "anomaly_score": 0.99,
        "score_mean": 0.5,
        "score_std": 0.1,
    }
    return json.dumps(payload).encode("utf-8")


def _regime_change_bytes(symbol="AAPL"):
    payload = {
        "symbol": symbol,
        "window_start": "2024-01-02T15:30:00+00:00",
        "window_end": "2024-01-02T15:30:10+00:00",
        "realized_volatility": 0.3,
        "changepoint_probability": 0.87,
    }
    return json.dumps(payload).encode("utf-8")


def _wire(monkeypatch, queue):
    fake_consumer = _FakeConsumer(config=None, queue=queue)
    monkeypatch.setattr(
        sink_module, "Consumer", lambda config: _set_config(fake_consumer, config)
    )
    fake_conn = _FakeConnection()
    monkeypatch.setattr(sink_module, "get_connection", lambda database_url=None: fake_conn)

    upserts = []
    monkeypatch.setattr(
        sink_module,
        "upsert_anomaly",
        lambda conn, ticker, window_start, anomaly_type, details: upserts.append(
            (ticker, window_start, anomaly_type, details)
        ),
    )
    return fake_consumer, fake_conn, upserts


def _set_config(fake_consumer, config):
    fake_consumer.config = config
    return fake_consumer


def test_volume_anomaly_upserted_then_committed(monkeypatch):
    msg = _FakeMessage(
        topic=sink_module.VOLUME_ANOMALIES_TOPIC, offset=1, value=_volume_anomaly_bytes()
    )
    fake_consumer, fake_conn, upserts = _wire(monkeypatch, [msg])

    with pytest.raises(_QueueExhausted):
        run_sink(bootstrap_servers="localhost:9092")

    [(ticker, window_start, anomaly_type, details)] = upserts
    assert ticker == "AAPL"
    assert window_start == "2024-01-02T15:30:00+00:00"
    assert anomaly_type == "volume_anomaly"
    assert details["anomaly_score"] == 0.99
    assert fake_consumer.committed == [msg]
    assert fake_conn.closed is True
    assert fake_consumer.closed is True


def test_regime_change_upserted_then_committed(monkeypatch):
    msg = _FakeMessage(
        topic=sink_module.REGIME_CHANGES_TOPIC, offset=1, value=_regime_change_bytes()
    )
    fake_consumer, _, upserts = _wire(monkeypatch, [msg])

    with pytest.raises(_QueueExhausted):
        run_sink(bootstrap_servers="localhost:9092")

    [(ticker, window_start, anomaly_type, details)] = upserts
    assert anomaly_type == "regime_change"
    assert details["changepoint_probability"] == 0.87
    assert fake_consumer.committed == [msg]


def test_malformed_payload_crashes_and_leaves_offset_uncommitted(monkeypatch):
    msg = _FakeMessage(topic=sink_module.VOLUME_ANOMALIES_TOPIC, offset=1, value=b"{not json")
    fake_consumer, _, upserts = _wire(monkeypatch, [msg])

    with pytest.raises(sink_module.AnomalyValidationError):
        run_sink(bootstrap_servers="localhost:9092")

    assert upserts == []
    assert fake_consumer.committed == []


def test_partition_eof_is_skipped_without_side_effects(monkeypatch):
    eof_msg = _FakeMessage(
        topic=sink_module.VOLUME_ANOMALIES_TOPIC,
        error=_FakeKafkaError(sink_module.KafkaError._PARTITION_EOF),
    )
    fake_consumer, _, upserts = _wire(monkeypatch, [eof_msg])

    with pytest.raises(_QueueExhausted):
        run_sink(bootstrap_servers="localhost:9092")

    assert upserts == []
    assert fake_consumer.committed == []


def test_kafka_error_is_logged_and_skipped(monkeypatch, caplog):
    error_msg = _FakeMessage(topic=sink_module.VOLUME_ANOMALIES_TOPIC, error=_FakeKafkaError(-1))
    fake_consumer, _, upserts = _wire(monkeypatch, [error_msg])

    with caplog.at_level("ERROR"), pytest.raises(_QueueExhausted):
        run_sink(bootstrap_servers="localhost:9092")

    assert upserts == []
    assert fake_consumer.committed == []
    assert "consumer error" in caplog.text


def test_none_message_is_skipped(monkeypatch):
    valid = _FakeMessage(
        topic=sink_module.VOLUME_ANOMALIES_TOPIC, offset=1, value=_volume_anomaly_bytes()
    )
    _, _, upserts = _wire(monkeypatch, [None, valid])

    with pytest.raises(_QueueExhausted):
        run_sink(bootstrap_servers="localhost:9092")

    assert len(upserts) == 1


def test_subscribes_to_both_topics_by_default(monkeypatch):
    fake_consumer, _, _ = _wire(monkeypatch, [])
    with pytest.raises(_QueueExhausted):
        run_sink(bootstrap_servers="localhost:9092")

    assert set(fake_consumer.subscribed) == {
        sink_module.VOLUME_ANOMALIES_TOPIC,
        sink_module.REGIME_CHANGES_TOPIC,
    }


def test_disables_auto_commit_and_reads_from_earliest(monkeypatch):
    fake_consumer, _, _ = _wire(monkeypatch, [])
    with pytest.raises(_QueueExhausted):
        run_sink(bootstrap_servers="localhost:9092")

    assert fake_consumer.config["enable.auto.commit"] is False
    assert fake_consumer.config["auto.offset.reset"] == "earliest"


def test_group_id_env_override(monkeypatch):
    fake_consumer, _, _ = _wire(monkeypatch, [])
    monkeypatch.setenv("STORAGE_CONSUMER_GROUP", "custom-group")
    with pytest.raises(_QueueExhausted):
        run_sink(bootstrap_servers="localhost:9092")

    assert fake_consumer.config["group.id"] == "custom-group"


def test_group_id_falls_back_to_default_when_env_var_is_present_but_empty(monkeypatch):
    """A `.env` line like `STORAGE_CONSUMER_GROUP=` sets the var to ""
    (present, not absent) -- confirmed live that a real confluent_kafka.Consumer
    constructed with group.id="" crashes with a native C assertion in
    Consumer_init, not a catchable Python exception. This was the actual
    root cause of a real crash, not a hypothetical.
    """
    fake_consumer, _, _ = _wire(monkeypatch, [])
    monkeypatch.setenv("STORAGE_CONSUMER_GROUP", "")
    with pytest.raises(_QueueExhausted):
        run_sink(bootstrap_servers="localhost:9092")

    assert fake_consumer.config["group.id"] == sink_module.DEFAULT_GROUP_ID
