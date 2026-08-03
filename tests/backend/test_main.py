"""backend/main.py tests. No real Kafka/Postgres: get_connection,
list_anomalies, consumer_group_lag, topic_size, load_state, and the
WebSocket's Consumer are all faked/monkeypatched at the point backend/main.py
imported them, following the same pattern as tests/streaming/test_consumer.py.
"""

from __future__ import annotations

from fastapi.testclient import TestClient

from backend import main as main_module


class _FakeTickMessage:
    def __init__(self, value, error=None):
        self._value = value
        self._error = error

    def value(self):
        return self._value

    def error(self):
        return self._error


class _FakeTicksConsumer:
    def __init__(self, config, messages):
        self.config = config
        self._messages = list(messages)
        self.subscribed = None
        self.closed = False

    def subscribe(self, topics):
        self.subscribed = topics

    def poll(self, timeout):
        if self._messages:
            return self._messages.pop(0)
        return None

    def close(self):
        self.closed = True


class _FakeConnection:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


def test_health():
    client = TestClient(main_module.app)
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_anomalies_passes_filters_through_and_closes_connection(monkeypatch):
    fake_conn = _FakeConnection()
    captured = {}

    monkeypatch.setattr(main_module, "get_connection", lambda: fake_conn)

    def _fake_list_anomalies(conn, ticker=None, anomaly_type=None, limit=50):
        captured["conn"] = conn
        captured["ticker"] = ticker
        captured["anomaly_type"] = anomaly_type
        captured["limit"] = limit
        return [{"ticker": "AAPL", "anomaly_type": "regime_change"}]

    monkeypatch.setattr(main_module, "list_anomalies", _fake_list_anomalies)

    client = TestClient(main_module.app)
    params = {"ticker": "AAPL", "anomaly_type": "regime_change", "limit": 5}
    response = client.get("/anomalies", params=params)

    assert response.status_code == 200
    assert response.json() == [{"ticker": "AAPL", "anomaly_type": "regime_change"}]
    assert captured["conn"] is fake_conn
    assert captured["ticker"] == "AAPL"
    assert captured["anomaly_type"] == "regime_change"
    assert captured["limit"] == 5
    assert fake_conn.closed is True


def test_anomalies_defaults_to_no_filters_and_limit_50(monkeypatch):
    fake_conn = _FakeConnection()
    captured = {}

    monkeypatch.setattr(main_module, "get_connection", lambda: fake_conn)

    def _fake_list_anomalies(conn, ticker=None, anomaly_type=None, limit=50):
        captured.update(ticker=ticker, anomaly_type=anomaly_type, limit=limit)
        return []

    monkeypatch.setattr(main_module, "list_anomalies", _fake_list_anomalies)

    client = TestClient(main_module.app)
    response = client.get("/anomalies")

    assert response.status_code == 200
    assert captured == {"ticker": None, "anomaly_type": None, "limit": 50}


def test_status_assembles_lag_dlq_and_model_freshness(monkeypatch):
    lag_calls = []

    def _fake_lag(bootstrap_servers, group_id, topics):
        lag_calls.append((bootstrap_servers, group_id, topics))
        return {"streaming-group": 12, "storage-group": 3}[group_id]

    monkeypatch.setattr(main_module, "consumer_group_lag", _fake_lag)
    monkeypatch.setattr(main_module, "topic_size", lambda bootstrap_servers, topic: 7)

    class _Model:
        def __init__(self, last_updated):
            self.last_updated = last_updated

    import datetime

    fresh = datetime.datetime(2024, 1, 2, 15, 30, tzinfo=datetime.UTC)
    monkeypatch.setattr(
        main_module, "load_state", lambda path: {"AAPL": _Model(fresh), "MSFT": _Model(None)}
    )

    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    monkeypatch.setenv("KAFKA_CONSUMER_GROUP", "streaming-group")
    monkeypatch.setenv("STORAGE_CONSUMER_GROUP", "storage-group")

    client = TestClient(main_module.app)
    response = client.get("/status")

    assert response.status_code == 200
    body = response.json()
    assert body["consumer_lag"] == {"streaming": 12, "storage_sink": 3}
    assert body["dlq_depth"] == 7
    assert body["model_freshness"] == {"AAPL": fresh.isoformat(), "MSFT": None}
    assert "checked_at" in body
    assert len(lag_calls) == 2


def test_ws_ticks_relays_raw_payload_and_subscribes_to_market_ticks(monkeypatch):
    tick_bytes = b'{"type": "trade", "symbol": "AAPL"}'
    fake_consumer = _FakeTicksConsumer(config=None, messages=[_FakeTickMessage(tick_bytes)])
    monkeypatch.setattr(main_module, "Consumer", lambda config: fake_consumer)
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    client = TestClient(main_module.app)
    with client.websocket_connect("/ws/ticks") as ws:
        received = ws.receive_text()
        assert received == tick_bytes.decode("utf-8")

    assert fake_consumer.subscribed == [main_module.MARKET_TICKS_TOPIC]
    assert fake_consumer.closed is True


def test_ws_ticks_cleans_up_on_disconnect_even_with_no_messages_ever_sent(monkeypatch):
    """Regression test for a real bug found running this live: a client
    disconnecting while idle (no ticks to relay) was never noticed by the
    old try/except-around-send_text() design, since the loop never
    attempted a send at all -- it just polled Kafka and slept, leaking the
    connection and its Kafka consumer forever (and hanging uvicorn on
    shutdown, waiting for a handler that would never finish). Disconnect
    detection now runs as its own concurrent task instead of piggybacking
    on send_text() failing.
    """
    fake_consumer = _FakeTicksConsumer(config=None, messages=[])
    monkeypatch.setattr(main_module, "Consumer", lambda config: fake_consumer)
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    client = TestClient(main_module.app)
    with client.websocket_connect("/ws/ticks"):
        pass

    assert fake_consumer.closed is True
