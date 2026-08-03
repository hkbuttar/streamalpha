"""alpaca_stream tests. build_stream() only registers subscriptions in
memory (confirmed during development that this doesn't touch the network --
see ingestion/run.py's module docstring), so it's safe to exercise with
dummy credentials. No real Alpaca connection anywhere in this file.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta, timezone
from types import SimpleNamespace

from config.universe import UNIVERSE

from ingestion import alpaca_stream


def _fake_trade(**overrides):
    defaults = dict(
        symbol="AAPL",
        timestamp=datetime(2024, 1, 2, 15, 30, tzinfo=UTC),
        price=231.5,
        size=100,
        exchange="V",
        id=12345,
        conditions=["@"],
        tape="C",
    )
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _fake_quote(**overrides):
    defaults = dict(
        symbol="AAPL",
        timestamp=datetime(2024, 1, 2, 15, 30, tzinfo=UTC),
        bid_price=231.4,
        bid_size=2,
        bid_exchange="V",
        ask_price=231.6,
        ask_size=3,
        ask_exchange="V",
        conditions=["R"],
        tape="C",
    )
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def test_trade_to_dict_maps_fields():
    result = alpaca_stream._trade_to_dict(_fake_trade())
    assert result == {
        "type": "trade",
        "symbol": "AAPL",
        "timestamp": "2024-01-02T15:30:00+00:00",
        "price": 231.5,
        "size": 100,
        "exchange": "V",
        "trade_id": 12345,
        "conditions": ["@"],
        "tape": "C",
    }


def test_trade_to_dict_converts_timestamp_to_utc():
    est = timezone(timedelta(hours=-5))
    trade = _fake_trade(timestamp=datetime(2024, 1, 2, 10, 30, tzinfo=est))
    result = alpaca_stream._trade_to_dict(trade)
    assert result["timestamp"] == "2024-01-02T15:30:00+00:00"


def test_quote_to_dict_maps_fields():
    result = alpaca_stream._quote_to_dict(_fake_quote())
    assert result == {
        "type": "quote",
        "symbol": "AAPL",
        "timestamp": "2024-01-02T15:30:00+00:00",
        "bid_price": 231.4,
        "bid_size": 2,
        "bid_exchange": "V",
        "ask_price": 231.6,
        "ask_size": 3,
        "ask_exchange": "V",
        "conditions": ["R"],
        "tape": "C",
    }


def test_resolve_symbols_returns_full_universe_by_default(monkeypatch):
    monkeypatch.delenv("ALPACA_MAX_SYMBOLS", raising=False)
    assert alpaca_stream._resolve_symbols() == UNIVERSE


def test_resolve_symbols_caps_when_env_set(monkeypatch):
    monkeypatch.setenv("ALPACA_MAX_SYMBOLS", "5")
    result = alpaca_stream._resolve_symbols()
    assert result == UNIVERSE[:5]
    assert len(result) == 5


def test_build_stream_subscribes_resolved_symbols_to_both_channels(monkeypatch):
    monkeypatch.setenv("ALPACA_API_KEY", "test")
    monkeypatch.setenv("ALPACA_SECRET_KEY", "test")
    monkeypatch.setenv("ALPACA_MAX_SYMBOLS", "3")

    stream = alpaca_stream.build_stream(SimpleNamespace(publish=lambda *a: None))

    assert set(stream._handlers["trades"]) == set(UNIVERSE[:3])
    assert set(stream._handlers["quotes"]) == set(UNIVERSE[:3])


def test_build_stream_defaults_to_iex_feed(monkeypatch):
    monkeypatch.setenv("ALPACA_API_KEY", "test")
    monkeypatch.setenv("ALPACA_SECRET_KEY", "test")
    monkeypatch.delenv("ALPACA_DATA_FEED", raising=False)

    stream = alpaca_stream.build_stream(SimpleNamespace(publish=lambda *a: None))
    assert stream._endpoint.endswith("/v2/iex")


def test_build_stream_uses_feed_from_env(monkeypatch):
    monkeypatch.setenv("ALPACA_API_KEY", "test")
    monkeypatch.setenv("ALPACA_SECRET_KEY", "test")
    monkeypatch.setenv("ALPACA_DATA_FEED", "sip")

    stream = alpaca_stream.build_stream(SimpleNamespace(publish=lambda *a: None))
    assert stream._endpoint.endswith("/v2/sip")


def test_trade_handler_publishes_to_producer(monkeypatch):
    monkeypatch.setenv("ALPACA_API_KEY", "test")
    monkeypatch.setenv("ALPACA_SECRET_KEY", "test")
    monkeypatch.setenv("ALPACA_MAX_SYMBOLS", "1")

    published = []
    producer = SimpleNamespace(publish=lambda symbol, payload: published.append((symbol, payload)))
    stream = alpaca_stream.build_stream(producer)

    symbol = UNIVERSE[0]
    trade = _fake_trade(symbol=symbol)
    asyncio.run(stream._handlers["trades"][symbol](trade))

    assert published == [(symbol, alpaca_stream._trade_to_dict(trade))]


def test_quote_handler_publishes_to_producer(monkeypatch):
    monkeypatch.setenv("ALPACA_API_KEY", "test")
    monkeypatch.setenv("ALPACA_SECRET_KEY", "test")
    monkeypatch.setenv("ALPACA_MAX_SYMBOLS", "1")

    published = []
    producer = SimpleNamespace(publish=lambda symbol, payload: published.append((symbol, payload)))
    stream = alpaca_stream.build_stream(producer)

    symbol = UNIVERSE[0]
    quote = _fake_quote(symbol=symbol)
    asyncio.run(stream._handlers["quotes"][symbol](quote))

    assert published == [(symbol, alpaca_stream._quote_to_dict(quote))]
