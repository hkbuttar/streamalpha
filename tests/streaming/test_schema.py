"""parse_tick tests. Every failure mode must raise TickValidationError and
nothing else -- that's what lets consumer.py route failures to the DLQ with
a single except clause.
"""

from __future__ import annotations

import json

import pytest

from streaming.schema import TickValidationError, parse_tick

VALID_TRADE = {
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

VALID_QUOTE = {
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


def _bytes(payload) -> bytes:
    return json.dumps(payload).encode("utf-8")


def test_valid_trade_parses_unchanged():
    assert parse_tick(_bytes(VALID_TRADE)) == VALID_TRADE


def test_valid_quote_parses_unchanged():
    assert parse_tick(_bytes(VALID_QUOTE)) == VALID_QUOTE


def test_invalid_utf8_raises_validation_error():
    with pytest.raises(TickValidationError, match="not valid utf-8"):
        parse_tick(b"\xff\xfe\x00\x01")


def test_invalid_json_raises_validation_error():
    with pytest.raises(TickValidationError, match="not valid json"):
        parse_tick(b"{not json")


def test_non_object_json_raises_validation_error():
    with pytest.raises(TickValidationError, match="expected a JSON object"):
        parse_tick(_bytes([1, 2, 3]))


def test_unknown_type_raises_validation_error():
    payload = dict(VALID_TRADE, type="bar")
    with pytest.raises(TickValidationError, match="unknown tick type"):
        parse_tick(_bytes(payload))


def test_missing_type_raises_validation_error():
    payload = {k: v for k, v in VALID_TRADE.items() if k != "type"}
    with pytest.raises(TickValidationError, match="unknown tick type"):
        parse_tick(_bytes(payload))


def test_missing_required_trade_field_raises_validation_error():
    payload = {k: v for k, v in VALID_TRADE.items() if k != "price"}
    with pytest.raises(TickValidationError, match="price"):
        parse_tick(_bytes(payload))


def test_missing_required_quote_field_raises_validation_error():
    payload = {k: v for k, v in VALID_QUOTE.items() if k != "bid_price"}
    with pytest.raises(TickValidationError, match="bid_price"):
        parse_tick(_bytes(payload))


def test_invalid_timestamp_raises_validation_error():
    payload = dict(VALID_TRADE, timestamp="not-a-timestamp")
    with pytest.raises(TickValidationError, match="invalid timestamp"):
        parse_tick(_bytes(payload))


def test_missing_timestamp_raises_validation_error():
    payload = {k: v for k, v in VALID_TRADE.items() if k != "timestamp"}
    with pytest.raises(TickValidationError, match="timestamp"):
        parse_tick(_bytes(payload))
