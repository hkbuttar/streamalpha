"""Validates raw market-ticks payloads before they're handed to a
processor. Anything that fails here is malformed or schema-drifted data,
not a processing bug -- see consumer.py for why that distinction decides
whether a message goes to the DLQ or is left to crash the consumer.
"""

from __future__ import annotations

import json
from datetime import datetime

REQUIRED_TRADE_FIELDS = {
    "symbol",
    "timestamp",
    "price",
    "size",
    "exchange",
    "trade_id",
    "conditions",
    "tape",
}
REQUIRED_QUOTE_FIELDS = {
    "symbol",
    "timestamp",
    "bid_price",
    "bid_size",
    "bid_exchange",
    "ask_price",
    "ask_size",
    "ask_exchange",
    "conditions",
    "tape",
}


class TickValidationError(ValueError):
    """Raised for any malformed or schema-drifted market-ticks payload."""


def parse_tick(raw: bytes) -> dict:
    """Decode and validate one market-ticks message value.

    Raises TickValidationError, and only TickValidationError, on any
    problem, so callers can route failures to the DLQ with a single except
    clause instead of guessing which built-in exception a given corruption
    happens to raise.
    """
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as e:
        raise TickValidationError(f"not valid utf-8: {e}") from e

    try:
        payload = json.loads(text)
    except json.JSONDecodeError as e:
        raise TickValidationError(f"not valid json: {e}") from e

    if not isinstance(payload, dict):
        raise TickValidationError(f"expected a JSON object, got {type(payload).__name__}")

    tick_type = payload.get("type")
    if tick_type == "trade":
        required = REQUIRED_TRADE_FIELDS
    elif tick_type == "quote":
        required = REQUIRED_QUOTE_FIELDS
    else:
        raise TickValidationError(f"unknown tick type: {tick_type!r}")

    missing = required - payload.keys()
    if missing:
        raise TickValidationError(f"missing required field(s): {sorted(missing)}")

    try:
        datetime.fromisoformat(payload["timestamp"])
    except (TypeError, ValueError) as e:
        raise TickValidationError(f"invalid timestamp: {payload.get('timestamp')!r}") from e

    return payload
