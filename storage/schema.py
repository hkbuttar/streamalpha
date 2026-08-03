"""Validates anomaly event payloads (from volume-anomalies/regime-changes)
before they're upserted.

Deliberately no DLQ here, unlike streaming/schema.py's tick validation.
These payloads are produced entirely by this project's own
streaming/anomaly_producer.py -- there's no external, semi-trusted data
source for the shape to drift out from under us the way Alpaca's raw
payloads could. A malformed anomaly payload here means a genuine bug in
this project's own producer or schema, not bad upstream data, so it's
treated like any other processing bug per streaming/consumer.py's
established distinction: let it crash the consumer loudly (see
storage/sink.py) rather than build a DLQ nobody would meaningfully
inspect-and-replay after fixing our own code.
"""

from __future__ import annotations

import json
from datetime import datetime

REQUIRED_VOLUME_ANOMALY_FIELDS = {
    "symbol",
    "window_start",
    "window_end",
    "volume",
    "trade_count",
    "anomaly_score",
    "score_mean",
    "score_std",
}
REQUIRED_REGIME_CHANGE_FIELDS = {
    "symbol",
    "window_start",
    "window_end",
    "realized_volatility",
    "changepoint_probability",
}

REQUIRED_FIELDS_BY_TYPE = {
    "volume_anomaly": REQUIRED_VOLUME_ANOMALY_FIELDS,
    "regime_change": REQUIRED_REGIME_CHANGE_FIELDS,
}


class AnomalyValidationError(ValueError):
    """Raised for a malformed anomaly event payload."""


def parse_anomaly_event(raw: bytes, anomaly_type: str) -> dict:
    """Decode and validate one anomaly event payload for the given type
    (the caller determines this from which topic the message came from --
    see storage/sink.py's TOPIC_TO_TYPE).
    """
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        raise AnomalyValidationError(f"not valid utf-8/json: {e}") from e

    if not isinstance(payload, dict):
        raise AnomalyValidationError(f"expected a JSON object, got {type(payload).__name__}")

    required = REQUIRED_FIELDS_BY_TYPE.get(anomaly_type)
    if required is None:
        raise AnomalyValidationError(f"unknown anomaly type: {anomaly_type!r}")

    missing = required - payload.keys()
    if missing:
        raise AnomalyValidationError(f"missing required field(s): {sorted(missing)}")

    try:
        datetime.fromisoformat(payload["window_start"])
    except (TypeError, ValueError) as e:
        window_start = payload.get("window_start")
        raise AnomalyValidationError(f"invalid window_start: {window_start!r}") from e

    return payload
