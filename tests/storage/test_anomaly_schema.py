"""parse_anomaly_event tests."""

from __future__ import annotations

import json

import pytest

from storage.schema import AnomalyValidationError, parse_anomaly_event

VALID_VOLUME_ANOMALY = {
    "symbol": "AAPL",
    "window_start": "2024-01-02T15:30:00+00:00",
    "window_end": "2024-01-02T15:30:10+00:00",
    "volume": 50000.0,
    "trade_count": 42,
    "anomaly_score": 0.99,
    "score_mean": 0.5,
    "score_std": 0.1,
}

VALID_REGIME_CHANGE = {
    "symbol": "AAPL",
    "window_start": "2024-01-02T15:30:00+00:00",
    "window_end": "2024-01-02T15:30:10+00:00",
    "realized_volatility": 0.3,
    "changepoint_probability": 0.87,
}


def _bytes(payload) -> bytes:
    return json.dumps(payload).encode("utf-8")


def test_valid_volume_anomaly_parses_unchanged():
    parsed = parse_anomaly_event(_bytes(VALID_VOLUME_ANOMALY), "volume_anomaly")
    assert parsed == VALID_VOLUME_ANOMALY


def test_valid_regime_change_parses_unchanged():
    assert parse_anomaly_event(_bytes(VALID_REGIME_CHANGE), "regime_change") == VALID_REGIME_CHANGE


def test_invalid_json_raises():
    with pytest.raises(AnomalyValidationError, match="not valid utf-8/json"):
        parse_anomaly_event(b"{not json", "volume_anomaly")


def test_non_object_json_raises():
    with pytest.raises(AnomalyValidationError, match="expected a JSON object"):
        parse_anomaly_event(_bytes([1, 2, 3]), "volume_anomaly")


def test_unknown_anomaly_type_raises():
    with pytest.raises(AnomalyValidationError, match="unknown anomaly type"):
        parse_anomaly_event(_bytes(VALID_VOLUME_ANOMALY), "not_a_real_type")


def test_missing_required_volume_anomaly_field_raises():
    payload = {k: v for k, v in VALID_VOLUME_ANOMALY.items() if k != "anomaly_score"}
    with pytest.raises(AnomalyValidationError, match="anomaly_score"):
        parse_anomaly_event(_bytes(payload), "volume_anomaly")


def test_missing_required_regime_change_field_raises():
    payload = {k: v for k, v in VALID_REGIME_CHANGE.items() if k != "changepoint_probability"}
    with pytest.raises(AnomalyValidationError, match="changepoint_probability"):
        parse_anomaly_event(_bytes(payload), "regime_change")


def test_invalid_window_start_raises():
    payload = dict(VALID_VOLUME_ANOMALY, window_start="not-a-timestamp")
    with pytest.raises(AnomalyValidationError, match="invalid window_start"):
        parse_anomaly_event(_bytes(payload), "volume_anomaly")


def test_a_volume_anomaly_payload_fails_as_a_regime_change():
    with pytest.raises(AnomalyValidationError, match="missing required field"):
        parse_anomaly_event(_bytes(VALID_VOLUME_ANOMALY), "regime_change")
