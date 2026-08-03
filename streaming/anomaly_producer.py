"""Idempotent producer for anomaly events: VolumeAnomaly goes to
volume-anomalies, RegimeChange goes to regime-changes.

Config mirrors ingestion/producer.py and streaming/dlq.py's idempotent
producer settings (enable.idempotence + acks=all) for the same reason:
these are also part of the pipeline's exactly-once story, not just raw
ticks. publish() flushes synchronously before returning -- like dlq.py,
not like ingestion/producer.py's fire-and-poll TickProducer -- because
streaming/run_consumer.py's process_tick calls this before consumer.py
commits the original tick's offset, and that commit must not happen until
any anomaly event it produced is durably written. This is only correct
because anomaly events are rare relative to raw ticks (at most a couple
per window close, not per tick), so the per-event flush cost is
negligible; TickProducer's design wouldn't be appropriate for a
per-message flush at raw tick volume.
"""

from __future__ import annotations

import json
import logging
import os

from confluent_kafka import Producer

from streaming.models import RegimeChange, VolumeAnomaly

log = logging.getLogger(__name__)

VOLUME_ANOMALIES_TOPIC = "volume-anomalies"
REGIME_CHANGES_TOPIC = "regime-changes"

AnomalyEvent = VolumeAnomaly | RegimeChange


def _volume_anomaly_payload(event: VolumeAnomaly) -> dict:
    return {
        "symbol": event.symbol,
        "window_start": event.window_start.isoformat(),
        "window_end": event.window_end.isoformat(),
        "volume": event.volume,
        "trade_count": event.trade_count,
        "anomaly_score": event.anomaly_score,
        "score_mean": event.score_mean,
        "score_std": event.score_std,
    }


def _regime_change_payload(event: RegimeChange) -> dict:
    return {
        "symbol": event.symbol,
        "window_start": event.window_start.isoformat(),
        "window_end": event.window_end.isoformat(),
        "realized_volatility": event.realized_volatility,
        "changepoint_probability": event.changepoint_probability,
    }


class AnomalyProducer:
    def __init__(self, bootstrap_servers: str | None = None) -> None:
        bootstrap_servers = bootstrap_servers or os.environ["KAFKA_BOOTSTRAP_SERVERS"]
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "enable.idempotence": True,
                "acks": "all",
                "client.id": "streamalpha-anomaly-producer",
            }
        )

    def publish(self, event: AnomalyEvent) -> None:
        if isinstance(event, VolumeAnomaly):
            topic, payload = VOLUME_ANOMALIES_TOPIC, _volume_anomaly_payload(event)
        elif isinstance(event, RegimeChange):
            topic, payload = REGIME_CHANGES_TOPIC, _regime_change_payload(event)
        else:
            raise TypeError(f"unknown anomaly event type: {type(event).__name__}")

        self._producer.produce(
            topic,
            key=event.symbol.encode("utf-8"),
            value=json.dumps(payload).encode("utf-8"),
        )
        remaining = self._producer.flush(10.0)
        if remaining > 0:
            raise RuntimeError(
                f"anomaly event publish to {topic} did not complete: "
                f"{remaining} message(s) undelivered"
            )

    def close(self) -> None:
        self._producer.flush(10.0)
