"""Idempotent producer that routes a malformed market-ticks message to
market-ticks-dlq, preserving enough of the original message (topic,
partition, offset, key, raw value) to inspect and replay it later with
streaming/dlq_tools.py.

Producer config mirrors ingestion/producer.py's (enable.idempotence +
acks=all) deliberately: the exactly-once story doesn't stop at the
ingestion boundary, so a duplicate DLQ write on retry needs the same
broker-side dedup guarantee a duplicate raw tick write does.
"""

from __future__ import annotations

import base64
import json
from datetime import UTC, datetime

from confluent_kafka import Message, Producer

from kafka_config import kafka_client_config

DLQ_TOPIC = "market-ticks-dlq"


class DLQProducer:
    def __init__(self, bootstrap_servers: str | None = None) -> None:
        self._producer = Producer(
            {
                **kafka_client_config(bootstrap_servers),
                "enable.idempotence": True,
                "acks": "all",
                "client.id": "streamalpha-dlq",
            }
        )

    def send(self, original: Message, error: Exception) -> None:
        """Publish original to the DLQ and block until it's durably written.

        Synchronous on purpose: consumer.py only commits the original
        message's offset after this returns, so a crash between the DLQ
        write and that commit can only cause the message to be reprocessed
        (and DLQ'd again, harmlessly) on restart -- never silently dropped.
        A failed/incomplete flush raises, which is exactly what should stop
        the commit from happening.
        """
        key = original.key()
        raw_value = original.value() or b""
        envelope = {
            "original_topic": original.topic(),
            "original_partition": original.partition(),
            "original_offset": original.offset(),
            "original_key": key.decode("utf-8", errors="replace") if key else None,
            "raw_value_b64": base64.b64encode(raw_value).decode("ascii"),
            "error": f"{type(error).__name__}: {error}",
            "failed_at": datetime.now(UTC).isoformat(),
        }
        self._producer.produce(
            DLQ_TOPIC,
            key=key,
            value=json.dumps(envelope).encode("utf-8"),
        )
        remaining = self._producer.flush(10.0)
        if remaining > 0:
            raise RuntimeError(f"DLQ write did not complete: {remaining} message(s) undelivered")

    def close(self) -> None:
        self._producer.flush(10.0)
