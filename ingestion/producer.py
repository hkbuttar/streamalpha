"""Idempotent Kafka producer for raw Alpaca trade/quote ticks.

enable.idempotence=true plus acks=all is the producer-side leg of the
exactly-once story: librdkafka tags each message with a producer ID and a
per-partition sequence number, so if a send has to be retried (broker
timeout, leader failover) the broker recognizes the retry by its sequence
number and drops the duplicate instead of appending it twice.

This only guards against *producer-side retries*. It does not dedupe a tick
we hand to publish() twice ourselves, and it says nothing about what happens
downstream if a consumer reprocesses a message after a crash -- that is what
the consumer's manual offset commit (streaming/consumer.py) and an idempotent
upsert on the storage sink are for. All three legs together are what make
the pipeline exactly-once end to end; this module is only one of them.
"""

from __future__ import annotations

import json
import logging
import os

from confluent_kafka import Producer

log = logging.getLogger(__name__)

MARKET_TICKS_TOPIC = "market-ticks"


class TickProducer:
    """Thin wrapper over confluent_kafka.Producer, configured for idempotence."""

    def __init__(self, bootstrap_servers: str | None = None) -> None:
        bootstrap_servers = bootstrap_servers or os.environ["KAFKA_BOOTSTRAP_SERVERS"]
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "enable.idempotence": True,
                "acks": "all",
                "client.id": "streamalpha-ingestion",
            }
        )

    def publish(self, symbol: str, payload: dict) -> None:
        """Publish one tick, keyed by ticker symbol.

        Keying by symbol (rather than letting the producer round-robin
        across partitions) guarantees every message for a given ticker
        lands in the same partition in send order. That per-partition
        ordering is what lets a per-ticker online model consume ticks in
        order without needing to reorder across partitions itself.
        """
        self._producer.produce(
            MARKET_TICKS_TOPIC,
            key=symbol.encode("utf-8"),
            value=json.dumps(payload).encode("utf-8"),
            callback=self._delivery_callback,
        )
        # produce() only enqueues; poll(0) is non-blocking and services any
        # delivery callbacks (and internal retry bookkeeping) that have
        # completed since the last call. Skipping this means the internal
        # queue fills up under sustained load and produce() starts raising
        # BufferError.
        self._producer.poll(0)

    @staticmethod
    def _delivery_callback(err, msg) -> None:
        if err is not None:
            log.error("delivery failed for key=%s: %s", msg.key(), err)

    def flush(self, timeout: float = 10.0) -> None:
        """Block until all buffered messages are delivered, or timeout.

        Call this on shutdown. Anything still unflushed when the process is
        killed outright (not given the chance to flush) is lost -- the same
        kind of gap as a WebSocket disconnect, just on the producer's exit
        path instead of Alpaca's send path. Polling after every publish()
        keeps that buffer small, which bounds the blast radius but doesn't
        eliminate it.
        """
        remaining = self._producer.flush(timeout)
        if remaining > 0:
            log.warning("%d messages still undelivered after flush timeout", remaining)
