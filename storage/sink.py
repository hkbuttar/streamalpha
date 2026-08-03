"""Manual-offset consumer that reads volume-anomalies and regime-changes
and upserts each into Postgres (storage/db.py) -- the idempotent sink leg
of the exactly-once story described there.

Same offset discipline as streaming/consumer.py: enable.auto.commit is
off, and an offset is committed only after the corresponding upsert has
been durably committed to Postgres. A crash between the two leaves both
undone, so a restart safely retries the same message and the upsert makes
that retry a no-op rather than a duplicate row.

Unlike streaming/consumer.py, there's no DLQ branch here -- see
storage/schema.py's module docstring for why a malformed payload on these
topics is treated as a bug worth crashing on, not bad data worth routing
aside.

Shutdown signal handling deliberately duplicates streaming/consumer.py's
pattern (custom signal.signal() handler plus a cooperatively-checked
flag) rather than relying on KeyboardInterrupt, for the same reason
documented there: confirmed via lldb, twice, in two unrelated C
extensions, that a blocking call with a timeout still can't be trusted to
let a pending signal through promptly. This is copied rather than shared
through a common helper -- duplicating an already-validated ~20 lines is
lower risk right now than refactoring two working, tested modules to
share one, though that refactor would be reasonable future cleanup.
"""

from __future__ import annotations

import logging
import os
import signal
import threading

from confluent_kafka import Consumer, KafkaError

from storage.db import get_connection, upsert_anomaly
from storage.schema import AnomalyValidationError, parse_anomaly_event

log = logging.getLogger(__name__)

VOLUME_ANOMALIES_TOPIC = "volume-anomalies"
REGIME_CHANGES_TOPIC = "regime-changes"
DEFAULT_GROUP_ID = "streamalpha-storage-sink"
POLL_TIMEOUT_SECONDS = 1.0

TOPIC_TO_ANOMALY_TYPE = {
    VOLUME_ANOMALIES_TOPIC: "volume_anomaly",
    REGIME_CHANGES_TOPIC: "regime_change",
}

_shutdown = threading.Event()


def _install_shutdown_handler() -> None:
    def _on_signal(signum: int, _frame: object) -> None:
        log.info("received signal %s, shutting down", signum)
        _shutdown.set()

    signal.signal(signal.SIGINT, _on_signal)
    signal.signal(signal.SIGTERM, _on_signal)


def run_sink(
    bootstrap_servers: str | None = None,
    database_url: str | None = None,
    group_id: str | None = None,
    topics: list[str] | None = None,
) -> None:
    _shutdown.clear()
    _install_shutdown_handler()

    topics = topics or [VOLUME_ANOMALIES_TOPIC, REGIME_CHANGES_TOPIC]
    bootstrap_servers = bootstrap_servers or os.environ["KAFKA_BOOTSTRAP_SERVERS"]

    # .get(KEY, DEFAULT) is wrong here: a `.env` line like
    # `STORAGE_CONSUMER_GROUP=` sets the var to "" (present, not absent),
    # so .get's default never fires and group.id ends up "". Confirmed
    # live: this crashed confluent_kafka.Consumer with a native C
    # assertion ("Consumer_init"), not a catchable Python exception --
    # reproduced by constructing a bare Consumer with group.id="" in
    # isolation.
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id or os.environ.get("STORAGE_CONSUMER_GROUP") or DEFAULT_GROUP_ID,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    conn = get_connection(database_url)
    consumer.subscribe(topics)

    try:
        while not _shutdown.is_set():
            msg = consumer.poll(POLL_TIMEOUT_SECONDS)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                log.error("consumer error: %s", msg.error())
                continue

            anomaly_type = TOPIC_TO_ANOMALY_TYPE[msg.topic()]
            try:
                payload = parse_anomaly_event(msg.value(), anomaly_type)
            except AnomalyValidationError:
                log.exception(
                    "malformed anomaly payload on %s -- bug in our own producer/schema, not bad "
                    "upstream data (see storage/schema.py); crashing rather than silently dropping",
                    msg.topic(),
                )
                raise

            upsert_anomaly(conn, payload["symbol"], payload["window_start"], anomaly_type, payload)
            consumer.commit(message=msg, asynchronous=False)
    finally:
        conn.close()
        consumer.close()
