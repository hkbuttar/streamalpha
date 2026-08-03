"""Manual-offset, DLQ-routing consumer for market-ticks. This is the
correctness foundation the rest of the pipeline builds on: the pattern here
(commit an offset only after whatever that message required is durably
done) is what makes a crash mid-processing safe rather than a silent tick
loss.

Two distinct failure categories, handled two different ways:

- Malformed or schema-drifted payloads (parse_tick raises
  TickValidationError) are not a bug in this process -- they're bad data,
  most likely from a producer-side change (Alpaca payload shape drift, a
  bug in ingestion/alpaca_stream.py) that this consumer has no way to fix.
  These are routed to market-ticks-dlq (see dlq.py) instead of crashing
  the consumer or being silently dropped, and the original offset is
  committed only after the DLQ write is confirmed durable.

- Anything process_tick itself raises is a bug in the processing logic
  (the online ML models, eventually), not bad data. This is intentionally
  NOT caught here: it propagates and crashes the consumer, leaving the
  offset uncommitted. On restart, the same message is redelivered from the
  last committed offset and reprocessed -- a crash mid-processing loses or
  silently skips nothing. Catching and swallowing processing exceptions
  here would trade that guarantee away for uptime, silently.

Shutdown signal handling. consumer.poll(), even called with a finite
timeout, cannot be trusted to let a pending SIGINT/SIGTERM through
promptly -- confirmed by attaching lldb to a genuinely hung process and
finding the main thread blocked inside librdkafka's C wait
(cnd_timedwait_abs), unresponsive to Ctrl+C for well over a minute despite
a 1-second poll timeout. This is the same class of platform behavior
already found and fixed in ingestion/run.py for a completely different
blocking call (threading.Thread.join()), so it's treated as a general
rule here rather than a one-off: don't rely on a blocking C call to
surface a signal as a Python exception, even with a timeout. Instead, a
custom signal.signal() handler sets a flag, checked cooperatively after
each poll() returns -- shutdown.ShutdownHandler, shared with
storage/sink.py and ingestion/run.py, which all need the same pattern.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Callable

from confluent_kafka import Consumer, KafkaError

from kafka_config import kafka_client_config
from shutdown import ShutdownHandler
from streaming.dlq import DLQProducer
from streaming.schema import TickValidationError, parse_tick

log = logging.getLogger(__name__)

MARKET_TICKS_TOPIC = "market-ticks"
DEFAULT_GROUP_ID = "streamalpha-consumers"
POLL_TIMEOUT_SECONDS = 1.0

ProcessTick = Callable[[dict], None]

_shutdown = ShutdownHandler()


def run_consumer(
    process_tick: ProcessTick,
    topics: list[str] | None = None,
    group_id: str | None = None,
    bootstrap_servers: str | None = None,
) -> None:
    """Consume topics (default [market-ticks]), calling process_tick(dict)
    for each valid message and committing its offset only afterward.

    Runs until a SIGINT/SIGTERM is received or process_tick raises.
    """
    _shutdown.clear()
    _shutdown.install()

    topics = topics or [MARKET_TICKS_TOPIC]

    # .get(KEY, DEFAULT) is wrong here: a `.env` line like `KAFKA_CONSUMER_GROUP=`
    # sets the var to "" (present, not absent), so .get's default never
    # fires and group.id ends up "". Confirmed to matter, not just
    # theoretical: an empty group.id crashes confluent_kafka.Consumer with
    # a native C assertion ("Consumer_init", not a catchable Python
    # exception), not a clean error.
    consumer = Consumer(
        {
            **kafka_client_config(bootstrap_servers),
            "group.id": group_id or os.environ.get("KAFKA_CONSUMER_GROUP") or DEFAULT_GROUP_ID,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    dlq = DLQProducer(bootstrap_servers=bootstrap_servers)
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

            try:
                tick = parse_tick(msg.value())
            except TickValidationError as e:
                log.warning("routing malformed message to DLQ: %s", e)
                dlq.send(msg, e)
                consumer.commit(message=msg, asynchronous=False)
                continue

            process_tick(tick)
            consumer.commit(message=msg, asynchronous=False)
    finally:
        dlq.close()
        consumer.close()
