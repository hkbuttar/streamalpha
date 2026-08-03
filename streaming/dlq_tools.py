"""Inspect and replay market-ticks-dlq records: a small script for looking
at DLQ'd messages and replaying them after a fix.

    python -m streaming.dlq_tools inspect [--limit N]
    python -m streaming.dlq_tools replay [--limit N]

inspect: non-destructive. Uses a fresh, timestamped consumer group every
run, so repeated inspection never advances a shared offset and never
competes with replay's own offsets.

replay: decodes each record's original raw payload and re-publishes it to
its original topic (market-ticks), so it flows back through the normal
consumer and gets re-validated against whatever fix was made. Uses its own
persistent consumer group (REPLAY_GROUP_ID) so a crash mid-replay resumes
on the next run instead of re-replaying everything, and commits a DLQ
offset only after the re-publish is confirmed durable -- the same
commit-after-durable-write discipline as consumer.py.
"""

from __future__ import annotations

import argparse
import base64
import json
import logging
import time
from collections.abc import Callable

from confluent_kafka import Consumer, KafkaError, Producer
from dotenv import load_dotenv

from kafka_config import kafka_client_config
from streaming.dlq import DLQ_TOPIC

log = logging.getLogger(__name__)

REPLAY_GROUP_ID = "streamalpha-dlq-replay"
POLL_TIMEOUT_SECONDS = 2.0

# Third attempt at "how do we know a drain is done." First: give up after
# one empty poll -- wrong, a fresh group's rebalance takes a few seconds
# and an empty poll during that window looks identical to "no more data."
# Second: precompute an exact count via get_watermark_offsets on the same
# consumer that was about to fetch from those partitions -- wrong in a
# different way, caught live: a real replay run got stuck indefinitely on
# the very last message, definitively still present and fetchable by an
# independent consumer (kafka-console-consumer read it fine), so mixing
# watermark queries into an active consuming client's session is the
# suspected cause, though not root-caused further. Both were reinventing
# something the Kafka client already does: enable.partition.eof makes
# poll() return a PARTITION_EOF pseudo-message exactly when a partition
# has been read up to where its high watermark was when that read caught
# up -- the built-in signal for "drained what was here," not a guess.
#
# A fourth bug surfaced later, in this same "third attempt" implementation,
# once the DLQ topic was genuinely empty for the first time (a good sign --
# no malformed messages had occurred yet) rather than having real records
# to drain first: `inspect` hung indefinitely. _drain() checked
# consumer.assignment() once, before the very first poll() -- but on an
# empty topic, PARTITION_EOF can arrive on that very first poll, before
# assignment() (checked immediately before it, in the same iteration) has
# any chance to reflect the rebalance that poll() itself just completed
# internally. assigned_count stayed None forever, the one-time EOF event
# was processed without ever satisfying the break condition, and no second
# EOF was coming to give it another chance. Confirmed live (attach a debug
# script and watch assignment() return [] the instant an EOF message is
# already in hand), then reproduced offline with a fake Consumer for a
# permanent regression test (tests/streaming/test_dlq_tools.py) -- _drain()
# itself had never been unit tested before, only exercised live. Fixed by
# checking assignment() *inside* the EOF branch instead of once at the top
# of the loop: a partition-specific EOF can only arrive for a partition
# already assigned, so checking right there is race-free by construction.


def _original_location(envelope: dict) -> str:
    topic = envelope["original_topic"]
    partition = envelope["original_partition"]
    offset = envelope["original_offset"]
    return f"{topic}[{partition}]@{offset}"


def _drain(consumer: Consumer, limit: int, handle_message: Callable[[object], None]) -> int:
    """Poll and hand each message to handle_message until either `limit`
    messages have been handled, or every assigned partition has reported
    PARTITION_EOF (requires enable.partition.eof=True on the consumer).
    Returns the number handled.

    assigned_count is (re-)checked inside the EOF branch, not once at the
    top of the loop -- found live, on a genuinely empty DLQ topic
    specifically: EOF can arrive on the very first substantive poll, before
    a top-of-loop consumer.assignment() call (issued *before* that poll)
    has any chance to observe the rebalance that poll() itself just
    completed internally. assigned_count stayed None forever as a result,
    the break condition could never fire, and the loop hung indefinitely
    polling a topic with nothing left to receive. Checking assignment()
    only when an EOF actually arrives sidesteps the race entirely: you
    cannot receive a partition-specific EOF for a partition you are not
    yet assigned to, so the rebalance is guaranteed complete by then.
    """
    handled = 0
    partitions_at_eof: set[tuple[str, int]] = set()
    assigned_count: int | None = None

    while handled < limit:
        msg = consumer.poll(POLL_TIMEOUT_SECONDS)
        if msg is None:
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                partitions_at_eof.add((msg.topic(), msg.partition()))
                assignment = consumer.assignment()
                if assignment:
                    assigned_count = len(assignment)
                if assigned_count is not None and len(partitions_at_eof) >= assigned_count:
                    break
                continue
            log.error("consumer error: %s", msg.error())
            continue

        handle_message(msg)
        handled += 1
    return handled


def inspect(limit: int) -> None:
    consumer = Consumer(
        {
            **kafka_client_config(),
            "group.id": f"streamalpha-dlq-inspect-{int(time.time())}",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "enable.partition.eof": True,
        }
    )
    consumer.subscribe([DLQ_TOPIC])
    seen = 0

    def _print_record(msg) -> None:
        nonlocal seen
        seen += 1
        envelope = json.loads(msg.value())
        raw = base64.b64decode(envelope["raw_value_b64"])
        print(f"--- DLQ record {seen} ---")
        print(f"  original: {_original_location(envelope)}")
        print(f"  key: {envelope['original_key']}")
        print(f"  error: {envelope['error']}")
        print(f"  failed_at: {envelope['failed_at']}")
        print(f"  raw_value: {raw!r}")

    try:
        _drain(consumer, limit, _print_record)
    finally:
        consumer.close()
    print(f"\n{seen} record(s) shown.")


def replay(limit: int) -> None:
    consumer = Consumer(
        {
            **kafka_client_config(),
            "group.id": REPLAY_GROUP_ID,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "enable.partition.eof": True,
        }
    )
    producer = Producer(
        {
            **kafka_client_config(),
            "enable.idempotence": True,
            "acks": "all",
            "client.id": "streamalpha-dlq-replay",
        }
    )
    consumer.subscribe([DLQ_TOPIC])
    replayed = 0

    def _replay_record(msg) -> None:
        nonlocal replayed
        envelope = json.loads(msg.value())
        raw = base64.b64decode(envelope["raw_value_b64"])
        key = envelope["original_key"].encode("utf-8") if envelope["original_key"] else None

        producer.produce(envelope["original_topic"], key=key, value=raw)
        remaining = producer.flush(10.0)
        if remaining > 0:
            raise RuntimeError("replay publish did not complete, stopping before commit")

        consumer.commit(message=msg, asynchronous=False)
        replayed += 1
        log.info("replayed %s", _original_location(envelope))

    try:
        _drain(consumer, limit, _replay_record)
    finally:
        consumer.close()
    print(f"{replayed} record(s) replayed.")


def main() -> None:
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    parser = argparse.ArgumentParser(description="Inspect and replay market-ticks-dlq records.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    inspect_parser = subparsers.add_parser("inspect", help="print DLQ records, non-destructively")
    inspect_parser.add_argument("--limit", type=int, default=20)

    replay_parser = subparsers.add_parser("replay", help="re-publish DLQ records to their source")
    replay_parser.add_argument("--limit", type=int, default=20)

    args = parser.parse_args()
    if args.command == "inspect":
        inspect(args.limit)
    elif args.command == "replay":
        replay(args.limit)


if __name__ == "__main__":
    main()
