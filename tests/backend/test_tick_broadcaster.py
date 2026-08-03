"""TickBroadcaster tests. No real Kafka: Consumer is faked at the point
tick_broadcaster.py imported it, following the same pattern as
tests/streaming/test_consumer.py. No pytest-asyncio dependency needed --
each async test body is self-contained and driven directly via
asyncio.run().
"""

from __future__ import annotations

import asyncio

from backend import tick_broadcaster as broadcaster_module
from backend.tick_broadcaster import TickBroadcaster


class _FakeMessage:
    def __init__(self, value, error=None):
        self._value = value
        self._error = error

    def value(self):
        return self._value

    def error(self):
        return self._error


class _FakeKafkaError:
    def __init__(self, code):
        self._code = code

    def code(self):
        return self._code

    def __str__(self):
        return f"fake kafka error {self._code}"


class _FakeConsumer:
    def __init__(self, config, messages):
        self.config = config
        self._messages = list(messages)
        self.subscribed = None
        self.closed = False

    def subscribe(self, topics):
        self.subscribed = topics

    def poll(self, timeout):
        if self._messages:
            return self._messages.pop(0)
        return None

    def close(self):
        self.closed = True


def _wire(monkeypatch, messages):
    fake_consumer = _FakeConsumer(config=None, messages=messages)
    monkeypatch.setattr(
        broadcaster_module, "Consumer", lambda config: _set_config(fake_consumer, config)
    )
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    return fake_consumer


def _set_config(fake_consumer, config):
    fake_consumer.config = config
    return fake_consumer


def test_first_subscribe_starts_the_relay_and_configures_the_consumer(monkeypatch):
    fake_consumer = _wire(monkeypatch, messages=[])

    async def _run():
        broadcaster = TickBroadcaster()
        broadcaster.subscribe()
        await asyncio.sleep(0.05)

    asyncio.run(_run())

    assert fake_consumer.subscribed == [broadcaster_module.MARKET_TICKS_TOPIC]
    assert fake_consumer.config["group.id"] == broadcaster_module.GROUP_ID
    assert fake_consumer.config["enable.auto.commit"] is False
    assert fake_consumer.config["auto.offset.reset"] == "latest"


def test_message_is_delivered_to_every_current_subscriber(monkeypatch):
    _wire(monkeypatch, messages=[_FakeMessage(b"tick-1")])

    async def _run():
        broadcaster = TickBroadcaster()
        q1 = broadcaster.subscribe()
        q2 = broadcaster.subscribe()
        msg1 = await asyncio.wait_for(q1.get(), timeout=2.0)
        msg2 = await asyncio.wait_for(q2.get(), timeout=2.0)
        return msg1, msg2

    msg1, msg2 = asyncio.run(_run())
    assert msg1 == "tick-1"
    assert msg2 == "tick-1"


def test_unsubscribed_queue_receives_nothing_further(monkeypatch):
    fake_consumer = _wire(monkeypatch, messages=[_FakeMessage(b"tick-1")])

    async def _run():
        broadcaster = TickBroadcaster()
        q1 = broadcaster.subscribe()
        q2 = broadcaster.subscribe()

        # both subscribed before tick-1 arrives -- both should get it
        assert await asyncio.wait_for(q1.get(), timeout=2.0) == "tick-1"
        assert await asyncio.wait_for(q2.get(), timeout=2.0) == "tick-1"

        broadcaster.unsubscribe(q1)
        # only made available for polling *after* unsubscribing, so there's
        # no ambiguity about whether the relay loop had already decided
        # who to deliver it to
        fake_consumer._messages.append(_FakeMessage(b"tick-2"))

        second_for_q2 = await asyncio.wait_for(q2.get(), timeout=2.0)
        await asyncio.sleep(0.2)  # give a (buggy) implementation a chance
        # to have delivered to q1 too, if it were still subscribed
        return second_for_q2, q1.empty()

    second_for_q2, q1_empty = asyncio.run(_run())
    assert second_for_q2 == "tick-2"
    assert q1_empty  # never received tick-2, unsubscribed before it arrived


def test_partition_eof_is_skipped_without_being_delivered(monkeypatch):
    eof_msg = _FakeMessage(
        value=None, error=_FakeKafkaError(broadcaster_module.KafkaError._PARTITION_EOF)
    )
    _wire(monkeypatch, messages=[eof_msg, _FakeMessage(b"tick-1")])

    async def _run():
        broadcaster = TickBroadcaster()
        queue = broadcaster.subscribe()
        return await asyncio.wait_for(queue.get(), timeout=2.0)

    delivered = asyncio.run(_run())
    assert delivered == "tick-1"


def test_full_queue_drops_messages_instead_of_blocking(monkeypatch):
    monkeypatch.setattr(broadcaster_module, "MAX_QUEUE_SIZE", 1)
    _wire(
        monkeypatch,
        messages=[_FakeMessage(b"tick-1"), _FakeMessage(b"tick-2"), _FakeMessage(b"tick-3")],
    )

    async def _run():
        broadcaster = TickBroadcaster()
        queue = broadcaster.subscribe()
        await asyncio.sleep(0.2)  # let the relay loop process all 3 messages
        first = await asyncio.wait_for(queue.get(), timeout=2.0)
        return first, queue.empty()

    first, empty_after = asyncio.run(_run())
    assert first == "tick-1"
    assert empty_after  # tick-2 and tick-3 were dropped, not queued behind tick-1
