"""Fan-out for /ws/ticks: one shared Kafka consumer relaying to every
connected WebSocket client, instead of one consumer per connection.

The original /ws/ticks gave each connection its own consumer group
(`streamalpha-backend-ws-{id(websocket)}`), which meant every past
connection left a permanent, never-cleaned-up group behind in Kafka --
confirmed live: enable.auto.commit defaults True and was never turned
off, so each of those throwaway groups actually had offsets committed,
not just registered. TickBroadcaster instead starts exactly one consumer,
lazily, the first time anything subscribes, and keeps it running for the
life of the process regardless of how many clients come and go -- never
torn down on zero subscribers, since restarting it on the next connection
would just re-pay the same "latest"-offset rebalance delay documented
below for no benefit.

enable.auto.commit is explicitly off here, and nothing ever calls
commit(): this is a live view, not a durable consumer -- a backend
restart should always pick up from "latest" again, not resume from
wherever a *previous* process happened to leave off. With a fixed
group.id (unlike the old per-connection design) and auto-commit left on,
a restart would instead resume from a stale committed offset and have to
churn through however much backlog accumulated while the backend was
down before catching up to "live," which defeats the point of this
endpoint.
"""

from __future__ import annotations

import asyncio
import logging
import os

from confluent_kafka import Consumer, KafkaError

from streaming.consumer import MARKET_TICKS_TOPIC

log = logging.getLogger(__name__)

GROUP_ID = "streamalpha-backend-ws-broadcast"
MAX_QUEUE_SIZE = 1000


class TickBroadcaster:
    """bootstrap_servers is read from KAFKA_BOOTSTRAP_SERVERS lazily, when
    the relay loop actually starts (first subscribe()), not at
    construction time -- the module-level instance in backend/main.py is
    constructed at import time, before .env is necessarily the source of
    truth a test wants (tests monkeypatch the env var per test, after
    that one-time import).
    """

    def __init__(self) -> None:
        self._subscribers: set[asyncio.Queue] = set()
        self._task: asyncio.Task | None = None

    def subscribe(self) -> asyncio.Queue:
        """Registers a new subscriber and (on the very first call ever)
        starts the shared relay task. Returns the queue this subscriber
        should read relayed tick payloads from.
        """
        queue: asyncio.Queue = asyncio.Queue(maxsize=MAX_QUEUE_SIZE)
        self._subscribers.add(queue)
        if self._task is None:
            self._task = asyncio.create_task(self._relay_loop())
        return queue

    def unsubscribe(self, queue: asyncio.Queue) -> None:
        self._subscribers.discard(queue)

    async def _relay_loop(self) -> None:
        consumer = Consumer(
            {
                "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
                "group.id": GROUP_ID,
                "enable.auto.commit": False,
                "auto.offset.reset": "latest",
            }
        )
        consumer.subscribe([MARKET_TICKS_TOPIC])
        try:
            while True:
                msg = consumer.poll(0)
                if msg is None:
                    await asyncio.sleep(0.1)
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    log.warning("tick broadcaster consumer error: %s", msg.error())
                    continue

                payload = msg.value().decode("utf-8")
                for queue in list(self._subscribers):
                    try:
                        queue.put_nowait(payload)
                    except asyncio.QueueFull:
                        # A slow subscriber drops ticks rather than
                        # blocking every other connected client -- this is
                        # a live view, not a durable feed, so a gap for
                        # one lagging viewer is the right tradeoff.
                        pass
                # Yield even when a message *was* processed, not just on
                # an empty poll: without this, a busy topic never hands
                # control back to the event loop, starving every other
                # coroutine sharing it (other WS connections, HTTP
                # handlers) for as long as messages keep arriving.
                await asyncio.sleep(0)
        finally:
            consumer.close()
