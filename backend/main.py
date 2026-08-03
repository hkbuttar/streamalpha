"""FastAPI backend: a read-only view over what the rest of streamalpha
produces -- detected anomalies (storage/db.py), pipeline health (Kafka
consumer lag, DLQ depth, per-ticker model freshness), and a live tick feed
relayed straight from Kafka. This process only reads; every write path
(ingestion, streaming, storage) is unchanged and keeps running
independently of whether this is even started.

    uvicorn backend.main:app --reload
"""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import UTC, datetime

from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket
from fastapi.middleware.cors import CORSMiddleware

from backend.kafka_admin import consumer_group_lag, topic_size
from storage.db import get_connection, list_anomalies
from storage.sink import DEFAULT_GROUP_ID as STORAGE_GROUP_ID
from storage.sink import REGIME_CHANGES_TOPIC, VOLUME_ANOMALIES_TOPIC
from streaming.consumer import DEFAULT_GROUP_ID as STREAMING_GROUP_ID
from streaming.consumer import MARKET_TICKS_TOPIC
from streaming.dlq import DLQ_TOPIC
from streaming.model_state import DEFAULT_STATE_PATH, load_state

log = logging.getLogger(__name__)

load_dotenv()

app = FastAPI(title="StreamAlpha", version="0.1.0")

_allowed_origins = os.environ.get("ALLOWED_ORIGINS") or "*"
app.add_middleware(
    CORSMiddleware,
    allow_origins=[origin.strip() for origin in _allowed_origins.split(",")],
    allow_methods=["GET"],
    allow_headers=["*"],
)


def _bootstrap_servers() -> str:
    return os.environ["KAFKA_BOOTSTRAP_SERVERS"]


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/anomalies")
def get_anomalies(
    ticker: str | None = None, anomaly_type: str | None = None, limit: int = 50
) -> list[dict]:
    conn = get_connection()
    try:
        return list_anomalies(conn, ticker=ticker, anomaly_type=anomaly_type, limit=limit)
    finally:
        conn.close()


@app.get("/status")
def status() -> dict:
    bootstrap_servers = _bootstrap_servers()
    streaming_group = os.environ.get("KAFKA_CONSUMER_GROUP") or STREAMING_GROUP_ID
    storage_group = os.environ.get("STORAGE_CONSUMER_GROUP") or STORAGE_GROUP_ID

    consumer_lag = {
        "streaming": consumer_group_lag(bootstrap_servers, streaming_group, [MARKET_TICKS_TOPIC]),
        "storage_sink": consumer_group_lag(
            bootstrap_servers, storage_group, [VOLUME_ANOMALIES_TOPIC, REGIME_CHANGES_TOPIC]
        ),
    }
    dlq_depth = topic_size(bootstrap_servers, DLQ_TOPIC)

    state_path = os.environ.get("MODEL_STATE_PATH") or str(DEFAULT_STATE_PATH)
    models_by_symbol = load_state(state_path)
    model_freshness = {
        symbol: last_updated.isoformat() if (last_updated := getattr(model, "last_updated", None))
        else None
        for symbol, model in models_by_symbol.items()
    }

    return {
        "consumer_lag": consumer_lag,
        "dlq_depth": dlq_depth,
        "model_freshness": model_freshness,
        "checked_at": datetime.now(UTC).isoformat(),
    }


@app.websocket("/ws/ticks")
async def ws_ticks(websocket: WebSocket) -> None:
    """Relays raw market-ticks payloads to the client as-is, in real time.

    A fresh, unique consumer group per connection, reading from "latest":
    each connection sees ticks from the moment it connects onward, and
    connections don't compete for partitions or share offsets with each
    other or with the real streaming consumer. This leaves one stale
    consumer-group entry behind per past connection (never cleaned up) --
    an acceptable simplification at this project's current scale, not
    something to build shared-broadcast/group-cleanup machinery for yet.

    No schema validation here (unlike streaming/consumer.py): this is a
    read-only passthrough for display, not the correctness-critical path,
    so a malformed payload is relayed as-is rather than DLQ'd.

    Disconnect detection runs as a separate concurrent task, not a
    try/except around send_text() -- confirmed live that the obvious
    version (catch WebSocketDisconnect around the send) leaks forever: a
    client that disconnects while idle (no new ticks to send) is never
    discovered, since the loop never attempts a send at all, it just polls
    Kafka and sleeps. That also means uvicorn hangs on shutdown waiting
    for the handler to finish. Racing the Kafka-relay loop against a task
    that awaits websocket.receive() (which surfaces the disconnect
    regardless of whether anything was being sent) fixes both.
    """
    await websocket.accept()
    consumer = Consumer(
        {
            "bootstrap.servers": _bootstrap_servers(),
            "group.id": f"streamalpha-backend-ws-{id(websocket)}",
            "auto.offset.reset": "latest",
        }
    )
    consumer.subscribe([MARKET_TICKS_TOPIC])

    async def _relay() -> None:
        while True:
            msg = consumer.poll(0)
            if msg is None:
                await asyncio.sleep(0.1)
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                log.warning("ws_ticks consumer error: %s", msg.error())
                continue
            await websocket.send_text(msg.value().decode("utf-8"))

    async def _wait_for_disconnect() -> None:
        while True:
            message = await websocket.receive()
            if message["type"] == "websocket.disconnect":
                return

    relay_task = asyncio.create_task(_relay())
    disconnect_task = asyncio.create_task(_wait_for_disconnect())
    try:
        await asyncio.wait({relay_task, disconnect_task}, return_when=asyncio.FIRST_COMPLETED)
    finally:
        relay_task.cancel()
        disconnect_task.cancel()
        consumer.close()
