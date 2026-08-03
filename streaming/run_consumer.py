"""Runnable entrypoint for the tick consumer: wires the manual-offset,
DLQ-routing consumer (streaming/consumer.py) to the anomaly detection
pipeline -- per-ticker windowed aggregation (streaming/aggregation.py),
then per-ticker volume/regime models (streaming/models.py), publishing
any resulting events (streaming/anomaly_producer.py).

Quotes are read but not fed into aggregation: only trade ticks carry a
transaction price and size, which is what volume/realized-volatility need
(see aggregation.py's module docstring for why quotes don't contribute).

At-least-once, not exactly-once, for anomaly events specifically. This
follows directly from consumer.py's design (an offset commits only after
process_tick returns without raising) applied to a callback that can do
more than one thing: if publishing a *second* anomaly event from the same
tick fails, the exception propagates as designed (see consumer.py's
module docstring), the offset is left uncommitted, and the tick is
redelivered on restart -- but the *first* event from that same tick may
already have been durably published. A duplicate row is possible in
volume-anomalies/regime-changes as a result. This is consistent with the
rest of the pipeline: Kafka topics here carry at-least-once delivery, and
true deduplication is the storage sink's job (upsert on ticker + timestamp
+ anomaly type), not every intermediate hop's.

Model state (streaming/model_state.py) is loaded on startup and saved
periodically plus on clean shutdown, so a restart resumes learning rather
than starting cold. Because saves happen less often than ticks are
processed, a crash between saves means the ticks since the last save get
reprocessed against the older snapshot once Kafka redelivers them from the
last committed offset -- correct (no tick's effect on the model is lost),
just not instantaneous.
"""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime

from dotenv import load_dotenv

from streaming.aggregation import TickerWindow
from streaming.anomaly_producer import AnomalyProducer
from streaming.consumer import run_consumer
from streaming.model_state import DEFAULT_STATE_PATH, load_state, save_state
from streaming.models import TickerModels

log = logging.getLogger(__name__)

DEFAULT_WINDOW_SECONDS = 10.0
STATE_SAVE_INTERVAL_SECONDS = 60.0


def main() -> None:
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    window_seconds = float(os.environ.get("ANOMALY_WINDOW_SECONDS", DEFAULT_WINDOW_SECONDS))
    state_path = os.environ.get("MODEL_STATE_PATH", DEFAULT_STATE_PATH)

    models_by_symbol: dict[str, TickerModels] = load_state(state_path)
    if models_by_symbol:
        log.info("resumed model state for %d ticker(s)", len(models_by_symbol))

    windows_by_symbol: dict[str, TickerWindow] = {}
    anomaly_producer = AnomalyProducer()
    last_save = time.monotonic()

    def process_tick(tick: dict) -> None:
        nonlocal last_save
        if tick["type"] != "trade":
            return

        symbol = tick["symbol"]
        window = windows_by_symbol.setdefault(symbol, TickerWindow(symbol, window_seconds))
        closed = window.add_trade(
            datetime.fromisoformat(tick["timestamp"]), tick["price"], tick["size"]
        )
        if closed is None:
            return

        models = models_by_symbol.setdefault(symbol, TickerModels(symbol))
        for event in models.process_window(closed):
            anomaly_producer.publish(event)
            log.info("anomaly: %r", event)

        if time.monotonic() - last_save > STATE_SAVE_INTERVAL_SECONDS:
            save_state(models_by_symbol, state_path)
            last_save = time.monotonic()
            log.info("saved model state for %d ticker(s)", len(models_by_symbol))

    log.info("starting tick consumer (window=%ss, state_path=%s)", window_seconds, state_path)
    try:
        run_consumer(process_tick)
    finally:
        save_state(models_by_symbol, state_path)
        anomaly_producer.close()
        log.info("saved model state on shutdown")


if __name__ == "__main__":
    main()
