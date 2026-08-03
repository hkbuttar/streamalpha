"""Subprocess entrypoint used by burst_test.py. Runs in its own OS process
(not a thread of burst_test.py) because streaming.consumer.run_consumer's
shutdown handler calls signal.signal(), which only works in a process's
main thread.

Reuses the real, already-tested manual-offset-commit consumer machinery
from streaming/consumer.py against the isolated chaos-burst-test topic,
with a fixed simulated per-message processing delay standing in for
realistic per-tick work (with ~0 processing cost the consumer would drain
a burst too fast for lag dynamics to be observable).
"""

from __future__ import annotations

import logging
import time

from streaming.consumer import run_consumer

TOPIC = "chaos-burst-test"
GROUP_ID = "chaos-burst-test-consumer"
SIMULATED_PROCESSING_SECONDS = 0.005


def process_tick(tick: dict) -> None:
    time.sleep(SIMULATED_PROCESSING_SECONDS)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
    )
    run_consumer(
        process_tick,
        topics=[TOPIC],
        group_id=GROUP_ID,
        bootstrap_servers="localhost:9092",
    )


if __name__ == "__main__":
    main()
