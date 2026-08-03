"""Runnable entrypoint for the tick consumer. process_tick here just logs
and counts -- real online anomaly detection replaces this later. This
module exists to exercise the manual-offset + DLQ pattern in consumer.py
end to end, not to do anything with the ticks yet.
"""

from __future__ import annotations

import logging

from dotenv import load_dotenv

from streaming.consumer import run_consumer

log = logging.getLogger(__name__)

_count = 0


def _log_tick(tick: dict) -> None:
    global _count
    _count += 1
    if _count % 100 == 0:
        log.info("processed %d ticks so far (last: %s %s)", _count, tick["symbol"], tick["type"])


def main() -> None:
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    log.info("starting tick consumer")
    run_consumer(_log_tick)


if __name__ == "__main__":
    main()
