"""Runnable entrypoint for the Postgres sink."""

from __future__ import annotations

import logging

from dotenv import load_dotenv

from storage.sink import run_sink

log = logging.getLogger(__name__)


def main() -> None:
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    log.info("starting storage sink")
    run_sink()


if __name__ == "__main__":
    main()
