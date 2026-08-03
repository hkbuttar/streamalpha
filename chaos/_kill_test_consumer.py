"""Subprocess entrypoint used by kill_test.py. Runs in its own OS process
so the driver can SIGKILL it to simulate a genuine mid-processing crash.

process_tick durably upserts each tick into chaos_kill_test_ticks *before*
returning -- committing that Postgres write is exactly what
streaming.consumer.run_consumer's commit-after-durable-write discipline is
meant to protect (it only commits the Kafka offset after process_tick
returns). attempts increments on every upsert of the same trade_id, so a
trade_id with attempts > 1 in the final table is direct proof Kafka
redelivered it after the kill, not just that nothing was lost.

For one designated trade_id (KILL_TRIGGER_TRADE_ID), process_tick stalls
*after* the Postgres commit but *before* returning -- this deterministically
opens the exact window the test needs to land the kill in (durable write
done, Kafka offset commit not yet issued), rather than hoping an
externally-timed SIGKILL happens to land in a gap measured in
milliseconds.
"""

from __future__ import annotations

import logging
import os
import time

import psycopg
from dotenv import load_dotenv

from streaming.consumer import run_consumer

TOPIC = "chaos-kill-test"
GROUP_ID = "chaos-kill-test-consumer"
TABLE = "chaos_kill_test_ticks"
KILL_TRIGGER_TRADE_ID = 150
POST_COMMIT_STALL_SECONDS = 3.0

log = logging.getLogger(__name__)


def _make_process_tick(conn: psycopg.Connection):
    def process_tick(tick: dict) -> None:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {TABLE} (trade_id, ticker)
                VALUES (%s, %s)
                ON CONFLICT (trade_id) DO UPDATE
                SET attempts = {TABLE}.attempts + 1, last_processed_at = now()
                """,
                (tick["trade_id"], tick["symbol"]),
            )
        conn.commit()

        if tick["trade_id"] == KILL_TRIGGER_TRADE_ID:
            log.info(
                "trade_id=%d durably written -- stalling %.1fs before this process would "
                "commit its Kafka offset, so the driver can SIGKILL here",
                KILL_TRIGGER_TRADE_ID,
                POST_COMMIT_STALL_SECONDS,
            )
            time.sleep(POST_COMMIT_STALL_SECONDS)

    return process_tick


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
    )
    load_dotenv()
    conn = psycopg.connect(os.environ["DATABASE_URL"])
    try:
        run_consumer(
            _make_process_tick(conn),
            topics=[TOPIC],
            group_id=GROUP_ID,
            bootstrap_servers="localhost:9092",
        )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
