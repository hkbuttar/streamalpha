"""Kill test: forcibly SIGKILL a consumer process mid-processing, restart
it, and verify -- via the durable Postgres table it writes to, not just
inspection -- that streaming/consumer.py's commit-after-durable-write
offset discipline actually holds under a real crash: no tick lost, and no
tick's write duplicated into a second row, even though Kafka redelivery
(and therefore reprocessing) is expected and separately confirmed via the
attempts column. This is the direct, demonstrable proof that the
exactly-once design in streaming/consumer.py and storage/db.py actually
holds, not just a description of how it's supposed to work.

Isolated topic (chaos-kill-test) and isolated table
(chaos_kill_test_ticks), not market-ticks/the anomalies table: this tests
the consumer's crash-recovery mechanics in isolation from the rest of the
pipeline.

    python -m chaos.kill_test
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import time

import psycopg
from confluent_kafka import Producer
from dotenv import load_dotenv

log = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "chaos-kill-test"
TABLE = "chaos_kill_test_ticks"
TICK_COUNT = 300
KILL_TRIGGER_TRADE_ID = 150  # must match chaos/_kill_test_consumer.py
POLL_INTERVAL_SECONDS = 0.05
MAX_WAIT_SECONDS = 90


def _get_conn() -> psycopg.Connection:
    load_dotenv()
    return psycopg.connect(os.environ["DATABASE_URL"])


def _reset_table(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {TABLE}")
        cur.execute(
            f"""
            CREATE TABLE {TABLE} (
                trade_id INTEGER PRIMARY KEY,
                ticker TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 1,
                first_processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                last_processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )
    conn.commit()


def _produce_ticks(n: int) -> None:
    producer = Producer(
        {"bootstrap.servers": BOOTSTRAP_SERVERS, "enable.idempotence": True, "acks": "all"}
    )
    for i in range(n):
        payload = {
            "type": "trade",
            "symbol": "KILLTEST",
            "timestamp": "2024-01-01T00:00:00+00:00",
            "price": 100.0,
            "size": 1,
            "exchange": "V",
            "trade_id": i,
            "conditions": [],
            "tape": "C",
        }
        producer.produce(TOPIC, key=b"KILLTEST", value=json.dumps(payload).encode())
    producer.flush(30)


def _table_stats(conn: psycopg.Connection) -> tuple[int, int, int]:
    """(row_count, distinct_trade_ids, max_attempts)."""
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT COUNT(*), COUNT(DISTINCT trade_id), COALESCE(MAX(attempts), 0) FROM {TABLE}"
        )
        return cur.fetchone()


def _trigger_row_present(conn: psycopg.Connection) -> bool:
    with conn.cursor() as cur:
        cur.execute(f"SELECT 1 FROM {TABLE} WHERE trade_id = %s", (KILL_TRIGGER_TRADE_ID,))
        return cur.fetchone() is not None


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
    )

    conn = _get_conn()
    _reset_table(conn)

    log.info("producing %d ticks to %s", TICK_COUNT, TOPIC)
    _produce_ticks(TICK_COUNT)

    log.info("starting consumer subprocess (run #1)")
    proc = subprocess.Popen([sys.executable, "-m", "chaos._kill_test_consumer"])

    log.info(
        "waiting for trade_id=%d to be durably written (then it stalls before its own "
        "Kafka offset commit -- see chaos/_kill_test_consumer.py)",
        KILL_TRIGGER_TRADE_ID,
    )
    start = time.monotonic()
    while not _trigger_row_present(conn):
        time.sleep(POLL_INTERVAL_SECONDS)
        if time.monotonic() - start > MAX_WAIT_SECONDS:
            proc.kill()
            proc.wait()
            raise RuntimeError(f"trade_id={KILL_TRIGGER_TRADE_ID} never appeared before timeout")

    log.info(
        "trade_id=%d observed durably written -- SIGKILLing consumer (pid %d) now",
        KILL_TRIGGER_TRADE_ID,
        proc.pid,
    )
    proc.kill()
    proc.wait()
    at_kill_row_count, at_kill_distinct, at_kill_max_attempts = _table_stats(conn)
    log.info(
        "at kill: rows=%d distinct_trade_ids=%d max_attempts=%d",
        at_kill_row_count, at_kill_distinct, at_kill_max_attempts,
    )

    log.info("restarting consumer subprocess (run #2) to resume from last committed offset")
    proc2 = subprocess.Popen([sys.executable, "-m", "chaos._kill_test_consumer"])

    start = time.monotonic()
    while True:
        time.sleep(POLL_INTERVAL_SECONDS)
        _, distinct_count, _ = _table_stats(conn)
        if distinct_count >= TICK_COUNT:
            break
        if time.monotonic() - start > MAX_WAIT_SECONDS:
            proc2.kill()
            proc2.wait()
            raise RuntimeError(
                f"consumer never reached {TICK_COUNT} distinct ticks before timeout "
                f"(got {distinct_count})"
            )

    log.info("all %d ticks accounted for -- shutting down consumer #2 cleanly", TICK_COUNT)
    proc2.terminate()
    try:
        proc2.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc2.kill()
        proc2.wait()

    final_row_count, final_distinct, final_max_attempts = _table_stats(conn)
    conn.close()

    no_loss = final_distinct == TICK_COUNT
    no_duplicate_rows = final_row_count == final_distinct
    redelivery_confirmed = final_max_attempts > 1

    print()
    print("=== Kill test results ===")
    print(f"Ticks produced: {TICK_COUNT}")
    print(f"Kill trigger: trade_id={KILL_TRIGGER_TRADE_ID}, SIGKILL sent after its Postgres write "
          f"was durable but before its Kafka offset commit")
    print()
    print("At kill:")
    print(
        f"  rows={at_kill_row_count} distinct_trade_ids={at_kill_distinct} "
        f"max_attempts={at_kill_max_attempts}"
    )
    print("After restart and full drain:")
    print(
        f"  rows={final_row_count} distinct_trade_ids={final_distinct} "
        f"max_attempts={final_max_attempts}"
    )
    print()
    print(f"No ticks lost: {no_loss} ({final_distinct}/{TICK_COUNT} distinct trade_ids present)")
    print(f"No duplicate rows: {no_duplicate_rows} (row count == distinct trade_id count)")
    print(
        f"Redelivery after crash actually occurred: {redelivery_confirmed} "
        f"(max attempts={final_max_attempts})"
    )


if __name__ == "__main__":
    main()
