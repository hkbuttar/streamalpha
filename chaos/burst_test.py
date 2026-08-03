"""Burst load test: publish a large batch of synthetic ticks to an isolated
test topic (chaos-burst-test, not market-ticks) as fast as the producer can
go, then run a consumer against it in a real subprocess and track how
Kafka consumer-group lag grows and drains.

Isolated topic, not market-ticks: this measures the consumer's own
throughput/lag mechanics under load using the real manual-offset-commit
machinery from streaming/consumer.py, without touching production data or
triggering spurious anomaly detections in Postgres.

The consumer runs in a genuine subprocess (chaos/_burst_consumer.py), not a
thread of this script: streaming.consumer.run_consumer installs a
signal.signal() handler for clean shutdown, and signal.signal() only works
in a process's main thread.

    python -m chaos.burst_test
"""

from __future__ import annotations

import json
import logging
import subprocess
import sys
import time

from confluent_kafka import Producer

log = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "chaos-burst-test"
GROUP_ID = "chaos-burst-test-consumer"
BURST_SIZE = 8000
LAG_POLL_INTERVAL_SECONDS = 1.0
MAX_WAIT_SECONDS = 120


def _produce_burst(n: int) -> float:
    producer = Producer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "enable.idempotence": True,
            "acks": "all",
        }
    )
    start = time.monotonic()
    for i in range(n):
        payload = {
            "type": "trade",
            "symbol": "CHAOSTEST",
            "timestamp": "2024-01-01T00:00:00+00:00",
            "price": 100.0,
            "size": 1,
            "exchange": "V",
            "trade_id": i,
            "conditions": [],
            "tape": "C",
        }
        producer.produce(TOPIC, key=b"CHAOSTEST", value=json.dumps(payload).encode())
        if i % 500 == 0:
            producer.poll(0)
    producer.flush(30)
    return time.monotonic() - start


def _consumer_group_lag() -> int | None:
    """Total lag across all partitions for GROUP_ID/TOPIC, parsed from
    kafka-consumer-groups.sh --describe's plain-text output. A one-off
    diagnostic script, so shelling out to the same CLI already used
    throughout this project for lag/offset inspection is fine here -- not
    worth a dedicated admin-client dependency for one measurement.
    """
    result = subprocess.run(
        [
            "docker",
            "exec",
            "streamalpha-kafka",
            "/opt/kafka/bin/kafka-consumer-groups.sh",
            "--bootstrap-server",
            "localhost:9092",
            "--describe",
            "--group",
            GROUP_ID,
        ],
        capture_output=True,
        text=True,
        timeout=15,
    )
    total_lag = 0
    found = False
    for line in result.stdout.splitlines():
        parts = line.split()
        if len(parts) >= 6 and parts[0] == GROUP_ID and parts[1] == TOPIC:
            try:
                total_lag += int(parts[5])
                found = True
            except ValueError:
                continue
    return total_lag if found else None


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
    )

    log.info("producing burst of %d messages to %s", BURST_SIZE, TOPIC)
    produce_seconds = _produce_burst(BURST_SIZE)
    produce_rate = BURST_SIZE / produce_seconds
    log.info("burst produced in %.2fs (%.0f msg/s)", produce_seconds, produce_rate)

    log.info("starting consumer subprocess")
    consumer_proc = subprocess.Popen([sys.executable, "-m", "chaos._burst_consumer"])

    lag_samples: list[tuple[float, int]] = []
    peak_lag = 0
    start = time.monotonic()
    try:
        while time.monotonic() - start < MAX_WAIT_SECONDS:
            time.sleep(LAG_POLL_INTERVAL_SECONDS)
            lag = _consumer_group_lag()
            elapsed = time.monotonic() - start
            if lag is None:
                continue
            lag_samples.append((elapsed, lag))
            peak_lag = max(peak_lag, lag)
            log.info("t=%.0fs lag=%d", elapsed, lag)
            if lag == 0 and elapsed > 2:
                log.info("lag fully drained at t=%.1fs", elapsed)
                break
    finally:
        consumer_proc.terminate()
        try:
            consumer_proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            consumer_proc.kill()
            consumer_proc.wait()

    recovery_time = lag_samples[-1][0] if lag_samples and lag_samples[-1][1] == 0 else None

    print()
    print("=== Burst test results ===")
    print(f"Burst size: {BURST_SIZE} messages")
    print(f"Produce rate: {produce_rate:.0f} msg/s ({produce_seconds:.2f}s to publish)")
    print(f"Peak consumer-group lag observed: {peak_lag}")
    if recovery_time is not None:
        print(f"Recovery time (lag -> 0): {recovery_time:.1f}s")
    else:
        print(f"Recovery time: did not fully drain within {MAX_WAIT_SECONDS}s timeout")
    print(f"Lag samples: {lag_samples}")


if __name__ == "__main__":
    main()
