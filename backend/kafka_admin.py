"""Kafka introspection for backend/main.py's /status endpoint: consumer
group lag and topic message counts, computed directly via confluent_kafka's
AdminClient/Consumer APIs rather than shelling out to
kafka-consumer-groups.sh (the approach used in chaos/'s one-off diagnostic
scripts) -- a real API endpoint needs to work against any Kafka broker, not
just a local Docker container with the CLI tools installed inside it.
"""

from __future__ import annotations

from confluent_kafka import OFFSET_INVALID, Consumer, TopicPartition
from confluent_kafka.admin import AdminClient

from kafka_config import kafka_client_config

METADATA_TIMEOUT_SECONDS = 5.0


def consumer_group_lag(bootstrap_servers: str, group_id: str, topics: list[str]) -> int:
    """Total lag (sum of high_watermark - committed_offset across every
    partition of every given topic) for one consumer group.

    A partition with no committed offset yet (group never ran, or never
    reached that partition) is treated as starting from the earliest
    retained message, not as zero lag -- this matches auto.offset.reset:
    "earliest", which every consumer in this project actually uses (see
    streaming/consumer.py, storage/sink.py), so the reported number is
    what that partition would actually have to work through once the
    group starts, not a misleadingly reassuring zero.
    """
    admin = AdminClient(kafka_client_config(bootstrap_servers))
    consumer = Consumer({**kafka_client_config(bootstrap_servers), "group.id": group_id})
    try:
        cluster_metadata = admin.list_topics(timeout=METADATA_TIMEOUT_SECONDS)
        partitions = [
            TopicPartition(topic, partition)
            for topic in topics
            if topic in cluster_metadata.topics
            for partition in cluster_metadata.topics[topic].partitions
        ]
        if not partitions:
            return 0

        committed = consumer.committed(partitions, timeout=METADATA_TIMEOUT_SECONDS)
        total_lag = 0
        for tp in committed:
            low, high = consumer.get_watermark_offsets(tp, timeout=METADATA_TIMEOUT_SECONDS)
            offset = low if tp.offset == OFFSET_INVALID else tp.offset
            total_lag += max(high - offset, 0)
        return total_lag
    finally:
        consumer.close()


def topic_size(bootstrap_servers: str, topic: str) -> int:
    """Total message count currently retained in a topic (sum of
    high - low watermark across partitions).

    Used for DLQ depth: there's no consumer group continuously draining
    market-ticks-dlq (see streaming/dlq_tools.py), so "lag" isn't the
    right concept there -- this reports "how many records are sitting in
    it right now" instead.
    """
    admin = AdminClient(kafka_client_config(bootstrap_servers))
    consumer = Consumer(
        {**kafka_client_config(bootstrap_servers), "group.id": "streamalpha-backend-topic-size"}
    )
    try:
        cluster_metadata = admin.list_topics(topic, timeout=METADATA_TIMEOUT_SECONDS)
        if topic not in cluster_metadata.topics:
            return 0
        total = 0
        for partition in cluster_metadata.topics[topic].partitions:
            low, high = consumer.get_watermark_offsets(
                TopicPartition(topic, partition), timeout=METADATA_TIMEOUT_SECONDS
            )
            total += max(high - low, 0)
        return total
    finally:
        consumer.close()
