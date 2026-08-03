"""kafka_admin.py tests. No real Kafka: AdminClient and Consumer are both
faked, following the same module-level monkeypatch pattern as
tests/streaming/test_consumer.py.
"""

from __future__ import annotations

from confluent_kafka import OFFSET_INVALID, TopicPartition

from backend import kafka_admin as kafka_admin_module
from backend.kafka_admin import consumer_group_lag, topic_size


class _FakeTopicMetadata:
    def __init__(self, partition_ids):
        self.partitions = dict.fromkeys(partition_ids)


class _FakeClusterMetadata:
    def __init__(self, topics: dict[str, list[int]]):
        self.topics = {name: _FakeTopicMetadata(pids) for name, pids in topics.items()}


class _FakeAdminClient:
    def __init__(self, config, cluster_metadata):
        self.config = config
        self._cluster_metadata = cluster_metadata

    def list_topics(self, topic=None, timeout=None):
        if topic is None:
            return self._cluster_metadata
        if topic in self._cluster_metadata.topics:
            partition_ids = list(self._cluster_metadata.topics[topic].partitions)
            return _FakeClusterMetadata({topic: partition_ids})
        return _FakeClusterMetadata({})


class _FakeConsumer:
    def __init__(self, config, committed_offsets, watermarks):
        self.config = config
        self._committed_offsets = committed_offsets  # {(topic, partition): offset}
        self._watermarks = watermarks  # {(topic, partition): (low, high)}
        self.closed = False

    def committed(self, partitions, timeout=None):
        result = []
        for tp in partitions:
            offset = self._committed_offsets.get((tp.topic, tp.partition), OFFSET_INVALID)
            result.append(TopicPartition(tp.topic, tp.partition, offset))
        return result

    def get_watermark_offsets(self, tp, timeout=None):
        return self._watermarks[(tp.topic, tp.partition)]

    def close(self):
        self.closed = True


def _wire(monkeypatch, cluster_metadata, committed_offsets=None, watermarks=None):
    monkeypatch.setattr(
        kafka_admin_module, "AdminClient", lambda config: _FakeAdminClient(config, cluster_metadata)
    )
    monkeypatch.setattr(
        kafka_admin_module,
        "Consumer",
        lambda config: _FakeConsumer(config, committed_offsets or {}, watermarks or {}),
    )


def test_consumer_group_lag_sums_across_partitions(monkeypatch):
    cluster_metadata = _FakeClusterMetadata({"market-ticks": [0, 1]})
    committed_offsets = {("market-ticks", 0): 10, ("market-ticks", 1): 20}
    watermarks = {("market-ticks", 0): (0, 15), ("market-ticks", 1): (0, 25)}
    _wire(monkeypatch, cluster_metadata, committed_offsets, watermarks)

    lag = consumer_group_lag("localhost:9092", "my-group", ["market-ticks"])

    assert lag == 5 + 5


def test_consumer_group_lag_uses_earliest_when_never_committed(monkeypatch):
    """No committed offset -- treated as starting from the low watermark
    (matching this project's own auto.offset.reset="earliest" consumers),
    so the reported lag is the full backlog, not a misleading zero.
    """
    cluster_metadata = _FakeClusterMetadata({"market-ticks": [0]})
    watermarks = {("market-ticks", 0): (5, 20)}
    _wire(monkeypatch, cluster_metadata, committed_offsets={}, watermarks=watermarks)

    lag = consumer_group_lag("localhost:9092", "my-group", ["market-ticks"])

    assert lag == 15


def test_consumer_group_lag_caught_up_is_zero(monkeypatch):
    cluster_metadata = _FakeClusterMetadata({"market-ticks": [0]})
    committed_offsets = {("market-ticks", 0): 30}
    watermarks = {("market-ticks", 0): (0, 30)}
    _wire(monkeypatch, cluster_metadata, committed_offsets, watermarks)

    lag = consumer_group_lag("localhost:9092", "my-group", ["market-ticks"])

    assert lag == 0


def test_consumer_group_lag_missing_topic_returns_zero(monkeypatch):
    cluster_metadata = _FakeClusterMetadata({})
    _wire(monkeypatch, cluster_metadata)

    lag = consumer_group_lag("localhost:9092", "my-group", ["does-not-exist"])

    assert lag == 0


def test_topic_size_sums_across_partitions(monkeypatch):
    cluster_metadata = _FakeClusterMetadata({"market-ticks-dlq": [0, 1, 2]})
    watermarks = {
        ("market-ticks-dlq", 0): (0, 3),
        ("market-ticks-dlq", 1): (5, 5),
        ("market-ticks-dlq", 2): (0, 2),
    }
    _wire(monkeypatch, cluster_metadata, watermarks=watermarks)

    size = topic_size("localhost:9092", "market-ticks-dlq")

    assert size == 3 + 0 + 2


def test_topic_size_missing_topic_returns_zero(monkeypatch):
    cluster_metadata = _FakeClusterMetadata({})
    _wire(monkeypatch, cluster_metadata)

    size = topic_size("localhost:9092", "does-not-exist")

    assert size == 0
