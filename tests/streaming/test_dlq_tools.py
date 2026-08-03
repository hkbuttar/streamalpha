"""dlq_tools tests: main()'s CLI wiring (env loading, subcommand dispatch),
plus a regression test for _drain()'s EOF-detection race -- see
test_dlq.py for the DLQ envelope logic that's unit-testable in isolation.
inspect()/replay() themselves still need a real or faked Kafka connection
and are exercised live, not here.

test_main_calls_load_dotenv_before_running_command exists because main()
originally didn't call load_dotenv() at all: KAFKA_BOOTSTRAP_SERVERS came
back as a KeyError for anyone relying on .env instead of exporting the
var manually in their shell (which is how this got missed during
development -- every manual test happened to export it first).
"""

from __future__ import annotations

import sys

import pytest

from streaming import dlq_tools


class _FakeKafkaError:
    def __init__(self, code):
        self._code = code

    def code(self):
        return self._code

    def __str__(self):
        return f"fake kafka error {self._code}"


class _FakeMessage:
    def __init__(self, error=None, topic="market-ticks-dlq", partition=0, value=b""):
        self._error = error
        self._topic = topic
        self._partition = partition
        self._value = value

    def error(self):
        return self._error

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def value(self):
        return self._value


class _PollExhausted(Exception):
    """Bounds the test if _drain regresses to hanging forever, instead of
    letting a reintroduced bug hang the whole test suite -- same role
    _QueueExhausted plays in tests/streaming/test_consumer.py.
    """


class _EofOnFirstPollConsumer:
    """Reproduces the exact race that caused a real hang, found live
    against a genuinely empty market-ticks-dlq topic: PARTITION_EOF
    arrives on the very first substantive poll(), before a
    consumer.assignment() call made *before* that poll() has any chance
    to observe the rebalance poll() itself just completed internally.
    assignment() here mirrors that: it's empty until poll() has been
    called at least once, matching real librdkafka's behavior of the
    rebalance not being reflected until a poll() actually happens.
    """

    def __init__(self, max_polls: int = 20) -> None:
        self._poll_calls = 0
        self._max_polls = max_polls

    def assignment(self):
        if self._poll_calls == 0:
            return []
        return [object()]  # _drain only calls len() on this

    def poll(self, timeout):
        self._poll_calls += 1
        if self._poll_calls > self._max_polls:
            raise _PollExhausted
        if self._poll_calls == 1:
            return _FakeMessage(error=_FakeKafkaError(dlq_tools.KafkaError._PARTITION_EOF))
        return None


def test_drain_terminates_when_eof_arrives_before_assignment_is_observed():
    """Regression test for a real hang found live: `streaming.dlq_tools
    inspect` never returned against a genuinely empty market-ticks-dlq
    topic. Against the old, buggy _drain() this fake would spin until
    _PollExhausted (20 polls); against the fix it terminates after
    exactly one.
    """
    consumer = _EofOnFirstPollConsumer()

    handled = dlq_tools._drain(consumer, limit=5, handle_message=lambda msg: None)

    assert handled == 0
    assert consumer._poll_calls == 1


def test_main_calls_load_dotenv_before_running_command(monkeypatch):
    calls = []
    monkeypatch.setattr(dlq_tools, "load_dotenv", lambda: calls.append("load_dotenv"))
    monkeypatch.setattr(dlq_tools, "inspect", lambda limit: calls.append(("inspect", limit)))
    monkeypatch.setattr(sys, "argv", ["dlq_tools", "inspect", "--limit", "5"])

    dlq_tools.main()

    assert calls == ["load_dotenv", ("inspect", 5)]


def test_main_dispatches_to_replay(monkeypatch):
    calls = []
    monkeypatch.setattr(dlq_tools, "load_dotenv", lambda: None)
    monkeypatch.setattr(dlq_tools, "replay", lambda limit: calls.append(limit))
    monkeypatch.setattr(sys, "argv", ["dlq_tools", "replay", "--limit", "3"])

    dlq_tools.main()

    assert calls == [3]


def test_inspect_and_replay_default_limit_is_20(monkeypatch):
    calls = []
    monkeypatch.setattr(dlq_tools, "load_dotenv", lambda: None)
    monkeypatch.setattr(dlq_tools, "inspect", lambda limit: calls.append(limit))
    monkeypatch.setattr(sys, "argv", ["dlq_tools", "inspect"])

    dlq_tools.main()

    assert calls == [20]


def test_main_requires_a_subcommand(monkeypatch):
    monkeypatch.setattr(dlq_tools, "load_dotenv", lambda: None)
    monkeypatch.setattr(sys, "argv", ["dlq_tools"])

    with pytest.raises(SystemExit):
        dlq_tools.main()
