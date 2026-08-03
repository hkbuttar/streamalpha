"""dlq_tools.main() tests: just the CLI wiring (env loading, subcommand
dispatch). inspect()/replay() themselves need a real or faked Kafka
connection and are exercised live rather than here -- see test_dlq.py for
the DLQ envelope logic that is unit-testable in isolation.

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
