"""shutdown.ShutdownHandler tests. install() is verified by capturing the
handler signal.signal() registers and invoking it directly, not by
sending a real OS signal: whether a blocking C call actually lets a
pending signal through promptly was verified live against real hung
processes with lldb (see streaming/consumer.py and ingestion/run.py's
module docstrings) -- that's not something a unit test can usefully
re-check by simulating a signal.
"""

from __future__ import annotations

import signal

from shutdown import ShutdownHandler


def test_starts_not_set():
    assert ShutdownHandler().is_set() is False


def test_set_and_clear():
    handler = ShutdownHandler()
    handler.set()
    assert handler.is_set() is True
    handler.clear()
    assert handler.is_set() is False


def test_install_registers_sigint_and_sigterm(monkeypatch):
    registered = {}
    monkeypatch.setattr(signal, "signal", lambda sig, fn: registered.__setitem__(sig, fn))

    ShutdownHandler().install()

    assert signal.SIGINT in registered
    assert signal.SIGTERM in registered


def test_installed_handler_sets_the_flag_when_invoked(monkeypatch):
    registered = {}
    monkeypatch.setattr(signal, "signal", lambda sig, fn: registered.__setitem__(sig, fn))

    handler = ShutdownHandler()
    handler.install()
    assert handler.is_set() is False

    registered[signal.SIGINT](signal.SIGINT, None)

    assert handler.is_set() is True


def test_two_instances_are_independent():
    a = ShutdownHandler()
    b = ShutdownHandler()

    a.set()

    assert a.is_set() is True
    assert b.is_set() is False
