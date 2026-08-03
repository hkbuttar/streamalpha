"""ingestion.run tests. All offline -- fake stream objects instead of real
Alpaca/Kafka connections.

_run_with_watchdog's shutdown handling was the subject of a real bug during
development: a bare thread.join(), and then even a *polled* thread.join()
wrapped in try/except KeyboardInterrupt, did not reliably respond to a
signal (confirmed by attaching lldb to a genuinely hung process). The fix
was a custom signal handler setting a threading.Event, checked cooperatively
in the poll loop instead of relying on an asynchronously raised exception.
These tests exercise that exact mechanism directly via the module's
_shutdown flag, not just the happy path -- they're regression coverage for
a bug that looked fixed twice before it actually was.
"""

from __future__ import annotations

import threading
import time

import pytest

from ingestion import run

# Generous but bounded: these assert "didn't hang," not an exact latency.
# The original bug hung for 10+ seconds; a correct implementation resolves
# within a couple of poll cycles (POLL_INTERVAL_SECONDS = 0.2).
NO_HANG_THRESHOLD_SECONDS = 2.0


@pytest.fixture(autouse=True)
def _reset_shutdown_flag():
    run._shutdown.clear()
    yield
    run._shutdown.clear()


class _FakeStream:
    """Connects immediately (sets _running) and idles until stop()."""

    def __init__(self):
        self._running = False
        self._stopped = threading.Event()

    def run(self):
        self._running = True
        self._stopped.wait()

    def stop(self):
        self._stopped.set()


class _NeverConnectsStream:
    """Mimics alpaca-py's real hot-loop: run() spins, _running never flips."""

    def __init__(self):
        self._running = False
        self._stopped = threading.Event()

    def run(self):
        while not self._stopped.is_set():
            time.sleep(0.01)

    def stop(self):
        self._stopped.set()


def _after_connected(stream, action):
    def _target():
        while not stream._running:
            time.sleep(0.01)
        action()

    thread = threading.Thread(target=_target)
    thread.start()
    return thread


def test_connects_and_returns_true_when_stopped_externally():
    stream = _FakeStream()
    stopper = _after_connected(stream, stream.stop)

    result = run._run_with_watchdog(stream, connect_timeout=5)

    stopper.join()
    assert result is True


def test_never_connects_returns_false_after_watchdog_timeout():
    stream = _NeverConnectsStream()
    start = time.monotonic()

    result = run._run_with_watchdog(stream, connect_timeout=0.5)

    assert result is False
    assert time.monotonic() - start < NO_HANG_THRESHOLD_SECONDS + 0.5


def test_shutdown_flag_stops_a_connected_stream_promptly():
    stream = _FakeStream()
    signaler = _after_connected(stream, run._shutdown.set)

    start = time.monotonic()
    result = run._run_with_watchdog(stream, connect_timeout=5)
    elapsed = time.monotonic() - start

    signaler.join()
    assert result is True
    assert elapsed < NO_HANG_THRESHOLD_SECONDS


def test_shutdown_flag_stops_a_never_connecting_stream_promptly():
    stream = _NeverConnectsStream()

    def _signal_soon():
        time.sleep(0.1)
        run._shutdown.set()

    signaler = threading.Thread(target=_signal_soon)
    signaler.start()

    start = time.monotonic()
    result = run._run_with_watchdog(stream, connect_timeout=10)
    elapsed = time.monotonic() - start

    signaler.join()
    assert result is False
    assert elapsed < NO_HANG_THRESHOLD_SECONDS


def test_join_interruptible_respects_shutdown_flag():
    blocker = threading.Event()
    thread = threading.Thread(target=blocker.wait)
    thread.start()

    def _signal_soon():
        time.sleep(0.1)
        run._shutdown.set()

    threading.Thread(target=_signal_soon).start()

    start = time.monotonic()
    run._join_interruptible(thread)
    elapsed = time.monotonic() - start

    blocker.set()  # release the thread so it doesn't leak past this test
    thread.join()
    assert elapsed < NO_HANG_THRESHOLD_SECONDS


def test_join_interruptible_respects_its_own_timeout():
    blocker = threading.Event()
    thread = threading.Thread(target=blocker.wait)
    thread.start()

    start = time.monotonic()
    run._join_interruptible(thread, timeout=0.3)
    elapsed = time.monotonic() - start

    blocker.set()
    thread.join()
    assert 0.3 <= elapsed < NO_HANG_THRESHOLD_SECONDS


def test_sleep_interruptible_returns_early_on_shutdown():
    def _signal_soon():
        time.sleep(0.1)
        run._shutdown.set()

    threading.Thread(target=_signal_soon).start()

    start = time.monotonic()
    run._sleep_interruptible(5)
    elapsed = time.monotonic() - start

    assert elapsed < NO_HANG_THRESHOLD_SECONDS


def test_sleep_interruptible_waits_full_duration_without_shutdown():
    start = time.monotonic()
    run._sleep_interruptible(0.3)
    elapsed = time.monotonic() - start

    assert elapsed >= 0.3


def test_main_fails_fast_on_missing_env(monkeypatch):
    monkeypatch.setattr(run, "load_dotenv", lambda: None)
    monkeypatch.delenv("ALPACA_API_KEY", raising=False)
    monkeypatch.delenv("ALPACA_SECRET_KEY", raising=False)
    monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVERS", raising=False)

    with pytest.raises(SystemExit, match="ALPACA_API_KEY"):
        run.main()
