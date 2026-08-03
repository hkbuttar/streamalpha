"""Shared graceful-shutdown coordination for long-running consumer
processes (ingestion/run.py, streaming/consumer.py, storage/sink.py):
install a signal handler that sets a flag instead of relying on
KeyboardInterrupt.

Extracted here after being copy-pasted identically three times -- see
streaming/consumer.py and storage/sink.py's module docstrings for why a
custom signal.signal() handler is used instead of the more obvious
try/except KeyboardInterrupt: a blocking call into a C extension, even
with a timeout, can't be trusted to let a pending signal through
promptly, confirmed via lldb in two unrelated C extensions (alpaca-py's
websocket internals, confluent-kafka's librdkafka).

A class, not a shared module-level Event: each of the three processes
(and each test that constructs one) needs its own independent flag, not
mutable state shared across unrelated call sites.
"""

from __future__ import annotations

import logging
import signal
import threading

log = logging.getLogger(__name__)


class ShutdownHandler:
    """Wraps a threading.Event set by SIGINT/SIGTERM once install() is
    called. set()/clear()/is_set() mirror threading.Event's own API.
    """

    def __init__(self) -> None:
        self._event = threading.Event()

    def install(self) -> None:
        def _on_signal(signum: int, _frame: object) -> None:
            log.info("received signal %s, shutting down", signum)
            self._event.set()

        signal.signal(signal.SIGINT, _on_signal)
        signal.signal(signal.SIGTERM, _on_signal)

    def set(self) -> None:
        self._event.set()

    def clear(self) -> None:
        self._event.clear()

    def is_set(self) -> bool:
        return self._event.is_set()
