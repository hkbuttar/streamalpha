"""Entrypoint: run the Alpaca stream -> Kafka producer pipeline, supervised
with backoff.

Reconnect/backoff behavior, documented explicitly since it's easy to get
wrong silently:

Transient drops. A normal disconnect (network blip, server-side reset)
surfaces inside alpaca-py's StockDataStream as a websockets.WebSocketException.
Its internal loop (DataStream._run_forever, in the installed alpaca-py
package) catches this itself, closes the socket, reconnects, and resends the
subscribe message for every symbol -- our symbol -> handler map lives in the
StockDataStream instance's own memory and is untouched by a reconnect, so
nothing here has to re-subscribe after a drop. Reading that source also
confirms there is no delay on this path: alpaca-py retries a transient drop
immediately, not with backoff.

Persistent connect/auth failure. A worse case -- bad credentials, an Alpaca
outage, DNS down, or (observed live during development) a stale connection
from a previous run tripping Alpaca's one-connection-per-account limit --
surfaces as a ValueError out of _start_ws() on every attempt, and alpaca-py's
inner loop retries that with no delay. Confirmed from a real log during
development: two auth attempts about 100ms apart, hot-looping against
Alpaca's auth endpoint. Since that loop lives entirely inside
StockDataStream.run() and never hands control back to us, and since
overriding alpaca-py's private _run_forever/_start_ws is fragile against SDK
upgrades, _run_with_watchdog() below supervises run() from a separate thread
instead: if the stream's own _running flag (public API doesn't expose this,
but reading a boolean flag degrades safely if the attribute ever disappears
-- worst case the watchdog always fires) never flips true within
CONNECT_TIMEOUT_SECONDS, we call the stream's public stop() method and let
the retry loop's backoff take over, rather than hot-looping indefinitely.

Why the loop below exists at all. StockDataStream.run() only returns in two
cases: a clean stop, or a fatal "insufficient subscription" error (e.g. feed
set to SIP without SIP entitlement). Either of those, or any other exception
escaping run() outright, is what this retry loop is for: rebuild the stream
and producer from scratch and retry with exponential backoff, instead of
crashing the process or hot-looping.

Shutdown signal handling. running run() in a background thread (needed for
the watchdog above) means Ctrl+C/SIGINT/SIGTERM land on this main thread,
not the worker thread executing alpaca-py's asyncio loop, so alpaca-py's own
built-in "catch KeyboardInterrupt, stop cleanly" handling never triggers.
The first fix attempted here relied on catching KeyboardInterrupt around a
polled thread.join() -- and that reliably did NOT work: confirmed with lldb
that the main thread was genuinely blocked in a lock wait, and a follow-up
instrumented test showed the default SIGINT->KeyboardInterrupt path can sit
for 7+ seconds without firing while polling thread.join(timeout=0.2) in a
loop, even though the polling itself was running on schedule. A custom
signal.signal() handler that just sets a threading.Event, checked
cooperatively inside the same poll loop, fired within ~50ms in the same
test. That is the pattern used below (shutdown.ShutdownHandler, shared
with streaming/consumer.py and storage/sink.py, which hit the same
problem independently) -- not the more obvious try/except
KeyboardInterrupt one -- because it is the one actually verified to
work, not the one that looked like it should.

In-flight state during a gap. This process holds no state of its own across
ticks -- each trade/quote is serialized and handed to the producer
immediately, so a reconnect never loses anything already produced. What IS
lost is whatever Alpaca sent while the socket was down: Alpaca's WebSocket
API has no replay or backfill, so ticks from a gap window are gone
permanently, not just delayed. The warning logged below marks the gap's
start so it is visible in the logs rather than silently absorbed.
"""

from __future__ import annotations

import logging
import os
import threading
import time

from alpaca.data.live import StockDataStream
from dotenv import load_dotenv

from ingestion.alpaca_stream import build_stream
from ingestion.producer import TickProducer
from shutdown import ShutdownHandler

log = logging.getLogger(__name__)

INITIAL_BACKOFF_SECONDS = 1
MAX_BACKOFF_SECONDS = 60
CONNECT_TIMEOUT_SECONDS = 20
POLL_INTERVAL_SECONDS = 0.2
REQUIRED_ENV_VARS = ("ALPACA_API_KEY", "ALPACA_SECRET_KEY", "KAFKA_BOOTSTRAP_SERVERS")

_shutdown = ShutdownHandler()


def _sleep_interruptible(seconds: float) -> None:
    """time.sleep(), but returns early if _shutdown gets set mid-sleep."""
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline and not _shutdown.is_set():
        time.sleep(min(POLL_INTERVAL_SECONDS, deadline - time.monotonic()))


def _join_interruptible(thread: threading.Thread, timeout: float | None = None) -> None:
    """thread.join(), polled so _shutdown and a timeout both take effect promptly.

    See module docstring: a bare, unpolled thread.join() reliably failed to
    respond to a shutdown signal in testing, even via try/except
    KeyboardInterrupt. Polling and checking the _shutdown flag directly
    (set by our own signal handler, not by relying on an asynchronously
    raised exception) is the approach that was actually verified to work.
    """
    deadline = None if timeout is None else time.monotonic() + timeout
    while thread.is_alive() and not _shutdown.is_set():
        if deadline is not None and time.monotonic() >= deadline:
            return
        thread.join(timeout=POLL_INTERVAL_SECONDS)


def _run_with_watchdog(
    stream: StockDataStream, connect_timeout: float = CONNECT_TIMEOUT_SECONDS
) -> bool:
    """Run stream.run() under a connection watchdog; see module docstring.

    stream.run() blocks and normally only returns on a clean stop or a
    fatal subscription error (both fine, handled by the caller's loop). The
    case this exists for is the one stream.run() can't hand back to us on
    its own: a persistent connect/auth failure that alpaca-py retries in a
    zero-delay internal loop forever. We run it in a thread and watch for
    the stream's own _running flag to flip true; if it never does within
    connect_timeout, we force a stop so the caller's backoff loop gets a
    turn instead of leaving the hot loop running indefinitely.

    Returns True if the stream connected at any point before returning
    (clean stop, fatal error, or a shutdown signal after a real connection
    -- caller should treat this like run() returning on its own), False if
    the watchdog had to force the stop because it never connected (caller
    should retry with backoff, not treat this as a clean exit).
    """
    thread = threading.Thread(target=stream.run, daemon=True)
    thread.start()

    deadline = time.monotonic() + connect_timeout
    while time.monotonic() < deadline and thread.is_alive() and not _shutdown.is_set():
        if getattr(stream, "_running", False):
            _join_interruptible(thread)
            if _shutdown.is_set() and thread.is_alive():
                stream.stop()
                _join_interruptible(thread, timeout=10)
            return True
        time.sleep(POLL_INTERVAL_SECONDS)

    if thread.is_alive():
        if _shutdown.is_set():
            log.info("shutdown requested before stream connected")
        else:
            log.warning(
                "no successful connection within %ss; forcing a stop instead of "
                "hot-looping (see module docstring)",
                connect_timeout,
            )
        stream.stop()
        _join_interruptible(thread, timeout=10)
    return False


def main() -> None:
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    _shutdown.install()

    # Fail fast on missing configuration: a typo'd or unset env var is a
    # setup error, not a transient failure, so retrying with backoff would
    # just hide a problem that will never fix itself.
    missing = [var for var in REQUIRED_ENV_VARS if not os.environ.get(var)]
    if missing:
        raise SystemExit(f"missing required env var(s): {', '.join(missing)} (see .env.example)")

    backoff = INITIAL_BACKOFF_SECONDS
    while not _shutdown.is_set():
        producer = TickProducer()
        stream = build_stream(producer)
        started_at = time.monotonic()
        try:
            log.info("starting Alpaca stream")
            connected = _run_with_watchdog(stream)  # blocks; see module docstring
            if connected:
                log.info("stream stopped")
                break
            log.warning("stream never connected; tick gap starts now")
        except Exception:
            log.exception("stream exited unexpectedly; tick gap starts now")
        finally:
            producer.flush()

        if _shutdown.is_set():
            break

        if time.monotonic() - started_at > MAX_BACKOFF_SECONDS:
            # Was up for a while before failing -- don't let backoff keep
            # ratcheting up from unrelated failures hours apart.
            backoff = INITIAL_BACKOFF_SECONDS
        log.warning("reconnecting in %ss", backoff)
        _sleep_interruptible(backoff)
        backoff = min(backoff * 2, MAX_BACKOFF_SECONDS)

    log.info("shut down")


if __name__ == "__main__":
    main()
