"""Per-ticker tumbling window aggregation on trade ticks: turns a raw tick
stream into one (volume, realized_volatility) observation per window
close, which is what the anomaly detectors in streaming/models.py actually
consume -- feeding raw per-trade size straight into an anomaly detector
would flag "one big trade," not the volume-spike pattern the plan calls
for.

Windows are event-time based (bounded by each trade's own timestamp, not
wall-clock arrival time), so aggregation behaves identically whether ticks
are processed live or replayed later from Kafka. Quotes don't contribute:
they're bid/ask book state, not a traded price or size, and folding them
into "trade volume" or "realized volatility from trade prices" would
conflate two things Alpaca's own message types already keep distinct (see
ingestion/alpaca_stream.py's trade vs quote schemas).
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class WindowSummary:
    symbol: str
    window_start: datetime
    window_end: datetime
    trade_count: int
    volume: float
    # None when the window had fewer than 2 trades -- not enough for even
    # one return, so there's nothing to report a volatility figure for.
    realized_volatility: float | None


def _stdev(values: list[float]) -> float:
    n = len(values)
    mean = sum(values) / n
    variance = sum((v - mean) ** 2 for v in values) / (n - 1)
    return math.sqrt(variance)


class TickerWindow:
    """Accumulates trade ticks for one ticker into a single tumbling
    window, closing it (and starting the next) whenever a trade arrives
    whose timestamp is window_seconds or more past the window's start.

    A window can only close as a side effect of a new trade arriving, so a
    closed window always has at least one trade. An illiquid ticker with a
    long gap between trades will produce one correspondingly long window
    rather than a series of empty ones -- a deliberate simplification, not
    a bug: this is event-driven, not a periodic timer.
    """

    def __init__(self, symbol: str, window_seconds: float) -> None:
        self.symbol = symbol
        self._window_seconds = window_seconds
        self._window_start: datetime | None = None
        self._volume = 0.0
        self._trade_count = 0
        self._log_returns: list[float] = []
        # Deliberately NOT reset when a window closes: a return spanning
        # the boundary between two windows is still a real price movement,
        # and dropping it would understate volatility for the first trade
        # of every new window.
        self._last_price: float | None = None

    def add_trade(self, timestamp: datetime, price: float, size: float) -> WindowSummary | None:
        """Feed one trade tick. Returns a WindowSummary if this trade
        closed the prior window (a new window then starts with this trade
        as its first observation), else None.
        """
        closed = None
        if self._window_start is None:
            self._window_start = timestamp
        elif (timestamp - self._window_start).total_seconds() >= self._window_seconds:
            closed = self._summary(window_end=timestamp)
            self._window_start = timestamp
            self._volume = 0.0
            self._trade_count = 0
            self._log_returns = []

        if self._last_price is not None and price > 0 and self._last_price > 0:
            self._log_returns.append(math.log(price / self._last_price))
        self._last_price = price
        self._volume += size
        self._trade_count += 1

        return closed

    def _summary(self, window_end: datetime) -> WindowSummary:
        volatility = _stdev(self._log_returns) if len(self._log_returns) >= 2 else None
        return WindowSummary(
            symbol=self.symbol,
            window_start=self._window_start,
            window_end=window_end,
            trade_count=self._trade_count,
            volume=self._volume,
            realized_volatility=volatility,
        )
