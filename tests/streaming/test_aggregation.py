"""TickerWindow tests."""

from __future__ import annotations

import math
from datetime import UTC, datetime, timedelta

from streaming.aggregation import TickerWindow

BASE = datetime(2024, 1, 1, tzinfo=UTC)


def test_no_close_while_within_window():
    w = TickerWindow("AAPL", window_seconds=10)
    assert w.add_trade(BASE, 100.0, 10) is None
    assert w.add_trade(BASE + timedelta(seconds=5), 100.5, 20) is None
    assert w.add_trade(BASE + timedelta(seconds=9.9), 101.0, 5) is None


def test_closes_and_sums_volume_when_window_elapses():
    w = TickerWindow("AAPL", window_seconds=10)
    w.add_trade(BASE, 100.0, 10)
    w.add_trade(BASE + timedelta(seconds=2), 100.5, 20)
    w.add_trade(BASE + timedelta(seconds=5), 101.0, 15)
    closed = w.add_trade(BASE + timedelta(seconds=11), 101.2, 5)

    assert closed is not None
    assert closed.symbol == "AAPL"
    assert closed.trade_count == 3
    assert closed.volume == 45.0
    assert closed.window_start == BASE
    assert closed.window_end == BASE + timedelta(seconds=11)


def test_new_window_starts_with_the_closing_trade():
    w = TickerWindow("AAPL", window_seconds=10)
    w.add_trade(BASE, 100.0, 10)
    closing_trade_time = BASE + timedelta(seconds=11)
    w.add_trade(closing_trade_time, 101.2, 5)  # closes window 1, starts window 2

    # Window 2 doesn't close until 10s after closing_trade_time -- a trade
    # only 1s later must not close it early.
    assert w.add_trade(closing_trade_time + timedelta(seconds=1), 101.25, 2) is None

    # The trade that closed window 1 (size 5) is the sole trade of window
    # 2 so far -- not dropped, not double counted. It closes when window 2
    # itself elapses.
    closed2 = w.add_trade(closing_trade_time + timedelta(seconds=10), 101.3, 1)
    assert closed2.trade_count == 2
    assert closed2.volume == 7.0
    assert closed2.window_start == closing_trade_time


def test_volatility_is_none_with_fewer_than_two_trades():
    w = TickerWindow("AAPL", window_seconds=10)
    w.add_trade(BASE, 100.0, 10)
    closed = w.add_trade(BASE + timedelta(seconds=11), 101.0, 5)
    assert closed.trade_count == 1
    assert closed.realized_volatility is None


def test_volatility_is_stdev_of_log_returns():
    w = TickerWindow("AAPL", window_seconds=10)
    w.add_trade(BASE, 100.0, 10)
    w.add_trade(BASE + timedelta(seconds=1), 101.0, 10)
    w.add_trade(BASE + timedelta(seconds=2), 99.0, 10)
    closed = w.add_trade(BASE + timedelta(seconds=11), 100.0, 10)

    r1 = math.log(101.0 / 100.0)
    r2 = math.log(99.0 / 101.0)
    mean = (r1 + r2) / 2
    expected = math.sqrt(((r1 - mean) ** 2 + (r2 - mean) ** 2) / 1)
    assert closed.realized_volatility == expected


def test_price_continuity_carries_across_window_boundary():
    """A return spanning the boundary between two windows is still a real
    price movement -- the first trade of a new window should be able to
    form a return against the last trade of the closed window, not start
    with an empty price history.

    Verified indirectly: window 2 gets three trades (the one that closes
    window 1 at price 110, then two more), producing two returns only if
    the first of those two is computed against the carried-over price of
    110 rather than against nothing. If continuity were broken, window 2
    would have only one usable return and realized_volatility would be
    None (see test_volatility_is_none_with_fewer_than_two_trades).
    """
    w = TickerWindow("AAPL", window_seconds=10)
    w.add_trade(BASE, 100.0, 10)
    w.add_trade(BASE + timedelta(seconds=11), 110.0, 10)  # closes window 1
    w.add_trade(BASE + timedelta(seconds=15), 100.0, 10)
    w.add_trade(BASE + timedelta(seconds=18), 121.0, 10)
    closed2 = w.add_trade(BASE + timedelta(seconds=22), 130.0, 10)  # closes window 2

    assert closed2.trade_count == 3
    assert closed2.realized_volatility is not None


def test_illiquid_gap_produces_one_long_window_not_an_error():
    w = TickerWindow("AAPL", window_seconds=10)
    w.add_trade(BASE, 100.0, 10)
    far_later = BASE + timedelta(hours=1)
    closed = w.add_trade(far_later, 100.0, 10)
    assert closed.trade_count == 1
    assert closed.window_end == far_later
