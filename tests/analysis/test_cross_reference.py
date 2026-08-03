"""cross_reference.py tests. No live Postgres/yfinance: load_anomalies is
tested against a faked psycopg connection, and compute_factor_move_z_scores
is tested against a synthetic price panel (still calling the *real*
alpha-signal-lab factor modules -- that integration is exactly what this
analysis depends on, so faking it away would test nothing meaningful).
"""

from __future__ import annotations

from datetime import UTC, datetime

import numpy as np
import pandas as pd

from analysis.cross_reference import (
    compute_factor_move_z_scores,
    cross_reference,
    load_anomalies,
    sharp_moves_only,
)


def test_sharp_moves_only_filters_by_absolute_threshold():
    factor_z = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
            "ticker": ["AAPL", "AAPL", "AAPL"],
            "factor": ["momentum", "momentum", "momentum"],
            "z": [1.0, -2.5, 2.5],
        }
    )
    result = sharp_moves_only(factor_z, threshold=2.0)
    assert sorted(result["z"].tolist()) == [-2.5, 2.5]


def test_cross_reference_matches_within_window():
    anomalies = pd.DataFrame(
        {
            "ticker": ["AAPL"],
            "date": [pd.Timestamp("2024-01-10")],
            "anomaly_type": ["volume_anomaly"],
        }
    )
    sharp_moves = pd.DataFrame(
        {
            "date": [pd.Timestamp("2024-01-11")],  # 1 day after, within window
            "ticker": ["AAPL"],
            "factor": ["momentum"],
            "z": [3.0],
        }
    )
    result = cross_reference(anomalies, sharp_moves, window_days=1)
    assert len(result) == 1
    assert bool(result.iloc[0]["coincides_with_sharp_factor_move"])
    assert result.iloc[0]["matches"] == [("momentum", pd.Timestamp("2024-01-11"), 3.0)]


def test_cross_reference_no_match_outside_window():
    anomalies = pd.DataFrame(
        {
            "ticker": ["AAPL"],
            "date": [pd.Timestamp("2024-01-10")],
            "anomaly_type": ["volume_anomaly"],
        }
    )
    sharp_moves = pd.DataFrame(
        {
            "date": [pd.Timestamp("2024-01-20")],  # far outside window
            "ticker": ["AAPL"],
            "factor": ["momentum"],
            "z": [3.0],
        }
    )
    result = cross_reference(anomalies, sharp_moves, window_days=1)
    assert not bool(result.iloc[0]["coincides_with_sharp_factor_move"])
    assert result.iloc[0]["matches"] == []


def test_cross_reference_ignores_a_different_tickers_sharp_move():
    anomalies = pd.DataFrame(
        {
            "ticker": ["AAPL"],
            "date": [pd.Timestamp("2024-01-10")],
            "anomaly_type": ["volume_anomaly"],
        }
    )
    sharp_moves = pd.DataFrame(
        {
            "date": [pd.Timestamp("2024-01-10")],
            "ticker": ["MSFT"],  # different ticker, same day
            "factor": ["momentum"],
            "z": [3.0],
        }
    )
    result = cross_reference(anomalies, sharp_moves, window_days=1)
    assert not bool(result.iloc[0]["coincides_with_sharp_factor_move"])


def test_cross_reference_empty_sharp_moves_produces_no_matches():
    anomalies = pd.DataFrame(
        {
            "ticker": ["AAPL"],
            "date": [pd.Timestamp("2024-01-10")],
            "anomaly_type": ["regime_change"],
        }
    )
    empty = pd.DataFrame(columns=["date", "ticker", "factor", "z"])
    result = cross_reference(anomalies, empty, window_days=1)
    assert not bool(result.iloc[0]["coincides_with_sharp_factor_move"])


class _FakeCursor:
    def __init__(self, rows, columns):
        self._rows = rows
        self._columns = columns
        self.description = [_Col(c) for c in columns]

    def execute(self, sql):
        pass

    def fetchall(self):
        return self._rows

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _Col:
    def __init__(self, name):
        self.name = name


class _FakeConnection:
    def __init__(self, rows, columns):
        self._rows = rows
        self._columns = columns

    def cursor(self):
        return _FakeCursor(self._rows, self._columns)


def test_load_anomalies_converts_tz_aware_window_start_to_naive_date():
    rows = [
        ("AAPL", datetime(2026, 8, 3, 18, 31, 35, 451315, tzinfo=UTC), "regime_change"),
    ]
    conn = _FakeConnection(rows, ["ticker", "window_start", "anomaly_type"])

    df = load_anomalies(conn)

    assert len(df) == 1
    assert df.iloc[0]["date"] == pd.Timestamp("2026-08-03")
    assert df.iloc[0]["date"].tzinfo is None


def test_load_anomalies_empty_table_returns_empty_frame_without_error():
    conn = _FakeConnection([], ["ticker", "window_start", "anomaly_type"])
    df = load_anomalies(conn)
    assert df.empty


def _synthetic_price_panel(tickers, n_days=300, jump_ticker=None, jump_day=250, seed=0):
    rng = np.random.default_rng(seed)
    dates = pd.bdate_range("2023-01-02", periods=n_days)
    rows = []
    for ticker in tickers:
        price = 100.0
        for i, d in enumerate(dates):
            daily_return = rng.normal(0, 0.01)
            if ticker == jump_ticker and i == jump_day:
                daily_return = 0.25  # a real, large one-day jump
            price *= 1 + daily_return
            rows.append(
                {
                    "date": d,
                    "ticker": ticker,
                    "open": price,
                    "high": price * 1.001,
                    "low": price * 0.999,
                    "close": price,
                    "volume": 1_000_000,
                }
            )
    return pd.DataFrame(rows)


def test_compute_factor_move_z_scores_flags_an_injected_jump():
    prices = _synthetic_price_panel(["JUMPY", "STABLE"], jump_ticker="JUMPY", jump_day=250)

    factor_z = compute_factor_move_z_scores(prices)

    assert not factor_z.empty
    jumpy_z = factor_z[factor_z["ticker"] == "JUMPY"]
    assert (jumpy_z["z"].abs() >= 2.0).any()


def test_compute_factor_move_z_scores_stable_series_has_few_sharp_moves():
    prices = _synthetic_price_panel(["STABLE"], jump_ticker=None)

    factor_z = compute_factor_move_z_scores(prices)
    sharp = sharp_moves_only(factor_z, threshold=3.0)

    # A handful of >=3-sigma moves can occur in pure noise across ~250 rows
    # x 3 factors; this isn't a hard statistical guarantee, just a sanity
    # bound against something being systematically over-triggering.
    assert len(sharp) < 10


def test_compute_factor_move_z_scores_requires_enough_history_for_rolling_window():
    # MIN_PERIODS=20 applies twice, stacked: once inside e.g.
    # mean_reversion's own factor computation, again in this module's own
    # rolling z-score of that factor's day-over-day change. 15 days isn't
    # enough for even the first MIN_PERIODS=20 to be satisfied once
    # (confirmed empirically -- ROLLING_WINDOW_DAYS-5=55 days turned out to
    # be *plenty*, not "not enough" as first assumed here).
    prices = _synthetic_price_panel(["SHORT"], n_days=15)
    factor_z = compute_factor_move_z_scores(prices)
    assert factor_z.empty
