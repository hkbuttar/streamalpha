"""TickerModels tests.

Volume anomaly detection is now a rolling z-score on volume itself, not
HalfSpaceTrees (see streaming/models.py's module docstring for why that
changed and what was actually tried first). Unlike the old design, this
one is swept across many seeds and asserted to catch *every* spike, not
just "can work with a known-good seed" -- confirmed empirically
(50/50 seeds on an isolated 100x spike, a 10x spike, and a sustained
elevated period) before writing these tests, not assumed.
"""

from __future__ import annotations

import random
from datetime import UTC, datetime, timedelta

from streaming.aggregation import WindowSummary
from streaming.models import RegimeChange, TickerModels, VolumeAnomaly

BASE = datetime(2024, 1, 1, tzinfo=UTC)


def _summary(i, volume, volatility, symbol="AAPL"):
    ts = BASE + timedelta(seconds=10 * i)
    return WindowSummary(
        symbol=symbol,
        window_start=ts,
        window_end=ts + timedelta(seconds=10),
        trade_count=5,
        volume=volume,
        realized_volatility=volatility,
    )


def test_isolated_spike_is_reliably_detected_across_many_seeds():
    """The old HalfSpaceTrees-based detector caught this on only 1-3/10
    seeds; this rolling-z-score design caught it on 50/50 in the sweep
    that picked DEFAULT_VOLUME_Z_THRESHOLD (see models.py's module
    docstring). 20 seeds here, not 50 -- enough to be a real regression
    guard without slowing the suite down for marginal extra confidence.
    """
    misses = []
    for seed in range(20):
        random.seed(seed)
        models = TickerModels("AAPL")
        volume_flags = []
        for i in range(700):
            volume = 100_000.0 if i == 600 else random.gauss(1000, 100)
            summary = _summary(i, volume, volatility=0.01)
            for event in models.process_window(summary):
                if isinstance(event, VolumeAnomaly):
                    volume_flags.append(i)
        if not any(abs(f - 600) <= 3 for f in volume_flags):
            misses.append(seed)

    assert misses == []


def test_sustained_elevated_volume_is_reliably_detected():
    """A multi-window elevated-volume period (not just a single isolated
    tick) -- the old detector also missed this reliably (1/10 seeds);
    this one catches it every time in the same sweep.
    """
    misses = []
    for seed in range(20):
        random.seed(seed)
        models = TickerModels("AAPL")
        volume_flags = []
        for i in range(800):
            if 600 <= i < 630:
                volume = random.gauss(8000, 800)
            else:
                volume = random.gauss(1000, 100)
            summary = _summary(i, volume, volatility=0.01)
            for event in models.process_window(summary):
                if isinstance(event, VolumeAnomaly):
                    volume_flags.append(i)
        if not any(597 <= f <= 633 for f in volume_flags):
            misses.append(seed)

    assert misses == []


def test_quiet_stream_does_not_flood_with_anomalies():
    random.seed(1)
    models = TickerModels("AAPL")
    volume_flags = 0

    for i in range(500):
        summary = _summary(i, volume=random.gauss(1000, 100), volatility=0.01)
        for event in models.process_window(summary):
            if isinstance(event, VolumeAnomaly):
                volume_flags += 1

    # The z-score threshold was tuned for a mean of 0.04 false positives
    # per 500 quiet windows across 50 seeds -- a handful, not zero, is
    # still consistent with that; flooding is not.
    assert volume_flags < 5


def test_regime_change_detected_on_a_clear_volatility_shift():
    random.seed(0)
    models = TickerModels("AAPL")
    regime_flags = []

    for i in range(400):
        vol_scale = 0.05 if i < 200 else 0.3
        volatility = abs(random.gauss(vol_scale, vol_scale * 0.1))
        summary = _summary(i, volume=random.gauss(1000, 100), volatility=volatility)
        for event in models.process_window(summary):
            if isinstance(event, RegimeChange):
                regime_flags.append(i)

    assert any(195 <= f <= 210 for f in regime_flags)


def test_no_regime_event_on_a_stable_volatility_stream():
    random.seed(2)
    models = TickerModels("AAPL")
    regime_flags = []

    for i in range(300):
        volatility = abs(random.gauss(0.05, 0.005))
        summary = _summary(i, volume=random.gauss(1000, 100), volatility=volatility)
        for event in models.process_window(summary):
            if isinstance(event, RegimeChange):
                regime_flags.append(i)

    assert regime_flags == []


def test_window_with_no_volatility_is_skipped_without_crashing():
    models = TickerModels("AAPL")
    summary = _summary(0, volume=1000.0, volatility=None)
    events = models.process_window(summary)
    assert all(not isinstance(e, RegimeChange) for e in events)


def test_last_updated_tracks_window_end_not_wall_clock():
    models = TickerModels("AAPL")
    assert models.last_updated is None

    summary = _summary(0, volume=1000.0, volatility=0.01)
    models.process_window(summary)
    assert models.last_updated == summary.window_end

    later_summary = _summary(5, volume=1000.0, volatility=0.01)
    models.process_window(later_summary)
    assert models.last_updated == later_summary.window_end


def test_events_carry_the_correct_symbol():
    models = TickerModels("MSFT")
    random.seed(0)
    events_seen = []
    for i in range(700):
        volume = 100_000.0 if i == 600 else random.gauss(1000, 100)
        summary = _summary(i, volume, volatility=0.01, symbol="MSFT")
        events_seen.extend(models.process_window(summary))

    assert events_seen
    assert all(e.symbol == "MSFT" for e in events_seen)
