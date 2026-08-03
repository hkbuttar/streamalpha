"""Per-ticker online anomaly models: a volume-spike detector (a rolling
z-score on volume itself) and a volatility regime-change detector
(streaming/changepoint.py's BOCPD). Deliberately separate models: a
single-tick volume spike and a genuine volatility regime shift are
different failure modes and shouldn't share one detector (see README.md's
Correctness Design section).

Volume detector history, replacing an earlier HalfSpaceTrees-based
design (river's streaming analogue of an isolation forest). That version
had a known, honestly-documented limitation: an isolated single-window
spike was detected only 1-3/10 times in testing, and neither reducing
tree height nor feeding a volume-ratio feature instead of raw volume
meaningfully improved it. Feeding it genuinely richer features (multiple
lookback ratios plus a trend feature, exactly what was proposed as
future work) was tried properly here -- and made recall *worse*, not
better (0/20 seeds on the same isolated-spike benchmark that the
single-feature baseline caught 1/20 seeds). Traced this to
HalfSpaceTrees itself: feeding it a z-score-like feature (volume's
deviation from its own recent rolling mean/std, in standard-deviation
units) got recall up to 19/20 -- but only by lowering its own score
threshold so far that quiet-stream false positives jumped from ~1.5 to
~12.5 per 500 windows, unusable. That result pointed at the actual fix:
HalfSpaceTrees' anomaly score was never the useful signal here, since a
z-score-shaped feature fed *through* it and re-thresholded needed such
an aggressive cutoff to recover recall that it was effectively just a
noisy proxy for the z-score itself. Skipping the isolation-forest layer
entirely and thresholding a rolling z-score on volume directly got
50/50 recall across 50 seeds on an isolated 100x spike, a 10x spike, and
a sustained multi-window elevated period, with a mean of 0.04 false
positives per 500 quiet windows -- and it held up under a slowly
drifting baseline too (rolling window naturally tracks drift, no flood
of false positives during the transition). `river` is no longer a
dependency of this project at all after this change (it was only ever
used here).

DEFAULT_VOLUME_Z_THRESHOLD=4.5 and DEFAULT_VOLUME_LOOKBACK=30 came from
that sweep: swept z in {3.0..6.0} and lookback in {10,20,30,50} against
the three recall benchmarks above plus quiet-stream and drifting-baseline
false-positive rates; 4.5/30 was the smallest threshold with zero
observed false positives across 20 (then 50, for confidence) seeds while
still catching every real spike tried, including one an order of
magnitude smaller (10x) than the original 100x benchmark.

Model-state pickle compatibility note: this replaces `TickerModels`'
internal volume-detector attributes entirely (no `_volume_pipeline`,
`_score_mean`, `_score_var` anymore), so a `model_state.pkl` saved by
the previous version cannot be loaded by this one without an
AttributeError the first time a ticker's window is processed --
unpickling itself succeeds (it just restores whatever attributes the old
object had), so this isn't caught by model_state.py's existing
load-failure handling. Delete any pre-existing `model_state.pkl` before
running this version; it is local, gitignored, easily-regenerated warm-
restart state, not something worth building migration logic for.
"""

from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass
from datetime import datetime

from streaming.aggregation import WindowSummary
from streaming.changepoint import BOCPD

DEFAULT_VOLUME_Z_THRESHOLD = 4.5
DEFAULT_VOLUME_LOOKBACK = 30
DEFAULT_VOLUME_WARMUP = 30

DEFAULT_BOCPD_HAZARD_LAMBDA = 100.0

# Floor to avoid log(0) when a window's realized_volatility is exactly 0
# (every trade in the window at the same price).
_VOLATILITY_EPSILON = 1e-8
DEFAULT_BOCPD_THRESHOLD = 0.5


@dataclass(frozen=True)
class VolumeAnomaly:
    symbol: str
    window_start: datetime
    window_end: datetime
    volume: float
    trade_count: int
    anomaly_score: float  # z-score: (volume - score_mean) / score_std
    score_mean: float  # rolling mean of the last DEFAULT_VOLUME_LOOKBACK volumes
    score_std: float  # rolling std of the same window


@dataclass(frozen=True)
class RegimeChange:
    symbol: str
    window_start: datetime
    window_end: datetime
    realized_volatility: float
    changepoint_probability: float


class TickerModels:
    """Owns one ticker's volume-anomaly and regime-change model state.
    Feed it closed windows; get back zero or more anomaly events.
    """

    def __init__(
        self,
        symbol: str,
        volume_z_threshold: float = DEFAULT_VOLUME_Z_THRESHOLD,
        volume_lookback: int = DEFAULT_VOLUME_LOOKBACK,
        volume_warmup: int = DEFAULT_VOLUME_WARMUP,
        bocpd_hazard_lambda: float = DEFAULT_BOCPD_HAZARD_LAMBDA,
        bocpd_threshold: float = DEFAULT_BOCPD_THRESHOLD,
    ) -> None:
        self.symbol = symbol
        self._volume_history: deque[float] = deque(maxlen=volume_lookback)
        self._volume_z_threshold = volume_z_threshold
        self._volume_windows_seen = 0
        self._volume_warmup = volume_warmup

        self._bocpd = BOCPD(
            hazard_lambda=bocpd_hazard_lambda,
            changepoint_probability_threshold=bocpd_threshold,
        )

        # Tick-time (window_end), not wall-clock -- same convention as
        # aggregation.py's windowing -- so this reflects "as of what point
        # in the data was this ticker's model last fed a window," which is
        # what backend/main.py's /status endpoint reports as per-ticker
        # model freshness: a ticker whose value stops advancing is a real
        # signal (it stopped trading, or its pipeline path broke), not
        # just bookkeeping.
        self.last_updated: datetime | None = None

    def process_window(self, summary: WindowSummary) -> list[VolumeAnomaly | RegimeChange]:
        events: list[VolumeAnomaly | RegimeChange] = []

        volume_anomaly = self._check_volume(summary)
        if volume_anomaly is not None:
            events.append(volume_anomaly)

        if summary.realized_volatility is not None:
            regime_change = self._check_regime(summary)
            if regime_change is not None:
                events.append(regime_change)

        self.last_updated = summary.window_end
        return events

    def _check_volume(self, summary: WindowSummary) -> VolumeAnomaly | None:
        self._volume_windows_seen += 1

        result = None
        if self._volume_windows_seen > self._volume_warmup and len(self._volume_history) >= 5:
            n = len(self._volume_history)
            mean = sum(self._volume_history) / n
            variance = sum((v - mean) ** 2 for v in self._volume_history) / n
            std = variance**0.5
            if std > 0:
                z_score = (summary.volume - mean) / std
                if z_score > self._volume_z_threshold:
                    result = VolumeAnomaly(
                        symbol=summary.symbol,
                        window_start=summary.window_start,
                        window_end=summary.window_end,
                        volume=summary.volume,
                        trade_count=summary.trade_count,
                        anomaly_score=z_score,
                        score_mean=mean,
                        score_std=std,
                    )

        self._volume_history.append(summary.volume)
        return result

    def _check_regime(self, summary: WindowSummary) -> RegimeChange | None:
        assert summary.realized_volatility is not None
        # BOCPD's prior is scaled for data around magnitude ~1; raw
        # realized volatility over a short window is typically tiny
        # (0.01-0.3ish), which starved the model of sensitivity -- confirmed
        # empirically: a 6x mean/std shift in raw volatility went almost
        # undetected (peak changepoint_probability ~0.03) until switched to
        # log-volatility, which detected the same shift cleanly (~0.96).
        # log is also the statistically conventional transform here anyway:
        # volatility is positive and multiplicative, not a natural fit for
        # a Normal model on its own.
        log_volatility = math.log(summary.realized_volatility + _VOLATILITY_EPSILON)
        is_changepoint = self._bocpd.update(log_volatility)
        if not is_changepoint:
            return None
        return RegimeChange(
            symbol=summary.symbol,
            window_start=summary.window_start,
            window_end=summary.window_end,
            realized_volatility=summary.realized_volatility,
            changepoint_probability=self._bocpd.changepoint_probability,
        )
