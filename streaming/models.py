"""Per-ticker online anomaly models: a volume-spike detector (river's
HalfSpaceTrees, its streaming analogue of an isolation forest) and a
volatility regime-change detector (streaming/changepoint.py's BOCPD).
Deliberately separate models: a single-tick volume spike and a genuine
volatility regime shift are different failure modes and shouldn't share
one detector (see README.md's Correctness Design section).

Threshold design for the volume detector. HalfSpaceTrees' raw anomaly
score isn't well calibrated as an absolute cutoff -- confirmed empirically
while building this: a synthetic 50x volume spike scored 0.994 against a
0.963 baseline, not a clean separation. river.anomaly.QuantileFilter looks
like the obvious fix (an adaptive quantile threshold on scores) but its
q parameter had zero effect across a 10x range (0.99 to 0.999) in testing
here, traced to its threshold coming from river.stats.Quantile, an online
P^2-style estimator that appears to have resolution problems at extreme
quantiles with only a few hundred samples -- not well understood, and
"had no effect across a 10x parameter range" is not something to build on
without understanding why. What's used instead is a threshold this module
owns directly and can verify: EWMean/EWVar (exponentially weighted, so it
adapts as a ticker's baseline volume drifts over the day) of recent scores,
flagging a score more than volume_score_k standard deviations above that
mean. Swept k in {3,4,5} and fading_factor in {0.02,0.05,0.1} against
synthetic data with several spikes spread across a stream; k=3.0 and
fading_factor=0.05 caught all of them with few false positives and are
the defaults.

Known limitation, found by testing rather than assumed: an isolated
single-window spike (one extreme window surrounded by hundreds of normal
ones) is detected unreliably -- swept 10 random seeds against a 100x
isolated spike with these exact settings and only 1-3/10 detected it.
Tried reducing HalfSpaceTrees' tree height and feeding a volume-ratio
feature instead of raw volume; neither meaningfully improved it, so this
is treated as a genuine characteristic of single-feature HalfSpaceTrees
scoring on an isolated outlier (its scores compress toward a narrow high
range for most points regardless of magnitude once the trees are built),
not a bug to keep chasing: a sustained multi-window elevated-volume period
showed the same weak recall in testing here (also 1/10 seeds), so this
isn't specific to single-tick spikes either. The BOCPD regime-change
detector, in contrast, performs reliably on both a single sharp shift and
a sustained one (see tests/streaming/test_changepoint.py). Richer volume
features (recent trend, multiple lookback ratios) would likely improve
recall; that's future work, not solved here -- shipped as-is with this
limitation stated plainly rather than papered over.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime

from river import anomaly, preprocessing, stats

from streaming.aggregation import WindowSummary
from streaming.changepoint import BOCPD

DEFAULT_VOLUME_SCORE_K = 3.0
DEFAULT_VOLUME_SCORE_FADING_FACTOR = 0.05
DEFAULT_VOLUME_SCORE_WARMUP = 30

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
    anomaly_score: float
    score_mean: float
    score_std: float


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
        volume_score_k: float = DEFAULT_VOLUME_SCORE_K,
        volume_score_fading_factor: float = DEFAULT_VOLUME_SCORE_FADING_FACTOR,
        volume_score_warmup: int = DEFAULT_VOLUME_SCORE_WARMUP,
        bocpd_hazard_lambda: float = DEFAULT_BOCPD_HAZARD_LAMBDA,
        bocpd_threshold: float = DEFAULT_BOCPD_THRESHOLD,
    ) -> None:
        self.symbol = symbol
        self._volume_pipeline = preprocessing.MinMaxScaler() | anomaly.HalfSpaceTrees(seed=42)
        self._score_mean = stats.EWMean(fading_factor=volume_score_fading_factor)
        self._score_var = stats.EWVar(fading_factor=volume_score_fading_factor)
        self._volume_score_k = volume_score_k
        self._volume_windows_seen = 0
        self._volume_score_warmup = volume_score_warmup

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
        x = {"volume": summary.volume}
        score = self._volume_pipeline.score_one(x)
        self._volume_pipeline.learn_one(x)
        self._volume_windows_seen += 1

        result = None
        mean = self._score_mean.get()
        variance = self._score_var.get()
        if self._volume_windows_seen > self._volume_score_warmup and mean is not None and variance:
            std = variance**0.5
            if std > 0 and score > mean + self._volume_score_k * std:
                result = VolumeAnomaly(
                    symbol=summary.symbol,
                    window_start=summary.window_start,
                    window_end=summary.window_end,
                    volume=summary.volume,
                    trade_count=summary.trade_count,
                    anomaly_score=score,
                    score_mean=mean,
                    score_std=std,
                )

        self._score_mean.update(score)
        self._score_var.update(score)
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
