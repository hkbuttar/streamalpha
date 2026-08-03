"""Bayesian online changepoint detection (Adams & MacKay, 2007), for
flagging volatility regime shifts -- a different failure mode from a
single-tick volume spike, worth detecting separately (see
streaming/models.py for how the two are combined).

river has no changepoint detector implementing this specific algorithm
(its drift module has ADWIN/KSWIN/PageHinkley, which are a different
family of techniques), so this is a from-scratch implementation. Kept
dependency-free (stdlib math only, no numpy/scipy) since the model is
just Normal-Gamma conjugate updates and a Student-t predictive density,
both of which are a few lines of closed-form arithmetic.

The algorithm maintains a probability distribution over "run length" (time
since the last regime change). At each new observation, every active
run-length hypothesis is scored against how well it predicted the new
point; probability mass shifts toward "run length 0" (a fresh regime just
started) when the observation is a poor fit for every hypothesis that
assumed the old regime continued. All arithmetic is done in log-space
since predictive probabilities underflow quickly in linear space as the
run-length distribution grows.
"""

from __future__ import annotations

import math
from dataclasses import dataclass


def _log_student_t_pdf(x: float, dof: float, loc: float, scale: float) -> float:
    """Log density of a Student-t distribution at x."""
    z = (x - loc) / scale
    return (
        math.lgamma((dof + 1) / 2)
        - math.lgamma(dof / 2)
        - 0.5 * math.log(dof * math.pi * scale**2)
        - ((dof + 1) / 2) * math.log1p((z**2) / dof)
    )


def _logsumexp(values: list[float]) -> float:
    m = max(values)
    if m == -math.inf:
        return -math.inf
    return m + math.log(sum(math.exp(v - m) for v in values))


@dataclass(frozen=True)
class _NormalGamma:
    """Sufficient statistics of a Normal-Gamma posterior over the (unknown
    mean, unknown precision) of a Normal likelihood -- i.e. how many
    observations (via kappa/alpha) this hypothesis has absorbed and what
    they implied about the mean (mu) and variance (via alpha/beta).
    """

    mu: float
    kappa: float
    alpha: float
    beta: float

    def predictive_log_pdf(self, x: float) -> float:
        """Student-t log density: the closed-form posterior predictive
        distribution for the next observation under a Normal-Gamma prior.
        """
        dof = 2 * self.alpha
        scale = math.sqrt(self.beta * (self.kappa + 1) / (self.alpha * self.kappa))
        return _log_student_t_pdf(x, dof, self.mu, scale)

    def update(self, x: float) -> _NormalGamma:
        new_kappa = self.kappa + 1
        new_mu = (self.kappa * self.mu + x) / new_kappa
        new_alpha = self.alpha + 0.5
        new_beta = self.beta + (self.kappa * (x - self.mu) ** 2) / (2 * new_kappa)
        return _NormalGamma(new_mu, new_kappa, new_alpha, new_beta)


class BOCPD:
    """Online changepoint detector for a scalar stream (e.g. realized
    volatility per window). Call update(x) once per new observation.

    hazard_lambda: expected number of observations between changepoints
    under the prior (a constant hazard rate of 1/hazard_lambda). Smaller
    values make the detector more trigger-happy.

    changepoint_probability_threshold: update(x) reports a changepoint
    when P(run length = 0 | data so far) exceeds this after incorporating
    x -- i.e. "a fresh regime starting right here" becomes the single most
    likely explanation by more than this margin.

    prune_below: hypotheses (and their run-length probability) are dropped
    once their probability falls below this, which bounds memory/CPU for
    an indefinitely long-running stream at the cost of a small, deliberate
    approximation (astronomically unlikely run lengths stop being tracked
    exactly rather than contributing negligible probability mass forever).
    Default is deliberately not tiny (1e-4, not 1e-9): on a genuinely
    stable stream the single "no changepoint ever" hypothesis decays only
    like exp(-n/hazard_lambda), so a stricter threshold delays pruning for
    thousands of updates -- confirmed empirically while tuning this: at
    1e-9 nothing had been pruned yet after 2000 updates on a stable
    synthetic stream with hazard_lambda=250, defeating the point of having
    a bound at all for a consumer meant to run indefinitely.
    """

    def __init__(
        self,
        hazard_lambda: float = 250.0,
        mu0: float = 0.0,
        kappa0: float = 1.0,
        alpha0: float = 1.0,
        beta0: float = 1.0,
        changepoint_probability_threshold: float = 0.5,
        short_run_threshold: int = 1,
        prune_below: float = 1e-4,
    ) -> None:
        self._log_hazard = math.log(1.0 / hazard_lambda)
        self._log_one_minus_hazard = math.log1p(-1.0 / hazard_lambda)
        self._prior = _NormalGamma(mu0, kappa0, alpha0, beta0)
        self._threshold = changepoint_probability_threshold
        self._short_run_threshold = short_run_threshold
        self._prune_below = math.log(prune_below)

        # Parallel lists: hypotheses[r] is the Normal-Gamma posterior
        # given a run of length r; log_run_length_probs[r] is
        # log P(run length = r | data so far).
        self._hypotheses: list[_NormalGamma] = [self._prior]
        self._log_run_length_probs: list[float] = [0.0]  # P(r=0) = 1 before any data
        self._n_updates = 0

        # During warm-up there aren't yet enough competing hypotheses for
        # P(run length <= short_run_threshold) to mean anything -- with
        # only short_run_threshold+1 or fewer hypotheses in existence, that
        # probability mass has nowhere else to go and trivially sums close
        # to 1. Confirmed empirically: the very first update() always
        # reports changepoint_probability == 1.0 regardless of the data,
        # for exactly this reason. Detection is suppressed until enough
        # observations have accumulated for the comparison to be real.
        self._warmup_updates = 2 * (short_run_threshold + 1)

    def update(self, x: float) -> bool:
        """Incorporate one new observation. Returns True if this point is
        flagged as a changepoint.

        Note on what "changepoint" means here: P(run length = 0) turns out
        to be mathematically *constant*, exactly equal to the hazard rate,
        independent of the data -- confirmed both algebraically and
        empirically during development (it never moved off 1/hazard_lambda
        across any test input, including an obvious 20-sigma outlier
        burst). That's a property of marginalizing over every prior
        hypothesis to compute both the changepoint and growth branches
        from the same total evidence, not a bug to fix in the update math.
        The signal that does respond to data is P(run length <= k) for a
        small k: right after a real regime shift, probability mass
        concentrates on "this run just started" (r=1, r=2, ...) rather
        than on r=0 specifically. short_run_threshold controls k.
        """
        log_pred = [h.predictive_log_pdf(x) for h in self._hypotheses]

        log_growth = [
            p + pred + self._log_one_minus_hazard
            for p, pred in zip(self._log_run_length_probs, log_pred, strict=True)
        ]
        log_cp = _logsumexp(
            [
                p + pred + self._log_hazard
                for p, pred in zip(self._log_run_length_probs, log_pred, strict=True)
            ]
        )

        new_log_probs = [log_cp, *log_growth]
        total = _logsumexp(new_log_probs)
        new_log_probs = [p - total for p in new_log_probs]

        new_hypotheses = [self._prior, *(h.update(x) for h in self._hypotheses)]

        # Prune negligible-probability hypotheses to bound memory/CPU.
        # Always keep enough of the head to compute changepoint_probability
        # regardless of individual probabilities there.
        keep_head = max(self._short_run_threshold + 1, 1)
        kept_probs = list(new_log_probs[:keep_head])
        kept_hypotheses = list(new_hypotheses[:keep_head])
        for p, h in zip(new_log_probs[keep_head:], new_hypotheses[keep_head:], strict=True):
            if p >= self._prune_below:
                kept_probs.append(p)
                kept_hypotheses.append(h)

        self._log_run_length_probs = kept_probs
        self._hypotheses = kept_hypotheses
        self._n_updates += 1

        if self._n_updates <= self._warmup_updates:
            return False
        return self.changepoint_probability >= self._threshold

    @property
    def changepoint_probability(self) -> float:
        """P(run length <= short_run_threshold | data so far): how likely
        it is that the current regime is only a few observations old,
        i.e. that a changepoint happened recently. See update()'s
        docstring for why this, and not P(run length = 0), is the signal
        used.
        """
        head = self._log_run_length_probs[: self._short_run_threshold + 1]
        return math.exp(_logsumexp(head))
