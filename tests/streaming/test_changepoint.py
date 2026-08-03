"""BOCPD tests against synthetic data with a known changepoint location.

changepoint_probability is deliberately P(run length <= short_run_threshold),
not P(run length == 0). The obvious choice, P(run length == 0), turned out
to be mathematically constant -- exactly the hazard rate, independent of
the data -- confirmed both algebraically and empirically while building
this: it never moved off 1/hazard_lambda for any input, including an
extreme outlier burst. test_p_run_length_zero_is_constant_regardless_of_data
pins that down explicitly so it can't silently regress back to the wrong
signal.
"""

from __future__ import annotations

import math
import random

import pytest

from streaming.changepoint import BOCPD, _NormalGamma


def test_p_run_length_zero_is_constant_regardless_of_data():
    """Documents *why* changepoint_probability isn't P(run length == 0):
    that quantity is provably constant under this formulation. See
    update()'s docstring in changepoint.py for the derivation.
    """
    hazard_lambda = 100.0
    bocpd = BOCPD(hazard_lambda=hazard_lambda)
    for x in [0.0, 1.0, -50.0, 200.0, -300.0]:
        bocpd.update(x)
        p_run_length_zero = math.exp(bocpd._log_run_length_probs[0])
        assert p_run_length_zero == pytest.approx(1.0 / hazard_lambda)


def test_detects_a_variance_changepoint():
    random.seed(42)
    stable = [random.gauss(0, 1) for _ in range(150)]
    shifted = [random.gauss(0, 5) for _ in range(150)]
    data = stable + shifted

    bocpd = BOCPD(hazard_lambda=250.0, changepoint_probability_threshold=0.5)
    flags = [i for i, x in enumerate(data) if bocpd.update(x)]

    assert flags == [150]


def test_detects_a_mean_shift():
    random.seed(1)
    stable = [random.gauss(0, 1) for _ in range(150)]
    shifted = [random.gauss(20, 1) for _ in range(150)]
    data = stable + shifted

    bocpd = BOCPD(hazard_lambda=250.0, changepoint_probability_threshold=0.5)
    flags = [i for i, x in enumerate(data) if bocpd.update(x)]

    assert 150 in flags


def test_no_false_positives_on_a_purely_stable_stream():
    random.seed(7)
    data = [random.gauss(0, 1) for _ in range(300)]

    bocpd = BOCPD(hazard_lambda=250.0, changepoint_probability_threshold=0.5)
    flags = [i for i, x in enumerate(data) if bocpd.update(x)]

    assert flags == []


def test_warmup_suppresses_the_trivial_first_observation_flag():
    """changepoint_probability trivially hits ~1.0 on the very first
    update() (too few competing hypotheses exist yet for the probability
    to go anywhere else), confirmed during development. update() must not
    report a changepoint during this warm-up window.
    """
    bocpd = BOCPD(hazard_lambda=250.0, changepoint_probability_threshold=0.5)
    assert bocpd.update(0.0) is False
    assert bocpd.update(1.0) is False


def test_hypothesis_list_is_pruned_and_stays_bounded():
    random.seed(3)
    bocpd = BOCPD(hazard_lambda=250.0, prune_below=1e-4)
    for _ in range(2000):
        bocpd.update(random.gauss(0, 1))
    # Should not grow unboundedly with the number of observations.
    assert len(bocpd._hypotheses) < 200


def test_normal_gamma_update_matches_closed_form_posterior_mean():
    prior = _NormalGamma(mu=0.0, kappa=1.0, alpha=1.0, beta=1.0)
    posterior = prior.update(4.0)
    # kappa0*mu0 + x, divided by kappa0+1 -- standard Normal-Gamma update.
    assert posterior.mu == pytest.approx((1.0 * 0.0 + 4.0) / 2.0)
    assert posterior.kappa == 2.0
    assert posterior.alpha == 1.5
