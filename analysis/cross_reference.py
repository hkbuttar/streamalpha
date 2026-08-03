"""Cross-references streamalpha's detected anomalies against
alpha-signal-lab's factor scores: for each detected anomaly, did the same
ticker also have an unusually large day-over-day move in one of
alpha-signal-lab's own factors around the same date? This is what makes
"extends alpha-signal-lab" concrete rather than a shared universe in name
only -- it reuses alpha-signal-lab's actual factor computation
(factors/momentum.py, volatility.py, mean_reversion.py) via the same
editable install config.universe already comes from, not a
reimplementation of "big price move."

Scope: momentum, volatility, mean_reversion only -- not sentiment, which
needs a NewsAPI key and an LLM call per headline (alpha-signal-lab's
data/news.py). That cost and complexity isn't needed here: this is
checking against alpha-signal-lab's price-based read of a name, not
reproducing its full factor stack.

"Moved sharply" is defined per ticker, not cross-sectionally: for each
factor, take the day-over-day change in that ticker's own score, then a
rolling z-score of that change series against the ticker's own trailing
history (not other tickers' same-day moves). This mirrors how
streamalpha's own detectors work (per-ticker, not a cross-sectional
comparison) and avoids conflating "this factor moved a lot for this name
specifically" with "this factor moved a lot relative to the other 49
names today," which is a different, cross-sectional question this
analysis isn't asking.
"""

from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
from config.universe import UNIVERSE
from data.prices import load_prices
from factors import mean_reversion, momentum, volatility

from storage.db import get_connection

FACTOR_MODULES = [momentum, volatility, mean_reversion]
ROLLING_WINDOW_DAYS = 60
MIN_PERIODS = 20
Z_THRESHOLD = 2.0
MATCH_WINDOW_DAYS = 1
PRICE_LOOKBACK_DAYS = 400  # comfortably covers momentum's 252+21 day requirement


def load_anomalies(conn) -> pd.DataFrame:
    """All detected anomalies, one row per (ticker, window_start, anomaly_type),
    with a normalized-to-midnight `date` column (UTC, tz-stripped) added for
    matching against factor score dates.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT ticker, window_start, anomaly_type FROM anomalies ORDER BY window_start"
        )
        rows = cur.fetchall()
        columns = [col.name for col in cur.description]
    df = pd.DataFrame(rows, columns=columns)
    if not df.empty:
        window_start_utc = pd.to_datetime(df["window_start"], utc=True)
        df["date"] = window_start_utc.dt.tz_localize(None).dt.normalize()
    return df


def compute_factor_move_z_scores(prices: pd.DataFrame) -> pd.DataFrame:
    """Long-format [date, ticker, factor, z]: for every (date, ticker,
    factor), the rolling z-score of that factor's day-over-day score
    change, relative to that ticker's own trailing history of changes.
    Not filtered by a threshold here -- see sharp_moves_only().
    """
    frames = []
    for module in FACTOR_MODULES:
        scores = module.compute(prices)
        wide = scores.pivot(index="date", columns="ticker", values="score")
        delta = wide.diff()
        rolling_mean = delta.rolling(ROLLING_WINDOW_DAYS, min_periods=MIN_PERIODS).mean()
        rolling_std = delta.rolling(ROLLING_WINDOW_DAYS, min_periods=MIN_PERIODS).std()
        z = (delta - rolling_mean) / rolling_std.replace(0, pd.NA)
        long_z = z.reset_index().melt(id_vars="date", var_name="ticker", value_name="z")
        long_z["factor"] = module.name
        frames.append(long_z.dropna(subset=["z"]))
    if not frames:
        return pd.DataFrame(columns=["date", "ticker", "factor", "z"])
    return pd.concat(frames, ignore_index=True)


def sharp_moves_only(factor_z: pd.DataFrame, threshold: float = Z_THRESHOLD) -> pd.DataFrame:
    return factor_z[factor_z["z"].abs() >= threshold].copy()


def cross_reference(
    anomalies: pd.DataFrame, sharp_moves: pd.DataFrame, window_days: int = MATCH_WINDOW_DAYS
) -> pd.DataFrame:
    """For each anomaly, find sharp factor moves for the same ticker within
    +/- window_days of the anomaly's date. One output row per anomaly, with
    `matches` listing every (factor, date, z) found (empty list if none).
    """
    results = []
    for _, anomaly in anomalies.iterrows():
        ticker = anomaly["ticker"]
        anomaly_date = anomaly["date"]
        candidates = sharp_moves[sharp_moves["ticker"] == ticker]
        nearby = candidates[
            (candidates["date"] >= anomaly_date - pd.Timedelta(days=window_days))
            & (candidates["date"] <= anomaly_date + pd.Timedelta(days=window_days))
        ]
        matches = list(nearby[["factor", "date", "z"]].itertuples(index=False, name=None))
        results.append(
            {
                "ticker": ticker,
                "anomaly_date": anomaly_date,
                "anomaly_type": anomaly["anomaly_type"],
                "matches": matches,
                "coincides_with_sharp_factor_move": len(matches) > 0,
            }
        )
    return pd.DataFrame(results)


def main() -> None:
    conn = get_connection()
    try:
        anomalies = load_anomalies(conn)
    finally:
        conn.close()

    if anomalies.empty:
        print("No anomalies in the database yet -- nothing to cross-reference.")
        return

    tickers = sorted(set(UNIVERSE) | set(anomalies["ticker"]))
    end = date.today() + timedelta(days=1)  # load_prices' end is exclusive
    start = end - timedelta(days=PRICE_LOOKBACK_DAYS)
    prices = load_prices(tickers, start.isoformat(), end.isoformat())

    factor_z = compute_factor_move_z_scores(prices)
    sharp = sharp_moves_only(factor_z)
    result = cross_reference(anomalies, sharp)

    n_matched = int(result["coincides_with_sharp_factor_move"].sum())
    n_total = len(result)
    print(
        f"{n_matched}/{n_total} detected anomalies coincided with a sharp "
        f"alpha-signal-lab factor move (|z| >= {Z_THRESHOLD}, "
        f"+/-{MATCH_WINDOW_DAYS}d window)."
    )
    for _, row in result.iterrows():
        marker = "MATCH   " if row["coincides_with_sharp_factor_move"] else "no match"
        anomaly_date = row["anomaly_date"].date()
        ticker, anomaly_type = row["ticker"], row["anomaly_type"]
        print(f"  [{marker}] {ticker:6s} {anomaly_type:15s} {anomaly_date}  {row['matches']}")


if __name__ == "__main__":
    main()
