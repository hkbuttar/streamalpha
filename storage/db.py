"""Idempotent Postgres sink for anomaly events. Same DATABASE_URL and
raw-SQL-no-ORM conventions as alpha-signal-lab's live/storage.py, but a
separate schema in a separate database -- this project's anomalies table
has nothing to do with alpha-signal-lab's orders/fills/snapshots tables.

One table, not two, for volume anomalies and regime changes: they share
the same identity (ticker, window, which kind of anomaly) and differ only
in their type-specific fields, which live in the JSONB details column
rather than as bespoke columns per anomaly type -- the same pattern
alpha-signal-lab's own portfolio_snapshots.positions_json uses for
similarly flexible, per-row-varying data.

The UNIQUE constraint on (ticker, window_start, anomaly_type) is what
completes the exactly-once story end to end: idempotent producer
(ingestion/producer.py, streaming/anomaly_producer.py) plus manual offset
commit (streaming/consumer.py, storage/sink.py) plus this upsert means a
DLQ replay or a consumer restart reprocessing the same anomaly event
updates the existing row instead of creating a duplicate.
"""

from __future__ import annotations

import os

import psycopg
from psycopg.types.json import Json

SCHEMA = """
CREATE TABLE IF NOT EXISTS anomalies (
    id SERIAL PRIMARY KEY,
    ticker TEXT NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    anomaly_type TEXT NOT NULL CHECK (anomaly_type IN ('volume_anomaly', 'regime_change')),
    details JSONB NOT NULL,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (ticker, window_start, anomaly_type)
);
"""


def get_connection(database_url: str | None = None) -> psycopg.Connection:
    """Connect using DATABASE_URL (or the given override) and ensure the
    schema exists.
    """
    database_url = database_url or os.environ["DATABASE_URL"]
    conn = psycopg.connect(database_url)
    with conn.cursor() as cur:
        cur.execute(SCHEMA)
    conn.commit()
    return conn


def upsert_anomaly(
    conn: psycopg.Connection, ticker: str, window_start: str, anomaly_type: str, details: dict
) -> None:
    """Insert or update one anomaly row and commit.

    storage/sink.py commits the corresponding Kafka offset only after this
    call returns, so a crash between the two leaves both undone -- a
    retried upsert with the same (ticker, window_start, anomaly_type) is a
    no-op change, not a duplicate row.
    """
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO anomalies (ticker, window_start, anomaly_type, details)
               VALUES (%s, %s, %s, %s)
               ON CONFLICT (ticker, window_start, anomaly_type)
               DO UPDATE SET details = excluded.details""",
            (ticker, window_start, anomaly_type, Json(details)),
        )
    conn.commit()


def list_anomalies(
    conn: psycopg.Connection,
    ticker: str | None = None,
    anomaly_type: str | None = None,
    limit: int = 50,
) -> list[dict]:
    """Most recent anomalies, newest first, optionally filtered by ticker
    and/or anomaly_type. Read path for backend/main.py's /anomalies
    endpoint -- this module was write-only (upsert_anomaly) until now.

    ticker/anomaly_type are always passed as bind parameters, never
    interpolated into the SQL string -- only the WHERE clause's shape
    (which conditions are present) is built from them, not their values.
    """
    conditions = []
    params: list = []
    if ticker is not None:
        conditions.append("ticker = %s")
        params.append(ticker)
    if anomaly_type is not None:
        conditions.append("anomaly_type = %s")
        params.append(anomaly_type)
    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    params.append(limit)

    with conn.cursor() as cur:
        cur.execute(
            f"""SELECT ticker, window_start, anomaly_type, details, detected_at
                FROM anomalies
                {where}
                ORDER BY detected_at DESC
                LIMIT %s""",
            params,
        )
        rows = cur.fetchall()
        columns = [col.name for col in cur.description]
    return [dict(zip(columns, row, strict=True)) for row in rows]
