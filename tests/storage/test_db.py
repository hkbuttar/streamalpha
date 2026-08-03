"""storage/db.py tests. psycopg is faked here so the standard test suite
doesn't need a live Postgres -- the real SQL (schema DDL, the upsert's
ON CONFLICT clause) was verified against an actual local Postgres
container during development, not just by inspection; see README.md's
Correctness Design for that verification.
"""

from __future__ import annotations

from psycopg.types.json import Json

from storage import db as db_module
from storage.db import get_connection, list_anomalies, upsert_anomaly


class _FakeCursor:
    def __init__(
        self, executed: list, rows: list | None = None, columns: list | None = None
    ) -> None:
        self._executed = executed
        self._rows = rows or []
        self.description = [_Col(c) for c in (columns or [])]

    def execute(self, sql, params=None) -> None:
        self._executed.append((sql, params))

    def fetchall(self):
        return self._rows

    def __enter__(self):
        return self

    def __exit__(self, *exc) -> bool:
        return False


class _Col:
    def __init__(self, name):
        self.name = name


class _FakeConnection:
    def __init__(self, rows: list | None = None, columns: list | None = None) -> None:
        self.executed: list = []
        self.commits = 0
        self.closed = False
        self._rows = rows
        self._columns = columns

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self.executed, self._rows, self._columns)

    def commit(self) -> None:
        self.commits += 1

    def close(self) -> None:
        self.closed = True


def test_get_connection_uses_database_url_env_var(monkeypatch):
    fake = _FakeConnection()
    captured = {}

    def _fake_connect(database_url):
        captured["database_url"] = database_url
        return fake

    monkeypatch.setattr(db_module.psycopg, "connect", _fake_connect)
    monkeypatch.setenv("DATABASE_URL", "postgresql://example/db")

    conn = get_connection()

    assert conn is fake
    assert captured["database_url"] == "postgresql://example/db"


def test_get_connection_creates_schema_and_commits(monkeypatch):
    fake = _FakeConnection()
    monkeypatch.setattr(db_module.psycopg, "connect", lambda database_url: fake)

    get_connection(database_url="postgresql://example/db")

    [(sql, params)] = fake.executed
    assert "CREATE TABLE IF NOT EXISTS anomalies" in sql
    assert params is None
    assert fake.commits == 1


def test_upsert_anomaly_executes_insert_with_correct_params_and_commits():
    fake = _FakeConnection()
    details = {"anomaly_score": 0.9, "volume": 50000.0}

    upsert_anomaly(fake, "AAPL", "2024-01-02T15:30:00+00:00", "volume_anomaly", details)

    [(sql, params)] = fake.executed
    assert "INSERT INTO anomalies" in sql
    assert "ON CONFLICT (ticker, window_start, anomaly_type)" in sql
    assert "DO UPDATE SET details = excluded.details" in sql

    ticker, window_start, anomaly_type, json_details = params
    assert ticker == "AAPL"
    assert window_start == "2024-01-02T15:30:00+00:00"
    assert anomaly_type == "volume_anomaly"
    assert isinstance(json_details, Json)
    assert json_details.obj == details

    assert fake.commits == 1


_ANOMALY_COLUMNS = ["ticker", "window_start", "anomaly_type", "details", "detected_at"]


def test_list_anomalies_with_no_filters():
    rows = [
        (
            "AAPL",
            "2024-01-02T15:30:00+00:00",
            "volume_anomaly",
            {"score": 0.9},
            "2024-01-02T15:31:00+00:00",
        )
    ]
    fake = _FakeConnection(rows=rows, columns=_ANOMALY_COLUMNS)

    result = list_anomalies(fake)

    [(sql, params)] = fake.executed
    assert "SELECT ticker, window_start, anomaly_type, details, detected_at" in sql
    assert "FROM anomalies" in sql
    assert "WHERE" not in sql
    assert "ORDER BY detected_at DESC" in sql
    assert params == [50]
    assert result == [
        {
            "ticker": "AAPL",
            "window_start": "2024-01-02T15:30:00+00:00",
            "anomaly_type": "volume_anomaly",
            "details": {"score": 0.9},
            "detected_at": "2024-01-02T15:31:00+00:00",
        }
    ]


def test_list_anomalies_filters_by_ticker_and_anomaly_type():
    fake = _FakeConnection(rows=[], columns=_ANOMALY_COLUMNS)

    list_anomalies(fake, ticker="AAPL", anomaly_type="regime_change", limit=10)

    [(sql, params)] = fake.executed
    assert "WHERE ticker = %s AND anomaly_type = %s" in sql
    assert params == ["AAPL", "regime_change", 10]


def test_list_anomalies_empty_table_returns_empty_list():
    fake = _FakeConnection(rows=[], columns=_ANOMALY_COLUMNS)
    assert list_anomalies(fake) == []
