"""storage/db.py tests. psycopg is faked here so the standard test suite
doesn't need a live Postgres -- the real SQL (schema DDL, the upsert's
ON CONFLICT clause) was verified against an actual local Postgres
container during development, not just by inspection; see README.md's
Correctness Design for that verification.
"""

from __future__ import annotations

from psycopg.types.json import Json

from storage import db as db_module
from storage.db import get_connection, upsert_anomaly


class _FakeCursor:
    def __init__(self, executed: list) -> None:
        self._executed = executed

    def execute(self, sql, params=None) -> None:
        self._executed.append((sql, params))

    def __enter__(self):
        return self

    def __exit__(self, *exc) -> bool:
        return False


class _FakeConnection:
    def __init__(self) -> None:
        self.executed: list = []
        self.commits = 0
        self.closed = False

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self.executed)

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
