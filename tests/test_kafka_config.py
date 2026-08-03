"""kafka_config.kafka_client_config tests."""

from __future__ import annotations

import pytest

from kafka_config import kafka_client_config


def test_defaults_to_plaintext_and_env_bootstrap_servers(monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    monkeypatch.delenv("KAFKA_SECURITY_PROTOCOL", raising=False)

    config = kafka_client_config()

    assert config["bootstrap.servers"] == "localhost:9092"
    assert config["security.protocol"] == "PLAINTEXT"
    assert "sasl.mechanism" not in config
    assert "sasl.username" not in config


def test_explicit_bootstrap_servers_overrides_env(monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "should-not-be-used:9092")

    config = kafka_client_config("explicit:9092")

    assert config["bootstrap.servers"] == "explicit:9092"


def test_sasl_ssl_adds_auth_fields(monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "managed:9092")
    monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
    monkeypatch.setenv("KAFKA_SASL_USERNAME", "user")
    monkeypatch.setenv("KAFKA_SASL_PASSWORD", "pass")
    monkeypatch.delenv("KAFKA_SASL_MECHANISM", raising=False)

    config = kafka_client_config()

    assert config["security.protocol"] == "SASL_SSL"
    assert config["sasl.mechanism"] == "PLAIN"  # default mechanism
    assert config["sasl.username"] == "user"
    assert config["sasl.password"] == "pass"


def test_sasl_mechanism_env_override(monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "managed:9092")
    monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
    monkeypatch.setenv("KAFKA_SASL_MECHANISM", "SCRAM-SHA-256")
    monkeypatch.setenv("KAFKA_SASL_USERNAME", "user")
    monkeypatch.setenv("KAFKA_SASL_PASSWORD", "pass")

    config = kafka_client_config()

    assert config["sasl.mechanism"] == "SCRAM-SHA-256"


def test_missing_bootstrap_servers_raises(monkeypatch):
    monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVERS", raising=False)

    with pytest.raises(KeyError):
        kafka_client_config()


def test_sasl_ssl_without_credentials_raises(monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "managed:9092")
    monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
    monkeypatch.delenv("KAFKA_SASL_USERNAME", raising=False)
    monkeypatch.setenv("KAFKA_SASL_PASSWORD", "pass")

    with pytest.raises(KeyError):
        kafka_client_config()


def test_empty_but_present_security_protocol_falls_back_to_plaintext(monkeypatch):
    """A `.env` line like `KAFKA_SECURITY_PROTOCOL=` sets the var to ""
    (present, not absent) -- the same empty-but-present bug class already
    found and fixed for group.id and DATABASE_URL elsewhere in this
    project (see streaming/consumer.py, storage/db.py). `.get(KEY) or
    DEFAULT`, not `.get(KEY, DEFAULT)`, is what actually handles it.
    """
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "")

    config = kafka_client_config()

    assert config["security.protocol"] == "PLAINTEXT"
