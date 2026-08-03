"""Shared Kafka client config, for connecting to either a local PLAINTEXT
broker (the default -- local dev via docker-compose) or an authenticated
managed Kafka provider (SASL_SSL) once deployed -- see README.md's
Deployment section for the managed-Kafka setup this exists for.

Every confluent_kafka Producer/Consumer/AdminClient in this project,
other than chaos/'s scripts (deliberately local-only, see their own
module docstrings), is built by extending this base config with whatever
else that specific client needs (group.id, enable.auto.commit, client.id,
etc.), so the auth logic lives in exactly one place instead of being
duplicated across every ingestion/streaming/storage/backend call site.
"""

from __future__ import annotations

import os


def kafka_client_config(bootstrap_servers: str | None = None) -> dict:
    """Base config: bootstrap.servers, plus SASL_SSL auth if
    KAFKA_SECURITY_PROTOCOL is set to anything other than the PLAINTEXT
    default. Local docker-compose Kafka needs no auth at all, so the
    default keeps every existing local-dev workflow unchanged; a managed
    provider is opted into purely by setting env vars, not by branching
    code per environment.
    """
    config: dict = {
        "bootstrap.servers": bootstrap_servers or os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        "security.protocol": os.environ.get("KAFKA_SECURITY_PROTOCOL") or "PLAINTEXT",
    }
    if config["security.protocol"] != "PLAINTEXT":
        config["sasl.mechanism"] = os.environ.get("KAFKA_SASL_MECHANISM") or "PLAIN"
        config["sasl.username"] = os.environ["KAFKA_SASL_USERNAME"]
        config["sasl.password"] = os.environ["KAFKA_SASL_PASSWORD"]
    return config
