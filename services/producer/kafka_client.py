"""Kafka producer client wrapper."""

from __future__ import annotations

import json
from typing import Any

from confluent_kafka import Producer


class KafkaEventProducer:
    """Small wrapper around confluent-kafka for JSON events."""

    def __init__(self, *, bootstrap_servers: str, client_id: str = "crypto-producer") -> None:
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "client.id": client_id,
                "acks": "all",
                "enable.idempotence": True,
                "retries": 10,
                "linger.ms": 50,
                "compression.type": "snappy",
            }
        )

    def produce_json(self, *, topic: str, value: dict[str, Any], key: str | None = None) -> None:
        payload = json.dumps(value, separators=(",", ":"), default=str).encode("utf-8")
        key_bytes = key.encode("utf-8") if key else None
        self._producer.produce(topic=topic, key=key_bytes, value=payload)
        self._producer.poll(0)

    def flush(self, timeout: float = 10.0) -> int:
        return self._producer.flush(timeout)
