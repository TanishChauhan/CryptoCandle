"""Binance WebSocket producer -> Kafka with shared validation + DLQ routing."""

from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from time import monotonic, time
import structlog

from services.producer.binance_ws import (
    build_combined_trade_stream_url,
    connect,
    normalize_trade_payload,
    recv_json,
)
from services.producer.kafka_client import KafkaEventProducer
from services.producer.logging_config import configure_logging
from shared.dlq import build_dlq_envelope
from shared.validation import ValidationIssue, validate_trade


@dataclass(frozen=True)
class ProducerConfig:
    kafka_bootstrap_servers: str
    trades_topic: str
    dlq_topic: str
    ws_base_url: str
    symbols: list[str]
    log_level: str
    json_logs: bool
    max_backoff_seconds: int = 60


def load_config() -> ProducerConfig:
    symbols = [s.strip().upper() for s in os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT").split(",")]
    symbols = [s for s in symbols if s]
    return ProducerConfig(
        kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
        trades_topic=os.getenv("KAFKA_TOPIC_TRADES", "crypto_trades"),
        dlq_topic=os.getenv("KAFKA_TOPIC_DLQ", "dead_letter_queue"),
        ws_base_url=os.getenv("BINANCE_WS_BASE", "wss://stream.binance.com:9443"),
        symbols=symbols,
        log_level=os.getenv("LOG_LEVEL", "INFO"),
        json_logs=os.getenv("LOG_JSON", "true").lower() == "true",
    )


def _safe_quote_qty(price: str, quantity: str) -> str | None:
    try:
        return str(Decimal(price) * Decimal(quantity))
    except (InvalidOperation, TypeError):
        return None


def _build_malformed_issue(message: str) -> ValidationIssue:
    from shared.validation import ValidationErrorCode  # local import avoids cycles in type checkers

    return ValidationIssue(code=ValidationErrorCode.MALFORMED_JSON, message=message)


async def run_producer(config: ProducerConfig) -> None:
    logger = structlog.get_logger("producer")
    kafka = KafkaEventProducer(bootstrap_servers=config.kafka_bootstrap_servers)
    ws_url = build_combined_trade_stream_url(config.ws_base_url, config.symbols)

    consecutive_failures = 0
    metrics = {
        "events_published": 0,
        "events_rejected": 0,
        "ws_disconnects": 0,
        "events_received": 0,
    }
    last_metrics_log = monotonic()

    logger.info(
        "producer_starting",
        ws_url=ws_url,
        symbols=config.symbols,
        trades_topic=config.trades_topic,
        dlq_topic=config.dlq_topic,
    )

    while True:
        try:
            async with await connect(ws_url) as ws:
                logger.info("ws_connected", url=ws_url)
                consecutive_failures = 0

                while True:
                    payload = await recv_json(ws)
                    metrics["events_received"] += 1
                    now_ms = int(time() * 1000)

                    try:
                        normalized = normalize_trade_payload(payload, ingested_at_ms=now_ms)
                        quote_qty = _safe_quote_qty(normalized["price"], normalized["quantity"])
                        if quote_qty:
                            normalized["quote_qty"] = quote_qty
                    except (KeyError, ValueError, TypeError) as exc:
                        issue = _build_malformed_issue(f"trade normalization failed: {exc}")
                        envelope = build_dlq_envelope(
                            original_payload=payload,
                            issue=issue,
                            stage="producer",
                            failed_at_ms=now_ms,
                        )
                        kafka.produce_json(topic=config.dlq_topic, value=envelope)
                        metrics["events_rejected"] += 1
                        logger.warning("event_rejected_malformed", error=str(exc))
                        continue

                    validation = validate_trade(normalized, now_ms=now_ms, allowed_symbols=set(config.symbols))
                    if not validation.is_valid or validation.issue is not None:
                        issue = validation.issue or _build_malformed_issue("unknown validation failure")
                        envelope = build_dlq_envelope(
                            original_payload=normalized,
                            issue=issue,
                            stage="producer",
                            symbol=normalized.get("symbol"),
                            failed_at_ms=now_ms,
                        )
                        kafka.produce_json(topic=config.dlq_topic, value=envelope, key=normalized.get("symbol"))
                        metrics["events_rejected"] += 1
                        logger.warning(
                            "event_rejected_validation",
                            symbol=normalized.get("symbol"),
                            error_code=str(issue.code),
                            reason=issue.message,
                        )
                        continue

                    kafka.produce_json(
                        topic=config.trades_topic,
                        value=validation.event.model_dump() if validation.event else normalized,
                        key=normalized["symbol"],
                    )
                    metrics["events_published"] += 1

                    now_monotonic = monotonic()
                    if now_monotonic - last_metrics_log >= 60:
                        logger.info("producer_metrics", **metrics)
                        last_metrics_log = now_monotonic

        except asyncio.CancelledError:
            logger.info("producer_cancelled")
            break
        except Exception as exc:  # pragma: no cover - runtime resilience path
            metrics["ws_disconnects"] += 1
            consecutive_failures += 1
            backoff_seconds = min(2 ** min(consecutive_failures, 5), config.max_backoff_seconds)
            logger.warning(
                "ws_disconnected_retrying",
                error=str(exc),
                consecutive_failures=consecutive_failures,
                retry_in_seconds=backoff_seconds,
                happened_at=datetime.now(UTC).isoformat(),
            )
            await asyncio.sleep(backoff_seconds)
        finally:
            kafka.flush()


def main() -> None:
    config = load_config()
    configure_logging(log_level=config.log_level, json_logs=config.json_logs)
    asyncio.run(run_producer(config))


if __name__ == "__main__":
    main()
