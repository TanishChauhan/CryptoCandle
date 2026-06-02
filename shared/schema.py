"""Canonical trade event schema used across producer and stream jobs."""

from pydantic import BaseModel, ConfigDict


class TradeEvent(BaseModel):
    """Normalized Binance trade payload expected in the pipeline."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    event_id: str
    symbol: str
    trade_id: int
    price: str
    quantity: str
    quote_qty: str | None = None
    trade_time_ms: int
    is_buyer_maker: bool | None = None
    ingested_at_ms: int | None = None
    source: str = "binance_ws"
