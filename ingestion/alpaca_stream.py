"""Builds the Alpaca WebSocket stream and wires it to the Kafka producer.

Subscribes to trades and quotes -- not REST polling -- for tickers in
config.universe (imported from alpha-signal-lab as an editable dependency;
see requirements.txt), and republishes each message onto market-ticks,
keyed by symbol.

ALPACA_MAX_SYMBOLS exists because Alpaca's WebSocket rejects a subscribe
request over your plan's symbol cap with error 405 "symbol limit exceeded".
Alpaca does not publish the exact number anywhere (checked the streaming
docs, the market data FAQ, and the pricing page -- none state a concrete
free-tier count), so this is a knob to find your actual limit empirically
rather than a hardcoded guess: lower it until the stream connects cleanly,
then raise ALPACA_MAX_SYMBOLS to that value. Unset, it subscribes the full
universe and lets Alpaca reject it the same way it just did.
"""

from __future__ import annotations

import logging
import os
from datetime import UTC

from alpaca.data.enums import DataFeed
from alpaca.data.live import StockDataStream
from alpaca.data.models import Quote, Trade
from config.universe import UNIVERSE

from ingestion.producer import TickProducer

log = logging.getLogger(__name__)


def _trade_to_dict(trade: Trade) -> dict:
    return {
        "type": "trade",
        "symbol": trade.symbol,
        "timestamp": trade.timestamp.astimezone(UTC).isoformat(),
        "price": trade.price,
        "size": trade.size,
        "exchange": trade.exchange,
        "trade_id": trade.id,
        "conditions": trade.conditions,
        "tape": trade.tape,
    }


def _quote_to_dict(quote: Quote) -> dict:
    return {
        "type": "quote",
        "symbol": quote.symbol,
        "timestamp": quote.timestamp.astimezone(UTC).isoformat(),
        "bid_price": quote.bid_price,
        "bid_size": quote.bid_size,
        "bid_exchange": quote.bid_exchange,
        "ask_price": quote.ask_price,
        "ask_size": quote.ask_size,
        "ask_exchange": quote.ask_exchange,
        "conditions": quote.conditions,
        "tape": quote.tape,
    }


def _resolve_symbols() -> list[str]:
    max_symbols = os.environ.get("ALPACA_MAX_SYMBOLS")
    if not max_symbols:
        return UNIVERSE
    return UNIVERSE[: int(max_symbols)]


def build_stream(producer: TickProducer) -> StockDataStream:
    """Construct a fresh StockDataStream with trade/quote handlers registered.

    Called once per connection attempt in ingestion/run.py's retry loop, so
    every retry starts from a clean stream instance rather than reusing one
    that may be in a bad state.
    """
    feed = DataFeed(os.environ.get("ALPACA_DATA_FEED", "iex"))
    stream = StockDataStream(
        api_key=os.environ["ALPACA_API_KEY"],
        secret_key=os.environ["ALPACA_SECRET_KEY"],
        feed=feed,
    )

    async def on_trade(trade: Trade) -> None:
        producer.publish(trade.symbol, _trade_to_dict(trade))

    async def on_quote(quote: Quote) -> None:
        producer.publish(quote.symbol, _quote_to_dict(quote))

    symbols = _resolve_symbols()
    log.info("subscribing to %d of %d universe symbols", len(symbols), len(UNIVERSE))
    stream.subscribe_trades(on_trade, *symbols)
    stream.subscribe_quotes(on_quote, *symbols)
    return stream
