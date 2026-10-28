"""Binance vendor: an exchange-grade crypto market data source.

Yahoo Finance is geo-blocked from mainland China and Sina publishes a single
BTC daily series with zero volume, so neither is a real exchange feed.
Binance's public market-data API needs no key, covers every spot pair the
exchange lists, and reports actual volume — which is what this vendor exposes.
"""

from tradingagents.dataflows.vendors.binance.market import (
    get_binance_indicator,
    get_binance_stock,
    load_ohlcv_binance,
    supports_symbol,
)

__all__ = [
    "get_binance_indicator",
    "get_binance_stock",
    "load_ohlcv_binance",
    "supports_symbol",
]
