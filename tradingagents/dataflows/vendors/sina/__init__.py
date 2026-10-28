"""Sina Finance vendor: a mainland-China-reachable market data source.

Yahoo Finance stopped serving mainland China on 2021-11-01 and answers every
request from there with an HTTP 403 regional block page, so ``yfinance`` cannot
return a single bar on such a network regardless of retries or proxies. Sina's
public ``GlobalFuturesService`` endpoint serves the same crypto daily series
(BTC against USD) with no API key, which is what this vendor exposes.
"""

from tradingagents.dataflows.vendors.sina.market import (
    get_sina_indicator,
    get_sina_stock,
    load_ohlcv_sina,
    supports_symbol,
)

__all__ = ["get_sina_indicator", "get_sina_stock", "load_ohlcv_sina", "supports_symbol"]
