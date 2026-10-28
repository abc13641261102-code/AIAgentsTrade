"""OKX (欧易) vendor: an exchange-grade crypto market data source.

Yahoo Finance is geo-blocked from mainland China and Sina publishes a single
BTC daily series with zero volume, so neither is a real exchange feed. OKX's
public market-data API needs no key, covers every spot pair the exchange lists,
and reports actual volume — which is what this vendor exposes.
"""

from tradingagents.dataflows.vendors.okx.market import (
    get_okx_indicator,
    get_okx_stock,
    load_ohlcv_okx,
    supports_symbol,
)

__all__ = ["get_okx_indicator", "get_okx_stock", "load_ohlcv_okx", "supports_symbol"]
