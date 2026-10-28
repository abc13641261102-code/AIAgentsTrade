"""Eastmoney vendor: mainland-China-reachable news for a region-blocked Yahoo."""

from tradingagents.dataflows.vendors.eastmoney.news import (
    get_global_news_eastmoney,
    get_news_eastmoney,
)

__all__ = ["get_global_news_eastmoney", "get_news_eastmoney"]
