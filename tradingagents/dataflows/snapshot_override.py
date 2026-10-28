"""离线快照覆盖层：让上游（如 OpenAlice）把已抓到的原始数据直接喂进分析链路。

动机
----
TradingAgents 的分析流程会反复向行情 / 新闻 vendor 取数。当它作为另一个
系统（OpenAlice）的子能力被调用时，上游往往已经持有同一批数据：再取一次
既慢，又会撞上 vendor 限流（Yahoo 尤其明显），还可能拿到与上游口径不一致
的数字。

本模块提供一个极小的注入通道：一次分析运行开始前 ``install()`` 一份快照，
``route_to_vendor()`` 在每次取数前先 ``lookup()``，命中即直接返回快照文本，
未命中照常走原有 vendor 链路。默认不安装任何快照，行为与历史版本完全一致。

快照格式（JSON）
----------------
同时支持两种组织方式，匹配优先级 by_ticker > by_method::

    {
      "ticker": "BTC-USD",
      "by_method": {
        "get_stock_data": "<现成的 OHLCV 表格文本>",
        "get_news": "<现成的新闻文本>"
      },
      "by_ticker": {
        "BTC-USD": {"get_stock_data": "<...>"}
      }
    }

``by_method`` 适合单标的运行（最常见）：不校验 symbol，命中即用；
``by_ticker`` 适合一次快照服务多标的调用，按 symbol 精确匹配。

安全
----
- 只读注入，不写任何上游文件；
- 未安装快照时 ``lookup()`` 直接返回 None，零开销；
- 可用环境变量 ``TRADINGAGENTS_SNAPSHOT_DISABLE=1`` 全局关闭该通道。
"""

from __future__ import annotations

import json
import logging
import os
import threading
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)

_LOCK = threading.RLock()
_ACTIVE: Optional[dict[str, Any]] = None
_SOURCE: Optional[str] = None

# 允许被快照接管的方法。限定白名单，避免上游误用任意方法名静默串数据。
SUPPORTED_METHODS: frozenset[str] = frozenset(
    {
        "get_stock_data",
        "get_indicators",
        "get_verified_market_snapshot",
        "get_fundamentals",
        "get_balance_sheet",
        "get_cashflow",
        "get_income_statement",
        "get_news",
        "get_global_news",
        "get_insider_transactions",
    }
)


class SnapshotError(ValueError):
    """快照内容不合法。"""


def _disabled() -> bool:
    return os.getenv("TRADINGAGENTS_SNAPSHOT_DISABLE", "").strip().lower() in {"1", "true", "yes", "on"}


def _validate(payload: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise SnapshotError("快照必须是 JSON 对象")

    by_method = payload.get("by_method") or {}
    by_ticker = payload.get("by_ticker") or {}
    if not isinstance(by_method, dict) or not isinstance(by_ticker, dict):
        raise SnapshotError("by_method / by_ticker 必须是对象")

    clean_method: dict[str, str] = {}
    for key, value in by_method.items():
        name = str(key).strip()
        if name not in SUPPORTED_METHODS:
            logger.warning("快照忽略未知方法 %r（不在白名单内）", name)
            continue
        if not isinstance(value, str) or not value.strip():
            logger.warning("快照忽略方法 %r 的非文本内容", name)
            continue
        clean_method[name] = value

    clean_ticker: dict[str, dict[str, str]] = {}
    for tkr, table in by_ticker.items():
        if not isinstance(table, dict):
            logger.warning("快照忽略标的 %r：其内容不是对象", tkr)
            continue
        row: dict[str, str] = {}
        for key, value in table.items():
            name = str(key).strip()
            if name not in SUPPORTED_METHODS:
                logger.warning("快照忽略未知方法 %r（不在白名单内）", name)
                continue
            if isinstance(value, str) and value.strip():
                row[name] = value
        if row:
            clean_ticker[str(tkr).strip().upper()] = row

    if not clean_method and not clean_ticker:
        raise SnapshotError("快照未包含任何可用数据")

    return {
        "ticker": str(payload.get("ticker") or "").strip().upper() or None,
        "by_method": clean_method,
        "by_ticker": clean_ticker,
    }


def install(payload: dict[str, Any], source: Optional[str] = None) -> dict[str, Any]:
    """安装一份快照，返回生效摘要。"""
    global _ACTIVE, _SOURCE
    clean = _validate(payload)
    with _LOCK:
        _ACTIVE = clean
        _SOURCE = source
    logger.info(
        "snapshot installed: methods=%s tickers=%s source=%s",
        sorted(clean["by_method"]),
        sorted(clean["by_ticker"]),
        source,
    )
    return describe()


def install_file(path: str | os.PathLike[str]) -> dict[str, Any]:
    file_path = Path(path)
    payload = json.loads(file_path.read_text(encoding="utf-8"))
    return install(payload, source=str(file_path))


def clear() -> None:
    global _ACTIVE, _SOURCE
    with _LOCK:
        _ACTIVE = None
        _SOURCE = None


def describe() -> dict[str, Any]:
    with _LOCK:
        if not _ACTIVE:
            return {"active": False}
        return {
            "active": True,
            "source": _SOURCE,
            "ticker": _ACTIVE.get("ticker"),
            "methods": sorted(_ACTIVE["by_method"]),
            "tickers": sorted(_ACTIVE["by_ticker"]),
        }


def _symbol_of(args: tuple[Any, ...], kwargs: dict[str, Any]) -> Optional[str]:
    candidate = kwargs.get("symbol") or kwargs.get("ticker")
    if candidate is None and args:
        candidate = args[0]
    if candidate is None:
        return None
    text = str(candidate).strip().upper()
    return text or None


def lookup(method: str, args: tuple[Any, ...], kwargs: dict[str, Any]) -> Optional[str]:
    """命中快照则返回其文本，否则 None（调用方继续走 vendor 链路）。"""
    if _disabled():
        return None
    with _LOCK:
        active = _ACTIVE
    if not active or method not in SUPPORTED_METHODS:
        return None

    symbol = _symbol_of(args, kwargs)
    if symbol:
        hit = active["by_ticker"].get(symbol, {}).get(method)
        if hit is not None:
            logger.info("snapshot hit: %s %s (by_ticker)", method, symbol)
            return hit

    hit = active["by_method"].get(method)
    if hit is not None:
        logger.info("snapshot hit: %s %s (by_method)", method, symbol or "-")
        return hit
    return None
