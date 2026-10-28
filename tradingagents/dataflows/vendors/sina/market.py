"""Sina Finance quotes for the crypto pairs it covers, in Yahoo's output shape.

Why this vendor exists: Yahoo Finance answers every request from a mainland
Chinese network with an HTTP 403 page stating it no longer serves the region
(since 2021-11-01), so ``yfinance`` fails on this network no matter how often it
is retried. The routing layer already supports an ordered vendor chain, so the
fix is another vendor rather than a patch to Yahoo's.

Scope is deliberately narrow. Sina's ``GlobalFuturesService`` covers the crypto
bases in ``_SINA_BASES``; any other symbol raises ``NoMarketDataError`` at once
(no request is made) so the chain moves on to the next vendor.
"""

import json
import logging
import os
import re
import time
from datetime import datetime
from typing import Annotated

import pandas as pd
import requests

from tradingagents.dataflows.config import get_config
from tradingagents.dataflows.errors import NoMarketDataError, VendorUnavailableError
from tradingagents.dataflows.files import replace_file
from tradingagents.dataflows.symbols import (
    crypto_base,
    normalize_symbol,
    safe_ticker_component,
)
from tradingagents.dataflows.vendors.yahoo.ohlcv import (
    _assert_ohlcv_not_stale,
    _cache_is_fresh,
    _clean_dataframe,
    _fill_price_gaps,
)

logger = logging.getLogger(__name__)

_DAILY_URL = (
    "https://stock2.finance.sina.com.cn/futures/api/jsonp.php/var%20_k=/"
    "GlobalFuturesService.getGlobalFuturesDailyKLine?symbol={symbol}"
)

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
    ),
    "Referer": "https://finance.sina.com.cn/",
}

_TIMEOUT = 15.0
_MAX_RETRIES = 3

# Crypto bases Sina's GlobalFutures endpoint actually returns a series for; the
# others answer ``null``. Extend only after checking the endpoint returns rows.
_SINA_BASES = frozenset({"BTC"})

# Indicators that need volume. Sina reports 0 for every crypto bar, so those
# series are not computable from this source and are reported as such rather
# than as a number derived from a zero denominator.
_VOLUME_INDICATORS = frozenset({"vwma", "mfi"})

_JSONP_ARRAY = re.compile(r"\(\s*(\[.*\])\s*\)\s*;?\s*$", re.S)


def supports_symbol(symbol: str) -> bool:
    """Whether Sina can serve ``symbol`` — lets callers route without a failed try."""
    try:
        _resolve(symbol)
    except NoMarketDataError:
        return False
    return True


def _resolve(symbol: str) -> tuple[str, str]:
    """``(canonical, base)`` for a covered crypto pair, else ``NoMarketDataError``."""
    canonical = normalize_symbol(symbol)
    base = crypto_base(canonical)
    if base is None:
        raise NoMarketDataError(
            symbol, canonical, "sina vendor serves USD-quoted crypto pairs only"
        )
    if base not in _SINA_BASES:
        raise NoMarketDataError(
            symbol, canonical, f"sina has no daily series for {base}"
        )
    return canonical, base


def _parse_daily(text: str) -> list:
    """The JSON array inside Sina's JSONP wrapper, or ``[]``."""
    match = _JSONP_ARRAY.search(text or "")
    if not match:
        return []
    try:
        rows = json.loads(match.group(1))
    except ValueError:
        return []
    return rows if isinstance(rows, list) else []


def _numeric(raw: pd.DataFrame, name: str) -> pd.Series:
    """Column ``name`` coerced to numbers, or an all-NaN series when absent."""
    if name not in raw.columns:
        return pd.Series([float("nan")] * len(raw), index=raw.index)
    return pd.to_numeric(raw[name], errors="coerce")


def _to_frame(rows: list) -> pd.DataFrame:
    """Sina rows -> Yahoo-shaped OHLCV frame holding ``Date`` as a column."""
    raw = pd.DataFrame(rows)
    dates = (
        pd.to_datetime(raw["date"], errors="coerce")
        if "date" in raw.columns
        else pd.Series([pd.NaT] * len(raw), index=raw.index)
    )
    frame = pd.DataFrame(
        {
            "Date": dates,
            "Open": _numeric(raw, "open"),
            "High": _numeric(raw, "high"),
            "Low": _numeric(raw, "low"),
            "Close": _numeric(raw, "close"),
            "Volume": _numeric(raw, "volume"),
        }
    )
    # Crypto spot has no dividends or splits, so the adjusted close is the close.
    frame["Adj Close"] = frame["Close"]
    frame = frame.dropna(subset=["Date", "Close"])
    frame = frame.sort_values("Date").drop_duplicates("Date", keep="last")
    return frame.reset_index(drop=True)


def _fetch_daily(base: str) -> pd.DataFrame:
    """Download the full daily history for ``base``, retrying transient failures."""
    last_error: Exception | None = None
    for attempt in range(_MAX_RETRIES + 1):
        try:
            response = requests.get(
                _DAILY_URL.format(symbol=base), headers=_HEADERS, timeout=_TIMEOUT
            )
            response.raise_for_status()
            rows = _parse_daily(response.text)
            if not rows:
                # An answer with no rows is about the symbol, not the transport.
                raise NoMarketDataError(base, base, "sina returned an empty daily series")
            frame = _to_frame(rows)
            if frame.empty:
                raise NoMarketDataError(base, base, "sina rows carried no usable prices")
            return frame
        except NoMarketDataError:
            raise
        except Exception as exc:  # network/transport: try again, then give up
            last_error = exc
            if attempt < _MAX_RETRIES:
                delay = 1.5 * (2 ** attempt)
                logger.warning(
                    "Sina Finance request failed (%s), retrying in %.1fs (attempt %d/%d)",
                    type(exc).__name__, delay, attempt + 1, _MAX_RETRIES,
                )
                time.sleep(delay)

    raise VendorUnavailableError(
        f"Sina Finance request failed after {_MAX_RETRIES} retries: "
        f"{type(last_error).__name__}"
    )


def load_ohlcv_sina(symbol: str, as_of_date: str, fill_gaps: bool = True) -> pd.DataFrame:
    """Sina OHLCV up to ``as_of_date``, cached per symbol on disk.

    Mirrors ``yahoo.ohlcv.load_ohlcv``: one cache file per symbol, rows after
    ``as_of_date`` filtered out so a backtest never reads the future, gaps
    carried forward for indicator continuity, and a stale frame rejected before
    it can feed an indicator.
    """
    canonical, base = _resolve(symbol)

    config = get_config()
    as_of_dt = pd.to_datetime(as_of_date).normalize()
    now = pd.Timestamp.today()

    os.makedirs(config["data_cache_dir"], exist_ok=True)
    data_file = os.path.join(
        config["data_cache_dir"], f"{safe_ticker_component(canonical)}-Sina-data.csv"
    )

    data = None
    if os.path.exists(data_file):
        try:
            cached = pd.read_csv(data_file, on_bad_lines="skip", encoding="utf-8")
        except Exception as exc:
            logger.warning("Unreadable Sina cache %s (%s); refetching", data_file, exc)
            cached = None
        if (
            cached is not None
            and not cached.empty
            and "Close" in cached.columns
            and _cache_is_fresh(data_file, as_of_dt, now)
        ):
            data = cached

    if data is None:
        downloaded = _fetch_daily(base)
        replace_file(
            data_file,
            lambda temp: downloaded.to_csv(temp, index=False, encoding="utf-8"),
        )
        data = downloaded

    data = _clean_dataframe(data)
    data = data[data["Date"] <= as_of_dt]
    if data.empty:
        raise NoMarketDataError(
            symbol, canonical, f"no {base} bar on or before {as_of_date}"
        )

    data = _fill_price_gaps(data) if fill_gaps else data.dropna(subset=["Close"]).copy()
    _assert_ohlcv_not_stale(data, as_of_date, symbol, canonical)
    return data


def get_sina_stock(
    symbol: Annotated[str, "ticker symbol of the company"],
    start_date: Annotated[str, "Start date in yyyy-mm-dd format"],
    end_date: Annotated[str, "End date in yyyy-mm-dd format"],
) -> str:
    """Daily OHLCV for ``start_date``..``end_date``, formatted like Yahoo's."""
    datetime.strptime(start_date, "%Y-%m-%d")
    datetime.strptime(end_date, "%Y-%m-%d")

    canonical = normalize_symbol(symbol)
    window = load_ohlcv_sina(symbol, end_date, fill_gaps=False)

    start_dt = pd.to_datetime(start_date)
    end_dt = pd.to_datetime(end_date)
    window = window[(window["Date"] >= start_dt) & (window["Date"] <= end_dt)].copy()
    if window.empty:
        raise NoMarketDataError(
            symbol, canonical, f"no rows between {start_date} and {end_date}"
        )

    for column in ("Open", "High", "Low", "Close", "Adj Close"):
        if column in window.columns:
            window[column] = window[column].round(2)
    window["Date"] = window["Date"].dt.strftime("%Y-%m-%d")

    label = canonical if canonical == symbol.upper() else f"{canonical} (from {symbol})"
    header = f"# Stock data for {label} from {start_date} to {end_date}\n"
    header += "# Source: Sina Finance daily series (Yahoo Finance is unavailable in mainland China)\n"
    header += f"# Total records: {len(window)}\n\n"

    return header + window.to_csv(index=False)


def get_sina_indicator(
    symbol: Annotated[str, "ticker symbol of the company"],
    indicator: Annotated[str, "technical indicator to get the analysis and report of"],
    as_of_date: Annotated[
        str, "The current trading date you are trading on, YYYY-mm-dd"
    ],
    look_back_days: Annotated[int, "how many days to look back"],
) -> str:
    """A technical indicator over Sina bars, computed by the Yahoo implementation.

    The indicator math and its report format live in one place; only the OHLCV
    source is swapped, so an indicator reads the same on either vendor.
    """
    from tradingagents.dataflows.vendors.yahoo.market import (
        get_stock_stats_indicators_window,
    )

    if indicator in _VOLUME_INDICATORS:
        raise NoMarketDataError(
            symbol,
            normalize_symbol(symbol),
            f"{indicator} needs volume, which Sina reports as 0 for crypto bars",
            definitive=True,
        )

    return get_stock_stats_indicators_window(
        symbol, indicator, as_of_date, look_back_days, loader=load_ohlcv_sina
    )
