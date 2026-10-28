"""Binance spot candles for crypto pairs, in Yahoo's output shape.

Why this vendor exists: the analyst stack can already read ``BTC-USD`` from
Yahoo / Sina, but neither is an exchange feed — Yahoo is geo-blocked from
mainland China and Sina publishes a single BTC daily series with zero volume.
Binance's public market-data API (no key required) covers every spot pair that
trades on the exchange and reports real volume, so volume-based indicators
(VWMA, MFI) become computable for crypto.

Scope is deliberate: only crypto pairs resolve. Anything else raises
``NoMarketDataError`` before a request is made, so the router's vendor chain
moves straight on to the next source for equities, FX, futures or indices.

Binance is listed after OKX in the default chain; it is also the deeper book
for altcoin pairs, so it is the natural second opinion when OKX misses a pair.
"""

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

#: Mirrors, tried in order. ``data-api.binance.vision`` is Binance's public
#: market-data-only host (it never requires a key and is not geo-fenced the way
#: ``api.binance.com`` can be), ``api-gcp.binance.com`` is the documented
#: alternate endpoint.
_HOSTS = (
    "https://api.binance.com",
    "https://data-api.binance.vision",
    "https://api-gcp.binance.com",
)
_KLINES_PATH = "/api/v3/klines"

#: Quote currency for the spot pair. USDT is the deepest book on Binance.
_QUOTE = "USDT"

#: Binance daily bars are labelled at 00:00 UTC.
_INTERVAL = "1d"

_TIMEOUT = 12.0
_MAX_RETRIES = 2

#: ``/api/v3/klines`` caps a page at 1000 rows (~2.7 years of daily bars).
_PAGE_LIMIT = 1000
_HISTORY_YEARS = 5
_MAX_PAGES = 6

#: An explicit pair the symbol table may not list (PEPE-USD, WIF/USDT). Its base
#: is accepted as-is; the exchange's own pair list is the real coverage check.
_EXPLICIT_PAIR = re.compile(r"^([A-Z0-9]{2,10})[-/](?:USD|USDT|USDC)$")


def supports_symbol(symbol: str) -> bool:
    """Whether Binance can serve ``symbol`` — lets callers route without a failed try."""
    try:
        _resolve(symbol)
    except NoMarketDataError:
        return False
    return True


def _resolve(symbol: str) -> tuple[str, str]:
    """``(canonical, base)`` for a crypto pair, else ``NoMarketDataError``.

    ``BTC-USD``, ``BTCUSDT`` and ``BTC-USDT`` all resolve to base ``BTC`` and
    are quoted against USDT. An explicit ``BASE-USD`` / ``BASE/USDT`` pair is
    accepted even for a base the symbol table does not list (``PEPE-USD``), so
    the exchange's own pair list decides coverage; ``NVDA``, ``600519.SS`` and
    ``XAUUSD`` do not resolve at all, and are rejected without a network call.
    """
    canonical = normalize_symbol(symbol)
    base = crypto_base(canonical) or crypto_base(symbol)
    if base is None:
        # A base outside the symbol table's crypto list still counts when the
        # caller spelled an explicit stablecoin quote (PEPE-USD, WIF/USDT).
        match = _EXPLICIT_PAIR.match(symbol.strip().upper().rstrip("+"))
        base = match.group(1) if match else None
    if base is None:
        raise NoMarketDataError(
            symbol,
            canonical,
            "Binance vendor serves crypto pairs only (e.g. BTC-USD, ETH-USD)",
        )
    return canonical, base


def _pair(base: str) -> str:
    return f"{base}{_QUOTE}"


def _get(params: dict) -> dict | list:
    """GET klines from the first reachable host, retrying transient failures."""
    last_error: Exception | None = None
    for host in _HOSTS:
        url = f"{host}{_KLINES_PATH}"
        for attempt in range(_MAX_RETRIES + 1):
            try:
                response = requests.get(
                    url,
                    params=params,
                    headers={"Accept": "application/json", "User-Agent": "tradingagents/binance"},
                    timeout=_TIMEOUT,
                )
                if response.status_code in (400, 404):
                    # An unknown symbol is about the pair, not the transport.
                    raise NoMarketDataError(
                        str(params.get("symbol", "")),
                        str(params.get("symbol", "")),
                        f"Binance rejected {params.get('symbol')}: {response.text[:120]}",
                        definitive=True,
                    )
                response.raise_for_status()
                return response.json()
            except NoMarketDataError:
                raise
            except Exception as exc:  # noqa: BLE001 - transport, retry then fall through
                last_error = exc
                if attempt < _MAX_RETRIES:
                    time.sleep(1.0 * (2**attempt))
        logger.info(
            "Binance host %s unreachable (%s); trying next mirror", host, type(last_error).__name__
        )

    raise VendorUnavailableError(
        f"Binance request failed on every mirror: {type(last_error).__name__}"
    )


def _to_frame(rows: list) -> pd.DataFrame:
    """Binance klines -> Yahoo-shaped OHLCV frame holding ``Date`` as a column.

    A kline row is ``[openTime, o, h, l, c, volume, closeTime, quoteVolume,
    trades, ...]``, oldest first, with ``openTime`` in epoch milliseconds.
    """
    records = [row for row in rows if isinstance(row, (list, tuple)) and len(row) >= 6]
    if not records:
        return pd.DataFrame()

    raw = pd.DataFrame(records)
    stamps = pd.to_numeric(pd.Series([row[0] for row in records]), errors="coerce")
    dates = pd.to_datetime(stamps, unit="ms", utc=True, errors="coerce")
    frame = pd.DataFrame(
        {
            "Date": dates.dt.tz_localize(None).dt.normalize(),
            "Open": pd.to_numeric(pd.Series([row[1] for row in records]), errors="coerce"),
            "High": pd.to_numeric(pd.Series([row[2] for row in records]), errors="coerce"),
            "Low": pd.to_numeric(pd.Series([row[3] for row in records]), errors="coerce"),
            "Close": pd.to_numeric(pd.Series([row[4] for row in records]), errors="coerce"),
            "Volume": pd.to_numeric(pd.Series([row[5] for row in records]), errors="coerce"),
        }
    )
    # Crypto spot has no dividends or splits, so the adjusted close is the close.
    frame["Adj Close"] = frame["Close"]
    frame = frame.dropna(subset=["Date", "Close"])
    frame = frame.sort_values("Date").drop_duplicates("Date", keep="last")
    return frame.reset_index(drop=True)


def _fetch_daily(base: str) -> pd.DataFrame:
    """Download ~5 years of daily bars for ``base``, walking back page by page."""
    pair = _pair(base)
    start_dt = pd.Timestamp.today().normalize() - pd.DateOffset(years=_HISTORY_YEARS)

    rows: list = []
    end_time: int | None = None
    for _ in range(_MAX_PAGES):
        params = {"symbol": pair, "interval": _INTERVAL, "limit": str(_PAGE_LIMIT)}
        if end_time is not None:
            params["endTime"] = str(end_time)
        page = _get(params)
        if not isinstance(page, list) or not page:
            break
        rows = page + rows
        oldest_open = min(
            (int(row[0]) for row in page if isinstance(row, (list, tuple)) and row),
            default=None,
        )
        if oldest_open is None:
            break
        if pd.to_datetime(oldest_open, unit="ms", utc=True).normalize().tz_localize(None) <= start_dt:
            break
        if len(page) < _PAGE_LIMIT:
            break
        # ``endTime`` is inclusive, so step one millisecond before the oldest row.
        end_time = oldest_open - 1

    frame = _to_frame(rows)
    if frame.empty:
        raise NoMarketDataError(base, pair, "Binance returned no usable klines")
    return frame


def load_ohlcv_binance(symbol: str, as_of_date: str, fill_gaps: bool = True) -> pd.DataFrame:
    """Binance OHLCV up to ``as_of_date``, cached per symbol on disk.

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
        config["data_cache_dir"], f"{safe_ticker_component(canonical)}-Binance-data.csv"
    )

    data = None
    if os.path.exists(data_file):
        try:
            cached = pd.read_csv(data_file, on_bad_lines="skip", encoding="utf-8")
        except Exception as exc:  # noqa: BLE001 - a corrupt cache is just a miss
            logger.warning("Unreadable Binance cache %s (%s); refetching", data_file, exc)
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


def get_binance_stock(
    symbol: Annotated[str, "ticker symbol of the crypto pair"],
    start_date: Annotated[str, "Start date in yyyy-mm-dd format"],
    end_date: Annotated[str, "End date in yyyy-mm-dd format"],
) -> str:
    """Daily OHLCV for ``start_date``..``end_date``, formatted like Yahoo's."""
    datetime.strptime(start_date, "%Y-%m-%d")
    datetime.strptime(end_date, "%Y-%m-%d")

    canonical = normalize_symbol(symbol)
    window = load_ohlcv_binance(symbol, end_date, fill_gaps=False)

    start_dt = pd.to_datetime(start_date)
    end_dt = pd.to_datetime(end_date)
    window = window[(window["Date"] >= start_dt) & (window["Date"] <= end_dt)].copy()
    if window.empty:
        raise NoMarketDataError(
            symbol, canonical, f"no rows between {start_date} and {end_date}"
        )

    # Crypto prices span orders of magnitude (BTC ~1e5, PEPE ~1e-5), so the
    # rows are kept at the exchange's own precision rather than rounded.
    window["Date"] = window["Date"].dt.strftime("%Y-%m-%d")

    label = canonical if canonical == symbol.upper() else f"{canonical} (from {symbol})"
    header = f"# Stock data for {label} from {start_date} to {end_date}\n"
    header += f"# Source: Binance spot {_pair(crypto_base(canonical) or canonical)} daily klines\n"
    header += f"# Total records: {len(window)}\n\n"

    return header + window.to_csv(index=False)


def get_binance_indicator(
    symbol: Annotated[str, "ticker symbol of the crypto pair"],
    indicator: Annotated[str, "technical indicator to get the analysis and report of"],
    as_of_date: Annotated[
        str, "The current trading date you are trading on, YYYY-mm-dd"
    ],
    look_back_days: Annotated[int, "how many days to look back"],
) -> str:
    """A technical indicator over Binance bars, computed by the Yahoo implementation.

    The indicator math and its report format live in one place; only the OHLCV
    source is swapped, so an indicator reads the same on either vendor.
    """
    from tradingagents.dataflows.vendors.yahoo.market import (
        get_stock_stats_indicators_window,
    )

    return get_stock_stats_indicators_window(
        symbol, indicator, as_of_date, look_back_days, loader=load_ohlcv_binance
    )
