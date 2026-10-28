"""OKX (欧易) spot candles for crypto pairs, in Yahoo's output shape.

Why this vendor exists: the analyst stack can already read ``BTC-USD`` from
Yahoo / Sina, but neither is an exchange feed — Yahoo is geo-blocked from
mainland China and Sina publishes a single BTC daily series with zero volume.
OKX's public market-data API (no key required) covers every spot pair that
trades on the exchange and reports real volume, so technical indicators that
depend on volume (VWMA, MFI) become computable for crypto.

Scope is deliberate: only crypto pairs resolve. Anything else raises
``NoMarketDataError`` before a request is made, so the router's vendor chain
moves straight on to the next source for equities, FX, futures or indices.
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

#: Mirrors, tried in order. ``aws.okx.com`` is the fallback endpoint OKX
#: documents for networks where the main host is unreachable.
_HOSTS = ("https://www.okx.com", "https://aws.okx.com")
_CANDLES_PATH = "/api/v5/market/history-candles"

#: Quote currency for the spot pair. USDT is the deepest book on OKX.
_QUOTE = "USDT"

#: Daily bars are labelled at UTC+8 midnight on OKX; the timestamp conversion
#: below reproduces that boundary so a bar's date matches what the exchange
#: shows on its own chart.
_BAR = "1D"
_BAR_TZ = "Asia/Shanghai"

_TIMEOUT = 12.0
_MAX_RETRIES = 2

#: ``history-candles`` caps a page at 100 rows.
_PAGE_LIMIT = 100
_HISTORY_YEARS = 5
#: Page ceiling: 5 years of daily bars needs ~19 pages; the cap only bounds a
#: pathological response that never walks back far enough.
_MAX_PAGES = 40

#: An explicit pair the symbol table may not list (PEPE-USD, WIF/USDT). Its base
#: is accepted as-is; the exchange's own pair list is the real coverage check.
_EXPLICIT_PAIR = re.compile(r"^([A-Z0-9]{2,10})[-/](?:USD|USDT|USDC)$")


def supports_symbol(symbol: str) -> bool:
    """Whether OKX can serve ``symbol`` — lets callers route without a failed try."""
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
            "OKX vendor serves crypto pairs only (e.g. BTC-USD, ETH-USD)",
        )
    return canonical, base


def _inst_id(base: str) -> str:
    return f"{base}-{_QUOTE}"


def _get(path: str, params: dict) -> tuple[dict, str]:
    """GET ``path`` from the first reachable host, retrying transient failures."""
    last_error: Exception | None = None
    for host in _HOSTS:
        url = f"{host}{path}"
        for attempt in range(_MAX_RETRIES + 1):
            try:
                response = requests.get(
                    url,
                    params=params,
                    headers={"Accept": "application/json", "User-Agent": "tradingagents/okx"},
                    timeout=_TIMEOUT,
                )
                response.raise_for_status()
                return response.json(), host
            except Exception as exc:  # noqa: BLE001 - transport, retry then fall through
                last_error = exc
                if attempt < _MAX_RETRIES:
                    time.sleep(1.0 * (2**attempt))
        logger.info("OKX host %s unreachable (%s); trying next mirror", host, type(last_error).__name__)

    raise VendorUnavailableError(
        f"OKX request failed on every mirror: {type(last_error).__name__}"
    )


def _to_frame(rows: list) -> pd.DataFrame:
    """OKX candles -> Yahoo-shaped OHLCV frame holding ``Date`` as a column.

    A candle row is ``[ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm]``,
    newest first, with ``confirm == "0"`` for the bar still forming.
    """
    records = []
    for row in rows:
        if not isinstance(row, (list, tuple)) or len(row) < 6:
            continue
        # The in-progress bar's close is not a close; keep only settled bars.
        if len(row) > 8 and str(row[8]) == "0":
            continue
        records.append(row[:6])

    if not records:
        return pd.DataFrame()

    raw = pd.DataFrame(records, columns=["ts", "Open", "High", "Low", "Close", "Volume"])
    stamps = pd.to_numeric(raw["ts"], errors="coerce")
    dates = pd.to_datetime(stamps, unit="ms", utc=True, errors="coerce")
    frame = pd.DataFrame(
        {
            "Date": dates.dt.tz_convert(_BAR_TZ).dt.tz_localize(None).dt.normalize(),
            "Open": pd.to_numeric(raw["Open"], errors="coerce"),
            "High": pd.to_numeric(raw["High"], errors="coerce"),
            "Low": pd.to_numeric(raw["Low"], errors="coerce"),
            "Close": pd.to_numeric(raw["Close"], errors="coerce"),
            "Volume": pd.to_numeric(raw["Volume"], errors="coerce"),
        }
    )
    # Crypto spot has no dividends or splits, so the adjusted close is the close.
    frame["Adj Close"] = frame["Close"]
    frame = frame.dropna(subset=["Date", "Close"])
    frame = frame.sort_values("Date").drop_duplicates("Date", keep="last")
    return frame.reset_index(drop=True)


def _fetch_daily(base: str) -> pd.DataFrame:
    """Download ~5 years of daily bars for ``base``, walking back page by page."""
    inst_id = _inst_id(base)
    start_dt = pd.Timestamp.today().normalize() - pd.DateOffset(years=_HISTORY_YEARS)

    rows: list = []
    after: str | None = None
    for _ in range(_MAX_PAGES):
        params = {"instId": inst_id, "bar": _BAR, "limit": str(_PAGE_LIMIT)}
        if after:
            # OKX returns candles *older* than ``after``.
            params["after"] = after
        payload, _host = _get(_CANDLES_PATH, params)

        code = str(payload.get("code", ""))
        if code not in ("0", ""):
            message = str(payload.get("msg") or "unknown error")
            if code in ("51001", "51000"):  # instrument does not exist
                raise NoMarketDataError(
                    base, inst_id, f"OKX has no spot pair {inst_id}: {message}", definitive=True
                )
            raise VendorUnavailableError(f"OKX error {code}: {message}")

        page = payload.get("data") or []
        if not page:
            break
        rows.extend(page)

        oldest_ts = min(
            (str(item[0]) for item in page if isinstance(item, (list, tuple)) and item),
            key=lambda value: int(value),
            default=None,
        )
        if oldest_ts is None:
            break
        oldest = pd.to_datetime(int(oldest_ts), unit="ms", utc=True)
        if oldest.tz_convert(_BAR_TZ).tz_localize(None).normalize() <= start_dt:
            break
        if len(page) < _PAGE_LIMIT:
            break
        after = oldest_ts

    frame = _to_frame(rows)
    if frame.empty:
        raise NoMarketDataError(base, inst_id, "OKX returned no usable candles")
    return frame


def load_ohlcv_okx(symbol: str, as_of_date: str, fill_gaps: bool = True) -> pd.DataFrame:
    """OKX OHLCV up to ``as_of_date``, cached per symbol on disk.

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
        config["data_cache_dir"], f"{safe_ticker_component(canonical)}-OKX-data.csv"
    )

    data = None
    if os.path.exists(data_file):
        try:
            cached = pd.read_csv(data_file, on_bad_lines="skip", encoding="utf-8")
        except Exception as exc:  # noqa: BLE001 - a corrupt cache is just a miss
            logger.warning("Unreadable OKX cache %s (%s); refetching", data_file, exc)
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


def get_okx_stock(
    symbol: Annotated[str, "ticker symbol of the crypto pair"],
    start_date: Annotated[str, "Start date in yyyy-mm-dd format"],
    end_date: Annotated[str, "End date in yyyy-mm-dd format"],
) -> str:
    """Daily OHLCV for ``start_date``..``end_date``, formatted like Yahoo's."""
    datetime.strptime(start_date, "%Y-%m-%d")
    datetime.strptime(end_date, "%Y-%m-%d")

    canonical = normalize_symbol(symbol)
    window = load_ohlcv_okx(symbol, end_date, fill_gaps=False)

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
    header += f"# Source: OKX spot {_inst_id(crypto_base(canonical) or canonical)} daily candles\n"
    header += f"# Total records: {len(window)}\n\n"

    return header + window.to_csv(index=False)


def get_okx_indicator(
    symbol: Annotated[str, "ticker symbol of the crypto pair"],
    indicator: Annotated[str, "technical indicator to get the analysis and report of"],
    as_of_date: Annotated[
        str, "The current trading date you are trading on, YYYY-mm-dd"
    ],
    look_back_days: Annotated[int, "how many days to look back"],
) -> str:
    """A technical indicator over OKX bars, computed by the Yahoo implementation.

    The indicator math and its report format live in one place; only the OHLCV
    source is swapped, so an indicator reads the same on either vendor.
    """
    from tradingagents.dataflows.vendors.yahoo.market import (
        get_stock_stats_indicators_window,
    )

    return get_stock_stats_indicators_window(
        symbol, indicator, as_of_date, look_back_days, loader=load_ohlcv_okx
    )
