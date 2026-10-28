"""Eastmoney 7x24 fast-news vendor: a mainland-China-reachable news source.

Yahoo Finance is region-blocked here (HTTP 403 since 2021-11-01), so its news
endpoints answer with a decode failure rather than headlines. Eastmoney's public
``getFastNewsList`` feed needs no key and returns titled, timestamped items,
which is what this vendor serves in Yahoo's report shape.

Coverage note: the feed is a market-wide 7x24 wire, not a per-symbol newsroom.
A symbol's report therefore carries the items that mention it, and when none do,
the report says so explicitly instead of passing market chatter off as its news.
"""

import logging
import re
import time
from datetime import datetime, timedelta

import requests

from tradingagents.dataflows.config import get_config
from tradingagents.dataflows.date_window import coverage_gap
from tradingagents.dataflows.errors import VendorUnavailableError
from tradingagents.dataflows.symbols import crypto_base, normalize_symbol

logger = logging.getLogger(__name__)

_FEED_URL = "https://np-listapi.eastmoney.com/comm/web/getFastNewsList"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
    ),
    "Referer": "https://kuaixun.eastmoney.com/",
}
_TIMEOUT = 15.0
_PAGE_SIZE = 100
_MAX_PAGES = 14
_MAX_RETRIES = 2

# Words that mark a wire item as crypto coverage, per base asset.
_ASSET_KEYWORDS = {
    "BTC": ("比特币", "BTC", "bitcoin"),
    "ETH": ("以太坊", "ETH", "ethereum"),
    "SOL": ("SOL", "Solana"),
    "XRP": ("XRP", "瑞波"),
    "DOGE": ("狗狗币", "DOGE"),
}
_CRYPTO_WORDS = ("加密货币", "数字货币", "虚拟货币", "加密资产", "稳定币", "区块链", "币圈")

_TAG = re.compile(r"<[^>]+>")


def _plain(text: str) -> str:
    """Wire items embed markup; the report is plain text."""
    return _TAG.sub("", text or "").replace("\xa0", " ").strip()


def _fetch_page(cursor: str = "") -> list[dict]:
    """One page of the fast-news wire older than ``cursor``, retried on transport errors.

    The wire pages by a ``sortEnd`` cursor (the oldest item's ``realSort``), not by
    page number: ``pageIndex`` is accepted and ignored, so only the cursor moves
    the window back in time.
    """
    last_error: Exception | None = None
    for attempt in range(_MAX_RETRIES + 1):
        try:
            response = requests.get(
                _FEED_URL,
                params={
                    "client": "web",
                    "biz": "web_724",
                    "fastColumn": "102",
                    "sortEnd": cursor,
                    "pageSize": _PAGE_SIZE,
                    "req_trace": int(time.time() * 1000),
                },
                headers=_HEADERS,
                timeout=_TIMEOUT,
            )
            response.raise_for_status()
            payload = response.json()
            items = (payload.get("data") or {}).get("fastNewsList") or []
            return [i for i in items if isinstance(i, dict)]
        except Exception as exc:
            last_error = exc
            if attempt < _MAX_RETRIES:
                time.sleep(1.5 * (2 ** attempt))
    raise VendorUnavailableError(
        f"Eastmoney fast-news request failed after {_MAX_RETRIES} retries: "
        f"{type(last_error).__name__}"
    )


def _published(item: dict) -> datetime | None:
    stamp = item.get("showTime") or ""
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(stamp, fmt)
        except ValueError:
            continue
    return None


def _collect(since: datetime, until: datetime, max_items: int) -> list[dict]:
    """Wire items in ``[since, until]``, newest first, following the cursor back.

    Stops at the window's start, at ``max_items``, or at ``_MAX_PAGES`` — the wire
    carries well over a thousand items a day, so a long window is read from its
    newest end rather than in full.
    """
    collected: list[dict] = []
    seen: set[str] = set()
    cursor = ""
    for _ in range(_MAX_PAGES):
        items = _fetch_page(cursor)
        if not items:
            break
        oldest = None
        for item in items:
            published = _published(item)
            if published is None:
                continue
            oldest = published if oldest is None or published < oldest else oldest
            if published > until or published < since:
                continue
            key = (item.get("code") or "") + _plain(item.get("title") or "")
            if key in seen:
                continue
            seen.add(key)
            collected.append(
                {
                    "title": _plain(item.get("title") or "No title"),
                    "summary": _plain(item.get("summary") or ""),
                    "publisher": "东方财富 7x24",
                    "link": "",
                    "pub_date": published,
                }
            )
        if len(collected) >= max_items:
            break
        # Stop once the wire has been read past the window's start.
        if oldest is not None and oldest < since:
            break
        next_cursor = str(items[-1].get("realSort") or "")
        if not next_cursor or next_cursor == cursor:
            break
        cursor = next_cursor
    collected.sort(key=lambda a: a["pub_date"], reverse=True)
    return collected[:max_items]


def _format_articles(articles: list[dict]) -> str:
    out = ""
    for data in articles:
        out += f"### {data['title']} (source: {data['publisher']})\n"
        if data["summary"]:
            out += f"{data['summary']}\n"
        out += "\n"
    return out


def _asset_keywords(canonical: str) -> tuple[str, ...]:
    base = crypto_base(canonical)
    if base and base in _ASSET_KEYWORDS:
        return _ASSET_KEYWORDS[base]
    if base:
        return (base,)
    # Non-crypto symbols are outside this feed's subject matter.
    return ()


def _mentions(article: dict, keywords: tuple[str, ...]) -> bool:
    haystack = f"{article['title']} {article['summary']}"
    lowered = haystack.lower()
    return any(
        (keyword.lower() in lowered) if keyword.isascii() else (keyword in haystack)
        for keyword in keywords
    )


def get_news_eastmoney(ticker: str, start_date: str, end_date: str) -> str:
    """Titled, timestamped wire items about ``ticker`` within the window."""
    limit = get_config()["news_article_limit"]
    canonical = normalize_symbol(ticker)
    resolved = "" if canonical == ticker else f" (resolved to {canonical})"

    keywords = _asset_keywords(canonical)
    if not keywords:
        raise VendorUnavailableError(
            f"Eastmoney 7x24 covers crypto and macro topics, not {canonical}"
        )

    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1) - timedelta(seconds=1)
    articles = _collect(start_dt, end_dt, _MAX_PAGES * _PAGE_SIZE)
    if not articles:
        raise VendorUnavailableError(
            f"Eastmoney 7x24 returned no items between {start_date} and {end_date}"
        )

    matched = [a for a in articles if _mentions(a, keywords)]
    crypto_tagged = [a for a in articles if _mentions(a, _CRYPTO_WORDS)]
    coverage = (f"Wire scanned from {articles[-1]['pub_date']:%Y-%m-%d %H:%M} to "
                f"{articles[0]['pub_date']:%Y-%m-%d %H:%M} "
                f"({len(articles)} items read from the newest end; a long window is "
                f"not read in full).\n\n")

    if matched:
        context = [a for a in crypto_tagged if a not in matched][: max(0, limit - len(matched))]
        return (
            f"## {ticker}{resolved} News, from {start_date} to {end_date}:\n\n"
            f"Coverage: {coverage}"
            f"{_format_articles(matched[:limit])}"
            f"{_format_articles(context)}"
        )

    if crypto_tagged:
        return (
            f"## {ticker}{resolved} News, from {start_date} to {end_date}:\n\n"
            f"Coverage: {coverage}"
            f"Note: no wire item named {canonical} in the scan above. The crypto-sector "
            f"items below come from the same wire as context, not as news specific to "
            f"{canonical}.\n\n"
            f"{_format_articles(crypto_tagged[:limit])}"
        )

    gap = coverage_gap(
        (a["pub_date"] for a in articles),
        start_date, end_date, "Eastmoney 7x24 news", f"news for {ticker}",
    )
    return gap or (
        f"## {ticker}{resolved} News, from {start_date} to {end_date}:\n\n"
        f"Coverage: {coverage}"
        f"No wire item in the scan mentions {canonical} or the crypto sector."
    )


def get_global_news_eastmoney(
    as_of_date: str,
    look_back_days: int | None = None,
    limit: int | None = None,
) -> str:
    """Market-wide 7x24 wire for the look-back window, newest first."""
    config = get_config()
    if look_back_days is None:
        look_back_days = config["global_news_lookback_days"]
    if limit is None:
        limit = config["global_news_article_limit"]

    curr_dt = datetime.strptime(as_of_date, "%Y-%m-%d")
    start_dt = curr_dt - timedelta(days=look_back_days)
    start_date = start_dt.strftime("%Y-%m-%d")

    articles = _collect(
        start_dt, curr_dt + timedelta(days=1) - timedelta(seconds=1), max(limit * 15, 200)
    )
    if not articles:
        gap = coverage_gap(
            (), start_date, as_of_date, "Eastmoney 7x24 news", "market news",
        )
        return gap or f"No global news found between {start_date} and {as_of_date}"

    coverage = (f"Wire scanned from {articles[-1]['pub_date']:%Y-%m-%d %H:%M} to "
                f"{articles[0]['pub_date']:%Y-%m-%d %H:%M} "
                f"({len(articles)} items read from the newest end).\n\n")
    return (
        f"## Global Market News, from {start_date} to {as_of_date}:\n\n"
        f"Coverage: {coverage}"
        f"{_format_articles(articles[:limit])}"
    )
