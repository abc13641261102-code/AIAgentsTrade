"""Vendor data-error taxonomy.

A single hierarchy so the routing layer reacts by *behavior*, not by vendor:
every condition where a vendor cannot return usable data derives from
``VendorError``, and the router catches the base types. A new vendor raises
these (or a thin vendor-named subclass) and needs no new ``except`` clause.

    VendorError
    ├── NoMarketDataError          no usable rows (empty result OR stale data)
    ├── VendorUnavailableError     throttled or failed request -> skip to next vendor
    └── VendorNotConfiguredError   missing API key/config -> vendor unavailable

The number of types is the number of distinct router reactions, not the number
of human-describable causes: empty and stale data get identical handling, so
they share ``NoMarketDataError`` and differ only in the free-text ``detail``.
"""

from __future__ import annotations


class VendorError(Exception):
    """Base for any condition where a vendor could not return usable data."""


class NoMarketDataError(VendorError):
    """A vendor returned no usable rows for a symbol (empty result or stale data).

    Carries both the symbol the user requested and the canonical symbol the
    vendor was actually queried with, plus a free-text ``detail``, so callers
    can build a clear message instead of emitting a vendor-specific empty
    string into the data channel.
    """

    def __init__(
        self,
        symbol: str,
        canonical: str | None = None,
        detail: str = "",
        definitive: bool = False,
    ):
        self.symbol = symbol
        self.canonical = canonical or symbol
        self.detail = detail
        # ``definitive`` marks a verdict about the DATA itself (e.g. "this bar
        # series carries no volume, so a volume-weighted indicator cannot be
        # computed") rather than about this vendor's coverage. The router then
        # reports it even when another vendor merely timed out, so the analyst
        # is not told "vendor request failed" when the field simply isn't there.
        self.definitive = definitive
        msg = f"No market data for {symbol!r}"
        if canonical and canonical != symbol:
            msg += f" (queried as {canonical!r})"
        if detail:
            msg += f": {detail}"
        super().__init__(msg)


class VendorUnavailableError(VendorError):
    """A vendor could not answer: it throttled or failed the request.

    Says nothing about the instrument; the router skips to the next vendor.
    """


class VendorNotConfiguredError(VendorError, ValueError):
    """A vendor was selected but its API key/configuration is missing.

    Also a ``ValueError`` so existing callers that catch ``ValueError`` keep
    working while the routing layer can treat it as "vendor unavailable".
    """
