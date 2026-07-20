"""Typed error hierarchy for xret.data.

All errors raised by this package inherit from :class:`XretDataError`, and
every raised instance chains through `raise ... from exc` when it wraps an
underlying CCXT, SQLite, or filesystem exception. Callers can catch the
whole family with one `except` clause, or catch a specific subclass to
react to one failure category. This is the exact 8-name public hierarchy
(Decision 21); no other public exception type exists.
"""

from __future__ import annotations

__all__ = [
    "XretDataError",
    "ConfigurationError",
    "InvalidRequestError",
    "UnsupportedMarketError",
    "ProviderError",
    "CoverageError",
    "SyncError",
    "CatalogError",
]


class XretDataError(Exception):
    """Base class for every error raised by xret.data."""


class ConfigurationError(XretDataError):
    """Xret configuration (`state_dir`, `data_dir`, `config.toml`) is invalid."""


class InvalidRequestError(XretDataError):
    """A caller-supplied value violates its documented contract.

    Examples include an unrecognized exchange slug or symbol syntax, an invalid
    timeframe, a naive datetime, `start >= end`, or `settle` supplied for spot.
    """


class UnsupportedMarketError(XretDataError):
    """The requested market family or capability is not operable.

    Raised for unsupported market families and provider capabilities, including
    unlisted symbols, unsupported timeframes, and ambiguous settlement.
    """


class ProviderError(XretDataError):
    """A market data provider (e.g. CCXT) call failed, or a `fetch`-path
    batch failed fatal data-quality validation.

    Distinct from `UnsupportedMarketError`, this signals a transient or
    permanent failure while attempting a supported call.
    """


class CoverageError(XretDataError):
    """Coverage interval bookkeeping is invalid or contradictory, or a
    `scan` request is not fully covered by local canonical data."""


class SyncError(XretDataError):
    """A synchronization operation could not complete, or an incomplete
    `SyncResult` was required to be complete."""


class CatalogError(XretDataError):
    """The catalog is inconsistent, unusable, or maintenance could not complete."""
