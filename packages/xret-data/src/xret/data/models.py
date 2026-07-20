"""Immutable domain values shared by the provider, storage and facade layers.

Everything here is a plain, frozen dataclass or `enum.Enum` with no I/O.
Construction validates its own contract (UTC-aware datetimes, half-open
`start < end`, path-safe dataset components, canonical market identity) so
downstream code can trust any instance it receives.
"""

from __future__ import annotations

import enum
import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Final

from xret.data.errors import InvalidRequestError, SyncError, UnsupportedMarketError
from xret.data.timeframe import TimeBar

if TYPE_CHECKING:
    import polars as pl

__all__ = [
    "CoverageStatus",
    "Market",
    "QualitySeverity",
    "MarketIdentity",
    "DataWarning",
    "DatasetKey",
    "NONE_SETTLE_SENTINEL",
    "YearMonth",
    "CoverageInterval",
    "SyncResult",
    "PartialScanResult",
    "CatalogValidationResult",
    "CatalogRebuildResult",
]


# --------------------------------------------------------------------------
# Enums
# --------------------------------------------------------------------------


class CoverageStatus(enum.Enum):
    """Per-interval coverage state for one dataset.

    Only observed facts persist: canonical data is ``AVAILABLE`` and a
    successful exact empty provider observation is ``UNAVAILABLE``.
    ``MISSING`` is computed from the absence of either fact and is never
    stored.
    """

    AVAILABLE = "available"
    UNAVAILABLE = "unavailable"
    MISSING = "missing"


class Market(enum.Enum):
    """Canonical market-family vocabulary (Decision 7).

    All four values are vocabulary-valid public strings. Only `SPOT` and
    `PERPETUAL` are operable in V1 (P-2): `FUTURE` and `OPTION` are
    structurally ambiguous without contract-attribute fields and always
    raise `UnsupportedMarketError` on `MarketIdentity` construction.
    """

    SPOT = "spot"
    PERPETUAL = "perpetual"
    FUTURE = "future"
    OPTION = "option"


#: Market families operable end-to-end in V1 (P-2).
_OPERABLE_MARKETS: frozenset[Market] = frozenset({Market.SPOT, Market.PERPETUAL})


class QualitySeverity(enum.Enum):
    """Severity of a data-quality finding."""

    #: Rejects the whole batch (schema/dtype/null/UTC, invariant, duplicate,
    #: ordering, or out-of-range violations).
    FATAL = "fatal"
    #: Recorded but does not block ingestion (timeframe gaps, statistical
    #: anomalies, provider coverage limits).
    WARNING = "warning"


# --------------------------------------------------------------------------
# Validation helpers
# --------------------------------------------------------------------------

_FORBIDDEN_PATH_CHARS: frozenset[str] = frozenset({"\\", "\0"})

#: Lowercase canonical exchange slug: `binance`, `okx`, `bybit`. No
#: uppercase, whitespace, or provider-native casing (Decision 6).
_EXCHANGE_PATTERN = re.compile(r"^[a-z][a-z0-9]*$")

#: A symbol has exactly one structural `BASE/QUOTE` boundary. Components are
#: otherwise any nonempty Unicode text representable as UTF-8.
_SYMBOL_PATTERN = re.compile(r"^[^/]+/[^/]+$")

#: Settlement is one nonempty UTF-8-representable component.
_SETTLE_PATTERN = re.compile(r"^[^/]+$")

#: Reserved empty-string sentinel for absent spot settlement. It is an internal
#: non-null operational identity for SQLite uniqueness; readable paths and
#: Parquet metadata retain the public distinction that spot has no settle.
#: Empty settlement components are invalid, so this cannot collide with a real
#: settlement currency.
NONE_SETTLE_SENTINEL: Final[str] = ""

_MARKET_BY_VALUE: dict[str, Market] = {member.value: member for member in Market}


def _ensure_path_safe(value: str, *, field_name: str, allow_slash: bool = False) -> str:
    """Validate that `value` is safe to use as one or more path components."""
    if not value:
        raise InvalidRequestError(f"{field_name} must not be empty")
    if value != value.strip():
        raise InvalidRequestError(
            f"{field_name} must not have leading/trailing whitespace: {value!r}"
        )
    for char in _FORBIDDEN_PATH_CHARS:
        if char in value:
            raise InvalidRequestError(f"{field_name} contains a forbidden character: {value!r}")
    if not allow_slash and "/" in value:
        raise InvalidRequestError(f"{field_name} must not contain '/': {value!r}")
    for segment in value.split("/"):
        if segment in ("", ".", ".."):
            raise InvalidRequestError(f"{field_name} contains an unsafe path segment: {value!r}")
    return value


def _ensure_utc_aware(value: datetime, *, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise InvalidRequestError(f"{field_name} must be timezone-aware: {value!r}")
    if value.utcoffset() != timedelta(0):
        raise InvalidRequestError(f"{field_name} must be UTC (zero offset): {value!r}")
    return value


def _coerce_market(value: Market | str) -> Market:
    if isinstance(value, Market):
        return value
    if isinstance(value, str):
        member = _MARKET_BY_VALUE.get(value)
        if member is not None:
            return member
        raise InvalidRequestError(
            f"unrecognized market: {value!r}; expected one of "
            f"{', '.join(m.value for m in Market)} (provider terms, shorthand, "
            "and plurals such as 'swap'/'perp'/'spots' are not accepted)"
        )
    raise InvalidRequestError(f"market must be a str or Market, got {value!r}")


def _validate_exchange(value: str) -> str:
    if not _EXCHANGE_PATTERN.match(value):
        raise InvalidRequestError(
            f"exchange must be a lowercase canonical slug (e.g. 'binance', 'okx'): {value!r}"
        )
    return value


def _normalize_representable(value: str, *, field_name: str) -> str:
    """NFC-normalize nonempty text that can be represented in UTF-8."""
    if not isinstance(value, str) or not value:
        raise InvalidRequestError(f"{field_name} must be a nonempty string: {value!r}")
    normalized = unicodedata.normalize("NFC", value)
    try:
        normalized.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise InvalidRequestError(
            f"{field_name} must contain UTF-8-representable Unicode text: {value!r}"
        ) from exc
    return normalized


def _validate_symbol(value: str) -> str:
    normalized = _normalize_representable(value, field_name="symbol")
    if not _SYMBOL_PATTERN.fullmatch(normalized):
        raise InvalidRequestError(
            f"symbol must contain exactly one '/' between nonempty BASE and QUOTE: {value!r}"
        )
    return normalized


def _validate_settle(value: str) -> str:
    normalized = _normalize_representable(value, field_name="settle")
    if not _SETTLE_PATTERN.fullmatch(normalized):
        raise InvalidRequestError(f"settle must not contain '/': {value!r}")
    return normalized


# --------------------------------------------------------------------------
# Market identity
# --------------------------------------------------------------------------


@dataclass(frozen=True, slots=True, kw_only=True)
class MarketIdentity:
    """Provider-independent public market identity (Decisions 4-9).

    `exchange` is a lowercase canonical slug, `symbol` is an NFC-normalized
    `BASE/QUOTE` pair with exactly one `/` boundary, and `market` is one of
    the canonical vocabulary values. Only `spot` and `perpetual` are
    operable in V1 (P-2); `future` and `option` are vocabulary-valid but
    always raise `UnsupportedMarketError`.

    `settle` is `None` for `spot` (providing one raises
    `InvalidRequestError`). For `perpetual`, `settle` may be omitted; safe
    inference from provider metadata (Decision 9) is a resolution-time
    concern (`fetch`/`sync`), not identity construction.
    """

    exchange: str
    symbol: str
    market: Market
    settle: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "market", _coerce_market(self.market))
        _validate_exchange(self.exchange)
        object.__setattr__(self, "symbol", _validate_symbol(self.symbol))
        if self.market not in _OPERABLE_MARKETS:
            raise UnsupportedMarketError(
                f"market {self.market.value!r} is not supported in V1: structurally "
                "ambiguous without contract-attribute fields; only 'spot' and "
                "'perpetual' are operable"
            )
        if self.market is Market.SPOT and self.settle is not None:
            raise InvalidRequestError("settle must not be provided for spot markets")
        if self.settle is not None:
            object.__setattr__(self, "settle", _validate_settle(self.settle))


# --------------------------------------------------------------------------
# Dataset / request identity
# --------------------------------------------------------------------------


@dataclass(frozen=True, slots=True, kw_only=True)
class DatasetKey:
    """Storage identity of one canonical dataset (IR-2).

    Extends `MarketIdentity` with `timeframe` for storage purposes. The
    exact tuple -- `exchange, symbol, market, settle, timeframe` -- keys
    Parquet metadata and SQLite `datasets` uniqueness. The physical path is
    a readable projection, not an identity encoding.

    "No settlement currency" (`market="spot"`) is represented internally by
    the reserved empty-string `NONE_SETTLE_SENTINEL`. Parquet rows keep
    `settle` as SQL `NULL` for `market="spot"` rows (see `schema.py`), and spot
    Parquet KV metadata omits the field.

    `settle` must equal `NONE_SETTLE_SENTINEL` exactly when `market` is
    `spot`, and must be a valid, non-sentinel settlement component for every
    other market. Construct from a public `MarketIdentity` (whose `settle`
    is nullable) via `DatasetKey.from_identity`.
    """

    exchange: str
    symbol: str
    market: Market
    settle: str
    timeframe: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "market", _coerce_market(self.market))
        _validate_exchange(self.exchange)
        object.__setattr__(self, "symbol", _validate_symbol(self.symbol))
        TimeBar.parse(self.timeframe)
        if self.market not in _OPERABLE_MARKETS:
            raise UnsupportedMarketError(
                f"market {self.market.value!r} is not supported in V1: structurally "
                "ambiguous without contract-attribute fields; only 'spot' and "
                "'perpetual' are operable"
            )
        if self.market is Market.SPOT:
            if self.settle != NONE_SETTLE_SENTINEL:
                raise InvalidRequestError(
                    f"settle must be the {NONE_SETTLE_SENTINEL!r} sentinel for spot "
                    f"datasets, got {self.settle!r}"
                )
        elif self.settle == NONE_SETTLE_SENTINEL:
            raise InvalidRequestError(
                f"settle must not be the {NONE_SETTLE_SENTINEL!r} sentinel for "
                f"{self.market.value!r} datasets"
            )
        else:
            object.__setattr__(self, "settle", _validate_settle(self.settle))

    @classmethod
    def from_identity(cls, identity: MarketIdentity, *, timeframe: str) -> DatasetKey:
        """Build storage identity from a public `MarketIdentity` + timeframe.

        Converts `MarketIdentity.settle` (`None` for spot) to the
        `NONE_SETTLE_SENTINEL` storage representation (IR-2).
        """
        return cls(
            exchange=identity.exchange,
            symbol=identity.symbol,
            market=identity.market,
            settle=identity.settle if identity.settle is not None else NONE_SETTLE_SENTINEL,
            timeframe=timeframe,
        )


@dataclass(frozen=True, slots=True, kw_only=True)
class BarRequest:
    """Internal UTC-aware, half-open `[start, end)` request for one dataset.

    Not part of the public surface (P-1 taxonomy/S5 sweep): `fetch` and
    `sync` build one to run fatal quality validation
    (`quality.evaluate_ohlcv_batch`/`enforce_ohlcv_batch`) against a
    fetched batch. Construction enforces the full contract: both bounds
    are UTC-aware and `start < end`.
    """

    identity: MarketIdentity
    timeframe: str
    start: datetime
    end: datetime

    def __post_init__(self) -> None:
        _ensure_path_safe(self.timeframe, field_name="timeframe")
        _ensure_utc_aware(self.start, field_name="start")
        _ensure_utc_aware(self.end, field_name="end")
        if self.start >= self.end:
            raise InvalidRequestError(
                f"start must be strictly before end: start={self.start!r} end={self.end!r}"
            )

    @property
    def dataset_key(self) -> DatasetKey:
        """The `DatasetKey` this request addresses."""
        return DatasetKey.from_identity(self.identity, timeframe=self.timeframe)


@dataclass(frozen=True, slots=True)
class YearMonth:
    """A calendar year/month, used to identify one monthly canonical file."""

    year: int
    month: int

    def __post_init__(self) -> None:
        if not (1 <= self.month <= 12):
            raise InvalidRequestError(f"month must be in 1..12: {self.month!r}")

    def __str__(self) -> str:
        return f"{self.year:04d}-{self.month:02d}"


@dataclass(frozen=True, slots=True)
class CoverageInterval:
    """A UTC-aware, half-open `[start, end)` interval tagged with a status."""

    start: datetime
    end: datetime
    status: CoverageStatus

    def __post_init__(self) -> None:
        _ensure_utc_aware(self.start, field_name="start")
        _ensure_utc_aware(self.end, field_name="end")
        if self.start >= self.end:
            raise InvalidRequestError(
                f"start must be strictly before end: start={self.start!r} end={self.end!r}"
            )


# --------------------------------------------------------------------------
# Results
# --------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class DataWarning:
    """One immutable structured warning attached to a `SyncResult` or
    `PartialScanResult` (Decision 17): a stable code, a human message, and
    the affected half-open range when the warning is range-scoped.
    """

    code: str
    message: str
    start: datetime | None = None
    end: datetime | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class SyncResult:
    """Outcome of `BarDataset.sync` (Decision 16).

    `run_id` is the single ingestion-run identifier generated for this
    call. A fully covered request is an observable no-op: `changed=False`,
    `fetched_rows=0`, `written_partitions=0`. Publication is incremental:
    earlier months may already be canonical when a later post-publication
    failure occurs. That failure raises `SyncError` fail-closed rather than
    returning partial success.
    """

    dataset_key: DatasetKey
    run_id: str
    changed: bool
    fetched_rows: int
    written_partitions: int
    covered: tuple[CoverageInterval, ...]
    gaps: tuple[CoverageInterval, ...] = ()
    warnings: tuple[DataWarning, ...] = ()

    @property
    def is_complete(self) -> bool:
        """Whether the requested range ended up with no remaining gaps."""
        return not self.gaps

    def require_complete(self) -> SyncResult:
        """Return `self` if `is_complete`, else raise `SyncError`."""
        if not self.is_complete:
            raise SyncError(
                f"sync of {self.dataset_key!r} did not fully complete: "
                f"{len(self.gaps)} gap(s) remain"
            )
        return self


@dataclass(frozen=True, slots=True, kw_only=True)
class PartialScanResult:
    """Outcome of `BarDataset.scan_partial`: local-only, possibly incomplete data.

    `data` is the lazy frame over whatever canonical coverage exists;
    `covered` and `gaps` describe that coverage as disjoint, normalized
    half-open intervals so callers can reason about what is missing. When
    no local rows exist at all, `data` is a canonical-schema empty
    `LazyFrame`, `covered` is empty, and `gaps` is the entire request.
    """

    dataset_key: DatasetKey
    data: pl.LazyFrame
    covered: tuple[CoverageInterval, ...]
    gaps: tuple[CoverageInterval, ...] = ()
    warnings: tuple[DataWarning, ...] = ()

    @property
    def is_complete(self) -> bool:
        """Whether the request range is fully covered (no gaps)."""
        return not self.gaps


@dataclass(frozen=True, slots=True)
class CatalogValidationResult:
    """Outcome of `validate_catalog`: indexed metadata vs. canonical files."""

    is_valid: bool
    checked_datasets: tuple[DatasetKey, ...] = ()
    issues: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class CatalogRebuildResult:
    """Outcome of `rebuild_catalog`.

    Rebuild restores available coverage and file-linked provenance from
    canonical Parquet metadata. It does not recover absence-only history
    (no-file fetch failures, not-listed decisions, transient unfinalized
    state); those reset to missing/unknown, recorded in `reset_datasets`.
    """

    rebuilt_datasets: tuple[DatasetKey, ...] = ()
    recovered_files: int = 0
    reset_datasets: tuple[DatasetKey, ...] = ()
    warnings: tuple[str, ...] = ()
