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

from xret.data.errors import InvalidRequestError, ProviderError, SyncError, UnsupportedMarketError
from xret.data.timeframe import TimeBar

if TYPE_CHECKING:
    import polars as pl

__all__ = [
    "DatasetFamily",
    "ReferencePriceKind",
    "CoverageStatus",
    "Availability",
    "BarFetchMode",
    "BarFinality",
    "Market",
    "QualitySeverity",
    "MarketIdentity",
    "BarRequest",
    "BarUpdate",
    "DataWarning",
    "CapabilityNotice",
    "OperationCapability",
    "TimeBarCapability",
    "ReferenceBarCapability",
    "ProviderEvidence",
    "FetchResult",
    "LiveSubscription",
    "DatasetKey",
    "SettledFundingKey",
    "ReferenceBarKey",
    "OpenInterestKey",
    "StorageKey",
    "storage_identity",
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


class DatasetFamily(enum.StrEnum):
    """Closed canonical dataset-family vocabulary."""

    TRADE_BARS = "trade_bars"
    SETTLED_FUNDING = "settled_funding"
    REFERENCE_BARS = "reference_bars"
    OPEN_INTEREST = "open_interest"


class ReferencePriceKind(enum.StrEnum):
    """Closed reference-price series vocabulary."""

    MARK = "mark"
    INDEX = "index"
    PREMIUM_INDEX = "premium_index"


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


class Availability(enum.StrEnum):
    """Whether a provider operation can satisfy one Xret request scope."""

    AVAILABLE = "available"
    UNAVAILABLE = "unavailable"
    INCOMPATIBLE = "incompatible"


class BarFetchMode(enum.StrEnum):
    """Whether a remote bar fetch returns the latest or final observation."""

    LATEST = "latest"
    FINAL = "final"


class BarFinality(enum.Enum):
    """Xret's time-based confidence in one observed bar state.

    Finality is independent of persistence. A ``FINAL`` update has passed
    Xret's finality grace, but it is canonical only after an explicit
    ``sync()`` reacquires, validates, and commits that timestamp.
    """

    FORMING = "forming"
    PROVISIONAL = "provisional"
    FINAL = "final"


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


def _require_resolved_perpetual(identity: MarketIdentity) -> None:
    if identity.market is not Market.PERPETUAL:
        raise InvalidRequestError("this dataset family requires market='perpetual'")
    if identity.settle is None:
        raise InvalidRequestError("this storage key requires a resolved perpetual settlement")


@dataclass(frozen=True, slots=True, kw_only=True)
class SettledFundingKey:
    """Provider-independent identity of settled public funding history."""

    identity: MarketIdentity

    def __post_init__(self) -> None:
        if not isinstance(self.identity, MarketIdentity):
            raise InvalidRequestError("identity must be a MarketIdentity")
        _require_resolved_perpetual(self.identity)


@dataclass(frozen=True, slots=True, kw_only=True)
class ReferenceBarKey:
    """Provider-independent identity of one reference-price bar series."""

    identity: MarketIdentity
    kind: ReferencePriceKind
    timeframe: str

    def __post_init__(self) -> None:
        if not isinstance(self.identity, MarketIdentity):
            raise InvalidRequestError("identity must be a MarketIdentity")
        _require_resolved_perpetual(self.identity)
        try:
            object.__setattr__(self, "kind", ReferencePriceKind(self.kind))
        except (TypeError, ValueError) as exc:
            raise InvalidRequestError(f"unrecognized reference price kind: {self.kind!r}") from exc
        TimeBar.parse(self.timeframe)


@dataclass(frozen=True, slots=True, kw_only=True)
class OpenInterestKey:
    """Provider-independent identity of one sampled open-interest series."""

    identity: MarketIdentity
    timeframe: str

    def __post_init__(self) -> None:
        if not isinstance(self.identity, MarketIdentity):
            raise InvalidRequestError("identity must be a MarketIdentity")
        _require_resolved_perpetual(self.identity)
        TimeBar.parse(self.timeframe)


StorageKey = DatasetKey | SettledFundingKey | ReferenceBarKey | OpenInterestKey


@dataclass(frozen=True, slots=True)
class _StorageIdentity:
    """Normalized non-null catalog/path identity; never part of the public API."""

    family: DatasetFamily
    variant: str
    exchange: str
    symbol: str
    market: Market
    settle: str
    timeframe: str


def storage_identity(key: StorageKey) -> _StorageIdentity:
    """Normalize a family-specific key for storage joins and projections."""
    if isinstance(key, DatasetKey):
        return _StorageIdentity(
            DatasetFamily.TRADE_BARS,
            "",
            key.exchange,
            key.symbol,
            key.market,
            key.settle,
            key.timeframe,
        )
    identity = key.identity
    assert identity.settle is not None
    if isinstance(key, SettledFundingKey):
        return _StorageIdentity(
            DatasetFamily.SETTLED_FUNDING,
            "",
            identity.exchange,
            identity.symbol,
            identity.market,
            identity.settle,
            "",
        )
    if isinstance(key, ReferenceBarKey):
        return _StorageIdentity(
            DatasetFamily.REFERENCE_BARS,
            key.kind.value,
            identity.exchange,
            identity.symbol,
            identity.market,
            identity.settle,
            key.timeframe,
        )
    if isinstance(key, OpenInterestKey):
        return _StorageIdentity(
            DatasetFamily.OPEN_INTEREST,
            "",
            identity.exchange,
            identity.symbol,
            identity.market,
            identity.settle,
            key.timeframe,
        )
    raise TypeError(f"unsupported storage key: {key!r}")


def _key_from_storage_identity(identity: _StorageIdentity) -> StorageKey:
    market_identity = MarketIdentity(
        exchange=identity.exchange,
        symbol=identity.symbol,
        market=identity.market,
        settle=None if identity.market is Market.SPOT else identity.settle,
    )
    if identity.family is DatasetFamily.TRADE_BARS:
        if identity.variant:
            raise InvalidRequestError("trade-bar storage variant must be empty")
        return DatasetKey.from_identity(market_identity, timeframe=identity.timeframe)
    if identity.family is DatasetFamily.SETTLED_FUNDING:
        if identity.variant or identity.timeframe:
            raise InvalidRequestError("settled-funding variant and timeframe must be empty")
        return SettledFundingKey(identity=market_identity)
    if identity.family is DatasetFamily.REFERENCE_BARS:
        return ReferenceBarKey(
            identity=market_identity,
            kind=ReferencePriceKind(identity.variant),
            timeframe=identity.timeframe,
        )
    if identity.family is DatasetFamily.OPEN_INTEREST:
        if identity.variant:
            raise InvalidRequestError("open-interest storage variant must be empty")
        return OpenInterestKey(identity=market_identity, timeframe=identity.timeframe)
    raise InvalidRequestError(f"unsupported dataset family: {identity.family!r}")


@dataclass(frozen=True, slots=True, kw_only=True)
class BarRequest:
    """UTC-aware, half-open `[start, end)` request for one bar dataset.

    Exported through `xret.data.providers` as part of the experimental
    historical-bar provider SPI. `fetch` and `sync` also build this value
    internally. Construction enforces a recognized timeframe, UTC-aware
    bounds, and `start < end`.
    """

    identity: MarketIdentity
    timeframe: str
    start: datetime
    end: datetime

    def __post_init__(self) -> None:
        TimeBar.parse(_ensure_path_safe(self.timeframe, field_name="timeframe"))
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


@dataclass(frozen=True, slots=True, kw_only=True)
class BarUpdate:
    """One validated full-state time-bar observation from a live session.

    ``timestamp`` is the inclusive UTC start of the bar. Multiple updates for
    the same timestamp are valid. ``finality`` describes the bar relative to
    Xret's receipt clock and finality grace; it never implies persistence.
    ``received_at`` records when Xret normalized the observation. OHLC values
    summarize eligible executed trades and ``volume`` is base-asset quantity.
    """

    identity: MarketIdentity
    timeframe: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    received_at: datetime
    finality: BarFinality

    def __post_init__(self) -> None:
        if not isinstance(self.identity, MarketIdentity):
            raise InvalidRequestError("bar update identity must be a MarketIdentity")
        time_bar = TimeBar.parse(_ensure_path_safe(self.timeframe, field_name="timeframe"))
        _ensure_utc_aware(self.timestamp, field_name="timestamp")
        _ensure_utc_aware(self.received_at, field_name="received_at")
        if not isinstance(self.finality, BarFinality):
            raise InvalidRequestError("bar update finality must be a BarFinality")
        if time_bar.floor(self.timestamp) != self.timestamp:
            raise InvalidRequestError(
                f"timestamp must be aligned to {self.timeframe}: {self.timestamp!r}"
            )

        from xret.data.quality import _validate_scalar_ohlcv

        values = _validate_scalar_ohlcv(
            self.open,
            self.high,
            self.low,
            self.close,
            self.volume,
            error_cls=InvalidRequestError,
        )
        for field_name, value in zip(
            ("open", "high", "low", "close", "volume"), values, strict=True
        ):
            object.__setattr__(self, field_name, value)


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


@dataclass(frozen=True, slots=True)
class CapabilityNotice:
    """One stable discovery-time fact about a provider operation."""

    code: str
    message: str

    def __post_init__(self) -> None:
        if not isinstance(self.code, str) or not self.code:
            raise InvalidRequestError("capability notice code must be a nonempty string")
        if not isinstance(self.message, str) or not self.message:
            raise InvalidRequestError("capability notice message must be a nonempty string")


@dataclass(frozen=True, slots=True)
class OperationCapability:
    """Current provider availability and notices for one operation."""

    availability: Availability
    notices: tuple[CapabilityNotice, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.availability, Availability):
            raise InvalidRequestError("operation availability must be an Availability")
        if not isinstance(self.notices, tuple) or not all(
            isinstance(notice, CapabilityNotice) for notice in self.notices
        ):
            raise InvalidRequestError("operation notices must be CapabilityNotice values")


@dataclass(frozen=True, slots=True)
class TimeBarCapability:
    """Historical and live capability facts for one canonical timeframe."""

    timeframe: str
    historical: OperationCapability
    live: OperationCapability

    def __post_init__(self) -> None:
        TimeBar.parse(self.timeframe)
        if not isinstance(self.historical, OperationCapability):
            raise InvalidRequestError("historical capability must be an OperationCapability")
        if not isinstance(self.live, OperationCapability):
            raise InvalidRequestError("live capability must be an OperationCapability")


@dataclass(frozen=True, slots=True)
class ReferenceBarCapability:
    """Provider availability for one reference kind and canonical timeframe."""

    kind: ReferencePriceKind
    timeframe: str
    historical: OperationCapability

    def __post_init__(self) -> None:
        try:
            object.__setattr__(self, "kind", ReferencePriceKind(self.kind))
        except (TypeError, ValueError) as exc:
            raise InvalidRequestError(f"unrecognized reference price kind: {self.kind!r}") from exc
        TimeBar.parse(self.timeframe)
        if not isinstance(self.historical, OperationCapability):
            raise InvalidRequestError("historical capability must be an OperationCapability")


@dataclass(frozen=True, slots=True)
class ProviderEvidence:
    """Provider-native provenance for one remote operation."""

    provider_name: str
    provider_version: str
    provider_api_version: int
    native_market_id: str
    native_symbol: str
    normalizations: tuple[str, ...] = ()
    reference_target_scope: str | None = None
    derivative_linear: bool | None = None
    derivative_inverse: bool | None = None
    contract_size: str | None = None
    source_field_mapping: str | None = None
    contributed_start: datetime | None = None
    contributed_end: datetime | None = None
    source_route: str | None = None
    object_key: str | None = None
    checksum_algorithm: str | None = None
    checksum_value: str | None = None
    source_revision: str | None = None
    retrieved_at: datetime | None = None
    source_rows: int | None = None
    canonical_rows: int | None = None
    duplicate_rows: int | None = None

    def __post_init__(self) -> None:
        for field_name in (
            "provider_name",
            "provider_version",
            "native_market_id",
            "native_symbol",
        ):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value:
                raise InvalidRequestError(f"{field_name} must be a nonempty string")
        if (
            isinstance(self.provider_api_version, bool)
            or not isinstance(self.provider_api_version, int)
            or self.provider_api_version <= 0
        ):
            raise InvalidRequestError("provider_api_version must be a positive integer")
        if not isinstance(self.normalizations, tuple) or not all(
            isinstance(value, str) and value for value in self.normalizations
        ):
            raise InvalidRequestError("provider normalizations must be nonempty strings")
        if len(set(self.normalizations)) != len(self.normalizations):
            raise InvalidRequestError("provider normalizations must not contain duplicates")
        if self.reference_target_scope not in (None, "contract", "pair"):
            raise InvalidRequestError("reference_target_scope must be None, 'contract', or 'pair'")
        for field_name in ("derivative_linear", "derivative_inverse"):
            value = getattr(self, field_name)
            if value is not None and not isinstance(value, bool):
                raise InvalidRequestError(f"{field_name} must be bool or None")
        for field_name in ("contract_size", "source_field_mapping"):
            value = getattr(self, field_name)
            if value is not None and (not isinstance(value, str) or not value):
                raise InvalidRequestError(f"{field_name} must be a nonempty string or None")
        range_values = (self.contributed_start, self.contributed_end)
        if any(value is not None for value in range_values):
            if any(value is None for value in range_values):
                raise InvalidRequestError("contributed_start/end must be paired")
            assert self.contributed_start is not None and self.contributed_end is not None
            _ensure_utc_aware(self.contributed_start, field_name="contributed_start")
            _ensure_utc_aware(self.contributed_end, field_name="contributed_end")
            if self.contributed_start >= self.contributed_end:
                raise InvalidRequestError("contributed range must be nonempty")
        for field_name in ("source_route", "source_revision"):
            value = getattr(self, field_name)
            if value is not None and (not isinstance(value, str) or not value):
                raise InvalidRequestError(f"{field_name} must be a nonempty string or None")
        if (self.checksum_algorithm is None) != (self.checksum_value is None):
            raise InvalidRequestError("checksum_algorithm/value must be paired")
        if self.retrieved_at is not None:
            _ensure_utc_aware(self.retrieved_at, field_name="retrieved_at")
        counts = (self.source_rows, self.canonical_rows, self.duplicate_rows)
        if any(value is not None for value in counts):
            if any(
                value is None or isinstance(value, bool) or not isinstance(value, int) or value < 0
                for value in counts
            ):
                raise InvalidRequestError("source row counts must be paired nonnegative integers")
            assert self.source_rows is not None
            assert self.canonical_rows is not None
            assert self.duplicate_rows is not None
            if self.canonical_rows + self.duplicate_rows != self.source_rows:
                raise InvalidRequestError("source row counts are inconsistent")


@dataclass(frozen=True, slots=True, kw_only=True)
class FetchResult:
    """Validated remote rows, evidence, and coverage for `BarDataset.fetch`."""

    dataset_key: DatasetKey
    data: pl.DataFrame
    covered: tuple[CoverageInterval, ...]
    source: ProviderEvidence
    gaps: tuple[CoverageInterval, ...] = ()
    warnings: tuple[DataWarning, ...] = ()

    @property
    def is_complete(self) -> bool:
        """Whether the provider observation proved the entire request."""
        return not self.gaps

    def require_complete(self) -> FetchResult:
        """Return `self` if complete, otherwise raise `ProviderError`."""
        if not self.is_complete:
            raise ProviderError(
                f"fetch of {self.dataset_key!r} did not fully complete: "
                f"{len(self.gaps)} gap(s) remain"
            )
        return self


@dataclass(frozen=True, slots=True, kw_only=True)
class LiveSubscription:
    """Accepted live subscription identity, source evidence, and notices."""

    dataset_key: DatasetKey
    source: ProviderEvidence
    warnings: tuple[DataWarning, ...] = ()


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
    source: ProviderEvidence | None = None

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
    checked_datasets: tuple[StorageKey, ...] = ()
    issues: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class CatalogRebuildResult:
    """Outcome of `rebuild_catalog`.

    Rebuild restores available coverage and file-linked provenance from
    canonical Parquet metadata. It does not recover absence-only history
    (no-file fetch failures, not-listed decisions, transient unfinalized
    state); those reset to missing/unknown, recorded in `reset_datasets`.
    """

    rebuilt_datasets: tuple[StorageKey, ...] = ()
    recovered_files: int = 0
    reset_datasets: tuple[StorageKey, ...] = ()
    warnings: tuple[str, ...] = ()
