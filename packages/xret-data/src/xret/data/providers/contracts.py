"""Public contracts for market-data provider authors."""

from __future__ import annotations

import re
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from typing import Final, Protocol, Self

import polars as pl
from xret.data.errors import InvalidRequestError, ProviderError
from xret.data.models import (
    Availability,
    BarRequest,
    Market,
    MarketIdentity,
    OperationCapability,
    ReferenceBarCapability,
    ReferencePriceKind,
    TimeBarCapability,
)
from xret.data.timeframe import TimeBar

__all__ = [
    "PROVIDER_API_VERSION",
    "PROVIDER_BAR_SCHEMA",
    "PROVIDER_FUNDING_SCHEMA",
    "PROVIDER_REFERENCE_BAR_SCHEMA",
    "PROVIDER_OPEN_INTEREST_SCHEMA",
    "BarObservation",
    "FundingObservation",
    "ReferenceBarObservation",
    "OpenInterestObservation",
    "OpenInterestSourceEvidence",
    "OpenInterestSyncPolicy",
    "FundingRequest",
    "ReferenceBarRequest",
    "OpenInterestRequest",
    "HistoricalFundingProvider",
    "HistoricalReferenceBarProvider",
    "HistoricalOpenInterestProvider",
    "BarRequest",
    "DerivativeInterpretation",
    "HistoricalBarProvider",
    "Market",
    "MarketDefinition",
    "MarketDefinitionProvider",
    "LiveBarProvider",
    "LiveBarSession",
    "MarketIdentity",
    "ObservedWindow",
    "ProviderDescriptor",
    "ProviderBarUpdate",
    "ResolvedBarMarket",
    "ResolvedFundingMarket",
    "ResolvedReferenceMarket",
    "ResolvedOpenInterestMarket",
]

PROVIDER_API_VERSION: Final[int] = 1
_PROVIDER_NAME_PATTERN: Final[re.Pattern[str]] = re.compile(r"^[a-z][a-z0-9-]*$")

#: Provider values before canonical identity columns are attached. OHLC must
#: summarize eligible executed trades in the requested half-open interval, and
#: volume must already be normalized to base-asset quantity. Providers must
#: reject a source that cannot satisfy those meanings losslessly.
PROVIDER_BAR_SCHEMA: Final[pl.Schema] = pl.Schema(
    {
        "timestamp": pl.Datetime(time_unit="ms", time_zone="UTC"),
        "open": pl.Float64(),
        "high": pl.Float64(),
        "low": pl.Float64(),
        "close": pl.Float64(),
        "volume": pl.Float64(),
    }
)

#: Provider reference values before canonical identity columns are attached. This is
#: intentionally independent from volume-bearing trade OHLCV.
PROVIDER_REFERENCE_BAR_SCHEMA: Final[pl.Schema] = pl.Schema(
    {
        "timestamp": pl.Datetime(time_unit="ms", time_zone="UTC"),
        "open": pl.Float64(),
        "high": pl.Float64(),
        "low": pl.Float64(),
        "close": pl.Float64(),
    }
)

#: Provider funding values before canonical identity columns are attached.
PROVIDER_FUNDING_SCHEMA: Final[pl.Schema] = pl.Schema(
    {
        "effective_at": pl.Datetime(time_unit="ms", time_zone="UTC"),
        "funding_rate": pl.Float64(),
        "funding_interval_seconds": pl.Int64(),
        "mark_price": pl.Float64(),
    }
)


#: Provider open-interest values before canonical identity columns are attached.
#: Amount is already normalized to base-asset-equivalent outstanding exposure;
#: value is optional quote-currency notional.
PROVIDER_OPEN_INTEREST_SCHEMA: Final[pl.Schema] = pl.Schema(
    {
        "timestamp": pl.Datetime(time_unit="ms", time_zone="UTC"),
        "open_interest_amount": pl.Float64(),
        "open_interest_value": pl.Float64(),
    }
)


@dataclass(frozen=True, slots=True)
class ProviderDescriptor:
    """Stable provider identity and the SPI major it implements."""

    name: str
    version: str
    api_version: int

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not _PROVIDER_NAME_PATTERN.fullmatch(self.name):
            raise InvalidRequestError(
                "provider name must be a lowercase slug containing letters, digits, or '-': "
                f"{self.name!r}"
            )
        if not isinstance(self.version, str) or not self.version:
            raise InvalidRequestError("provider version must be a nonempty string")
        if not isinstance(self.api_version, int) or isinstance(self.api_version, bool):
            raise InvalidRequestError("provider api_version must be an integer")


@dataclass(frozen=True, slots=True)
class DerivativeInterpretation:
    """Provider-neutral facts required to interpret a derivative market."""

    linear: bool | None = None
    inverse: bool | None = None
    contract_size: str | None = None

    def __post_init__(self) -> None:
        if self.linear is not None and not isinstance(self.linear, bool):
            raise InvalidRequestError("derivative linear must be bool or None")
        if self.inverse is not None and not isinstance(self.inverse, bool):
            raise InvalidRequestError("derivative inverse must be bool or None")
        if self.contract_size is not None and (
            not isinstance(self.contract_size, str) or not self.contract_size
        ):
            raise InvalidRequestError("derivative contract_size must be a nonempty string or None")


def _validate_derivative(
    value: DerivativeInterpretation | None,
    *,
    owner: str,
) -> None:
    if value is not None and not isinstance(value, DerivativeInterpretation):
        raise InvalidRequestError(f"{owner} derivative must be a DerivativeInterpretation or None")


@dataclass(frozen=True, slots=True)
class ResolvedBarMarket:
    """A canonical market resolved to one provider-native historical-bar target.

    `timeframes` declares what Xret may request from this market, so every
    entry must be a canonical Xret timeframe whose interval origin, event
    universe, volume unit, finality, and exhaustive observation behavior the
    provider can satisfy. Providers exclude native bar types outside that
    vocabulary rather than passing them through: a venue must stay resolvable
    even when it offers a bar type Xret cannot express or normalize exactly.
    """

    identity: MarketIdentity
    native_market_id: str
    native_symbol: str
    timeframes: frozenset[str]
    derivative: DerivativeInterpretation | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.native_market_id, str) or not self.native_market_id:
            raise InvalidRequestError("native_market_id must be a nonempty string")
        if not isinstance(self.native_symbol, str) or not self.native_symbol:
            raise InvalidRequestError("native_symbol must be a nonempty string")
        if not isinstance(self.timeframes, frozenset):
            raise InvalidRequestError("resolved market timeframes must be a frozenset")
        for timeframe in self.timeframes:
            TimeBar.parse(timeframe)
        _validate_derivative(self.derivative, owner="resolved market")
        if self.identity.market is Market.PERPETUAL and self.identity.settle is None:
            raise InvalidRequestError("resolved perpetual market must include settle")
        if self.identity.market is Market.SPOT and self.derivative is not None:
            raise InvalidRequestError("spot market must not include derivative interpretation")


@dataclass(frozen=True, slots=True, kw_only=True)
class FundingRequest:
    """Arbitrary UTC half-open request for final public funding events."""

    identity: MarketIdentity
    start: datetime
    end: datetime

    def __post_init__(self) -> None:
        if not isinstance(self.identity, MarketIdentity):
            raise InvalidRequestError("funding request identity must be a MarketIdentity")
        if self.identity.market is not Market.PERPETUAL:
            raise InvalidRequestError("settled funding requires market='perpetual'")
        for name, value in (("start", self.start), ("end", self.end)):
            if value.tzinfo is None or value.utcoffset() != timedelta(0):
                raise InvalidRequestError(f"funding request {name} must be UTC-aware")
        if self.start >= self.end:
            raise InvalidRequestError("funding request start must be before end")


@dataclass(frozen=True, slots=True)
class ResolvedFundingMarket:
    """Canonical perpetual resolved to one native settled-history target."""

    identity: MarketIdentity
    native_market_id: str
    native_symbol: str

    def __post_init__(self) -> None:
        if self.identity.market is not Market.PERPETUAL or self.identity.settle is None:
            raise InvalidRequestError("resolved funding market requires a settled perpetual")
        if not isinstance(self.native_market_id, str) or not self.native_market_id:
            raise InvalidRequestError("native_market_id must be a nonempty string")
        if not isinstance(self.native_symbol, str) or not self.native_symbol:
            raise InvalidRequestError("native_symbol must be a nonempty string")


@dataclass(frozen=True, slots=True)
class MarketDefinition:
    """One provider-advertised market translated into Xret vocabulary.

    `timeframes` contains provider-advertised bar types that Xret can express;
    it is not an Xret qualification or exhaustive-pagination claim. `tick_size`
    and `size_increment` are exact fixed increments when the provider exposes
    them with unambiguous semantics, otherwise `None`.
    """

    identity: MarketIdentity
    active: bool | None
    timeframes: frozenset[str]
    tick_size: Decimal | None
    size_increment: Decimal | None
    derivative: DerivativeInterpretation | None = None
    bar_capabilities: tuple[TimeBarCapability, ...] = ()
    funding_history: OperationCapability = OperationCapability(Availability.UNAVAILABLE)
    reference_bar_capabilities: tuple[ReferenceBarCapability, ...] = ()
    open_interest_capabilities: tuple[TimeBarCapability, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.identity, MarketIdentity):
            raise InvalidRequestError("market definition identity must be a MarketIdentity")
        if self.active is not None and not isinstance(self.active, bool):
            raise InvalidRequestError("market definition active must be bool or None")
        if not isinstance(self.timeframes, frozenset):
            raise InvalidRequestError("market definition timeframes must be a frozenset")
        for timeframe in self.timeframes:
            TimeBar.parse(timeframe)
        _validate_increment(self.tick_size, field_name="tick_size")
        _validate_increment(self.size_increment, field_name="size_increment")
        _validate_derivative(self.derivative, owner="market definition")
        if not isinstance(self.bar_capabilities, tuple) or not all(
            isinstance(capability, TimeBarCapability) for capability in self.bar_capabilities
        ):
            raise InvalidRequestError(
                "market definition bar_capabilities must be TimeBarCapability values"
            )
        capability_timeframes = [capability.timeframe for capability in self.bar_capabilities]
        if len(set(capability_timeframes)) != len(capability_timeframes):
            raise InvalidRequestError(
                "market definition bar_capabilities must not repeat a timeframe"
            )
        if not isinstance(self.funding_history, OperationCapability):
            raise InvalidRequestError(
                "market definition funding_history must be an OperationCapability"
            )
        if not isinstance(self.reference_bar_capabilities, tuple) or not all(
            isinstance(capability, ReferenceBarCapability)
            for capability in self.reference_bar_capabilities
        ):
            raise InvalidRequestError(
                "market definition reference_bar_capabilities must be ReferenceBarCapability values"
            )
        reference_keys = [
            (capability.kind, capability.timeframe)
            for capability in self.reference_bar_capabilities
        ]
        if len(set(reference_keys)) != len(reference_keys):
            raise InvalidRequestError(
                "market definition reference_bar_capabilities must not repeat a kind/timeframe"
            )
        if not isinstance(self.open_interest_capabilities, tuple) or not all(
            isinstance(capability, TimeBarCapability)
            for capability in self.open_interest_capabilities
        ):
            raise InvalidRequestError(
                "market definition open_interest_capabilities must be TimeBarCapability values"
            )
        oi_timeframes = [capability.timeframe for capability in self.open_interest_capabilities]
        if len(set(oi_timeframes)) != len(oi_timeframes):
            raise InvalidRequestError(
                "market definition open_interest_capabilities must not repeat a timeframe"
            )
        if self.identity.market is Market.PERPETUAL and self.identity.settle is None:
            raise InvalidRequestError("perpetual market definition must include settle")
        if self.identity.market is Market.SPOT and self.derivative is not None:
            raise InvalidRequestError("spot market definition must not include derivative metadata")


def _validate_increment(value: Decimal | None, *, field_name: str) -> None:
    if value is None:
        return
    if not isinstance(value, Decimal) or not value.is_finite() or value <= 0:
        raise InvalidRequestError(
            f"market definition {field_name} must be a positive finite Decimal or None"
        )


@dataclass(frozen=True, slots=True)
class ObservedWindow:
    """One exhaustively observed UTC half-open provider time window.

    A provider observation may contain an ordered, non-overlapping subset of
    its request. Absence has meaning only inside these windows.
    """

    start: datetime
    end: datetime

    def __post_init__(self) -> None:
        if (
            self.start.tzinfo is None
            or self.start.utcoffset() != timedelta(0)
            or self.end.tzinfo is None
            or self.end.utcoffset() != timedelta(0)
        ):
            raise ProviderError("observed window boundaries must be UTC-aware")
        if self.start >= self.end:
            raise ProviderError(f"observed window must be nonempty: [{self.start!r}, {self.end!r})")


@dataclass(frozen=True, slots=True)
class BarObservation:
    """Untrusted canonical trade bars plus windows their absence can describe.

    Rows use inclusive UTC interval starts, trade-derived OHLC values, and
    base-asset volume. A provider must fail before constructing an observation
    when its native source cannot satisfy those meanings losslessly. `observed`
    may be a partial subset of the request, but every returned row must fall
    inside one of its exhaustive windows.
    """

    frame: pl.DataFrame
    observed: tuple[ObservedWindow, ...]


@dataclass(frozen=True, slots=True)
class FundingObservation:
    """Untrusted settled-funding rows and exhaustively observed spans."""

    frame: pl.DataFrame
    observed: tuple[ObservedWindow, ...]


class HistoricalFundingProvider(Protocol):
    """Structural SPI for settled public funding history."""

    @property
    def descriptor(self) -> ProviderDescriptor: ...

    def resolve_funding_market(self, identity: MarketIdentity) -> ResolvedFundingMarket: ...

    def observe_funding(
        self,
        request: FundingRequest,
        market: ResolvedFundingMarket,
    ) -> FundingObservation: ...


class HistoricalBarProvider(Protocol):
    """Structural provider SPI for exhaustive historical time-bar observations."""

    @property
    def descriptor(self) -> ProviderDescriptor: ...

    def resolve_market(self, identity: MarketIdentity) -> ResolvedBarMarket: ...

    def observe_bars(
        self,
        request: BarRequest,
        market: ResolvedBarMarket,
    ) -> BarObservation: ...


@dataclass(frozen=True, slots=True, kw_only=True)
class ProviderBarUpdate:
    """One canonical trade-bar update before Xret receipt normalization.

    OHLC values summarize eligible executed trades and volume is base-asset
    quantity. Provider-native mark, index, settlement, quote-volume, or
    contract-count values are not valid here unless an exact approved
    normalization produced this canonical value.
    """

    identity: MarketIdentity
    timeframe: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float


class LiveBarSession(Protocol):
    """One provider-owned live-bar connection scope."""

    async def __aenter__(self) -> Self: ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: object | None,
    ) -> bool | None: ...

    async def subscribe_bar_updates(
        self,
        market: ResolvedBarMarket,
        timeframe: str,
    ) -> None: ...

    def __aiter__(self) -> AsyncIterator[ProviderBarUpdate]: ...


class LiveBarProvider(Protocol):
    """Optional provider capability for live time-bar updates."""

    def open_live_bars(self, *, exchange: str) -> LiveBarSession: ...


class MarketDefinitionProvider(Protocol):
    """Optional provider capability for current market-definition snapshots."""

    def fetch_markets(
        self,
        *,
        exchange: str,
        market: Market,
    ) -> tuple[MarketDefinition, ...]: ...


@dataclass(frozen=True, slots=True, kw_only=True)
class ReferenceBarRequest:
    """Aligned UTC half-open request for one reference-price bar series."""

    identity: MarketIdentity
    kind: ReferencePriceKind
    timeframe: str
    start: datetime
    end: datetime

    def __post_init__(self) -> None:
        if not isinstance(self.identity, MarketIdentity):
            raise InvalidRequestError("reference request identity must be a MarketIdentity")
        if self.identity.market is not Market.PERPETUAL:
            raise InvalidRequestError("reference bars require market='perpetual'")
        try:
            object.__setattr__(self, "kind", ReferencePriceKind(self.kind))
        except (TypeError, ValueError) as exc:
            raise InvalidRequestError(f"unrecognized reference price kind: {self.kind!r}") from exc
        time_bar = TimeBar.parse(self.timeframe)
        for name, value in (("start", self.start), ("end", self.end)):
            if value.tzinfo is None or value.utcoffset() != timedelta(0):
                raise InvalidRequestError(f"reference request {name} must be UTC-aware")
            if time_bar.floor(value) != value:
                raise InvalidRequestError(
                    f"reference request {name} must align to {self.timeframe}"
                )
        if self.start >= self.end:
            raise InvalidRequestError("reference request start must be before end")


@dataclass(frozen=True, slots=True)
class ResolvedReferenceMarket:
    """Canonical perpetual resolved to one native reference-series target."""

    identity: MarketIdentity
    native_market_id: str
    native_symbol: str
    kind: ReferencePriceKind
    timeframes: frozenset[str]
    reference_target_scope: str = "contract"

    def __post_init__(self) -> None:
        if self.identity.market is not Market.PERPETUAL or self.identity.settle is None:
            raise InvalidRequestError("resolved reference market requires a settled perpetual")
        if not isinstance(self.native_market_id, str) or not self.native_market_id:
            raise InvalidRequestError("native_market_id must be a nonempty string")
        if not isinstance(self.native_symbol, str) or not self.native_symbol:
            raise InvalidRequestError("native_symbol must be a nonempty string")
        try:
            object.__setattr__(self, "kind", ReferencePriceKind(self.kind))
        except (TypeError, ValueError) as exc:
            raise InvalidRequestError(f"unrecognized reference price kind: {self.kind!r}") from exc
        if not isinstance(self.timeframes, frozenset):
            raise InvalidRequestError("resolved reference timeframes must be a frozenset")
        for timeframe in self.timeframes:
            TimeBar.parse(timeframe)
        if self.reference_target_scope not in ("contract", "pair"):
            raise InvalidRequestError("reference_target_scope must be 'contract' or 'pair'")
        if self.kind is not ReferencePriceKind.INDEX and self.reference_target_scope != "contract":
            raise InvalidRequestError("only index reference targets may be pair-scoped")


@dataclass(frozen=True, slots=True)
class ReferenceBarObservation:
    """Untrusted no-volume reference OHLC rows and exhaustive windows."""

    frame: pl.DataFrame
    observed: tuple[ObservedWindow, ...]


class HistoricalReferenceBarProvider(Protocol):
    """Structural SPI for historical mark/index/premium-index bars."""

    @property
    def descriptor(self) -> ProviderDescriptor: ...

    def resolve_reference_market(
        self, identity: MarketIdentity, kind: ReferencePriceKind
    ) -> ResolvedReferenceMarket: ...

    def observe_reference_bars(
        self,
        request: ReferenceBarRequest,
        market: ResolvedReferenceMarket,
    ) -> ReferenceBarObservation: ...


class OpenInterestSyncPolicy(StrEnum):
    """Static synchronization and source-revision semantics for resolved OI history."""

    MISSING_ONLY = "missing_only"
    REVISIONED_ARCHIVE = "revisioned_archive"


@dataclass(frozen=True, slots=True, kw_only=True)
class OpenInterestRequest:
    """Aligned UTC half-open request for sampled historical open interest."""

    identity: MarketIdentity
    timeframe: str
    start: datetime
    end: datetime

    def __post_init__(self) -> None:
        if not isinstance(self.identity, MarketIdentity):
            raise InvalidRequestError("open-interest request identity must be a MarketIdentity")
        if self.identity.market is not Market.PERPETUAL:
            raise InvalidRequestError("open interest requires market='perpetual'")
        time_bar = TimeBar.parse(self.timeframe)
        for name, value in (("start", self.start), ("end", self.end)):
            if value.tzinfo is None or value.utcoffset() != timedelta(0):
                raise InvalidRequestError(f"open-interest request {name} must be UTC-aware")
            if time_bar.floor(value) != value:
                raise InvalidRequestError(
                    f"open-interest request {name} must align to {self.timeframe}"
                )
        if self.start >= self.end:
            raise InvalidRequestError("open-interest request start must be before end")


@dataclass(frozen=True, slots=True)
class ResolvedOpenInterestMarket:
    """Canonical perpetual resolved to one losslessly interpretable OI target."""

    identity: MarketIdentity
    native_market_id: str
    native_symbol: str
    timeframes: frozenset[str]
    derivative: DerivativeInterpretation
    sync_policy: OpenInterestSyncPolicy = OpenInterestSyncPolicy.MISSING_ONLY

    def __post_init__(self) -> None:
        if self.identity.market is not Market.PERPETUAL or self.identity.settle is None:
            raise InvalidRequestError("resolved open-interest market requires a settled perpetual")
        if not isinstance(self.native_market_id, str) or not self.native_market_id:
            raise InvalidRequestError("native_market_id must be a nonempty string")
        if not isinstance(self.native_symbol, str) or not self.native_symbol:
            raise InvalidRequestError("native_symbol must be a nonempty string")
        if not isinstance(self.timeframes, frozenset):
            raise InvalidRequestError("resolved open-interest timeframes must be a frozenset")
        for timeframe in self.timeframes:
            TimeBar.parse(timeframe)
        if not isinstance(self.derivative, DerivativeInterpretation):
            raise InvalidRequestError("resolved open-interest derivative is required")
        if not isinstance(self.sync_policy, OpenInterestSyncPolicy):
            raise InvalidRequestError("resolved open-interest sync_policy is required")


@dataclass(frozen=True, slots=True)
class OpenInterestSourceEvidence:
    """One immutable source revision contributing an OI observation interval."""

    start: datetime
    end: datetime
    source_route: str
    source_revision: str
    object_key: str = ""
    checksum_algorithm: str = ""
    checksum_value: str = ""
    retrieved_at: datetime | None = None
    source_rows: int = 0
    canonical_rows: int = 0
    duplicate_rows: int = 0
    normalizations: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        for name, value in (("start", self.start), ("end", self.end)):
            if value.tzinfo is None or value.utcoffset() != timedelta(0):
                raise InvalidRequestError(f"OI source {name} must be UTC-aware")
        if self.start >= self.end:
            raise InvalidRequestError("OI source range must be nonempty")
        for name in ("source_route", "source_revision"):
            if not isinstance(getattr(self, name), str) or not getattr(self, name):
                raise InvalidRequestError(f"OI source {name} must be a nonempty string")
        if bool(self.checksum_algorithm) != bool(self.checksum_value):
            raise InvalidRequestError("OI source checksum algorithm/value must be paired")
        if self.retrieved_at is not None and (
            self.retrieved_at.tzinfo is None or self.retrieved_at.utcoffset() != timedelta(0)
        ):
            raise InvalidRequestError("OI source retrieved_at must be UTC-aware")
        counts = (self.source_rows, self.canonical_rows, self.duplicate_rows)
        if any(
            isinstance(value, bool) or not isinstance(value, int) or value < 0 for value in counts
        ):
            raise InvalidRequestError("OI source row counts must be nonnegative integers")
        if self.canonical_rows + self.duplicate_rows != self.source_rows:
            raise InvalidRequestError("OI source row counts are inconsistent")
        if not isinstance(self.normalizations, tuple) or not all(
            isinstance(value, str) and value for value in self.normalizations
        ):
            raise InvalidRequestError("OI source normalizations must be nonempty strings")


@dataclass(frozen=True, slots=True)
class OpenInterestObservation:
    """Untrusted sampled gauge rows, source-exhaustive windows, and revisions."""

    frame: pl.DataFrame
    observed: tuple[ObservedWindow, ...]
    sources: tuple[OpenInterestSourceEvidence, ...] = ()
    normalizations: tuple[str, ...] = ()
    source_field_mapping: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.normalizations, tuple) or not all(
            isinstance(value, str) and value for value in self.normalizations
        ):
            raise InvalidRequestError(
                "OI observation normalizations must be a tuple of nonempty strings"
            )
        if self.source_field_mapping is not None and (
            not isinstance(self.source_field_mapping, str) or not self.source_field_mapping
        ):
            raise InvalidRequestError(
                "OI observation source_field_mapping must be a nonempty string or None"
            )


class HistoricalOpenInterestProvider(Protocol):
    """Structural SPI for historical sampled open interest."""

    @property
    def descriptor(self) -> ProviderDescriptor: ...

    def resolve_open_interest_market(
        self, identity: MarketIdentity
    ) -> ResolvedOpenInterestMarket: ...

    def observe_open_interest(
        self,
        request: OpenInterestRequest,
        market: ResolvedOpenInterestMarket,
    ) -> OpenInterestObservation: ...
