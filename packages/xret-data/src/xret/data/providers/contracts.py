"""Public contracts for market-data provider authors."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Final, Protocol

import polars as pl
from xret.data.errors import InvalidRequestError, ProviderError
from xret.data.models import BarRequest, Market, MarketIdentity
from xret.data.timeframe import TimeBar

__all__ = [
    "PROVIDER_API_VERSION",
    "PROVIDER_BAR_SCHEMA",
    "BarObservation",
    "BarRequest",
    "DerivativeInterpretation",
    "HistoricalBarProvider",
    "Market",
    "MarketDefinition",
    "MarketDefinitionProvider",
    "MarketIdentity",
    "ObservedWindow",
    "ProviderDescriptor",
    "ResolvedBarMarket",
]

PROVIDER_API_VERSION: Final[int] = 1
_PROVIDER_NAME_PATTERN: Final[re.Pattern[str]] = re.compile(r"^[a-z][a-z0-9-]*$")

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
    entry must be a canonical Xret timeframe. Providers exclude native bar
    types outside that vocabulary rather than passing them through: a venue
    must stay resolvable even when it offers bar types Xret cannot express.
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


@dataclass(frozen=True, slots=True)
class MarketDefinition:
    """One provider-advertised market translated into Xret vocabulary.

    `timeframes` contains provider-advertised bar types that Xret can express;
    it is not an Xret verification or exhaustive-pagination claim. `tick_size`
    and `size_increment` are exact fixed increments when the provider exposes
    them with unambiguous semantics, otherwise `None`.
    """

    identity: MarketIdentity
    active: bool | None
    timeframes: frozenset[str]
    tick_size: Decimal | None
    size_increment: Decimal | None
    derivative: DerivativeInterpretation | None = None

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
    """One exhaustively observed UTC half-open provider time window."""

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
    """Untrusted provider rows plus the windows their absence can describe."""

    frame: pl.DataFrame
    observed: tuple[ObservedWindow, ...]


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


class MarketDefinitionProvider(Protocol):
    """Optional provider capability for current market-definition snapshots."""

    def fetch_markets(
        self,
        *,
        exchange: str,
        market: Market,
    ) -> tuple[MarketDefinition, ...]: ...
