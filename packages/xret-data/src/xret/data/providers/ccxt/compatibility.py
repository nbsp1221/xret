"""Typed, evidence-backed compatibility facts for CCXT endpoint scopes."""

from __future__ import annotations

import enum
from dataclasses import dataclass
from datetime import timedelta
from typing import Final, Literal

from xret.data.errors import ProviderError, UnsupportedMarketError
from xret.data.timeframe import TimeBar

MarketFamily = Literal["spot", "perpetual"]


class VolumeMode(enum.Enum):
    """Native candle-volume representation for one endpoint scope."""

    BASE_ASSET = "base_asset"
    LINEAR_CONTRACT_COUNT = "linear_contract_count"


class WindowParameterFormat(enum.Enum):
    """Native bounded-window value representation accepted by CCXT params."""

    RFC3339_MILLISECONDS = "rfc3339_milliseconds"


@dataclass(frozen=True, slots=True)
class CompatibilityScope:
    """A client-wide or market-family-specific semantic rule key."""

    client_id: str
    market_family: MarketFamily | None = None


@dataclass(frozen=True, slots=True)
class EndpointScope:
    """The narrow operational scope of a qualified historical endpoint."""

    client_id: str
    market_family: MarketFamily
    settle: str | None = None


@dataclass(frozen=True, slots=True)
class CompatibilityPolicy:
    """Lossless semantic corrections known for one effective endpoint scope."""

    excluded_timeframes: frozenset[str] = frozenset()
    volume_mode: VolumeMode = VolumeMode.BASE_ASSET


@dataclass(frozen=True, slots=True)
class NativeWindowParameters:
    """Exact native parameter names for an adapter with broken unified bounds."""

    start: str
    end: str
    format: WindowParameterFormat


@dataclass(frozen=True, slots=True)
class ObservationProfile:
    """Qualified bounded-window behavior for one historical endpoint scope."""

    max_bars: int
    max_span: timedelta | None = None
    send_page_limit: bool = True
    send_unified_until: bool = True
    until_inclusive: bool = True
    accept_end_boundary: bool = False
    native_window_parameters: NativeWindowParameters | None = None


@dataclass(frozen=True, slots=True)
class TransportPolicy:
    """Qualified transport constraints not represented accurately by CCXT."""

    minimum_ohlcv_interval_seconds: float = 0.0


_COMPATIBILITY_POLICIES: Final[dict[CompatibilityScope, CompatibilityPolicy]] = {
    CompatibilityScope("aster"): CompatibilityPolicy(excluded_timeframes=frozenset({"1h", "3d"})),
    CompatibilityScope("bingx"): CompatibilityPolicy(excluded_timeframes=frozenset({"1M"})),
    CompatibilityScope("bingx", "spot"): CompatibilityPolicy(
        excluded_timeframes=frozenset({"6h", "12h", "1d", "3d", "1w"})
    ),
    CompatibilityScope("binance"): CompatibilityPolicy(excluded_timeframes=frozenset({"3d"})),
    CompatibilityScope("binanceusdm"): CompatibilityPolicy(
        excluded_timeframes=frozenset({"1s", "3d", "1w"})
    ),
    CompatibilityScope("bitfinex"): CompatibilityPolicy(excluded_timeframes=frozenset({"1w"})),
    CompatibilityScope("bitget", "spot"): CompatibilityPolicy(
        excluded_timeframes=frozenset({"2h"})
    ),
    CompatibilityScope("bitrue", "spot"): CompatibilityPolicy(
        excluded_timeframes=frozenset({"1d", "1w"})
    ),
    CompatibilityScope("bitstamp", "spot"): CompatibilityPolicy(
        excluded_timeframes=frozenset({"1w"})
    ),
    CompatibilityScope("bitso", "spot"): CompatibilityPolicy(
        excluded_timeframes=frozenset({"1d", "1w"})
    ),
    CompatibilityScope("btcturk", "spot"): CompatibilityPolicy(
        excluded_timeframes=frozenset({"1w"})
    ),
    CompatibilityScope("deribit"): CompatibilityPolicy(
        excluded_timeframes=frozenset({"3h", "6h", "12h", "1d"})
    ),
    CompatibilityScope("hyperliquid"): CompatibilityPolicy(
        excluded_timeframes=frozenset({"1w", "1M"})
    ),
    CompatibilityScope("htx"): CompatibilityPolicy(
        excluded_timeframes=frozenset({"1d", "1w", "1M"})
    ),
    CompatibilityScope("krakenfutures", "perpetual"): CompatibilityPolicy(
        excluded_timeframes=frozenset({"1w"})
    ),
    CompatibilityScope("kucoin", "spot"): CompatibilityPolicy(
        excluded_timeframes=frozenset({"1w"})
    ),
    CompatibilityScope("kucoinfutures", "perpetual"): CompatibilityPolicy(
        excluded_timeframes=frozenset({"3m", "6h", "1M"}),
        volume_mode=VolumeMode.LINEAR_CONTRACT_COUNT,
    ),
    CompatibilityScope("mexc", "spot"): CompatibilityPolicy(
        excluded_timeframes=frozenset({"8h", "1M"})
    ),
    CompatibilityScope("pacifica", "perpetual"): CompatibilityPolicy(
        excluded_timeframes=frozenset({"1h"})
    ),
    CompatibilityScope("xt"): CompatibilityPolicy(excluded_timeframes=frozenset({"3d"})),
    CompatibilityScope("woo"): CompatibilityPolicy(
        excluded_timeframes=frozenset({"4h", "12h", "1d", "1w", "1M"})
    ),
    **{
        CompatibilityScope(client_id, "perpetual"): CompatibilityPolicy(
            volume_mode=VolumeMode.LINEAR_CONTRACT_COUNT
        )
        for client_id in ("apex", "gate", "hashkey", "mexc", "toobit", "xt")
    },
}


def _profile(
    max_bars: int,
    *,
    max_span: timedelta | None = None,
    send_page_limit: bool = True,
    send_unified_until: bool = True,
    until_inclusive: bool = True,
    accept_end_boundary: bool = False,
    native_window_parameters: NativeWindowParameters | None = None,
) -> ObservationProfile:
    return ObservationProfile(
        max_bars=max_bars,
        max_span=max_span,
        send_page_limit=send_page_limit,
        send_unified_until=send_unified_until,
        until_inclusive=until_inclusive,
        accept_end_boundary=accept_end_boundary,
        native_window_parameters=native_window_parameters,
    )


_OBSERVATION_PROFILES: Final[dict[EndpointScope, ObservationProfile]] = {
    EndpointScope("apex", "perpetual", "USDT"): _profile(100),
    EndpointScope("aster", "spot"): _profile(100),
    EndpointScope("aster", "perpetual", "USDT"): _profile(100),
    EndpointScope("binance", "spot"): _profile(1000),
    EndpointScope("binanceusdm", "perpetual", "USDT"): _profile(1000),
    EndpointScope("bingx", "perpetual", "USDT"): _profile(100),
    EndpointScope("bitfinex", "spot"): _profile(1000, accept_end_boundary=True),
    EndpointScope("bitfinex", "perpetual", "USDT"): _profile(
        1000,
        accept_end_boundary=True,
    ),
    EndpointScope("bitget", "spot"): _profile(100, max_span=timedelta(days=90)),
    EndpointScope("bitget", "perpetual", "USDT"): _profile(100, max_span=timedelta(days=90)),
    EndpointScope("bitrue", "spot"): _profile(1000),
    EndpointScope("bitstamp", "spot"): _profile(100),
    EndpointScope("bitso", "spot"): _profile(1000, accept_end_boundary=True),
    EndpointScope("bitvavo", "spot"): _profile(100, until_inclusive=False),
    EndpointScope("btcturk", "spot"): _profile(100, accept_end_boundary=True),
    EndpointScope("bybit", "spot"): _profile(1000),
    EndpointScope("bybit", "perpetual", "USDT"): _profile(1000),
    EndpointScope("coinbase", "spot"): _profile(300),
    EndpointScope("coinbase", "perpetual", "USDC"): _profile(300),
    EndpointScope("cryptocom", "spot"): _profile(100),
    EndpointScope("cryptocom", "perpetual", "USD"): _profile(100),
    EndpointScope("deribit", "spot"): _profile(100),
    EndpointScope("deribit", "perpetual", "USDC"): _profile(100),
    EndpointScope("dydx", "perpetual", "USDC"): _profile(
        1000,
        native_window_parameters=NativeWindowParameters(
            start="fromISO",
            end="toISO",
            format=WindowParameterFormat.RFC3339_MILLISECONDS,
        ),
    ),
    EndpointScope("hashkey", "spot"): _profile(1000),
    EndpointScope("hashkey", "perpetual", "USDT"): _profile(1000),
    EndpointScope("hyperliquid", "spot"): _profile(1000),
    EndpointScope("hyperliquid", "perpetual", "USDC"): _profile(1000),
    EndpointScope("htx", "spot"): _profile(100, accept_end_boundary=True),
    EndpointScope("htx", "perpetual", "USDT"): _profile(
        100,
        send_page_limit=False,
        accept_end_boundary=True,
    ),
    EndpointScope("krakenfutures", "perpetual", "USD"): _profile(
        100,
        send_unified_until=False,
    ),
    EndpointScope("kucoin", "spot"): _profile(100, send_unified_until=False),
    EndpointScope("kucoinfutures", "perpetual", "USDT"): _profile(
        100,
        send_unified_until=False,
        accept_end_boundary=True,
    ),
    EndpointScope("mexc", "spot"): _profile(100),
    EndpointScope("mexc", "perpetual", "USDT"): _profile(100),
    EndpointScope("okx", "spot"): _profile(100),
    EndpointScope("okx", "perpetual", "USDT"): _profile(100),
    EndpointScope("pacifica", "perpetual", "USDC"): _profile(1000),
    EndpointScope("phemex", "perpetual", "USDT"): _profile(100),
    EndpointScope("toobit", "spot"): _profile(100),
    EndpointScope("upbit", "spot"): _profile(100, send_unified_until=False),
    EndpointScope("woo", "spot"): _profile(100),
    EndpointScope("woo", "perpetual", "USDT"): _profile(100),
    EndpointScope("xt", "spot"): _profile(100, until_inclusive=False),
    EndpointScope("xt", "perpetual", "USDT"): _profile(
        100,
        until_inclusive=False,
        accept_end_boundary=True,
    ),
}


_TRANSPORT_POLICIES: Final[dict[str, TransportPolicy]] = {
    # Bitfinex documents 30 public candle requests per minute. The small
    # margin avoids boundary bursts in its rolling limiter.
    "bitfinex": TransportPolicy(minimum_ohlcv_interval_seconds=2.2),
    # The public indexer returns sustained 429 responses at CCXT's current
    # cadence. A one-second interval completed the qualification campaign.
    "dydx": TransportPolicy(minimum_ohlcv_interval_seconds=1.0),
    # Hyperliquid assigns candleSnapshot a base weight of 20 plus one unit per
    # 60 returned rows. CCXT 4.5.65 prices it as four base units, so its
    # built-in limiter can exceed the venue's 1,200-weight/minute IP limit.
    "hyperliquid": TransportPolicy(minimum_ohlcv_interval_seconds=2.0),
    # Pacifica charges 12 of an anonymous IP's 100 credits per 60-second
    # rolling window for each kline request.
    "pacifica": TransportPolicy(minimum_ohlcv_interval_seconds=8.0),
}


def _validated_market_family(value: str) -> MarketFamily:
    if value == "spot":
        return "spot"
    if value == "perpetual":
        return "perpetual"
    raise UnsupportedMarketError(f"unsupported CCXT market family: {value!r}")


def compatibility_policy(
    client_id: str,
    market_family: str | None,
) -> CompatibilityPolicy:
    """Resolve client-wide and family-specific facts into one immutable policy."""
    shared = _COMPATIBILITY_POLICIES.get(
        CompatibilityScope(client_id),
        CompatibilityPolicy(),
    )
    if market_family is None:
        return shared
    family = _validated_market_family(market_family)
    specific = _COMPATIBILITY_POLICIES.get(CompatibilityScope(client_id, family))
    if specific is None:
        return shared
    return CompatibilityPolicy(
        excluded_timeframes=shared.excluded_timeframes | specific.excluded_timeframes,
        volume_mode=specific.volume_mode,
    )


def observation_profile(
    client_id: str,
    market_family: str,
    settle: str | None = None,
) -> ObservationProfile:
    """Return the exact or family-wide qualified historical profile, failing closed."""
    family = _validated_market_family(market_family)
    exact = EndpointScope(client_id, family, settle)
    profile = _OBSERVATION_PROFILES.get(exact)
    if profile is None and settle is not None:
        profile = _OBSERVATION_PROFILES.get(EndpointScope(client_id, family))
    if profile is None:
        suffix = f"/{settle}" if settle is not None else ""
        raise UnsupportedMarketError(
            f"{client_id}/{market_family}{suffix} has no qualified exhaustive "
            "fetchOHLCV pagination contract"
        )
    return profile


def qualified_client_ids() -> tuple[str, ...]:
    """Installed CCXT client IDs covered by at least one observation profile."""
    return tuple(sorted({scope.client_id for scope in _OBSERVATION_PROFILES}))


def has_observation_profile(client_id: str, market_family: MarketFamily) -> bool:
    """Whether any settlement scope in one endpoint family is qualified."""
    return any(
        scope.client_id == client_id and scope.market_family == market_family
        for scope in _OBSERVATION_PROFILES
    )


def observation_profiles(
    client_id: str,
    market_family: MarketFamily,
) -> tuple[ObservationProfile, ...]:
    """All qualified settlement profiles for one endpoint family."""
    return tuple(
        profile
        for scope, profile in _OBSERVATION_PROFILES.items()
        if scope.client_id == client_id and scope.market_family == market_family
    )


def transport_policy(client_id: str) -> TransportPolicy:
    """Return the qualified client-wide transport correction, if any."""
    return _TRANSPORT_POLICIES.get(client_id, TransportPolicy())


def validate_registries() -> None:
    """Reject malformed compatibility facts before they reach live I/O."""
    for scope, policy in _COMPATIBILITY_POLICIES.items():
        if not scope.client_id:
            raise ProviderError("CCXT compatibility client ID must not be empty")
        for timeframe in policy.excluded_timeframes:
            TimeBar.parse(timeframe)
    for scope, profile in _OBSERVATION_PROFILES.items():
        if not scope.client_id:
            raise ProviderError("CCXT observation client ID must not be empty")
        if profile.max_bars <= 0:
            raise ProviderError(f"CCXT observation max_bars must be positive for {scope!r}")
        if profile.max_span is not None and profile.max_span <= timedelta(0):
            raise ProviderError(f"CCXT observation max_span must be positive for {scope!r}")
        native_window = profile.native_window_parameters
        if native_window is not None and (
            not native_window.start
            or not native_window.end
            or native_window.start == native_window.end
        ):
            raise ProviderError(f"CCXT native window parameters are invalid for {scope!r}")
    for client_id, policy in _TRANSPORT_POLICIES.items():
        if not client_id:
            raise ProviderError("CCXT transport client ID must not be empty")
        if policy.minimum_ohlcv_interval_seconds < 0:
            raise ProviderError(f"CCXT OHLCV interval must not be negative for {client_id!r}")
