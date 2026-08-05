"""Provider-independent execution and validation for remote market data."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import cast

import polars as pl
from xret.data.errors import ProviderError, UnsupportedMarketError
from xret.data.models import BarRequest, Market, MarketIdentity
from xret.data.providers.contracts import (
    PROVIDER_API_VERSION,
    PROVIDER_BAR_SCHEMA,
    BarObservation,
    HistoricalBarProvider,
    MarketDefinition,
    ObservedWindow,
    ProviderDescriptor,
    ResolvedBarMarket,
)
from xret.data.quality import enforce_ohlcv_batch
from xret.data.schema import OHLCV_SCHEMA
from xret.data.timeframe import TimeBar

DEFAULT_FINALITY_GRACE = timedelta(seconds=5)


@dataclass(frozen=True, slots=True)
class ProviderSnapshot:
    descriptor: ProviderDescriptor
    native_market_id: str
    native_symbol: str


@dataclass(frozen=True, slots=True)
class ValidatedBarObservation:
    frame: pl.DataFrame
    observed: tuple[ObservedWindow, ...]
    market: ResolvedBarMarket
    source: ProviderSnapshot
    evidence_at: datetime
    completed_at: datetime


class MarketDefinitionRuntime:
    """Validate one optional provider market-definition operation."""

    def __init__(self, provider: HistoricalBarProvider) -> None:
        self._provider = provider
        self._descriptor = validate_provider_descriptor(provider)

    def fetch_markets(
        self,
        *,
        exchange: str,
        market: Market,
    ) -> tuple[MarketDefinition, ...]:
        try:
            method = getattr(self._provider, "fetch_markets", None)
        except Exception as exc:
            raise ProviderError(
                f"provider {self._descriptor.name!r} market-definition capability "
                f"access failed: {exc}"
            ) from exc
        if not callable(method):
            raise UnsupportedMarketError(
                f"provider {self._descriptor.name!r} has no market-definition capability"
            )
        try:
            result = method(exchange=exchange, market=market)
        except (UnsupportedMarketError, ProviderError):
            raise
        except Exception as exc:
            raise ProviderError(
                f"provider {self._descriptor.name!r} failed to fetch market definitions "
                f"for {exchange}/{market.value}: {exc}"
            ) from exc
        if not isinstance(result, tuple):
            raise ProviderError("provider fetch_markets() must return a tuple")
        identities: set[MarketIdentity] = set()
        for definition in result:
            if not isinstance(definition, MarketDefinition):
                raise ProviderError("provider market entries must be MarketDefinition values")
            identity = definition.identity
            if identity.exchange != exchange or identity.market is not market:
                raise ProviderError("provider returned a market definition outside requested scope")
            if identity in identities:
                raise ProviderError(f"provider returned duplicate canonical identity: {identity!r}")
            identities.add(identity)
        return result


def _default_clock() -> datetime:
    return datetime.now(UTC)


_clock_override: Callable[[], datetime] | None = None


def _set_clock_override(clock: Callable[[], datetime] | None) -> None:
    global _clock_override
    _clock_override = clock


def _clock() -> datetime:
    return _clock_override() if _clock_override is not None else _default_clock()


def default_end(time_bar: TimeBar, *, grace: timedelta = DEFAULT_FINALITY_GRACE) -> datetime:
    return time_bar.floor(_clock() - grace)


def _validate_descriptor(descriptor: object) -> ProviderDescriptor:
    if not isinstance(descriptor, ProviderDescriptor):
        raise ProviderError("provider descriptor must be a ProviderDescriptor")
    if descriptor.api_version != PROVIDER_API_VERSION:
        raise ProviderError(
            f"provider {descriptor.name!r} implements API version "
            f"{descriptor.api_version}, expected {PROVIDER_API_VERSION}"
        )
    return descriptor


def validate_provider_descriptor(provider: object) -> ProviderDescriptor:
    """Validate the descriptor of an untrusted direct or discovered provider."""
    try:
        descriptor = cast("HistoricalBarProvider", provider).descriptor
    except Exception as exc:
        raise ProviderError(f"provider descriptor access failed: {exc}") from exc
    return _validate_descriptor(descriptor)


def _validate_resolved_market(
    requested: MarketIdentity,
    resolved: object,
) -> ResolvedBarMarket:
    if not isinstance(resolved, ResolvedBarMarket):
        raise ProviderError("provider resolve_market() must return ResolvedBarMarket")
    actual = resolved.identity
    if (
        actual.exchange != requested.exchange
        or actual.symbol != requested.symbol
        or actual.market is not requested.market
    ):
        raise ProviderError("provider resolved market changed canonical venue, symbol, or market")
    if requested.settle is not None and actual.settle != requested.settle:
        raise ProviderError("provider resolved market changed requested settlement")
    return resolved


def _validate_windows(
    windows: object,
    request: BarRequest,
    time_bar: TimeBar,
) -> None:
    if not isinstance(windows, tuple):
        raise ProviderError("provider observed windows must be a tuple")
    expected = request.start
    for window in windows:
        if not isinstance(window, ObservedWindow):
            raise ProviderError("provider observed entries must be ObservedWindow values")
        if window.start != expected or window.end > request.end:
            raise ProviderError(
                "incomplete provider observation: observed windows do not "
                f"contiguously cover [{request.start.isoformat()}, {request.end.isoformat()})"
            )
        if time_bar.floor(window.start) != window.start or time_bar.floor(window.end) != window.end:
            raise ProviderError(
                "invalid provider observation: window boundaries are not aligned to "
                f"{request.timeframe}"
            )
        expected = window.end
    if expected != request.end:
        raise ProviderError(
            "incomplete provider observation: observed windows end at "
            f"{expected.isoformat()}, expected {request.end.isoformat()}"
        )


def _validate_provider_frame(frame: object) -> pl.DataFrame:
    if not isinstance(frame, pl.DataFrame):
        raise ProviderError("provider observation frame must be a Polars DataFrame")
    if frame.schema != PROVIDER_BAR_SCHEMA:
        raise ProviderError(
            f"provider observation frame schema mismatch: expected {PROVIDER_BAR_SCHEMA}, "
            f"got {frame.schema}"
        )
    return frame


def _canonical_frame(
    frame: pl.DataFrame,
    request: BarRequest,
    market: ResolvedBarMarket,
    evidence_at: datetime,
    time_bar: TimeBar,
) -> pl.DataFrame:
    finalizable_end = min(
        request.end,
        time_bar.floor(evidence_at - DEFAULT_FINALITY_GRACE),
    )
    finalized = frame.filter(
        (pl.col("timestamp") >= request.start) & (pl.col("timestamp") < finalizable_end)
    )
    n = finalized.height
    identity = market.identity
    return pl.DataFrame(
        {
            "exchange": pl.Series("exchange", [identity.exchange] * n, dtype=pl.String),
            "symbol": pl.Series("symbol", [identity.symbol] * n, dtype=pl.String),
            "market": pl.Series("market", [identity.market.value] * n, dtype=pl.String),
            "settle": pl.Series("settle", [identity.settle] * n, dtype=pl.String),
            "timeframe": pl.Series("timeframe", [request.timeframe] * n, dtype=pl.String),
            **{name: finalized.get_column(name) for name in PROVIDER_BAR_SCHEMA.names()},
        },
        schema=OHLCV_SCHEMA,
    )


class ProviderRuntime:
    """One provider-bound validation context for a remote operation."""

    def __init__(self, provider: HistoricalBarProvider) -> None:
        self._provider = provider
        self._descriptor = validate_provider_descriptor(provider)

    @property
    def descriptor(self) -> ProviderDescriptor:
        """The immutable descriptor snapshot used by this runtime."""
        return self._descriptor

    def resolve_market(
        self,
        identity: MarketIdentity,
    ) -> ResolvedBarMarket:
        try:
            resolved = self._provider.resolve_market(identity)
        except (UnsupportedMarketError, ProviderError):
            raise
        except Exception as exc:
            raise ProviderError(
                f"provider {self._descriptor.name!r} failed to resolve "
                f"{identity.exchange}/{identity.symbol}: {exc}"
            ) from exc
        return _validate_resolved_market(identity, resolved)

    def observe(
        self,
        request: BarRequest,
        *,
        market: ResolvedBarMarket | None = None,
    ) -> ValidatedBarObservation:
        resolved = (
            self.resolve_market(request.identity)
            if market is None
            else _validate_resolved_market(request.identity, market)
        )
        if request.timeframe not in resolved.timeframes:
            raise UnsupportedMarketError(
                f"provider {self._descriptor.name!r} does not support timeframe "
                f"{request.timeframe!r} for {request.identity.exchange}/{request.identity.symbol}"
            )
        time_bar = TimeBar.parse(request.timeframe)
        if (
            time_bar.floor(request.start) != request.start
            or time_bar.floor(request.end) != request.end
        ):
            raise ProviderError(
                f"provider request boundaries are not aligned to {request.timeframe}"
            )
        evidence_at = _clock()
        try:
            raw = self._provider.observe_bars(request, resolved)
        except (UnsupportedMarketError, ProviderError):
            raise
        except Exception as exc:
            raise ProviderError(
                f"provider {self._descriptor.name!r} failed to observe "
                f"{request.identity.exchange}/{request.identity.symbol}: {exc}"
            ) from exc
        completed_at = _clock()
        if completed_at < evidence_at:
            raise ProviderError(
                "provider observation clock moved backwards: "
                f"evidence_at={evidence_at.isoformat()} "
                f"completed_at={completed_at.isoformat()}"
            )
        if not isinstance(raw, BarObservation):
            raise ProviderError("provider observe_bars() must return BarObservation")
        frame = _validate_provider_frame(raw.frame)
        _validate_windows(raw.observed, request, time_bar)
        timestamps = frame.get_column("timestamp")
        if timestamps.null_count():
            raise ProviderError("provider observation contains null timestamps")
        timestamp_values = timestamps.to_list()
        for timestamp in timestamp_values:
            if timestamp < request.start or timestamp >= request.end:
                raise ProviderError("provider observation contains rows outside the request")
            if not any(window.start <= timestamp < window.end for window in raw.observed):
                raise ProviderError("provider observation contains a row outside observed windows")
        canonical = _canonical_frame(frame, request, resolved, evidence_at, time_bar)
        enforce_ohlcv_batch(canonical, request, error_cls=ProviderError)
        return ValidatedBarObservation(
            frame=canonical,
            observed=raw.observed,
            market=resolved,
            source=ProviderSnapshot(
                descriptor=self._descriptor,
                native_market_id=resolved.native_market_id,
                native_symbol=resolved.native_symbol,
            ),
            evidence_at=evidence_at,
            completed_at=completed_at,
        )
