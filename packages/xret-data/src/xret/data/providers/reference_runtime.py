"""Provider-independent validation for historical reference-price bars."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import cast

import polars as pl
from xret.data.errors import ProviderError, UnsupportedMarketError
from xret.data.models import MarketIdentity, ReferenceBarKey
from xret.data.providers.contracts import (
    PROVIDER_API_VERSION,
    PROVIDER_REFERENCE_BAR_SCHEMA,
    HistoricalReferenceBarProvider,
    ObservedWindow,
    ProviderDescriptor,
    ReferenceBarObservation,
    ReferenceBarRequest,
    ResolvedReferenceMarket,
)
from xret.data.providers.runtime import (
    DEFAULT_FINALITY_GRACE,
    ProviderSnapshot,
    validate_aligned_observed_windows,
    validate_provider_descriptor,
)
from xret.data.reference_quality import enforce_reference_bars
from xret.data.schema import REFERENCE_BAR_SCHEMA
from xret.data.timeframe import TimeBar


@dataclass(frozen=True, slots=True)
class ValidatedReferenceBarObservation:
    frame: pl.DataFrame
    observed: tuple[ObservedWindow, ...]
    market: ResolvedReferenceMarket
    source: ProviderSnapshot
    evidence_at: datetime
    completed_at: datetime


def _clock() -> datetime:
    return datetime.now(UTC)


def _resolved(requested: MarketIdentity, kind: object, value: object) -> ResolvedReferenceMarket:
    if not isinstance(value, ResolvedReferenceMarket):
        raise ProviderError(
            "provider resolve_reference_market() must return ResolvedReferenceMarket"
        )
    actual = value.identity
    if (
        actual.exchange != requested.exchange
        or actual.symbol != requested.symbol
        or actual.market is not requested.market
        or (requested.settle is not None and actual.settle != requested.settle)
        or value.kind is not kind
    ):
        raise ProviderError("provider resolved reference market changed canonical identity or kind")
    return value


def _windows(value: object, request: ReferenceBarRequest) -> tuple[ObservedWindow, ...]:
    if not isinstance(value, tuple):
        raise ProviderError("provider observed windows must be a tuple")
    previous: datetime | None = None
    for window in value:
        if not isinstance(window, ObservedWindow):
            raise ProviderError("provider observed entries must be ObservedWindow values")
        if window.start < request.start or window.end > request.end:
            raise ProviderError("reference observed window falls outside the request")
        if previous is not None and window.start < previous:
            raise ProviderError("reference observed windows must be ordered and non-overlapping")
        previous = window.end
    return cast("tuple[ObservedWindow, ...]", value)


class ReferenceBarProviderRuntime:
    """Validate one provider's exact historical-reference capability pair."""

    def __init__(self, provider: object, *, clock: Callable[[], datetime] | None = None) -> None:
        self._provider = provider
        self._descriptor = validate_provider_descriptor(provider)
        self._clock = clock or _clock
        if not callable(getattr(provider, "resolve_reference_market", None)) or not callable(
            getattr(provider, "observe_reference_bars", None)
        ):
            raise UnsupportedMarketError(
                f"provider {self._descriptor.name!r} has no reference-bar capability"
            )

    @property
    def descriptor(self) -> ProviderDescriptor:
        return self._descriptor

    def resolve_market(self, request: ReferenceBarRequest) -> ResolvedReferenceMarket:
        provider = cast("HistoricalReferenceBarProvider", self._provider)
        try:
            value = provider.resolve_reference_market(request.identity, request.kind)
        except (ProviderError, UnsupportedMarketError):
            raise
        except Exception as exc:
            raise ProviderError(
                f"provider {self._descriptor.name!r} failed to resolve {request.kind.value} "
                f"reference bars for {request.identity.exchange}/{request.identity.symbol}: {exc}"
            ) from exc
        resolved = _resolved(request.identity, request.kind, value)
        if request.timeframe not in resolved.timeframes:
            raise UnsupportedMarketError(
                f"provider {self._descriptor.name!r} does not support {request.kind.value} "
                f"reference timeframe {request.timeframe!r}"
            )
        return resolved

    def observe(
        self,
        request: ReferenceBarRequest,
        *,
        market: ResolvedReferenceMarket | None = None,
    ) -> ValidatedReferenceBarObservation:
        provider = cast("HistoricalReferenceBarProvider", self._provider)
        resolved = (
            self.resolve_market(request)
            if market is None
            else _resolved(request.identity, request.kind, market)
        )
        if request.timeframe not in resolved.timeframes:
            raise UnsupportedMarketError(
                f"provider {self._descriptor.name!r} does not support {request.kind.value} "
                f"reference timeframe {request.timeframe!r}"
            )
        evidence_at = self._clock()
        try:
            raw = provider.observe_reference_bars(request, resolved)
        except (ProviderError, UnsupportedMarketError):
            raise
        except Exception as exc:
            raise ProviderError(
                f"provider {self._descriptor.name!r} failed to observe {request.kind.value} "
                f"reference bars: {exc}"
            ) from exc
        completed_at = self._clock()
        if completed_at < evidence_at:
            raise ProviderError("provider observation clock moved backwards")
        if not isinstance(raw, ReferenceBarObservation):
            raise ProviderError(
                "provider observe_reference_bars() must return ReferenceBarObservation"
            )
        if (
            not isinstance(raw.frame, pl.DataFrame)
            or raw.frame.schema != PROVIDER_REFERENCE_BAR_SCHEMA
        ):
            raise ProviderError(
                "provider reference frame schema mismatch: "
                f"expected {PROVIDER_REFERENCE_BAR_SCHEMA}"
            )
        time_bar = TimeBar.parse(request.timeframe)
        observed = validate_aligned_observed_windows(
            raw.observed,
            start=request.start,
            end=request.end,
            time_bar=time_bar,
            family="reference",
        )
        timestamps = raw.frame.get_column("timestamp")
        if timestamps.null_count():
            raise ProviderError("provider reference observation contains null timestamps")
        finalizable_end = min(request.end, time_bar.floor(evidence_at - DEFAULT_FINALITY_GRACE))
        for timestamp in timestamps.to_list():
            if timestamp < request.start or timestamp >= request.end:
                raise ProviderError("provider reference observation contains rows outside request")
            if time_bar.next_boundary(timestamp) > finalizable_end:
                raise ProviderError("provider reference observation contains a non-final bar")
            if not any(window.start <= timestamp < window.end for window in observed):
                raise ProviderError("provider reference row falls outside observed windows")
        identity = resolved.identity
        n = raw.frame.height
        canonical = pl.DataFrame(
            {
                "exchange": pl.Series("exchange", [identity.exchange] * n, dtype=pl.String),
                "symbol": pl.Series("symbol", [identity.symbol] * n, dtype=pl.String),
                "market": pl.Series("market", [identity.market.value] * n, dtype=pl.String),
                "settle": pl.Series("settle", [identity.settle] * n, dtype=pl.String),
                "timeframe": pl.Series("timeframe", [request.timeframe] * n, dtype=pl.String),
                **{
                    name: raw.frame.get_column(name)
                    for name in PROVIDER_REFERENCE_BAR_SCHEMA.names()
                },
            },
            schema=REFERENCE_BAR_SCHEMA,
        )
        enforce_reference_bars(
            canonical,
            ReferenceBarKey(identity=identity, kind=request.kind, timeframe=request.timeframe),
            start=request.start,
            end=request.end,
            error_cls=ProviderError,
        )
        return ValidatedReferenceBarObservation(
            frame=canonical,
            observed=observed,
            market=resolved,
            source=ProviderSnapshot(
                descriptor=ProviderDescriptor(
                    self._descriptor.name, self._descriptor.version, PROVIDER_API_VERSION
                ),
                native_market_id=resolved.native_market_id,
                native_symbol=resolved.native_symbol,
                normalizations=(f"reference_target_scope.{resolved.reference_target_scope}",),
            ),
            evidence_at=evidence_at,
            completed_at=completed_at,
        )
