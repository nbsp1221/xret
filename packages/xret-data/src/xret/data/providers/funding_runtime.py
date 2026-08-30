"""Provider-independent validation for settled public funding history."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import cast

import polars as pl
from xret.data.errors import ProviderError, UnsupportedMarketError
from xret.data.funding_quality import enforce_settled_funding
from xret.data.models import MarketIdentity, SettledFundingKey
from xret.data.providers.contracts import (
    PROVIDER_API_VERSION,
    PROVIDER_FUNDING_SCHEMA,
    FundingObservation,
    FundingRequest,
    HistoricalFundingProvider,
    ObservedWindow,
    ProviderDescriptor,
    ResolvedFundingMarket,
)
from xret.data.providers.runtime import ProviderSnapshot, validate_provider_descriptor
from xret.data.schema import SETTLED_FUNDING_SCHEMA


@dataclass(frozen=True, slots=True)
class ValidatedFundingObservation:
    frame: pl.DataFrame
    observed: tuple[ObservedWindow, ...]
    market: ResolvedFundingMarket
    source: ProviderSnapshot
    evidence_at: datetime
    completed_at: datetime


def _clock() -> datetime:
    return datetime.now(UTC)


def _resolved(requested: MarketIdentity, value: object) -> ResolvedFundingMarket:
    if not isinstance(value, ResolvedFundingMarket):
        raise ProviderError("provider resolve_funding_market() must return ResolvedFundingMarket")
    actual = value.identity
    if (
        actual.exchange != requested.exchange
        or actual.symbol != requested.symbol
        or actual.market is not requested.market
        or (requested.settle is not None and actual.settle != requested.settle)
    ):
        raise ProviderError("provider resolved funding market changed canonical identity")
    return value


def _windows(value: object, request: FundingRequest) -> tuple[ObservedWindow, ...]:
    if not isinstance(value, tuple):
        raise ProviderError("provider observed windows must be a tuple")
    previous: datetime | None = None
    for window in value:
        if not isinstance(window, ObservedWindow):
            raise ProviderError("provider observed entries must be ObservedWindow values")
        if window.start < request.start or window.end > request.end:
            raise ProviderError("funding observed window falls outside the request")
        if previous is not None and window.start < previous:
            raise ProviderError("funding observed windows must be ordered and non-overlapping")
        previous = window.end
    return cast("tuple[ObservedWindow, ...]", value)


class FundingProviderRuntime:
    """Validate one provider's exact settled-funding capability pair."""

    def __init__(
        self,
        provider: object,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._provider = provider
        self._descriptor = validate_provider_descriptor(provider)
        self._clock = clock or _clock
        if not callable(getattr(provider, "resolve_funding_market", None)) or not callable(
            getattr(provider, "observe_funding", None)
        ):
            raise UnsupportedMarketError(
                f"provider {self._descriptor.name!r} has no settled-funding capability"
            )

    @property
    def descriptor(self) -> ProviderDescriptor:
        return self._descriptor

    def resolve_market(self, identity: MarketIdentity) -> ResolvedFundingMarket:
        provider = cast("HistoricalFundingProvider", self._provider)
        try:
            value = provider.resolve_funding_market(identity)
        except (ProviderError, UnsupportedMarketError):
            raise
        except Exception as exc:
            raise ProviderError(
                f"provider {self._descriptor.name!r} failed to resolve settled funding for "
                f"{identity.exchange}/{identity.symbol}: {exc}"
            ) from exc
        return _resolved(identity, value)

    def observe(
        self,
        request: FundingRequest,
        *,
        market: ResolvedFundingMarket | None = None,
    ) -> ValidatedFundingObservation:
        provider = cast("HistoricalFundingProvider", self._provider)
        resolved = (
            self.resolve_market(request.identity)
            if market is None
            else _resolved(request.identity, market)
        )
        evidence_at = self._clock()
        try:
            raw = provider.observe_funding(request, resolved)
        except (ProviderError, UnsupportedMarketError):
            raise
        except Exception as exc:
            raise ProviderError(
                f"provider {self._descriptor.name!r} failed to observe settled funding for "
                f"{request.identity.exchange}/{request.identity.symbol}: {exc}"
            ) from exc
        completed_at = self._clock()
        if completed_at < evidence_at:
            raise ProviderError("provider observation clock moved backwards")
        if not isinstance(raw, FundingObservation):
            raise ProviderError("provider observe_funding() must return FundingObservation")
        if not isinstance(raw.frame, pl.DataFrame) or raw.frame.schema != PROVIDER_FUNDING_SCHEMA:
            raise ProviderError(
                f"provider funding frame schema mismatch: expected {PROVIDER_FUNDING_SCHEMA}"
            )
        observed = tuple(
            ObservedWindow(window.start, min(window.end, evidence_at))
            for window in _windows(raw.observed, request)
            if window.start < evidence_at
        )
        timestamps = raw.frame.get_column("effective_at")
        if timestamps.null_count():
            raise ProviderError("provider funding observation contains null timestamps")
        for timestamp in timestamps.to_list():
            if timestamp < request.start or timestamp >= request.end:
                raise ProviderError("provider funding observation contains rows outside request")
            if not any(window.start <= timestamp < window.end for window in observed):
                raise ProviderError("provider funding row falls outside observed windows")
        identity = resolved.identity
        n = raw.frame.height
        canonical = pl.DataFrame(
            {
                "exchange": pl.Series("exchange", [identity.exchange] * n, dtype=pl.String),
                "symbol": pl.Series("symbol", [identity.symbol] * n, dtype=pl.String),
                "market": pl.Series("market", [identity.market.value] * n, dtype=pl.String),
                "settle": pl.Series("settle", [identity.settle] * n, dtype=pl.String),
                **{name: raw.frame.get_column(name) for name in PROVIDER_FUNDING_SCHEMA.names()},
            },
            schema=SETTLED_FUNDING_SCHEMA,
        )
        enforce_settled_funding(
            canonical,
            SettledFundingKey(identity=identity),
            start=request.start,
            end=request.end,
            error_cls=ProviderError,
        )
        return ValidatedFundingObservation(
            frame=canonical,
            observed=observed,
            market=resolved,
            source=ProviderSnapshot(
                descriptor=ProviderDescriptor(
                    self._descriptor.name,
                    self._descriptor.version,
                    PROVIDER_API_VERSION,
                ),
                native_market_id=resolved.native_market_id,
                native_symbol=resolved.native_symbol,
                normalizations=raw.normalizations,
            ),
            evidence_at=evidence_at,
            completed_at=completed_at,
        )
