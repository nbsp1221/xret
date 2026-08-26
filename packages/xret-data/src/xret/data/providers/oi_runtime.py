"""Provider-independent validation for historical open-interest observations."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import cast

import polars as pl
from xret.data.errors import ProviderError, UnsupportedMarketError
from xret.data.models import MarketIdentity, OpenInterestKey
from xret.data.open_interest_quality import enforce_open_interest
from xret.data.providers.contracts import (
    PROVIDER_API_VERSION,
    PROVIDER_OPEN_INTEREST_SCHEMA,
    HistoricalOpenInterestProvider,
    ObservedWindow,
    OpenInterestObservation,
    OpenInterestRequest,
    OpenInterestSourceEvidence,
    ProviderDescriptor,
    ResolvedOpenInterestMarket,
)
from xret.data.providers.runtime import (
    ProviderSnapshot,
    validate_aligned_observed_windows,
    validate_provider_descriptor,
)
from xret.data.schema import OPEN_INTEREST_SCHEMA
from xret.data.timeframe import TimeBar


@dataclass(frozen=True, slots=True)
class ValidatedOpenInterestObservation:
    frame: pl.DataFrame
    observed: tuple[ObservedWindow, ...]
    market: ResolvedOpenInterestMarket
    source: ProviderSnapshot
    source_field_mapping: str | None
    contributors: tuple[OpenInterestSourceEvidence, ...]
    evidence_at: datetime
    completed_at: datetime


def _clock() -> datetime:
    return datetime.now(UTC)


def _resolved(requested: MarketIdentity, value: object) -> ResolvedOpenInterestMarket:
    if not isinstance(value, ResolvedOpenInterestMarket):
        raise ProviderError(
            "provider resolve_open_interest_market() must return ResolvedOpenInterestMarket"
        )
    actual = value.identity
    if (
        actual.exchange != requested.exchange
        or actual.symbol != requested.symbol
        or actual.market is not requested.market
        or (requested.settle is not None and actual.settle != requested.settle)
    ):
        raise ProviderError("provider resolved open-interest market changed canonical identity")
    return value


def _windows(value: object, request: OpenInterestRequest) -> tuple[ObservedWindow, ...]:
    if not isinstance(value, tuple):
        raise ProviderError("provider observed windows must be a tuple")
    previous: datetime | None = None
    for window in value:
        if not isinstance(window, ObservedWindow):
            raise ProviderError("provider observed entries must be ObservedWindow values")
        if window.start < request.start or window.end > request.end:
            raise ProviderError("open-interest observed window falls outside the request")
        if previous is not None and window.start < previous:
            raise ProviderError(
                "open-interest observed windows must be ordered and non-overlapping"
            )
        previous = window.end
    return cast("tuple[ObservedWindow, ...]", value)


def _contributors(
    value: object,
    request: OpenInterestRequest,
    observed: tuple[ObservedWindow, ...],
    descriptor: ProviderDescriptor,
    market: ResolvedOpenInterestMarket,
    completed_at: datetime,
    timestamps: list[datetime],
) -> tuple[OpenInterestSourceEvidence, ...]:
    if not isinstance(value, tuple):
        raise ProviderError("provider OI source contributors must be a tuple")
    if not value:
        return tuple(
            OpenInterestSourceEvidence(
                start=window.start,
                end=window.end,
                source_route=market.native_market_id,
                source_revision=f"provider:{descriptor.name}:{descriptor.version}",
                retrieved_at=completed_at,
                source_rows=sum(window.start <= timestamp < window.end for timestamp in timestamps),
                canonical_rows=sum(
                    window.start <= timestamp < window.end for timestamp in timestamps
                ),
            )
            for window in observed
        )
    previous: datetime | None = None
    result: list[OpenInterestSourceEvidence] = []
    for item in value:
        if not isinstance(item, OpenInterestSourceEvidence):
            raise ProviderError("provider OI contributors must be OpenInterestSourceEvidence")
        if item.start < request.start or item.end > request.end:
            raise ProviderError("provider OI contributor falls outside the request")
        if previous is not None and item.start < previous:
            raise ProviderError("provider OI contributors must be ordered and non-overlapping")
        expected_rows = sum(item.start <= timestamp < item.end for timestamp in timestamps)
        if item.canonical_rows != expected_rows:
            raise ProviderError(
                "provider OI contributor canonical row count does not match returned rows"
            )
        previous = item.end
        result.append(item)
    for window in observed:
        cursor = window.start
        for item in result:
            if item.end <= cursor:
                continue
            if item.start > cursor:
                break
            cursor = max(cursor, item.end)
            if cursor >= window.end:
                break
        if cursor < window.end:
            raise ProviderError("provider OI contributors must cover every observed window")
    return tuple(result)


class OpenInterestProviderRuntime:
    """Validate one provider's exact historical open-interest capability pair."""

    def __init__(self, provider: object, *, clock: Callable[[], datetime] | None = None) -> None:
        self._provider = provider
        self._descriptor = validate_provider_descriptor(provider)
        self._clock = clock or _clock
        if not callable(getattr(provider, "resolve_open_interest_market", None)) or not callable(
            getattr(provider, "observe_open_interest", None)
        ):
            raise UnsupportedMarketError(
                f"provider {self._descriptor.name!r} has no open-interest capability"
            )

    @property
    def descriptor(self) -> ProviderDescriptor:
        return self._descriptor

    def resolve_market(self, identity: MarketIdentity) -> ResolvedOpenInterestMarket:
        provider = cast("HistoricalOpenInterestProvider", self._provider)
        try:
            value = provider.resolve_open_interest_market(identity)
        except (ProviderError, UnsupportedMarketError):
            raise
        except Exception as exc:
            raise ProviderError(
                f"provider {self._descriptor.name!r} failed to resolve open interest for "
                f"{identity.exchange}/{identity.symbol}: {exc}"
            ) from exc
        return _resolved(identity, value)

    def observe(
        self,
        request: OpenInterestRequest,
        *,
        market: ResolvedOpenInterestMarket | None = None,
    ) -> ValidatedOpenInterestObservation:
        provider = cast("HistoricalOpenInterestProvider", self._provider)
        resolved = (
            self.resolve_market(request.identity)
            if market is None
            else _resolved(request.identity, market)
        )
        if request.timeframe not in resolved.timeframes:
            raise UnsupportedMarketError(
                f"provider {self._descriptor.name!r} does not support open-interest "
                f"timeframe {request.timeframe!r}"
            )
        evidence_at = self._clock()
        try:
            raw = provider.observe_open_interest(request, resolved)
        except (ProviderError, UnsupportedMarketError):
            raise
        except Exception as exc:
            raise ProviderError(
                f"provider {self._descriptor.name!r} failed to observe open interest: {exc}"
            ) from exc
        completed_at = self._clock()
        if completed_at < evidence_at:
            raise ProviderError("provider observation clock moved backwards")
        if not isinstance(raw, OpenInterestObservation):
            raise ProviderError(
                "provider observe_open_interest() must return OpenInterestObservation"
            )
        if (
            not isinstance(raw.frame, pl.DataFrame)
            or raw.frame.schema != PROVIDER_OPEN_INTEREST_SCHEMA
        ):
            raise ProviderError(
                "provider open-interest frame schema mismatch: "
                f"expected {PROVIDER_OPEN_INTEREST_SCHEMA}"
            )
        observed = validate_aligned_observed_windows(
            raw.observed,
            start=request.start,
            end=request.end,
            time_bar=TimeBar.parse(request.timeframe),
            family="open-interest",
        )
        timestamps = raw.frame.get_column("timestamp")
        if timestamps.null_count():
            raise ProviderError("provider open-interest observation contains null timestamps")
        for timestamp in timestamps.to_list():
            if timestamp < request.start or timestamp >= request.end:
                raise ProviderError(
                    "provider open-interest observation contains rows outside request"
                )
            if not any(window.start <= timestamp < window.end for window in observed):
                raise ProviderError("provider open-interest row falls outside observed windows")
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
                    for name in PROVIDER_OPEN_INTEREST_SCHEMA.names()
                },
            },
            schema=OPEN_INTEREST_SCHEMA,
        )
        enforce_open_interest(
            canonical,
            OpenInterestKey(identity=identity, timeframe=request.timeframe),
            start=request.start,
            end=request.end,
            error_cls=ProviderError,
        )
        normalizations: tuple[str, ...] = ()
        hook = getattr(self._provider, "_open_interest_normalizations", None)
        if callable(hook):
            candidate = hook(resolved)
            if not isinstance(candidate, tuple) or not all(
                isinstance(item, str) and item for item in candidate
            ):
                raise ProviderError(
                    "provider open-interest normalizations hook returned invalid values"
                )
            normalizations = candidate
        source_field_mapping: str | None = None
        mapping_hook = getattr(self._provider, "_open_interest_source_field_mapping", None)
        if callable(mapping_hook):
            candidate = mapping_hook(resolved)
            if not isinstance(candidate, str) or not candidate:
                raise ProviderError(
                    "provider open-interest source-field mapping hook returned an invalid value"
                )
            source_field_mapping = candidate
        contributors = _contributors(
            raw.sources,
            request,
            observed,
            self._descriptor,
            resolved,
            completed_at,
            timestamps.to_list(),
        )
        for timestamp in timestamps.to_list():
            if not any(item.start <= timestamp < item.end for item in contributors):
                raise ProviderError("provider open-interest row falls outside contributors")
        normalizations = tuple(
            dict.fromkeys(
                (
                    *normalizations,
                    *(item for source in raw.sources for item in source.normalizations),
                )
            )
        )
        return ValidatedOpenInterestObservation(
            frame=canonical,
            observed=observed,
            market=resolved,
            source=ProviderSnapshot(
                descriptor=ProviderDescriptor(
                    self._descriptor.name, self._descriptor.version, PROVIDER_API_VERSION
                ),
                native_market_id=resolved.native_market_id,
                native_symbol=resolved.native_symbol,
                normalizations=normalizations,
            ),
            source_field_mapping=source_field_mapping,
            contributors=contributors,
            evidence_at=evidence_at,
            completed_at=completed_at,
        )
