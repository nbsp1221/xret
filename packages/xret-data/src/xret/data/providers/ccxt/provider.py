"""CCXT implementation of Xret's historical crypto-bar provider contract."""

from __future__ import annotations

import threading
import time
from _thread import LockType
from collections.abc import Callable
from dataclasses import dataclass, replace
from decimal import Decimal, InvalidOperation

import polars as pl
from xret.data.errors import ProviderError, UnsupportedMarketError
from xret.data.models import (
    BarRequest,
    DataWarning,
    Market,
    MarketIdentity,
    ReferencePriceKind,
)
from xret.data.providers.ccxt import (
    capabilities,
    client,
    compatibility,
    funding_pagination,
    markets,
    oi_pagination,
    pagination,
    reference_pagination,
    semantics,
)
from xret.data.providers.ccxt.live import (
    CcxtLiveBarSession,
    LiveExchangeFactory,
    create_live_exchange,
)
from xret.data.providers.contracts import (
    PROVIDER_API_VERSION,
    PROVIDER_BAR_SCHEMA,
    PROVIDER_FUNDING_SCHEMA,
    PROVIDER_OPEN_INTEREST_SCHEMA,
    PROVIDER_REFERENCE_BAR_SCHEMA,
    BarObservation,
    FundingObservation,
    FundingRequest,
    MarketDefinition,
    OpenInterestObservation,
    OpenInterestRequest,
    ProviderDescriptor,
    ReferenceBarObservation,
    ReferenceBarRequest,
    ResolvedBarMarket,
    ResolvedFundingMarket,
    ResolvedOpenInterestMarket,
    ResolvedReferenceMarket,
)
from xret.data.timeframe import TimeBar

DEFAULT_PAGE_LIMIT = 1000
DEFAULT_MAX_RETRIES = 5
DEFAULT_RETRY_BACKOFF_BASE = 0.5


@dataclass(frozen=True, slots=True)
class _Resolution:
    """Stable ownership of one resolved CCXT market and client."""

    client_id: str
    market: ResolvedBarMarket
    exchange: client.CCXTExchange
    native_market: markets.CcxtMarket
    compatibility_policy: compatibility.CompatibilityPolicy
    observation_lock: LockType


def _market_key(market: ResolvedBarMarket) -> tuple[MarketIdentity, str, str]:
    return (market.identity, market.native_market_id, market.native_symbol)


class CcxtProvider:
    """Crypto market definitions and historical bars through unified CCXT."""

    def __init__(
        self,
        *,
        exchange_factory: client.ExchangeFactory = client.create_exchange,
        version_provider: Callable[[], str] = client.installed_version,
        page_limit: int = DEFAULT_PAGE_LIMIT,
        max_retries: int = DEFAULT_MAX_RETRIES,
        retry_backoff_base: float = DEFAULT_RETRY_BACKOFF_BASE,
        sleep: Callable[[float], None] | None = None,
        monotonic: Callable[[], float] = time.monotonic,
        tick_size_precision_mode_provider: Callable[[], int] = client.tick_size_precision_mode,
        live_exchange_factory: LiveExchangeFactory = create_live_exchange,
        live_capability_provider: capabilities.LiveCapabilityProvider = (
            capabilities.installed_live_ohlcv
        ),
    ) -> None:
        self._exchange_factory = exchange_factory
        self._version_provider = version_provider
        self._page_limit = page_limit
        self._max_retries = max_retries
        self._retry_backoff_base = retry_backoff_base
        self._sleep = time.sleep if sleep is None else sleep
        self._monotonic = monotonic
        self._tick_size_precision_mode_provider = tick_size_precision_mode_provider
        self._live_exchange_factory = live_exchange_factory
        self._funding_duplicate_counts: dict[tuple[MarketIdentity, str, str], int] = {}
        self._open_interest_duplicate_counts: dict[tuple[MarketIdentity, str, str], int] = {}
        self._live_capability_provider = live_capability_provider
        self._resolution_lock = threading.RLock()
        self._clients_by_id: dict[str, client.CCXTExchange] = {}
        self._live_capabilities_by_id: dict[str, capabilities.LiveOHLCVCapability] = {}
        self._pacers_by_id: dict[str, client.RequestPacer] = {}
        self._resolutions_by_request: dict[MarketIdentity, _Resolution] = {}
        self._resolutions_by_market: dict[
            tuple[MarketIdentity, str, str],
            _Resolution,
        ] = {}

    def _client(self, client_id: str) -> client.CCXTExchange:
        with self._resolution_lock:
            exchange = self._clients_by_id.get(client_id)
            if exchange is None:
                exchange = self._exchange_factory(client_id)
                self._clients_by_id[client_id] = exchange
                self._pacers_by_id[client_id] = client.RequestPacer(
                    minimum_interval_seconds=compatibility.transport_policy(
                        client_id
                    ).minimum_ohlcv_interval_seconds,
                    monotonic=self._monotonic,
                    sleep=self._sleep,
                )
            return exchange

    def _live_capability(self, client_id: str) -> capabilities.LiveOHLCVCapability:
        with self._resolution_lock:
            capability = self._live_capabilities_by_id.get(client_id)
            if capability is None:
                capability = self._live_capability_provider(client_id)
                self._live_capabilities_by_id[client_id] = capability
            return capability

    @property
    def descriptor(self) -> ProviderDescriptor:
        return ProviderDescriptor(
            name="ccxt",
            version=self._version_provider(),
            api_version=PROVIDER_API_VERSION,
        )

    def resolve_market(self, identity: MarketIdentity) -> ResolvedBarMarket:
        with self._resolution_lock:
            cached = self._resolutions_by_request.get(identity)
            if cached is not None:
                return cached.market

            native_client_id = markets.client_id(identity)
            try:
                exchange = self._client(native_client_id)
                native_market = markets.resolve(identity, exchange, reload=True)
            except (ProviderError, UnsupportedMarketError):
                raise
            except Exception as exc:
                raise ProviderError(
                    f"CCXT failed to resolve {identity.exchange}/{identity.symbol}: {exc}"
                ) from exc

            resolved_identity = (
                identity
                if native_market.settle is None or identity.settle == native_market.settle
                else replace(identity, settle=native_market.settle)
            )
            compatibility_policy = compatibility.compatibility_policy(
                native_client_id,
                identity.market.value,
            )
            market = ResolvedBarMarket(
                identity=resolved_identity,
                native_market_id=native_market.native_market_id,
                native_symbol=native_market.native_symbol,
                timeframes=semantics.canonical_timeframes(
                    native_client_id,
                    set(markets.supported_timeframes(exchange)),
                    native_market.metadata,
                    policy=compatibility_policy,
                ),
                derivative=(
                    markets.derivative_interpretation(native_market)
                    if identity.market is Market.PERPETUAL
                    else None
                ),
            )
            key = _market_key(market)
            resolution = self._resolutions_by_market.get(key)
            if resolution is None:
                resolution = _Resolution(
                    client_id=native_client_id,
                    market=market,
                    exchange=exchange,
                    native_market=native_market,
                    compatibility_policy=compatibility_policy,
                    observation_lock=threading.Lock(),
                )
                self._resolutions_by_market[key] = resolution
            self._resolutions_by_request[identity] = resolution
            return resolution.market

    def resolve_funding_market(self, identity: MarketIdentity) -> ResolvedFundingMarket:
        if identity.market is not Market.PERPETUAL:
            raise UnsupportedMarketError("settled funding is available only for perpetual markets")
        market = self.resolve_market(identity)
        with self._resolution_lock:
            resolution = self._resolutions_by_market[_market_key(market)]
        if not resolution.exchange.has.get("fetchFundingRateHistory"):
            raise UnsupportedMarketError(
                f"CCXT {resolution.exchange.id!r} does not advertise fetchFundingRateHistory"
            )
        return ResolvedFundingMarket(
            identity=market.identity,
            native_market_id=market.native_market_id,
            native_symbol=market.native_symbol,
        )

    def observe_funding(
        self,
        request: FundingRequest,
        market: ResolvedFundingMarket,
    ) -> FundingObservation:
        key = (market.identity, market.native_market_id, market.native_symbol)
        with self._resolution_lock:
            resolution = self._resolutions_by_market.get(key)
        if resolution is None:
            raise ProviderError("CCXT funding market was not resolved by this provider instance")
        retry = client.RetryPolicy(
            max_retries=self._max_retries,
            backoff=lambda attempt: client.exponential_backoff(
                attempt, base=self._retry_backoff_base
            ),
            sleep=self._sleep,
        )
        with resolution.observation_lock:
            result = funding_pagination.paginate_funding_history(
                exchange_id=resolution.exchange.id,
                start=request.start,
                end=request.end,
                page_limit=self._page_limit,
                fetch_page=lambda since, limit, params: client.fetch_funding_page(
                    resolution.exchange,
                    resolution.native_market.native_symbol,
                    since,
                    limit,
                    params,
                    retry,
                    self._pacers_by_id[resolution.client_id],
                ),
            )
            with self._resolution_lock:
                self._funding_duplicate_counts[key] = result.duplicate_count
        return FundingObservation(
            frame=_provider_funding_frame(result.rows),
            observed=result.observed,
        )

    def resolve_open_interest_market(self, identity: MarketIdentity) -> ResolvedOpenInterestMarket:
        if identity.market is not Market.PERPETUAL:
            raise UnsupportedMarketError("open interest is available only for perpetual markets")
        market = self.resolve_market(identity)
        with self._resolution_lock:
            resolution = self._resolutions_by_market[_market_key(market)]
        timeframes = markets.supported_open_interest_timeframes(resolution.exchange)
        if not timeframes:
            raise UnsupportedMarketError(
                f"CCXT {resolution.exchange.id!r} has no compatible fetchOpenInterestHistory"
            )
        derivative = markets.derivative_interpretation(resolution.native_market)
        is_quanto = resolution.native_market.metadata.get("quanto") is True
        if is_quanto or derivative.linear is not True or derivative.inverse is not False:
            raise UnsupportedMarketError(
                "open interest requires an unambiguous linear, non-inverse, non-quanto contract"
            )
        try:
            contract_size = Decimal(derivative.contract_size or "")
        except (InvalidOperation, ValueError) as exc:
            raise UnsupportedMarketError(
                "open interest requires an exact positive contract size"
            ) from exc
        if not contract_size.is_finite() or contract_size <= 0:
            raise UnsupportedMarketError("open interest requires an exact positive contract size")
        return ResolvedOpenInterestMarket(
            identity=market.identity,
            native_market_id=market.native_market_id,
            native_symbol=market.native_symbol,
            timeframes=timeframes,
            derivative=derivative,
        )

    def observe_open_interest(
        self,
        request: OpenInterestRequest,
        market: ResolvedOpenInterestMarket,
    ) -> OpenInterestObservation:
        key = (market.identity, market.native_market_id, market.native_symbol)
        with self._resolution_lock:
            resolution = self._resolutions_by_market.get(key)
        if resolution is None:
            raise ProviderError(
                "CCXT open-interest market was not resolved by this provider instance"
            )
        retry = client.RetryPolicy(
            max_retries=self._max_retries,
            backoff=lambda attempt: client.exponential_backoff(
                attempt, base=self._retry_backoff_base
            ),
            sleep=self._sleep,
        )
        with resolution.observation_lock:
            result = oi_pagination.paginate_open_interest_history(
                exchange_id=resolution.exchange.id,
                timeframe=request.timeframe,
                start=request.start,
                end=request.end,
                page_limit=min(self._page_limit, 500),
                fetch_page=lambda timeframe, since, limit, params: client.fetch_open_interest_page(
                    resolution.exchange,
                    resolution.native_market.native_symbol,
                    timeframe,
                    since,
                    limit,
                    params,
                    retry,
                    self._pacers_by_id[resolution.client_id],
                ),
            )
            with self._resolution_lock:
                self._open_interest_duplicate_counts[key] = result.duplicate_count
        return OpenInterestObservation(
            frame=_provider_open_interest_frame(result.rows),
            observed=result.observed,
        )

    def _open_interest_normalizations(self, market: ResolvedOpenInterestMarket) -> tuple[str, ...]:
        key = (market.identity, market.native_market_id, market.native_symbol)
        with self._resolution_lock:
            duplicates = self._open_interest_duplicate_counts.pop(key, 0)
        result: list[str] = []
        if duplicates:
            result.append("open_interest.identical_duplicate_dedup")
        return tuple(result)

    def _open_interest_source_field_mapping(self, market: ResolvedOpenInterestMarket) -> str:
        return "openInterestAmount->open_interest_amount;openInterestValue->open_interest_value"

    def resolve_reference_market(
        self, identity: MarketIdentity, kind: ReferencePriceKind
    ) -> ResolvedReferenceMarket:
        if identity.market is not Market.PERPETUAL:
            raise UnsupportedMarketError("reference bars are available only for perpetual markets")
        kind = ReferencePriceKind(kind)
        market = self.resolve_market(identity)
        with self._resolution_lock:
            resolution = self._resolutions_by_market[_market_key(market)]
        capability = {
            ReferencePriceKind.MARK: "fetchMarkOHLCV",
            ReferencePriceKind.INDEX: "fetchIndexOHLCV",
            ReferencePriceKind.PREMIUM_INDEX: "fetchPremiumIndexOHLCV",
        }[kind]
        if not resolution.exchange.has.get(capability):
            raise UnsupportedMarketError(
                f"CCXT {resolution.exchange.id!r} does not advertise {capability}"
            )
        timeframes = markets.supported_reference_timeframes(resolution.exchange)
        if not timeframes:
            raise UnsupportedMarketError(
                f"CCXT {resolution.exchange.id!r} has no canonical reference timeframes"
            )
        pair_scoped = kind is ReferencePriceKind.INDEX
        return ResolvedReferenceMarket(
            identity=market.identity,
            native_market_id=(identity.symbol if pair_scoped else market.native_market_id),
            native_symbol=(identity.symbol if pair_scoped else market.native_symbol),
            kind=kind,
            timeframes=timeframes,
            reference_target_scope="pair" if pair_scoped else "contract",
        )

    def observe_reference_bars(
        self,
        request: ReferenceBarRequest,
        market: ResolvedReferenceMarket,
    ) -> ReferenceBarObservation:
        with self._resolution_lock:
            candidates = tuple(
                value
                for value in self._resolutions_by_market.values()
                if value.market.identity == market.identity
            )
        if len(candidates) != 1:
            raise ProviderError("CCXT reference market was not resolved by this provider instance")
        resolution = candidates[0]
        method_name = {
            ReferencePriceKind.MARK: "fetch_mark_ohlcv",
            ReferencePriceKind.INDEX: "fetch_index_ohlcv",
            ReferencePriceKind.PREMIUM_INDEX: "fetch_premium_index_ohlcv",
        }[request.kind]
        retry = client.RetryPolicy(
            max_retries=self._max_retries,
            backoff=lambda attempt: client.exponential_backoff(
                attempt, base=self._retry_backoff_base
            ),
            sleep=self._sleep,
        )
        with resolution.observation_lock:
            result = reference_pagination.paginate_reference_history(
                exchange_id=resolution.exchange.id,
                time_bar=TimeBar.parse(request.timeframe),
                start=request.start,
                end=request.end,
                page_limit=self._page_limit,
                fetch_page=lambda since, limit, params: client.fetch_reference_page(
                    resolution.exchange,
                    method_name,
                    market.native_symbol,
                    request.timeframe,
                    since,
                    limit,
                    params,
                    retry,
                    self._pacers_by_id[resolution.client_id],
                ),
            )
        return ReferenceBarObservation(
            frame=_provider_reference_frame(result.rows),
            observed=result.observed,
        )

    def _funding_normalizations(self, market: ResolvedFundingMarket) -> tuple[str, ...]:
        key = (market.identity, market.native_market_id, market.native_symbol)
        with self._resolution_lock:
            duplicates = self._funding_duplicate_counts.pop(key, 0)
        return ("funding.identical_duplicate_dedup",) if duplicates else ()

    def fetch_markets(
        self,
        *,
        exchange: str,
        market: Market,
    ) -> tuple[MarketDefinition, ...]:
        native_client_id = markets.scoped_client_id(exchange, market)
        try:
            ccxt_exchange = self._client(native_client_id)
            native_markets = ccxt_exchange.load_markets(reload=True)
            if not isinstance(native_markets, dict):
                raise ProviderError("CCXT load_markets() must return a dict")
            compatible_markets = {
                symbol: metadata
                for symbol, metadata in native_markets.items()
                if not isinstance(metadata, dict)
                or semantics.supports_canonical_volume(native_client_id, metadata)
            }
            return markets.market_definitions(
                canonical_exchange=exchange,
                market_family=market,
                native_markets=compatible_markets,
                exchange=ccxt_exchange,
                tick_size_precision_mode=self._tick_size_precision_mode_provider(),
                client_id=native_client_id,
                live_capability=self._live_capability(native_client_id),
            )
        except (ProviderError, UnsupportedMarketError):
            raise
        except Exception as exc:
            raise ProviderError(
                f"CCXT failed to fetch market definitions for {exchange}/{market.value}: {exc}"
            ) from exc

    def open_live_bars(self, *, exchange: str) -> CcxtLiveBarSession:
        """Open one lazy CCXT Pro live-bar connection scope."""
        return CcxtLiveBarSession(
            exchange=exchange,
            exchange_factory=self._live_exchange_factory,
        )

    def _historical_normalizations(
        self,
        market: ResolvedBarMarket,
    ) -> tuple[str, ...]:
        policy = compatibility.compatibility_policy(
            markets.client_id(market.identity),
            market.identity.market.value,
        )
        if policy.volume_mode is compatibility.VolumeMode.BASE_ASSET:
            return ()
        return (f"volume.{policy.volume_mode.value}",)

    def _live_supports_timeframe(
        self,
        market: ResolvedBarMarket,
        timeframe: str,
    ) -> bool:
        """Interpret static Pro capability before the session confirms it again."""
        client_id = markets.client_id(market.identity)
        capability = self._live_capability(client_id)
        if not capability.available:
            return False
        if capability.timeframes is None:
            return True
        policy = compatibility.compatibility_policy(
            client_id,
            market.identity.market.value,
        )
        timeframes = semantics.canonical_timeframes(
            client_id,
            set(capability.timeframes),
            policy=policy,
            operation="live",
        )
        return timeframe in timeframes

    def _live_normalizations(
        self,
        market: ResolvedBarMarket,
    ) -> tuple[str, ...]:
        policy = compatibility.compatibility_policy(
            markets.client_id(market.identity),
            market.identity.market.value,
        )
        mode = policy.live_volume_mode or policy.volume_mode
        if mode is compatibility.VolumeMode.BASE_ASSET:
            return ()
        return (f"live_volume.{mode.value}",)

    def _live_warnings(
        self,
        market: ResolvedBarMarket,
        timeframe: str,
    ) -> tuple[DataWarning, ...]:
        capability = self._live_capability(markets.client_id(market.identity))
        if capability.available and capability.timeframes is None:
            return (
                DataWarning(
                    "provider.live_timeframes_unreported",
                    f"CCXT Pro accepted {timeframe!r} without publishing a timeframe catalog",
                ),
            )
        return ()

    def observe_bars(
        self,
        request: BarRequest,
        market: ResolvedBarMarket,
    ) -> BarObservation:
        with self._resolution_lock:
            resolution = self._resolutions_by_market.get(_market_key(market))
        if resolution is None:
            raise ProviderError(
                "CCXT observation market was not resolved by this provider instance"
            )

        with resolution.observation_lock:
            retry = client.RetryPolicy(
                max_retries=self._max_retries,
                backoff=lambda attempt: client.exponential_backoff(
                    attempt,
                    base=self._retry_backoff_base,
                ),
                sleep=self._sleep,
            )
            result = pagination.paginate_ohlcv(
                profile=compatibility.find_observation_profile(
                    resolution.client_id,
                    market.identity.market.value,
                    market.identity.settle,
                ),
                exchange_id=resolution.exchange.id,
                native_symbol=resolution.native_market.native_symbol,
                time_bar=TimeBar.parse(request.timeframe),
                start=request.start,
                end=request.end,
                requested_limit=self._page_limit,
                provider_limit=capabilities.ohlcv_page_limit(
                    resolution.exchange,
                    market_family=market.identity.market.value,
                    metadata=resolution.native_market.metadata,
                ),
                fetch_page=lambda since_ms, limit, params: client.fetch_page(
                    resolution.exchange,
                    resolution.native_market.native_symbol,
                    request.timeframe,
                    since_ms,
                    limit,
                    params,
                    retry,
                    self._pacers_by_id[resolution.client_id],
                ),
            )
        normalized_rows = semantics.normalize_ohlcv(
            resolution.client_id,
            resolution.native_market.metadata,
            result.rows,
            policy=resolution.compatibility_policy,
        )
        return BarObservation(
            frame=_provider_frame(normalized_rows),
            observed=result.observed,
        )


def _provider_funding_frame(
    rows: tuple[tuple[int, float, int | None, float | None], ...],
) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "effective_at": (
                pl.Series("effective_at", [row[0] for row in rows], dtype=pl.Int64)
                .cast(pl.Datetime(time_unit="ms"))
                .dt.replace_time_zone("UTC")
            ),
            "funding_rate": pl.Series("funding_rate", [row[1] for row in rows], dtype=pl.Float64),
            "funding_interval_seconds": pl.Series(
                "funding_interval_seconds", [row[2] for row in rows], dtype=pl.Int64
            ),
            "mark_price": pl.Series("mark_price", [row[3] for row in rows], dtype=pl.Float64),
        },
        schema=PROVIDER_FUNDING_SCHEMA,
    )


def _provider_frame(rows: tuple[tuple[float, ...], ...]) -> pl.DataFrame:
    ordered = sorted(rows, key=lambda row: row[0])
    return pl.DataFrame(
        {
            "timestamp": (
                pl.Series(
                    "timestamp",
                    [int(row[0]) for row in ordered],
                    dtype=pl.Int64,
                )
                .cast(pl.Datetime(time_unit="ms"))
                .dt.replace_time_zone("UTC")
            ),
            "open": pl.Series("open", [float(row[1]) for row in ordered], dtype=pl.Float64),
            "high": pl.Series("high", [float(row[2]) for row in ordered], dtype=pl.Float64),
            "low": pl.Series("low", [float(row[3]) for row in ordered], dtype=pl.Float64),
            "close": pl.Series("close", [float(row[4]) for row in ordered], dtype=pl.Float64),
            "volume": pl.Series("volume", [float(row[5]) for row in ordered], dtype=pl.Float64),
        },
        schema=PROVIDER_BAR_SCHEMA,
    )


def _provider_reference_frame(
    rows: tuple[tuple[float, float, float, float, float], ...],
) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "timestamp": (
                pl.Series("timestamp", [int(row[0]) for row in rows], dtype=pl.Int64)
                .cast(pl.Datetime(time_unit="ms"))
                .dt.replace_time_zone("UTC")
            ),
            "open": pl.Series("open", [row[1] for row in rows], dtype=pl.Float64),
            "high": pl.Series("high", [row[2] for row in rows], dtype=pl.Float64),
            "low": pl.Series("low", [row[3] for row in rows], dtype=pl.Float64),
            "close": pl.Series("close", [row[4] for row in rows], dtype=pl.Float64),
        },
        schema=PROVIDER_REFERENCE_BAR_SCHEMA,
    )


def _provider_open_interest_frame(
    rows: tuple[tuple[int, float, float | None], ...],
) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "timestamp": (
                pl.Series("timestamp", [row[0] for row in rows], dtype=pl.Int64)
                .cast(pl.Datetime(time_unit="ms"))
                .dt.replace_time_zone("UTC")
            ),
            "open_interest_amount": pl.Series(
                "open_interest_amount", [row[1] for row in rows], dtype=pl.Float64
            ),
            "open_interest_value": pl.Series(
                "open_interest_value", [row[2] for row in rows], dtype=pl.Float64
            ),
        },
        schema=PROVIDER_OPEN_INTEREST_SCHEMA,
    )
