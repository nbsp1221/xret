"""CCXT implementation of Xret's historical crypto-bar provider contract."""

from __future__ import annotations

import threading
import time
from _thread import LockType
from collections.abc import Callable
from dataclasses import dataclass, replace

import polars as pl
from xret.data.errors import ProviderError, UnsupportedMarketError
from xret.data.models import BarRequest, Market, MarketIdentity
from xret.data.providers.ccxt import client, compatibility, markets, pagination, semantics
from xret.data.providers.ccxt.live import (
    CcxtLiveBarSession,
    LiveExchangeFactory,
    create_live_exchange,
)
from xret.data.providers.contracts import (
    PROVIDER_API_VERSION,
    PROVIDER_BAR_SCHEMA,
    BarObservation,
    MarketDefinition,
    ProviderDescriptor,
    ResolvedBarMarket,
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
        self._resolution_lock = threading.RLock()
        self._clients_by_id: dict[str, client.CCXTExchange] = {}
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
                native_market = markets.resolve(identity, exchange)
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

    def fetch_markets(
        self,
        *,
        exchange: str,
        market: Market,
    ) -> tuple[MarketDefinition, ...]:
        native_client_id = markets.scoped_client_id(exchange, market)
        try:
            ccxt_exchange = self._client(native_client_id)
            native_markets = ccxt_exchange.load_markets()
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
                profile=compatibility.observation_profile(
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
