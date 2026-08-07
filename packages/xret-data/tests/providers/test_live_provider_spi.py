from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest
from xret.data.errors import ProviderError, UnsupportedMarketError
from xret.data.models import MarketIdentity
from xret.data.providers import (
    PROVIDER_API_VERSION,
    ProviderBarUpdate,
    ProviderDescriptor,
    ResolvedBarMarket,
)
from xret.data.providers.live_runtime import LiveBarRuntime

T0 = datetime(2026, 8, 7, tzinfo=UTC)


class Session:
    def __init__(self, values: list[object] | None = None, *, failure: Exception | None = None):
        self.values = values or []
        self.failure = failure
        self.index = 0

    async def __aenter__(self) -> Session:
        return self

    async def __aexit__(self, *args: object) -> None:
        return None

    async def subscribe_bar_updates(self, market: ResolvedBarMarket, timeframe: str) -> None:
        return None

    def __aiter__(self) -> Session:
        return self

    async def __anext__(self) -> object:
        if self.failure is not None:
            raise self.failure
        if self.index >= len(self.values):
            raise StopAsyncIteration
        value = self.values[self.index]
        self.index += 1
        return value


class Provider:
    descriptor = ProviderDescriptor(name="live-spi", version="1", api_version=PROVIDER_API_VERSION)

    def __init__(self, session: Session, *, timeframes: frozenset[str] | None = None):
        self.session = session
        self.timeframes = timeframes or frozenset({"1m"})

    def resolve_market(self, identity: MarketIdentity) -> ResolvedBarMarket:
        return ResolvedBarMarket(
            identity=identity,
            native_market_id="BTCUSDT",
            native_symbol="BTC/USDT",
            timeframes=self.timeframes,
        )

    def observe_bars(self, request: object, market: object) -> object:
        raise AssertionError("historical observation is not part of this test")

    def open_live_bars(self, *, exchange: str) -> Session:
        return self.session


def _identity(symbol: str = "BTC/USDT") -> MarketIdentity:
    return MarketIdentity(exchange="binance", symbol=symbol, market="spot")


def _update(*, identity: MarketIdentity | None = None, timeframe: str = "1m"):
    return ProviderBarUpdate(
        identity=identity or _identity(),
        timeframe=timeframe,
        timestamp=T0,
        open=100.0,
        high=102.0,
        low=99.0,
        close=101.0,
        volume=1.0,
    )


def test_runtime_rejects_wrong_update_type_and_scope() -> None:
    async def scenario(value: object, match: str) -> None:
        runtime = LiveBarRuntime(Provider(Session([value])), exchange="binance")
        async with runtime:
            await runtime.subscribe(_identity(), "1m")
            with pytest.raises(ProviderError, match=match):
                await anext(runtime.updates())

    asyncio.run(scenario(object(), "must yield ProviderBarUpdate"))
    asyncio.run(scenario(_update(identity=_identity("ETH/USDT")), "inactive subscription"))
    asyncio.run(scenario(_update(timeframe="1h"), "inactive subscription"))


def test_runtime_rejects_provider_unsupported_timeframe_before_subscribe() -> None:
    async def scenario() -> None:
        runtime = LiveBarRuntime(
            Provider(Session(), timeframes=frozenset({"1h"})), exchange="binance"
        )
        async with runtime:
            with pytest.raises(UnsupportedMarketError, match="does not support timeframe"):
                await runtime.subscribe(_identity(), "1m")

    asyncio.run(scenario())


def test_runtime_chains_unknown_stream_failure() -> None:
    async def scenario() -> None:
        failure = RuntimeError("socket failed")
        runtime = LiveBarRuntime(Provider(Session(failure=failure)), exchange="binance")
        async with runtime:
            await runtime.subscribe(_identity(), "1m")
            with pytest.raises(ProviderError, match="stream failed") as captured:
                await anext(runtime.updates())
            assert captured.value.__cause__ is failure

    asyncio.run(scenario())


def test_runtime_treats_unexpected_stream_end_as_terminal_failure() -> None:
    async def scenario() -> None:
        runtime = LiveBarRuntime(Provider(Session()), exchange="binance")
        async with runtime:
            await runtime.subscribe(_identity(), "1m")
            with pytest.raises(ProviderError, match="stream ended unexpectedly"):
                await anext(runtime.updates())

    asyncio.run(scenario())


def test_runtime_rejects_invalid_receipt_clock() -> None:
    async def scenario() -> None:
        runtime = LiveBarRuntime(
            Provider(Session([_update()])),
            exchange="binance",
            clock=lambda: datetime(2026, 8, 7),
        )
        async with runtime:
            await runtime.subscribe(_identity(), "1m")
            with pytest.raises(ProviderError, match="received_at"):
                await anext(runtime.updates())

    asyncio.run(scenario())
