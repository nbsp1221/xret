from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

import polars as pl
import pytest
from xret.data import BarDataset, BarFinality, BarUpdate, MarketData
from xret.data.config import MarketDataConfig
from xret.data.errors import InvalidRequestError, ProviderError, UnsupportedMarketError
from xret.data.live import LiveMarketData
from xret.data.models import MarketIdentity
from xret.data.providers import (
    PROVIDER_API_VERSION,
    PROVIDER_BAR_SCHEMA,
    BarObservation,
    BarRequest,
    ObservedWindow,
    ProviderBarUpdate,
    ProviderDescriptor,
    ResolvedBarMarket,
)
from xret.data.providers import runtime as provider_runtime
from xret.data.providers.discovery import ProviderHandle

T0 = datetime(2026, 8, 7, 0, 0, tzinfo=UTC)
T1 = datetime(2026, 8, 7, 0, 1, tzinfo=UTC)


class FakeLiveSession:
    def __init__(self, updates: list[ProviderBarUpdate] | None = None) -> None:
        self.updates = updates or []
        self.subscriptions: list[tuple[ResolvedBarMarket, str]] = []
        self.entered = 0
        self.exited = 0
        self._index = 0
        self._wait = asyncio.Event()

    async def __aenter__(self) -> FakeLiveSession:
        self.entered += 1
        return self

    async def __aexit__(self, *args: object) -> None:
        self.exited += 1

    async def subscribe_bar_updates(self, market: ResolvedBarMarket, timeframe: str) -> None:
        self.subscriptions.append((market, timeframe))

    def __aiter__(self) -> FakeLiveSession:
        return self

    async def __anext__(self) -> ProviderBarUpdate:
        if self._index < len(self.updates):
            update = self.updates[self._index]
            self._index += 1
            await asyncio.sleep(0)
            return update
        await self._wait.wait()
        raise StopAsyncIteration


class FakeProvider:
    descriptor = ProviderDescriptor(name="fake", version="1", api_version=PROVIDER_API_VERSION)

    def __init__(self, session: FakeLiveSession | None = None) -> None:
        self.session = session or FakeLiveSession()
        self.resolve_calls = 0
        self.open_calls = 0

    def resolve_market(self, identity: MarketIdentity) -> ResolvedBarMarket:
        self.resolve_calls += 1
        return ResolvedBarMarket(
            identity=identity,
            native_market_id=identity.symbol.replace("/", ""),
            native_symbol=identity.symbol,
            timeframes=frozenset({"1m", "1h"}),
        )

    def observe_bars(self, request: object, market: object) -> BarObservation:
        return BarObservation(
            frame=pl.DataFrame(schema=PROVIDER_BAR_SCHEMA),
            observed=(ObservedWindow(T0, T1),),
        )

    def open_live_bars(self, *, exchange: str) -> FakeLiveSession:
        self.open_calls += 1
        assert exchange == "binance"
        return self.session


def _identity() -> MarketIdentity:
    return MarketIdentity(exchange="binance", symbol="BTC/USDT", market="spot")


def _raw(timestamp: datetime = T0, *, close: float = 101.0) -> ProviderBarUpdate:
    return ProviderBarUpdate(
        identity=_identity(),
        timeframe="1m",
        timestamp=timestamp,
        open=100.0,
        high=max(102.0, close),
        low=99.0,
        close=close,
        volume=12.0,
    )


def test_live_binding_performs_no_io_and_happy_path_allows_same_timestamp() -> None:
    async def scenario() -> None:
        provider = FakeProvider(FakeLiveSession([_raw(close=101.0), _raw(close=102.0)]))
        market_data = MarketData(provider=provider)
        bars = market_data.bars(
            exchange="binance", symbol="BTC/USDT", market="spot", timeframe="1m"
        )

        live = market_data.live(exchange="binance")
        assert provider.open_calls == provider.resolve_calls == 0

        async with live:
            assert provider.open_calls == 1
            assert provider.resolve_calls == 0
            await live.subscribe_bar_updates(bars)
            first = await anext(live)
            second = await anext(live)

        assert isinstance(first, BarUpdate)
        assert first.timestamp == second.timestamp == T0
        assert first.close == 101.0
        assert second.close == 102.0
        assert first.received_at.tzinfo is UTC
        assert provider.resolve_calls == 1
        assert provider.session.exited == 1

    asyncio.run(scenario())


def test_live_rejects_foreign_dataset_and_duplicate_subscription() -> None:
    async def scenario() -> None:
        provider = FakeProvider()
        first = MarketData(provider=provider)
        second = MarketData(provider=provider)
        own = first.bars(exchange="binance", symbol="BTC/USDT", market="spot", timeframe="1m")
        foreign = second.bars(exchange="binance", symbol="BTC/USDT", market="spot", timeframe="1m")
        async with first.live(exchange="binance") as live:
            with pytest.raises(InvalidRequestError, match="same MarketData"):
                await live.subscribe_bar_updates(foreign)
            await live.subscribe_bar_updates(own)
            with pytest.raises(InvalidRequestError, match="already subscribed"):
                await live.subscribe_bar_updates(own)

    asyncio.run(scenario())


def test_caught_subscription_activation_cancellation_leaves_session_failed() -> None:
    class BlockingSubscriptionSession(FakeLiveSession):
        def __init__(self) -> None:
            super().__init__()
            self.started = asyncio.Event()
            self.release = asyncio.Event()

        async def subscribe_bar_updates(
            self,
            market: ResolvedBarMarket,
            timeframe: str,
        ) -> None:
            self.subscriptions.append((market, timeframe))
            self.started.set()
            await self.release.wait()

    async def scenario() -> None:
        provider = FakeProvider(BlockingSubscriptionSession())
        market_data = MarketData(provider=provider)
        bars = market_data.bars(
            exchange="binance", symbol="BTC/USDT", market="spot", timeframe="1m"
        )

        with pytest.raises(ProviderError, match="cancelled during provider activation"):
            async with market_data.live(exchange="binance") as live:
                subscription = asyncio.create_task(live.subscribe_bar_updates(bars))
                await provider.session.started.wait()
                subscription.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await subscription
                with pytest.raises(InvalidRequestError, match="require an open session"):
                    await live.subscribe_bar_updates(bars)

        assert provider.session.exited == 1

    asyncio.run(scenario())


def test_live_requires_subscription_before_iteration_and_is_one_shot() -> None:
    async def scenario() -> None:
        live = MarketData(provider=FakeProvider()).live(exchange="binance")
        async with live:
            with pytest.raises(InvalidRequestError, match="subscribe before"):
                await anext(live)
        with pytest.raises(InvalidRequestError, match="entered once"):
            async with live:
                pass

    asyncio.run(scenario())


def test_live_backward_timestamp_is_terminal_provider_error() -> None:
    async def scenario() -> None:
        provider = FakeProvider(FakeLiveSession([_raw(T1), _raw(T0)]))
        market_data = MarketData(provider=provider)
        bars = market_data.bars(
            exchange="binance", symbol="BTC/USDT", market="spot", timeframe="1m"
        )
        async with market_data.live(exchange="binance") as live:
            await live.subscribe_bar_updates(bars)
            assert (await anext(live)).timestamp == T1
            with pytest.raises(ProviderError, match="moved backwards"):
                await anext(live)

    asyncio.run(scenario())


def test_live_queue_overflow_discards_stale_events_and_fails() -> None:
    async def scenario() -> None:
        provider = FakeProvider(FakeLiveSession([_raw(close=101.0), _raw(close=102.0)]))
        handle = ProviderHandle(provider)
        market_data = MarketData(provider=provider)
        bars = market_data.bars(
            exchange="binance", symbol="BTC/USDT", market="spot", timeframe="1m"
        )
        object.__setattr__(bars, "_provider", handle)
        live = LiveMarketData(provider=handle, exchange="binance", queue_size=1)
        async with live:
            await live.subscribe_bar_updates(bars)
            await asyncio.sleep(0.01)
            with pytest.raises(ProviderError, match="queue exceeded"):
                await anext(live)

    asyncio.run(scenario())


def test_unconsumed_background_failure_is_raised_on_context_exit() -> None:
    async def scenario() -> None:
        provider = FakeProvider(FakeLiveSession([_raw(T1), _raw(T0)]))
        market_data = MarketData(provider=provider)
        bars = market_data.bars(
            exchange="binance", symbol="BTC/USDT", market="spot", timeframe="1m"
        )
        with pytest.raises(ProviderError, match="moved backwards"):
            async with market_data.live(exchange="binance") as live:
                await live.subscribe_bar_updates(bars)
                await asyncio.sleep(0.01)

    asyncio.run(scenario())


def test_unexpected_provider_stream_end_fails_public_consumer() -> None:
    class EndingSession(FakeLiveSession):
        async def __anext__(self) -> ProviderBarUpdate:
            raise StopAsyncIteration

    async def scenario() -> None:
        provider = FakeProvider(EndingSession())
        market_data = MarketData(provider=provider)
        bars = market_data.bars(
            exchange="binance", symbol="BTC/USDT", market="spot", timeframe="1m"
        )
        async with market_data.live(exchange="binance") as live:
            await live.subscribe_bar_updates(bars)
            with pytest.raises(ProviderError, match="stream ended unexpectedly"):
                async with asyncio.timeout(0.1):
                    await anext(live)

    asyncio.run(scenario())


def test_live_preserves_operation_and_cleanup_failures() -> None:
    class CleanupFailureSession(FakeLiveSession):
        async def __aexit__(self, *args: object) -> None:
            raise RuntimeError("close failed")

    async def scenario() -> None:
        provider = FakeProvider(CleanupFailureSession())
        with pytest.raises(BaseExceptionGroup) as captured:
            async with MarketData(provider=provider).live(exchange="binance"):
                raise InvalidRequestError("body failed")
        errors = captured.value.exceptions
        assert isinstance(errors[0], InvalidRequestError)
        assert isinstance(errors[1], ProviderError)
        assert isinstance(errors[1].__cause__, RuntimeError)

    asyncio.run(scenario())


def test_provider_without_live_capability_is_unsupported() -> None:
    class HistoricalOnly(FakeProvider):
        open_live_bars = None  # type: ignore[assignment]

    async def scenario() -> None:
        with pytest.raises(UnsupportedMarketError, match="no live-bar capability"):
            async with MarketData(provider=HistoricalOnly()).live(exchange="binance"):
                pass

    asyncio.run(scenario())


def test_bar_update_enforces_alignment_and_ohlcv_quality() -> None:
    with pytest.raises(InvalidRequestError, match="aligned"):
        BarUpdate(
            identity=_identity(),
            timeframe="1m",
            timestamp=datetime(2026, 8, 7, 0, 0, 1, tzinfo=UTC),
            open=1,
            high=1,
            low=1,
            close=1,
            volume=0,
            received_at=T0,
            finality=BarFinality.FORMING,
        )
    with pytest.raises(InvalidRequestError, match="high violates"):
        BarUpdate(
            identity=_identity(),
            timeframe="1m",
            timestamp=T0,
            open=2,
            high=1,
            low=1,
            close=1,
            volume=0,
            received_at=T0,
            finality=BarFinality.FORMING,
        )


class RecentSnapshotProvider(FakeProvider):
    def observe_bars(
        self,
        request: BarRequest,
        market: ResolvedBarMarket,
    ) -> BarObservation:
        start = request.start
        end = request.end
        timestamps: list[datetime] = []
        cursor = start
        while cursor < end:
            timestamps.append(cursor)
            cursor += timedelta(minutes=1)
        count = len(timestamps)
        return BarObservation(
            frame=pl.DataFrame(
                {
                    "timestamp": timestamps,
                    "open": [100.0 + index for index in range(count)],
                    "high": [102.0 + index for index in range(count)],
                    "low": [99.0 + index for index in range(count)],
                    "close": [101.0 + index for index in range(count)],
                    "volume": [5.0 + index for index in range(count)],
                },
                schema=PROVIDER_BAR_SCHEMA,
            ),
            observed=(ObservedWindow(start, end),),
        )


def _bootstrap_live(
    provider: FakeProvider,
    bars: BarDataset,
    *,
    now: datetime,
    queue_size: int = 1024,
) -> LiveMarketData:
    handle = ProviderHandle(provider)
    object.__setattr__(bars, "_provider", handle)
    return LiveMarketData(
        provider=handle,
        exchange="binance",
        queue_size=queue_size,
        clock=lambda: now,
    )


def test_bootstrap_merges_recent_closed_bars_before_live_without_storage(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        now = T1 + timedelta(seconds=2)
        provider = RecentSnapshotProvider(FakeLiveSession([_raw(T1)]))
        config = MarketDataConfig(
            state_dir=tmp_path / "state",
            data_dir=tmp_path / "data",
        )
        market_data = MarketData(config=config, provider=provider)
        bars = market_data.bars(
            exchange="binance", symbol="BTC/USDT", market="spot", timeframe="1m"
        )
        live = _bootstrap_live(provider, bars, now=now)

        async with live:
            await live.subscribe_bar_updates(bars, bootstrap=True)
            updates = [await anext(live) for _ in range(3)]

        assert [update.timestamp for update in updates] == [
            T0 - timedelta(minutes=1),
            T0,
            T1,
        ]
        assert [update.finality for update in updates] == [
            BarFinality.FINAL,
            BarFinality.PROVISIONAL,
            BarFinality.FORMING,
        ]
        assert not config.state_dir.exists()
        assert not config.data_dir.exists()

        provider_runtime._set_clock_override(lambda: now + timedelta(seconds=10))
        try:
            sync_result = bars.sync(T0, T1)
        finally:
            provider_runtime._set_clock_override(None)

        assert sync_result.is_complete
        assert sync_result.fetched_rows == 1
        assert config.state_dir.exists()
        assert list(config.data_dir.rglob("*.parquet"))

    asyncio.run(scenario())


def test_bootstrap_live_overlap_wins_deterministically() -> None:
    async def scenario() -> None:
        now = T1 + timedelta(seconds=2)
        provider = RecentSnapshotProvider(FakeLiveSession([_raw(T0, close=110.0)]))
        market_data = MarketData(provider=provider)
        bars = market_data.bars(
            exchange="binance", symbol="BTC/USDT", market="spot", timeframe="1m"
        )
        live = _bootstrap_live(provider, bars, now=now)

        async with live:
            await live.subscribe_bar_updates(bars, bootstrap=True)
            updates = [await anext(live) for _ in range(2)]

        assert [update.timestamp for update in updates] == [T0 - timedelta(minutes=1), T0]
        assert updates[-1].close == 110.0
        assert updates[-1].received_at == now

    asyncio.run(scenario())


def test_bootstrap_rejects_non_boolean_before_provider_io() -> None:
    async def scenario() -> None:
        provider = FakeProvider()
        market_data = MarketData(provider=provider)
        bars = market_data.bars(
            exchange="binance", symbol="BTC/USDT", market="spot", timeframe="1m"
        )
        async with market_data.live(exchange="binance") as live:
            with pytest.raises(InvalidRequestError, match="bootstrap must be a bool"):
                await live.subscribe_bar_updates(bars, bootstrap=cast("bool", 1))
        assert provider.resolve_calls == 0

    asyncio.run(scenario())


def test_bootstrap_provider_failure_is_session_terminal() -> None:
    class FailingSession(FakeLiveSession):
        async def __anext__(self) -> ProviderBarUpdate:
            raise RuntimeError("socket failed before first update")

    async def scenario() -> None:
        provider = FakeProvider(FailingSession())
        market_data = MarketData(provider=provider)
        bars = market_data.bars(
            exchange="binance", symbol="BTC/USDT", market="spot", timeframe="1m"
        )

        with pytest.raises(ProviderError, match="stream failed"):
            async with market_data.live(exchange="binance") as live:
                await live.subscribe_bar_updates(bars, bootstrap=True)

        assert provider.session.exited == 1

    asyncio.run(scenario())


def test_bootstrap_buffer_overflow_fails_without_publishing_stale_rows() -> None:
    class SlowSnapshotProvider(RecentSnapshotProvider):
        def observe_bars(
            self,
            request: BarRequest,
            market: ResolvedBarMarket,
        ) -> BarObservation:
            time.sleep(0.02)
            return super().observe_bars(request, market)

    async def scenario() -> None:
        now = T1 + timedelta(seconds=2)
        provider = SlowSnapshotProvider(
            FakeLiveSession([_raw(T1, close=101.0), _raw(T1, close=102.0)])
        )
        market_data = MarketData(provider=provider)
        bars = market_data.bars(
            exchange="binance", symbol="BTC/USDT", market="spot", timeframe="1m"
        )
        live = _bootstrap_live(provider, bars, now=now, queue_size=1)

        with pytest.raises(ProviderError, match="bootstrap buffer exceeded"):
            async with live:
                await live.subscribe_bar_updates(bars, bootstrap=True)

    asyncio.run(scenario())


def test_caught_bootstrap_cancellation_leaves_session_failed() -> None:
    async def scenario() -> None:
        provider = FakeProvider()
        market_data = MarketData(provider=provider)
        bars = market_data.bars(
            exchange="binance", symbol="BTC/USDT", market="spot", timeframe="1m"
        )

        with pytest.raises(ProviderError, match="bootstrap was cancelled"):
            async with market_data.live(exchange="binance") as live:
                subscription = asyncio.create_task(live.subscribe_bar_updates(bars, bootstrap=True))
                async with asyncio.timeout(1):
                    while not provider.session.subscriptions:
                        await asyncio.sleep(0)
                await asyncio.sleep(0)
                subscription.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await subscription

        assert provider.session.exited == 1

    asyncio.run(scenario())
