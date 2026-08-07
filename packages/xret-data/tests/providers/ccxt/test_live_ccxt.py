from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest
from xret.data.errors import ProviderError, UnsupportedMarketError
from xret.data.models import MarketIdentity
from xret.data.providers import ResolvedBarMarket
from xret.data.providers.ccxt.live import CcxtLiveBarSession


class FakeExchange:
    id = "binance"

    def __init__(self, rows: object, *, supported: bool = True) -> None:
        self.has = {"watchOHLCV": supported}
        self.rows = rows
        self.closed = 0
        self.loaded = 0
        self.calls: list[tuple[str, str]] = []
        self._wait = asyncio.Event()

    async def load_markets(self) -> dict[str, object]:
        self.loaded += 1
        return {}

    async def watch_ohlcv(self, symbol: str, timeframe: str) -> object:
        self.calls.append((symbol, timeframe))
        if self.rows is not None:
            rows, self.rows = self.rows, None
            return rows
        await self._wait.wait()
        return []

    async def close(self) -> None:
        self.closed += 1


class BlockingCloseExchange(FakeExchange):
    def __init__(
        self,
        *,
        started: asyncio.Event,
        release: asyncio.Event,
        close_error: Exception | None = None,
    ) -> None:
        super().__init__([])
        self.started = started
        self.release = release
        self.close_error = close_error

    async def close(self) -> None:
        self.started.set()
        await self.release.wait()
        if self.close_error is not None:
            raise self.close_error
        self.closed += 1


def _market(symbol: str = "BTC/USDT") -> ResolvedBarMarket:
    identity = MarketIdentity(exchange="binance", symbol=symbol, market="spot")
    return ResolvedBarMarket(
        identity=identity,
        native_market_id=symbol.replace("/", ""),
        native_symbol=symbol,
        timeframes=frozenset({"1m"}),
    )


def test_ccxt_live_session_reuses_client_and_normalizes_rows() -> None:
    async def scenario() -> None:
        client = FakeExchange([[1786060800000, 100, 102, 99, 101, 4]])
        factory_calls: list[str] = []

        def factory(client_id: str) -> FakeExchange:
            factory_calls.append(client_id)
            return client

        session = CcxtLiveBarSession(exchange="binance", exchange_factory=factory)
        async with session:
            await session.subscribe_bar_updates(_market(), "1m")
            await session.subscribe_bar_updates(_market("ETH/USDT"), "1m")
            update = await anext(session)

        assert factory_calls == ["binance"]
        assert client.loaded == 1
        assert client.closed == 1
        assert update.timestamp == datetime(2026, 8, 7, 0, 0, tzinfo=UTC)
        assert update.close == 101.0

    asyncio.run(scenario())


def test_ccxt_live_session_rejects_missing_watch_capability() -> None:
    async def scenario() -> None:
        client = FakeExchange([], supported=False)
        session = CcxtLiveBarSession(exchange="binance", exchange_factory=lambda _: client)
        async with session:
            with pytest.raises(UnsupportedMarketError, match="watchOHLCV"):
                await session.subscribe_bar_updates(_market(), "1m")
        assert client.closed == 1

    asyncio.run(scenario())


def test_ccxt_live_session_malformed_row_fails_stream() -> None:
    async def scenario() -> None:
        client = FakeExchange([[1, 2]])
        session = CcxtLiveBarSession(exchange="binance", exchange_factory=lambda _: client)
        async with session:
            await session.subscribe_bar_updates(_market(), "1m")
            with pytest.raises(ProviderError, match="malformed"):
                await anext(session)

    asyncio.run(scenario())


def test_ccxt_live_merge_queue_overflow_is_terminal() -> None:
    async def scenario() -> None:
        rows = [
            [1786060800000, 100, 102, 99, 101, 4],
            [1786060860000, 101, 103, 100, 102, 5],
        ]
        client = FakeExchange(rows)
        session = CcxtLiveBarSession(
            exchange="binance",
            exchange_factory=lambda _: client,
            queue_size=1,
        )
        async with session:
            await session.subscribe_bar_updates(_market(), "1m")
            await asyncio.sleep(0.01)
            with pytest.raises(ProviderError, match="merge queue exceeded"):
                await anext(session)

    asyncio.run(scenario())


def test_ccxt_live_session_is_one_shot() -> None:
    async def scenario() -> None:
        session = CcxtLiveBarSession(
            exchange="binance",
            exchange_factory=lambda _: FakeExchange([]),
        )
        async with session:
            pass
        with pytest.raises(ProviderError, match="entered once"):
            async with session:
                pass

    asyncio.run(scenario())


def test_ccxt_cleanup_finishes_and_preserves_external_cancellation() -> None:
    async def scenario() -> None:
        first_started = asyncio.Event()
        second_started = asyncio.Event()
        release = asyncio.Event()
        first = BlockingCloseExchange(started=first_started, release=release)
        second = BlockingCloseExchange(started=second_started, release=release)
        session = CcxtLiveBarSession(
            exchange="binance",
            exchange_factory=lambda _: first,
        )
        await session.__aenter__()
        session._clients.update(first=first, second=second)

        cleanup = asyncio.create_task(session.__aexit__(None, None, None))
        await first_started.wait()
        cleanup.cancel()
        release.set()

        with pytest.raises(asyncio.CancelledError):
            await cleanup
        assert second_started.is_set()
        assert first.closed == second.closed == 1

    asyncio.run(scenario())


def test_ccxt_cleanup_preserves_cancellation_and_close_failure() -> None:
    async def scenario() -> None:
        first_started = asyncio.Event()
        second_started = asyncio.Event()
        release = asyncio.Event()
        close_failure = RuntimeError("close failed")
        first = BlockingCloseExchange(
            started=first_started,
            release=release,
            close_error=close_failure,
        )
        second = BlockingCloseExchange(started=second_started, release=release)
        session = CcxtLiveBarSession(
            exchange="binance",
            exchange_factory=lambda _: first,
        )
        await session.__aenter__()
        session._clients.update(first=first, second=second)

        cleanup = asyncio.create_task(session.__aexit__(None, None, None))
        await first_started.wait()
        cleanup.cancel()
        release.set()

        with pytest.raises(BaseExceptionGroup) as captured:
            await cleanup
        cancellation, provider_error = captured.value.exceptions
        assert isinstance(cancellation, asyncio.CancelledError)
        assert isinstance(provider_error, ProviderError)
        assert isinstance(provider_error.__cause__, RuntimeError)
        assert provider_error.__cause__ is close_failure
        assert second_started.is_set()
        assert second.closed == 1

    asyncio.run(scenario())
