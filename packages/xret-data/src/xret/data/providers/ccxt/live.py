"""CCXT Pro implementation of the optional live-bar provider capability."""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any, Protocol, Self, cast

from xret.data.errors import ProviderError, UnsupportedMarketError
from xret.data.providers import ProviderBarUpdate, ResolvedBarMarket
from xret.data.providers.ccxt import markets

DEFAULT_LIVE_QUEUE_SIZE = 1024


class AsyncCcxtExchange(Protocol):
    id: str
    has: dict[str, object]

    async def load_markets(self) -> object: ...

    async def watch_ohlcv(self, symbol: str, timeframe: str) -> object: ...

    async def close(self) -> object: ...


LiveExchangeFactory = Callable[[str], AsyncCcxtExchange]


def create_live_exchange(client_id: str) -> AsyncCcxtExchange:
    """Create one rate-limited CCXT Pro client in incremental-update mode."""
    try:
        import ccxt.pro as ccxtpro

        exchange_type = getattr(ccxtpro, client_id)
        return exchange_type({"enableRateLimit": True, "newUpdates": True})
    except Exception as exc:
        raise ProviderError(f"failed to create CCXT Pro client {client_id!r}: {exc}") from exc


class _Failure:
    def __init__(self, error: BaseException) -> None:
        self.error = error


class CcxtLiveBarSession:
    """One canonical-exchange CCXT Pro session with per-client readers."""

    def __init__(
        self,
        *,
        exchange: str,
        exchange_factory: LiveExchangeFactory,
        queue_size: int = DEFAULT_LIVE_QUEUE_SIZE,
    ) -> None:
        self._exchange = exchange
        self._exchange_factory = exchange_factory
        self._queue: asyncio.Queue[ProviderBarUpdate | _Failure] = asyncio.Queue(queue_size)
        self._clients: dict[str, AsyncCcxtExchange] = {}
        self._tasks: set[asyncio.Task[None]] = set()
        self._subscriptions: set[tuple[object, str]] = set()
        self._open = False
        self._entered = False
        self._failure: BaseException | None = None

    async def __aenter__(self) -> Self:
        if self._entered:
            raise ProviderError("CCXT live session can only be entered once")
        self._entered = True
        self._open = True
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: object | None,
    ) -> None:
        self._open = False
        cleanup_task = asyncio.create_task(self._shutdown())
        try:
            await asyncio.shield(cleanup_task)
        except asyncio.CancelledError as cancellation:
            current_task = asyncio.current_task()
            if current_task is None or not current_task.cancelling():
                raise
            try:
                await cleanup_task
            except BaseException as cleanup_error:
                raise BaseExceptionGroup(
                    "CCXT live session cancellation and cleanup both failed",
                    [cancellation, cleanup_error],
                ) from None
            raise

    async def _shutdown(self) -> None:
        tasks = tuple(self._tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        close_errors: list[Exception] = []
        for client in self._clients.values():
            try:
                await client.close()
            except Exception as close_error:
                close_errors.append(close_error)
        self._clients.clear()
        self._tasks.clear()
        if close_errors:
            error = ProviderError("one or more CCXT Pro clients failed to close")
            if len(close_errors) == 1:
                raise error from close_errors[0]
            raise error from ExceptionGroup("CCXT Pro close failures", close_errors)

    async def subscribe_bar_updates(
        self,
        market: ResolvedBarMarket,
        timeframe: str,
    ) -> None:
        if not self._open or self._failure is not None:
            raise ProviderError("CCXT live session is not available for subscriptions")
        if market.identity.exchange != self._exchange:
            raise ProviderError("CCXT live subscription is outside the session exchange")
        key = (market.identity, timeframe)
        if key in self._subscriptions:
            raise ProviderError(f"duplicate CCXT live subscription: {key!r}")
        client_id = markets.client_id(market.identity)
        client = self._clients.get(client_id)
        if client is None:
            client = self._exchange_factory(client_id)
            try:
                loaded = await client.load_markets()
            except Exception as exc:
                with contextlib.suppress(Exception):
                    await client.close()
                raise ProviderError(
                    f"CCXT Pro failed to load markets for {client_id!r}: {exc}"
                ) from exc
            if not isinstance(loaded, dict):
                with contextlib.suppress(Exception):
                    await client.close()
                raise ProviderError("CCXT Pro load_markets() must return a dict")
            if not bool(client.has.get("watchOHLCV")):
                with contextlib.suppress(Exception):
                    await client.close()
                raise UnsupportedMarketError(
                    f"CCXT Pro client {client_id!r} does not support watchOHLCV"
                )
            self._clients[client_id] = client
        self._subscriptions.add(key)
        task = asyncio.create_task(self._watch(client, market, timeframe))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    def __aiter__(self) -> CcxtLiveBarSession:
        return self

    async def __anext__(self) -> ProviderBarUpdate:
        if self._failure is not None:
            raise self._failure
        if not self._open:
            raise StopAsyncIteration
        item = await self._queue.get()
        if isinstance(item, _Failure):
            raise item.error
        return item

    async def _watch(
        self,
        client: AsyncCcxtExchange,
        market: ResolvedBarMarket,
        timeframe: str,
    ) -> None:
        try:
            while self._open and self._failure is None:
                rows = await client.watch_ohlcv(market.native_symbol, timeframe)
                if not isinstance(rows, list):
                    raise ProviderError("CCXT Pro watch_ohlcv() must return a list")
                for row in rows:
                    self._publish(_parse_update(row, market=market, timeframe=timeframe))
                    if self._failure is not None:
                        return
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            error = (
                exc
                if isinstance(exc, ProviderError)
                else ProviderError(
                    f"CCXT Pro watchOHLCV failed for {market.native_symbol} {timeframe}: {exc}"
                )
            )
            self._fail(error)

    def _publish(self, update: ProviderBarUpdate) -> None:
        try:
            self._queue.put_nowait(update)
        except asyncio.QueueFull:
            self._fail(
                ProviderError(
                    f"CCXT live merge queue exceeded its {self._queue.maxsize}-event capacity"
                )
            )

    def _fail(self, error: BaseException) -> None:
        if self._failure is not None:
            return
        self._failure = error
        while not self._queue.empty():
            with contextlib.suppress(asyncio.QueueEmpty):
                self._queue.get_nowait()
        self._queue.put_nowait(_Failure(error))
        current = asyncio.current_task()
        for task in tuple(self._tasks):
            if task is not current:
                task.cancel()


def _parse_update(
    row: object,
    *,
    market: ResolvedBarMarket,
    timeframe: str,
) -> ProviderBarUpdate:
    if not isinstance(row, (list, tuple)) or len(row) < 6:
        raise ProviderError(f"CCXT Pro returned a malformed OHLCV row: {row!r}")
    try:
        values = cast("list[Any] | tuple[Any, ...]", row)
        timestamp_ms = int(values[0])
        return ProviderBarUpdate(
            identity=market.identity,
            timeframe=timeframe,
            timestamp=datetime.fromtimestamp(timestamp_ms / 1000, tz=UTC),
            open=float(values[1]),
            high=float(values[2]),
            low=float(values[3]),
            close=float(values[4]),
            volume=float(values[5]),
        )
    except (TypeError, ValueError, OverflowError) as exc:
        raise ProviderError(f"CCXT Pro returned a malformed OHLCV row: {row!r}") from exc
