"""Public one-shot live market-data session."""

from __future__ import annotations

import asyncio
import enum
from dataclasses import dataclass
from types import TracebackType

from xret.data.dataset import BarDataset
from xret.data.errors import InvalidRequestError, ProviderError
from xret.data.models import BarUpdate
from xret.data.providers.discovery import ProviderHandle
from xret.data.providers.live_runtime import LiveBarRuntime

_DEFAULT_QUEUE_SIZE = 1024


class _State(enum.Enum):
    CREATED = "created"
    ENTERING = "entering"
    OPEN = "open"
    CLOSING = "closing"
    CLOSED = "closed"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class _Failure:
    error: BaseException


class LiveMarketData:
    """A single-exchange, one-shot stream of normalized market-data events."""

    def __init__(
        self,
        *,
        provider: ProviderHandle,
        exchange: str,
        queue_size: int = _DEFAULT_QUEUE_SIZE,
    ) -> None:
        self._provider = provider
        self._exchange = exchange
        self._queue: asyncio.Queue[BarUpdate | _Failure] = asyncio.Queue(queue_size)
        self._state = _State.CREATED
        self._runtime: LiveBarRuntime | None = None
        self._reader: asyncio.Task[None] | None = None
        self._failure: BaseException | None = None
        self._failure_observed = False
        self._consumer: asyncio.Task[object] | None = None
        self._requested: set[tuple[object, str]] = set()

    async def __aenter__(self) -> LiveMarketData:
        if self._state is not _State.CREATED:
            raise InvalidRequestError("a live session can only be entered once")
        self._state = _State.ENTERING
        runtime = LiveBarRuntime(self._provider.get(), exchange=self._exchange)
        try:
            await runtime.__aenter__()
        except BaseException:
            self._state = _State.FAILED
            raise
        self._runtime = runtime
        self._state = _State.OPEN
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None:
        if self._state is _State.CLOSED:
            return None
        self._state = _State.CLOSING
        unobserved_failure = (
            self._failure
            if exc is None and self._failure is not None and not self._failure_observed
            else None
        )
        cleanup_error: BaseException | None = None
        reader, self._reader = self._reader, None
        if reader is not None and not reader.done():
            reader.cancel()
            try:
                await reader
            except asyncio.CancelledError:
                pass
            except BaseException as reader_error:
                if exc is None and reader_error is not self._failure:
                    cleanup_error = reader_error
        runtime, self._runtime = self._runtime, None
        if runtime is not None:
            try:
                effective_error = exc if exc is not None else unobserved_failure
                await runtime.__aexit__(
                    type(effective_error) if effective_error is not None else None,
                    effective_error,
                    traceback,
                )
            except BaseException as runtime_error:
                cleanup_error = runtime_error
        self._state = _State.CLOSED
        primary_error = exc if exc is not None else unobserved_failure
        if cleanup_error is not None and primary_error is not None:
            raise BaseExceptionGroup(
                "live session operation and cleanup both failed",
                [primary_error, cleanup_error],
            )
        if cleanup_error is not None:
            raise cleanup_error
        if unobserved_failure is not None:
            raise unobserved_failure
        return None

    async def subscribe_bar_updates(self, bars: BarDataset) -> None:
        if self._state is not _State.OPEN or self._runtime is None:
            raise InvalidRequestError("live subscriptions require an open session")
        if not isinstance(bars, BarDataset):
            raise InvalidRequestError("bars must be a BarDataset")
        if bars._provider is not self._provider:
            raise InvalidRequestError(
                "bars must be created by the same MarketData instance as this live session"
            )
        if bars.identity.exchange != self._exchange:
            raise InvalidRequestError(
                f"bars exchange {bars.identity.exchange!r} does not match live session "
                f"exchange {self._exchange!r}"
            )
        requested = (bars.identity, bars.timeframe)
        if requested in self._requested:
            raise InvalidRequestError("the same bar dataset is already subscribed")
        await self._runtime.subscribe(bars.identity, bars.timeframe)
        self._requested.add(requested)
        if self._reader is None:
            self._reader = asyncio.create_task(self._read_updates())

    def __aiter__(self) -> LiveMarketData:
        return self

    async def __anext__(self) -> BarUpdate:
        consumer = asyncio.current_task()
        if consumer is None:
            raise ProviderError("live updates require an asyncio task")
        if self._consumer is None:
            self._consumer = consumer
        elif self._consumer is not consumer:
            raise InvalidRequestError("a live session supports exactly one consuming task")
        if self._failure is not None:
            self._failure_observed = True
            raise self._failure
        if self._state not in (_State.OPEN, _State.FAILED):
            raise StopAsyncIteration
        if self._reader is None:
            raise InvalidRequestError("subscribe before consuming live updates")
        item = await self._queue.get()
        if isinstance(item, _Failure):
            self._failure_observed = True
            raise item.error
        return item

    async def _read_updates(self) -> None:
        assert self._runtime is not None
        try:
            async for update in self._runtime.updates():
                try:
                    self._queue.put_nowait(update)
                except asyncio.QueueFull:
                    error = ProviderError(
                        f"live update queue exceeded its {self._queue.maxsize}-event capacity"
                    )
                    self._publish_failure(error)
                    return
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            error = exc if isinstance(exc, ProviderError) else ProviderError(str(exc))
            self._publish_failure(error)

    def _publish_failure(self, error: BaseException) -> None:
        self._failure = error
        self._state = _State.FAILED
        while not self._queue.empty():
            try:
                self._queue.get_nowait()
            except asyncio.QueueEmpty:
                break
        self._queue.put_nowait(_Failure(error))
