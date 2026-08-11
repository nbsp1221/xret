"""Public one-shot live market-data session."""

from __future__ import annotations

import asyncio
import enum
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from types import TracebackType

from xret.data.dataset import BarDataset
from xret.data.errors import InvalidRequestError, ProviderError
from xret.data.models import BarUpdate, MarketIdentity
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


@dataclass(slots=True)
class _BootstrapGate:
    buffer: list[BarUpdate] = field(default_factory=list)
    ready: asyncio.Event = field(default_factory=asyncio.Event)
    failure: BaseException | None = None


def _default_clock() -> datetime:
    return datetime.now(UTC)


class LiveMarketData:
    """A single-exchange, one-shot stream of normalized market-data events."""

    def __init__(
        self,
        *,
        provider: ProviderHandle,
        exchange: str,
        queue_size: int = _DEFAULT_QUEUE_SIZE,
        clock: Callable[[], datetime] = _default_clock,
    ) -> None:
        self._provider = provider
        self._exchange = exchange
        self._clock = clock
        self._queue: asyncio.Queue[BarUpdate | _Failure] = asyncio.Queue(queue_size)
        self._state = _State.CREATED
        self._runtime: LiveBarRuntime | None = None
        self._reader: asyncio.Task[None] | None = None
        self._failure: BaseException | None = None
        self._failure_observed = False
        self._consumer: asyncio.Task[object] | None = None
        self._requested: set[tuple[object, str]] = set()
        self._routes: dict[
            tuple[MarketIdentity, str],
            _BootstrapGate | None,
        ] = {}

    async def __aenter__(self) -> LiveMarketData:
        if self._state is not _State.CREATED:
            raise InvalidRequestError("a live session can only be entered once")
        self._state = _State.ENTERING
        runtime = LiveBarRuntime(
            self._provider.get(),
            exchange=self._exchange,
            clock=self._clock,
        )
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

    async def subscribe_bar_updates(
        self,
        bars: BarDataset,
        *,
        bootstrap: bool = False,
    ) -> None:
        if self._state is not _State.OPEN or self._runtime is None:
            raise InvalidRequestError("live subscriptions require an open session")
        if not isinstance(bars, BarDataset):
            raise InvalidRequestError("bars must be a BarDataset")
        if not isinstance(bootstrap, bool):
            raise InvalidRequestError("bootstrap must be a bool")
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
        resolved = await self._runtime.resolve_subscription(bars.identity, bars.timeframe)
        key = (resolved.identity, bars.timeframe)
        if key in self._routes:
            raise InvalidRequestError("the same resolved bar dataset is already subscribed")
        gate = _BootstrapGate() if bootstrap else None
        self._routes[key] = gate
        try:
            await self._runtime.subscribe_resolved(resolved, bars.timeframe)
        except asyncio.CancelledError:
            self._publish_failure(
                ProviderError("live subscription was cancelled during provider activation")
            )
            raise
        except BaseException:
            self._routes.pop(key, None)
            raise
        self._requested.add(requested)
        if self._reader is None:
            self._reader = asyncio.create_task(self._read_updates())
        if gate is not None:
            await self._bootstrap(key, gate)

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
                if not self._route_update(update):
                    return
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            error = exc if isinstance(exc, ProviderError) else ProviderError(str(exc))
            self._publish_failure(error)

    async def _bootstrap(
        self,
        key: tuple[MarketIdentity, str],
        gate: _BootstrapGate,
    ) -> None:
        assert self._runtime is not None
        try:
            await gate.ready.wait()
            if gate.failure is not None:
                raise gate.failure
            snapshot = await self._runtime.recent_closed(key, count=2)
            if gate.failure is not None:
                raise gate.failure
            merged = {update.timestamp: update for update in snapshot}
            for update in gate.buffer:
                merged[update.timestamp] = update
            for timestamp in sorted(merged):
                if not self._enqueue(merged[timestamp]):
                    assert self._failure is not None
                    raise self._failure
            self._routes[key] = None
        except asyncio.CancelledError:
            self._publish_failure(
                ProviderError("live bootstrap was cancelled after subscription activation")
            )
            raise
        except Exception as exc:
            if self._failure is None:
                error = exc if isinstance(exc, ProviderError) else ProviderError(str(exc))
                self._publish_failure(error)
            raise

    def _route_update(self, update: BarUpdate) -> bool:
        if self._failure is not None:
            return False
        key = (update.identity, update.timeframe)
        if key not in self._routes:
            self._publish_failure(ProviderError("live update has no active public subscription"))
            return False
        gate = self._routes[key]
        if gate is None:
            return self._enqueue(update)
        if len(gate.buffer) >= self._queue.maxsize:
            self._publish_failure(
                ProviderError(
                    f"live bootstrap buffer exceeded its {self._queue.maxsize}-event capacity"
                )
            )
            return False
        gate.buffer.append(update)
        gate.ready.set()
        return True

    def _enqueue(self, update: BarUpdate) -> bool:
        try:
            self._queue.put_nowait(update)
        except asyncio.QueueFull:
            self._publish_failure(
                ProviderError(
                    f"live update queue exceeded its {self._queue.maxsize}-event capacity"
                )
            )
            return False
        return True

    def _publish_failure(self, error: BaseException) -> None:
        if self._failure is not None:
            return
        self._failure = error
        self._state = _State.FAILED
        reader = self._reader
        current = asyncio.current_task()
        if reader is not None and reader is not current and not reader.done():
            reader.cancel()
        for gate in self._routes.values():
            if gate is not None:
                gate.failure = error
                gate.ready.set()
        while not self._queue.empty():
            try:
                self._queue.get_nowait()
            except asyncio.QueueEmpty:
                break
        self._queue.put_nowait(_Failure(error))
