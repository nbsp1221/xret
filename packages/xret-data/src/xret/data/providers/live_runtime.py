"""Provider-independent validation for optional live-bar capabilities."""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator, Callable
from datetime import UTC, datetime, timedelta
from typing import cast

from xret.data.errors import ProviderError, UnsupportedMarketError
from xret.data.models import BarFinality, BarRequest, BarUpdate, MarketIdentity
from xret.data.providers.contracts import (
    HistoricalBarProvider,
    LiveBarSession,
    ProviderBarUpdate,
    ResolvedBarMarket,
)
from xret.data.providers.runtime import (
    DEFAULT_FINALITY_GRACE,
    ProviderRuntime,
    validate_provider_descriptor,
)
from xret.data.timeframe import TimeBar


def _default_clock() -> datetime:
    return datetime.now(UTC)


def _bar_finality(
    timestamp: datetime,
    timeframe: str,
    received_at: datetime,
) -> BarFinality:
    if timestamp.tzinfo is None or timestamp.utcoffset() != timedelta(0):
        raise ValueError("timestamp must be UTC-aware")
    if received_at.tzinfo is None or received_at.utcoffset() != timedelta(0):
        raise ValueError("received_at must be UTC-aware")
    bar_end = TimeBar.parse(timeframe).next_boundary(timestamp)
    if received_at < bar_end:
        return BarFinality.FORMING
    if received_at < bar_end + DEFAULT_FINALITY_GRACE:
        return BarFinality.PROVISIONAL
    return BarFinality.FINAL


def _previous_boundary(time_bar: TimeBar, boundary: datetime) -> datetime:
    return time_bar.floor(boundary - timedelta(microseconds=1))


class LiveBarRuntime:
    """Validate one provider live session and normalize its updates."""

    def __init__(
        self,
        provider: object,
        *,
        exchange: str,
        clock: Callable[[], datetime] = _default_clock,
    ) -> None:
        self._provider = provider
        self._exchange = exchange
        self._clock = clock
        self._descriptor = validate_provider_descriptor(provider)
        self._historical = ProviderRuntime(
            cast("HistoricalBarProvider", provider),
            clock=clock,
        )
        self._session: LiveBarSession | None = None
        self._active: dict[tuple[MarketIdentity, str], ResolvedBarMarket] = {}
        self._last_timestamp: dict[tuple[MarketIdentity, str], datetime] = {}

    async def __aenter__(self) -> LiveBarRuntime:
        try:
            opener = getattr(self._provider, "open_live_bars", None)
        except Exception as exc:
            raise ProviderError(
                f"provider {self._descriptor.name!r} live capability access failed: {exc}"
            ) from exc
        if not callable(opener):
            raise UnsupportedMarketError(
                f"provider {self._descriptor.name!r} has no live-bar capability"
            )
        try:
            session = opener(exchange=self._exchange)
            entered = await session.__aenter__()
        except (UnsupportedMarketError, ProviderError):
            raise
        except Exception as exc:
            raise ProviderError(
                f"provider {self._descriptor.name!r} failed to open a live-bar session "
                f"for {self._exchange}: {exc}"
            ) from exc
        if entered is not session:
            with contextlib.suppress(Exception):
                await session.__aexit__(None, None, None)
            raise ProviderError("provider live session __aenter__() must return itself")
        self._session = session
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: object | None,
    ) -> bool | None:
        session, self._session = self._session, None
        if session is None:
            return None
        try:
            return await session.__aexit__(exc_type, exc, traceback)
        except ProviderError:
            raise
        except Exception as cleanup_error:
            raise ProviderError(
                f"provider {self._descriptor.name!r} failed to close live-bar session: "
                f"{cleanup_error}"
            ) from cleanup_error

    async def subscribe(
        self,
        identity: MarketIdentity,
        timeframe: str,
    ) -> tuple[MarketIdentity, str]:
        resolved = await self.resolve_subscription(identity, timeframe)
        return await self.subscribe_resolved(resolved, timeframe)

    async def resolve_subscription(
        self,
        identity: MarketIdentity,
        timeframe: str,
    ) -> ResolvedBarMarket:
        self._require_session()
        time_bar = TimeBar.parse(timeframe)
        try:
            resolved = await asyncio.to_thread(self._historical.resolve_market, identity)
        except (UnsupportedMarketError, ProviderError):
            raise
        if resolved.identity.exchange != self._exchange:
            raise ProviderError("provider resolved a live market outside the session exchange")
        if timeframe not in resolved.timeframes:
            raise UnsupportedMarketError(
                f"provider {self._descriptor.name!r} does not support timeframe "
                f"{timeframe!r} for {identity.exchange}/{identity.symbol}"
            )
        key = (resolved.identity, str(time_bar))
        if key in self._active:
            raise ProviderError(f"duplicate live-bar subscription: {key!r}")
        return resolved

    async def subscribe_resolved(
        self,
        resolved: ResolvedBarMarket,
        timeframe: str,
    ) -> tuple[MarketIdentity, str]:
        session = self._require_session()
        key = (resolved.identity, str(TimeBar.parse(timeframe)))
        if key in self._active:
            raise ProviderError(f"duplicate live-bar subscription: {key!r}")
        self._active[key] = resolved
        try:
            await session.subscribe_bar_updates(resolved, timeframe)
        except (UnsupportedMarketError, ProviderError):
            self._active.pop(key, None)
            raise
        except Exception as exc:
            self._active.pop(key, None)
            raise ProviderError(
                f"provider {self._descriptor.name!r} failed to subscribe to "
                f"{resolved.identity.exchange}/{resolved.identity.symbol} {timeframe}: {exc}"
            ) from exc
        return key

    async def recent_closed(
        self,
        key: tuple[MarketIdentity, str],
        *,
        count: int,
    ) -> tuple[BarUpdate, ...]:
        """Observe a small closed window for an active live subscription."""
        market = self._active.get(key)
        if market is None:
            raise ProviderError("recent live bootstrap requires an active subscription")
        if isinstance(count, bool) or not isinstance(count, int) or count <= 0:
            raise ProviderError("recent live bootstrap count must be a positive integer")
        identity, timeframe = key
        time_bar = TimeBar.parse(timeframe)
        cutover = time_bar.floor(self._clock())
        start = cutover
        for _ in range(count):
            start = _previous_boundary(time_bar, start)
        request = BarRequest(
            identity=identity,
            timeframe=timeframe,
            start=start,
            end=cutover,
        )
        observation = await asyncio.to_thread(
            self._historical.observe_recent_closed,
            request,
            market=market,
        )
        received_at = observation.completed_at
        updates: list[BarUpdate] = []
        for row in observation.frame.iter_rows(named=True):
            timestamp = cast("datetime", row["timestamp"])
            updates.append(
                BarUpdate(
                    identity=identity,
                    timeframe=timeframe,
                    timestamp=timestamp,
                    open=cast("float", row["open"]),
                    high=cast("float", row["high"]),
                    low=cast("float", row["low"]),
                    close=cast("float", row["close"]),
                    volume=cast("float", row["volume"]),
                    received_at=received_at,
                    finality=_bar_finality(timestamp, timeframe, received_at),
                )
            )
        return tuple(updates)

    async def updates(self) -> AsyncIterator[BarUpdate]:
        session = self._require_session()
        try:
            iterator = session.__aiter__()
            async for raw in iterator:
                yield self._normalize(raw)
            raise ProviderError(
                f"provider {self._descriptor.name!r} live-bar stream ended unexpectedly"
            )
        except (UnsupportedMarketError, ProviderError):
            raise
        except Exception as exc:
            raise ProviderError(
                f"provider {self._descriptor.name!r} live-bar stream failed: {exc}"
            ) from exc

    def _normalize(self, raw: object) -> BarUpdate:
        if not isinstance(raw, ProviderBarUpdate):
            raise ProviderError("provider live session must yield ProviderBarUpdate values")
        key = (raw.identity, raw.timeframe)
        if key not in self._active:
            raise ProviderError("provider yielded an update for an inactive subscription")
        previous = self._last_timestamp.get(key)
        if previous is not None and raw.timestamp < previous:
            raise ProviderError(
                "provider live-bar timestamp moved backwards for one dataset: "
                f"{raw.timestamp.isoformat()} < {previous.isoformat()}"
            )
        try:
            received_at = self._clock()
            update = BarUpdate(
                identity=raw.identity,
                timeframe=raw.timeframe,
                timestamp=raw.timestamp,
                open=raw.open,
                high=raw.high,
                low=raw.low,
                close=raw.close,
                volume=raw.volume,
                received_at=received_at,
                finality=_bar_finality(raw.timestamp, raw.timeframe, received_at),
            )
        except Exception as exc:
            raise ProviderError(f"provider yielded an invalid live-bar update: {exc}") from exc
        self._last_timestamp[key] = update.timestamp
        return update

    def _require_session(self) -> LiveBarSession:
        if self._session is None:
            raise ProviderError("live-bar provider session is not open")
        return self._session
