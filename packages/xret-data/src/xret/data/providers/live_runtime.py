"""Provider-independent validation for optional live-bar capabilities."""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator, Callable
from datetime import UTC, datetime
from typing import cast

from xret.data.errors import ProviderError, UnsupportedMarketError
from xret.data.models import BarUpdate, MarketIdentity
from xret.data.providers.contracts import (
    HistoricalBarProvider,
    LiveBarSession,
    ProviderBarUpdate,
)
from xret.data.providers.runtime import ProviderRuntime, validate_provider_descriptor
from xret.data.timeframe import TimeBar


def _default_clock() -> datetime:
    return datetime.now(UTC)


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
        self._historical = ProviderRuntime(cast("HistoricalBarProvider", provider))
        self._session: LiveBarSession | None = None
        self._active: set[tuple[MarketIdentity, str]] = set()
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
        session = self._require_session()
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
        try:
            await session.subscribe_bar_updates(resolved, timeframe)
        except (UnsupportedMarketError, ProviderError):
            raise
        except Exception as exc:
            raise ProviderError(
                f"provider {self._descriptor.name!r} failed to subscribe to "
                f"{identity.exchange}/{identity.symbol} {timeframe}: {exc}"
            ) from exc
        self._active.add(key)
        return key

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
            update = BarUpdate(
                identity=raw.identity,
                timeframe=raw.timeframe,
                timestamp=raw.timestamp,
                open=raw.open,
                high=raw.high,
                low=raw.low,
                close=raw.close,
                volume=raw.volume,
                received_at=self._clock(),
            )
        except Exception as exc:
            raise ProviderError(f"provider yielded an invalid live-bar update: {exc}") from exc
        self._last_timestamp[key] = update.timestamp
        return update

    def _require_session(self) -> LiveBarSession:
        if self._session is None:
            raise ProviderError("live-bar provider session is not open")
        return self._session
