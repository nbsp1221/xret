"""CCXT client construction, transport retry, and version inspection."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Final, Protocol

from xret.data.errors import ProviderError


class CCXTExchange(Protocol):
    """Minimal unified CCXT exchange surface required by Xret."""

    id: str
    has: dict[str, Any]
    markets: dict[str, Any] | None
    precisionMode: int
    timeframes: dict[str, Any] | None

    def load_markets(self, reload: bool = False) -> dict[str, Any]: ...

    def fetch_ohlcv(
        self,
        symbol: str,
        timeframe: str,
        since: int | None = None,
        limit: int | None = None,
        params: dict[str, int | str] | None = None,
    ) -> list[list[float]]: ...

    def fetch_mark_ohlcv(
        self,
        symbol: str,
        timeframe: str = "1m",
        since: int | None = None,
        limit: int | None = None,
        params: dict[str, int | str] | None = None,
    ) -> list[list[float]]: ...

    def fetch_index_ohlcv(
        self,
        symbol: str,
        timeframe: str = "1m",
        since: int | None = None,
        limit: int | None = None,
        params: dict[str, int | str] | None = None,
    ) -> list[list[float]]: ...

    def fetch_premium_index_ohlcv(
        self,
        symbol: str,
        timeframe: str = "1m",
        since: int | None = None,
        limit: int | None = None,
        params: dict[str, int | str] | None = None,
    ) -> list[list[float]]: ...

    def fetch_funding_rate_history(
        self,
        symbol: str,
        since: int | None = None,
        limit: int | None = None,
        params: dict[str, int | str] | None = None,
    ) -> list[dict[str, Any]]: ...

    def fetch_open_interest_history(
        self,
        symbol: str,
        timeframe: str = "5m",
        since: int | None = None,
        limit: int | None = None,
        params: dict[str, int | str] | None = None,
    ) -> list[dict[str, Any]]: ...


ExchangeFactory = Callable[[str], CCXTExchange]

_TRANSIENT_ERROR_NAMES: Final[frozenset[str]] = frozenset(
    {
        "NetworkError",
        "RequestTimeout",
        "ExchangeNotAvailable",
        "OnMaintenance",
        "DDoSProtection",
        "RateLimitExceeded",
    }
)


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    max_retries: int
    backoff: Callable[[int], float]
    sleep: Callable[[float], None]


@dataclass(slots=True)
class RequestPacer:
    """Serialize one client's OHLCV calls at a proven minimum interval."""

    minimum_interval_seconds: float
    monotonic: Callable[[], float] = time.monotonic
    sleep: Callable[[float], None] = time.sleep
    _next_request_at: float = field(default=0.0, init=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False)

    def wait(self) -> None:
        with self._lock:
            now = self.monotonic()
            scheduled = max(now, self._next_request_at)
            delay = scheduled - now
            if delay > 0:
                self.sleep(delay)
            self._next_request_at = scheduled + self.minimum_interval_seconds


def create_exchange(client_id: str) -> CCXTExchange:
    """Construct one rate-limited unified CCXT exchange client."""
    try:
        import ccxt
    except ImportError as exc:
        raise ProviderError(
            f"ccxt is not installed; cannot construct exchange client {client_id!r}"
        ) from exc
    exchange_class = getattr(ccxt, client_id, None)
    if exchange_class is None:
        raise ProviderError(f"unknown ccxt exchange id: {client_id!r}")
    return exchange_class({"enableRateLimit": True})


def installed_version() -> str:
    """Return the installed CCXT version for audit provenance."""
    try:
        import ccxt
    except ImportError as exc:
        raise ProviderError("ccxt is not installed; cannot determine its version") from exc
    version = getattr(ccxt, "__version__", None)
    if not isinstance(version, str) or not version:
        raise ProviderError("ccxt does not expose a version")
    return version


def tick_size_precision_mode() -> int:
    """Return CCXT's installed `TICK_SIZE` precision-mode discriminator."""
    try:
        import ccxt
    except ImportError as exc:
        raise ProviderError("ccxt is not installed; cannot interpret market precision") from exc
    mode = getattr(ccxt, "TICK_SIZE", None)
    if not isinstance(mode, int):
        raise ProviderError("ccxt does not expose an integer TICK_SIZE precision mode")
    return mode


def exponential_backoff(attempt: int, *, base: float) -> float:
    return base * (2 ** (attempt - 1))


def _is_transient_error(exc: BaseException) -> bool:
    mro_names = {klass.__name__ for klass in type(exc).__mro__}
    return not mro_names.isdisjoint(_TRANSIENT_ERROR_NAMES)


def fetch_page(
    exchange: CCXTExchange,
    native_symbol: str,
    timeframe: str,
    since_ms: int | None,
    page_limit: int | None,
    params: dict[str, int | str],
    retry: RetryPolicy,
    pacer: RequestPacer,
) -> list[list[float]]:
    """Fetch one native page with bounded transient-error retries."""
    attempt = 0
    while True:
        try:
            pacer.wait()
            return exchange.fetch_ohlcv(
                native_symbol,
                timeframe,
                since_ms,
                page_limit,
                params,
            )
        except Exception as exc:
            if not _is_transient_error(exc) or attempt >= retry.max_retries:
                raise ProviderError(
                    f"fetchOHLCV failed for {native_symbol} on {exchange.id}: {exc}"
                ) from exc
            attempt += 1
            retry.sleep(retry.backoff(attempt))


def fetch_funding_page(
    exchange: CCXTExchange,
    native_symbol: str,
    since_ms: int,
    page_limit: int,
    params: dict[str, int | str],
    retry: RetryPolicy,
    pacer: RequestPacer,
) -> list[dict[str, Any]]:
    """Fetch one unified funding-history page with bounded retries."""
    attempt = 0
    while True:
        try:
            pacer.wait()
            return exchange.fetch_funding_rate_history(
                native_symbol,
                since_ms,
                page_limit,
                params,
            )
        except Exception as exc:
            if not _is_transient_error(exc) or attempt >= retry.max_retries:
                raise ProviderError(
                    f"fetchFundingRateHistory failed for {native_symbol} on {exchange.id}: {exc}"
                ) from exc
            attempt += 1
            retry.sleep(retry.backoff(attempt))


def fetch_reference_page(
    exchange: CCXTExchange,
    method_name: str,
    native_symbol: str,
    timeframe: str,
    since_ms: int | None,
    page_limit: int | None,
    params: dict[str, int | str],
    retry: RetryPolicy,
    pacer: RequestPacer,
) -> list[list[float]]:
    """Fetch one reference OHLC page through a pinned CCXT convenience API."""
    method = getattr(exchange, method_name, None)
    if not callable(method):
        raise ProviderError(f"CCXT {exchange.id!r} has no callable {method_name}()")
    attempt = 0
    while True:
        try:
            pacer.wait()
            return method(native_symbol, timeframe, since_ms, page_limit, params)
        except Exception as exc:
            if not _is_transient_error(exc) or attempt >= retry.max_retries:
                raise ProviderError(
                    f"{method_name} failed for {native_symbol} on {exchange.id}: {exc}"
                ) from exc
            attempt += 1
            retry.sleep(retry.backoff(attempt))


def fetch_open_interest_page(
    exchange: CCXTExchange,
    native_symbol: str,
    timeframe: str,
    since_ms: int,
    page_limit: int,
    params: dict[str, int | str],
    retry: RetryPolicy,
    pacer: RequestPacer,
) -> list[dict[str, Any]]:
    """Fetch one unified open-interest-history page with bounded retries."""
    attempt = 0
    while True:
        try:
            pacer.wait()
            return exchange.fetch_open_interest_history(
                native_symbol, timeframe, since_ms, page_limit, params
            )
        except Exception as exc:
            mro_names = {klass.__name__ for klass in type(exc).__mro__}
            message = str(exc)
            if (
                exchange.id in {"binance", "binanceusdm"}
                and "BadRequest" in mro_names
                and "startTime" in message
                and "invalid" in message
            ):
                return []
            if not _is_transient_error(exc) or attempt >= retry.max_retries:
                raise ProviderError(
                    f"fetchOpenInterestHistory failed for {native_symbol} on {exchange.id}: {exc}"
                ) from exc
            attempt += 1
            retry.sleep(retry.backoff(attempt))
