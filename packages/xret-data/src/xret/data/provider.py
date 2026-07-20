"""One CCXT OHLCV provider boundary (network-only; no local store I/O).

This module implements the single, capability-driven path xret.data uses to
pull historical OHLCV candles from CCXT. It never bypasses CCXT's unified
interface (no native REST calls, no raw exchange SDKs, no raw "final flag"
shortcuts): every fact this module trusts comes from `has`, `markets`,
`timeframes` and `fetchOHLCV`.

Responsibilities:
- resolve a provider-independent `MarketIdentity` (Decisions 4-9) to a CCXT
  client id and a CCXT-native symbol, inferring an omitted perpetual
  `settle` only when provider metadata yields exactly one safe candidate;
- lazily create (or accept a test-only injected) CCXT exchange instance;
- verify `fetchOHLCV`, market listing and timeframe capability before ever
  attempting a fetch;
- paginate monotonically over the requested half-open `[start, end)` range
  using `TimeBar` boundaries (calendar-correct for `w`/`M`), refusing to
  loop forever when the exchange stops making progress;
- retry only transient network/rate-limit failures, with a bounded number
  of attempts and exponential backoff;
- drop candles that are not yet final (`bar_end + grace <= now`);
- return one canonical, provider-independent Polars `DataFrame`. This
  module never touches the canonical store.

There is no public provider or clock injection (P-3, Decision 22): the
constructor-level seams tests need are module-private and undocumented.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from typing import Any, Final, Protocol

import polars as pl
from xret.data.errors import ProviderError, UnsupportedMarketError
from xret.data.models import Market, MarketIdentity
from xret.data.schema import OHLCV_SCHEMA
from xret.data.storage.parquet import DerivativeInterpretation, ProviderIdentity
from xret.data.timeframe import TimeBar

__all__ = [
    "CCXTExchange",
    "DEFAULT_FINALITY_GRACE",
    "DEFAULT_PAGE_LIMIT",
    "DEFAULT_MAX_RETRIES",
    "default_end",
    "fetch_bars",
    "resolve_identity",
]


# --------------------------------------------------------------------------
# CCXT surface this module relies on (typed for tests, not enforced at runtime)
# --------------------------------------------------------------------------


class CCXTExchange(Protocol):
    """The minimal unified CCXT exchange surface this provider uses.

    Real CCXT exchange instances satisfy this structurally. Tests inject a
    small fake implementing exactly this surface through the module-private
    seam below, so no test ever needs `ccxt` installed or the network.
    """

    id: str
    has: dict[str, Any]
    markets: dict[str, Any] | None
    timeframes: dict[str, Any] | None

    def load_markets(self, reload: bool = False) -> dict[str, Any]: ...

    def fetch_ohlcv(
        self,
        symbol: str,
        timeframe: str,
        since: int | None = None,
        limit: int | None = None,
    ) -> list[list[float]]: ...


# --------------------------------------------------------------------------
# Exchange slug -> CCXT client id (Decisions 5-6)
# --------------------------------------------------------------------------

#: Xret exchange slugs map 1:1 onto CCXT unified client ids for spot
#: markets. Some exchanges split derivatives into a dedicated CCXT client
#: rather than exposing both market families from one unified class; this
#: table carries only those known exceptions. Exchanges absent here use the
#: spot slug unchanged for perpetual markets too (the common case).
_PERPETUAL_CCXT_CLIENT_IDS: Final[dict[str, str]] = {
    "binance": "binanceusdm",
    "kraken": "krakenfutures",
    "kucoin": "kucoinfutures",
}


def _ccxt_client_id(identity: MarketIdentity) -> str:
    """Resolve the official CCXT client id backing one Xret market identity."""
    if identity.market is Market.PERPETUAL:
        return _PERPETUAL_CCXT_CLIENT_IDS.get(identity.exchange, identity.exchange)
    return identity.exchange


# --------------------------------------------------------------------------
# Transient error classification
# --------------------------------------------------------------------------

#: CCXT unified error class names that signal a transient failure worth
#: retrying (network hiccups, rate limits, temporary unavailability).
#: Matched by class name across the exception's MRO so this module never
#: needs to import `ccxt` to recognize `ccxt.NetworkError` and friends.
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


def _is_transient_error(exc: BaseException) -> bool:
    mro_names = {klass.__name__ for klass in type(exc).__mro__}
    return not mro_names.isdisjoint(_TRANSIENT_ERROR_NAMES)


# --------------------------------------------------------------------------
# Defaults
# --------------------------------------------------------------------------

#: A candle is final only once `bar_end + grace <= now_utc` (Decision 13).
#: Owned by this adapter, never by `TimeBar`, config, or individual calls.
DEFAULT_FINALITY_GRACE: Final[timedelta] = timedelta(seconds=5)
#: Candles requested per `fetchOHLCV` page.
DEFAULT_PAGE_LIMIT: Final[int] = 1000
#: Bounded retry attempts for transient failures, per page.
DEFAULT_MAX_RETRIES: Final[int] = 5
#: Base delay (seconds) for exponential retry backoff.
DEFAULT_RETRY_BACKOFF_BASE: Final[float] = 0.5


def _default_clock() -> datetime:
    return datetime.now(UTC)


def _default_backoff(attempt: int, *, base: float) -> float:
    return base * (2 ** (attempt - 1))


# --------------------------------------------------------------------------
# Module-private test seam (P-3)
#
# `MarketData`/`BarDataset` take no public provider or clock parameter.
# Lane 1 tests reach into these names directly; they are intentionally
# absent from `__all__` and are not part of the documented contract.
# --------------------------------------------------------------------------

_exchange_factory_overrides: dict[str, Callable[[], CCXTExchange]] = {}
_clock_override: Callable[[], datetime] | None = None


def _register_exchange_factory(client_id: str, factory: Callable[[], CCXTExchange]) -> None:
    _exchange_factory_overrides[client_id] = factory


def _reset_test_seams() -> None:
    _exchange_factory_overrides.clear()
    _set_clock_override(None)


def _set_clock_override(clock: Callable[[], datetime] | None) -> None:
    global _clock_override
    _clock_override = clock


def _current_clock() -> Callable[[], datetime]:
    return _clock_override if _clock_override is not None else _default_clock


# --------------------------------------------------------------------------
# Exchange lifecycle
# --------------------------------------------------------------------------


def _get_exchange(client_id: str) -> CCXTExchange:
    factory = _exchange_factory_overrides.get(client_id)
    if factory is not None:
        return factory()
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


def _load_markets(exchange: CCXTExchange) -> dict[str, Any]:
    return exchange.load_markets()


# --------------------------------------------------------------------------
# Identity resolution: Xret identity -> CCXT native symbol (Decisions 5-9)
# --------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ResolvedMarket:
    """One `MarketIdentity` resolved against live provider metadata."""

    native_symbol: str
    settle: str | None
    metadata: dict[str, Any]


def _market_native_symbol(symbol: str, market: dict[str, Any]) -> str:
    native_symbol = market.get("symbol")
    return native_symbol if isinstance(native_symbol, str) and native_symbol else symbol


def _spot_candidate(identity: MarketIdentity, markets: dict[str, Any]) -> ResolvedMarket:
    market = markets.get(identity.symbol)
    if market is None or not market.get("spot"):
        raise UnsupportedMarketError(f"{identity.symbol!r} is not a listed spot market")
    return ResolvedMarket(
        native_symbol=_market_native_symbol(identity.symbol, market), settle=None, metadata=market
    )


def _perpetual_candidates(identity: MarketIdentity, markets: dict[str, Any]) -> dict[str, Any]:
    base, quote = identity.symbol.split("/")
    return {
        symbol: market
        for symbol, market in markets.items()
        if market.get("base") == base and market.get("quote") == quote and market.get("swap")
    }


def _perpetual_candidate(identity: MarketIdentity, markets: dict[str, Any]) -> ResolvedMarket:
    candidates = _perpetual_candidates(identity, markets)
    if identity.settle is not None:
        matches = {
            symbol: market
            for symbol, market in candidates.items()
            if market.get("settle") == identity.settle
        }
        if not matches:
            raise UnsupportedMarketError(
                f"no perpetual market for {identity.symbol!r} settling in "
                f"{identity.settle!r} is listed"
            )
        if len(matches) != 1:
            raise UnsupportedMarketError(
                f"ambiguous perpetual market for {identity.symbol!r} settling in "
                f"{identity.settle!r}"
            )
        native_symbol, market = next(iter(matches.items()))
        return ResolvedMarket(
            native_symbol=_market_native_symbol(native_symbol, market),
            settle=identity.settle,
            metadata=market,
        )

    settle_candidates = sorted(
        {market["settle"] for market in candidates.values() if market.get("settle")}
    )
    if not settle_candidates:
        raise UnsupportedMarketError(
            f"no perpetual settlement candidates found for {identity.symbol!r}; "
            "pass settle= explicitly"
        )
    if len(settle_candidates) > 1:
        raise UnsupportedMarketError(
            f"ambiguous perpetual settlement for {identity.symbol!r}: "
            f"candidates={settle_candidates!r}; pass settle= explicitly"
        )
    settle = settle_candidates[0]
    matches = {
        symbol: market for symbol, market in candidates.items() if market.get("settle") == settle
    }
    if len(matches) != 1:
        raise UnsupportedMarketError(
            f"ambiguous perpetual market for {identity.symbol!r} settling in {settle!r}"
        )
    native_symbol, market = next(iter(matches.items()))
    return ResolvedMarket(
        native_symbol=_market_native_symbol(native_symbol, market),
        settle=settle,
        metadata=market,
    )


def _resolve_market(identity: MarketIdentity, exchange: CCXTExchange) -> ResolvedMarket:
    if not exchange.has.get("fetchOHLCV"):
        raise UnsupportedMarketError(f"{exchange.id} does not support fetchOHLCV")
    markets = _load_markets(exchange)
    if identity.market is Market.SPOT:
        return _spot_candidate(identity, markets)
    return _perpetual_candidate(identity, markets)


def _ensure_timeframe_supported(exchange: CCXTExchange, timeframe: str) -> None:
    timeframes = getattr(exchange, "timeframes", None)
    if not isinstance(timeframes, Mapping) or timeframe not in timeframes:
        raise UnsupportedMarketError(f"{exchange.id} does not support timeframe {timeframe!r}")


# --------------------------------------------------------------------------
# Pagination
# --------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _RetryConfig:
    max_retries: int
    backoff: Callable[[int], float]
    sleep: Callable[[float], None]


def _fetch_page(
    exchange: CCXTExchange,
    native_symbol: str,
    timeframe: str,
    since_ms: int,
    page_limit: int,
    retry: _RetryConfig,
) -> list[list[float]]:
    attempt = 0
    while True:
        try:
            return exchange.fetch_ohlcv(native_symbol, timeframe, since_ms, page_limit)
        except Exception as exc:  # noqa: BLE001 - reclassified below
            if not _is_transient_error(exc) or attempt >= retry.max_retries:
                raise ProviderError(
                    f"fetchOHLCV failed for {native_symbol} on {exchange.id}: {exc}"
                ) from exc
            attempt += 1
            retry.sleep(retry.backoff(attempt))


def _assert_ascending(batch: list[list[float]], *, native_symbol: str, exchange_id: str) -> None:
    previous: float | None = None
    for row in batch:
        try:
            if len(row) < 6:
                raise ValueError(f"expected at least 6 OHLCV fields, got {len(row)}")
            ts = float(row[0])
            for value in row[1:6]:
                float(value)
        except (TypeError, ValueError) as exc:
            raise ProviderError(
                f"fetchOHLCV returned a malformed candle for {native_symbol} on {exchange_id}"
            ) from exc
        if previous is not None and ts < previous:
            raise ProviderError(
                f"fetchOHLCV returned non-ascending candles for {native_symbol} on {exchange_id}"
            )
        previous = ts


def _paginate(
    exchange: CCXTExchange,
    native_symbol: str,
    timeframe: str,
    time_bar: TimeBar,
    start: datetime,
    end: datetime,
    page_limit: int,
    retry: _RetryConfig,
) -> list[list[float]]:
    since_ms = _to_epoch_ms(start)
    end_ms = _to_epoch_ms(end)
    rows: list[list[float]] = []
    while since_ms < end_ms:
        batch = _fetch_page(exchange, native_symbol, timeframe, since_ms, page_limit, retry)
        if not batch:
            break
        _assert_ascending(batch, native_symbol=native_symbol, exchange_id=exchange.id)
        rows.extend(batch)
        newest_ts_ms = int(batch[-1][0])
        newest_open = datetime.fromtimestamp(newest_ts_ms / 1000, tz=UTC)
        next_since_ms = _to_epoch_ms(time_bar.next_boundary(newest_open))
        if next_since_ms <= since_ms:
            raise ProviderError(
                f"pagination made no progress for {native_symbol} on {exchange.id}: "
                f"since={since_ms} newest={newest_ts_ms}"
            )
        since_ms = next_since_ms
    return rows


# --------------------------------------------------------------------------
# Finality / filtering (Decision 13)
# --------------------------------------------------------------------------


def _finalized_rows(
    rows: list[list[float]],
    time_bar: TimeBar,
    start: datetime,
    end: datetime,
    now: datetime,
    grace: timedelta,
) -> list[list[float]]:
    start_ms = _to_epoch_ms(start)
    end_ms = _to_epoch_ms(end)
    finalized: list[list[float]] = []
    for row in rows:
        ts_ms = int(row[0])
        if not (start_ms <= ts_ms < end_ms):
            continue
        bar_open = datetime.fromtimestamp(ts_ms / 1000, tz=UTC)
        bar_end = time_bar.next_boundary(bar_open)
        if bar_end + grace <= now:
            finalized.append(row)
    return finalized


def default_end(
    time_bar: TimeBar,
    *,
    grace: timedelta = DEFAULT_FINALITY_GRACE,
) -> datetime:
    """The end of the latest completed bar right now, honoring `grace` (IR-3).

    Only `fetch`/`sync` use this: reads default the omitted `end` to the
    plain local boundary with no grace (owned by `timeframe.py`, not here).
    """
    now = _current_clock()()
    return time_bar.floor(now - grace)


# --------------------------------------------------------------------------
# Canonical DataFrame construction
# --------------------------------------------------------------------------


def _build_dataframe(
    rows: list[list[float]],
    identity: MarketIdentity,
    resolved: ResolvedMarket,
    timeframe: str,
) -> pl.DataFrame:
    rows = sorted(rows, key=lambda row: row[0])
    n = len(rows)
    timestamps = (
        pl.Series("timestamp", [int(row[0]) for row in rows], dtype=pl.Int64)
        .cast(pl.Datetime(time_unit="ms"))
        .dt.replace_time_zone("UTC")
    )
    return pl.DataFrame(
        {
            "exchange": pl.Series("exchange", [identity.exchange] * n, dtype=pl.String),
            "symbol": pl.Series("symbol", [identity.symbol] * n, dtype=pl.String),
            "market": pl.Series("market", [identity.market.value] * n, dtype=pl.String),
            "settle": pl.Series("settle", [resolved.settle] * n, dtype=pl.String),
            "timeframe": pl.Series("timeframe", [timeframe] * n, dtype=pl.String),
            "timestamp": timestamps,
            "open": pl.Series("open", [float(row[1]) for row in rows], dtype=pl.Float64),
            "high": pl.Series("high", [float(row[2]) for row in rows], dtype=pl.Float64),
            "low": pl.Series("low", [float(row[3]) for row in rows], dtype=pl.Float64),
            "close": pl.Series("close", [float(row[4]) for row in rows], dtype=pl.Float64),
            "volume": pl.Series("volume", [float(row[5]) for row in rows], dtype=pl.Float64),
        },
        schema=OHLCV_SCHEMA,
    )


# --------------------------------------------------------------------------
# Public entry point
# --------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _Observation:
    """Immutable evidence for one completed provider observation."""

    frame: pl.DataFrame
    provider: ProviderIdentity
    derivative: DerivativeInterpretation | None
    completed_at: datetime


def _ccxt_version() -> str:
    try:
        import ccxt
    except ImportError as exc:
        raise ProviderError("ccxt is not installed; cannot determine its version") from exc
    version = getattr(ccxt, "__version__", None)
    if not isinstance(version, str) or not version:
        raise ProviderError("ccxt does not expose a version")
    return version


def _provider_identity(exchange: CCXTExchange, resolved: ResolvedMarket) -> ProviderIdentity:
    market_id = resolved.metadata.get("id")
    native_symbol = resolved.metadata.get("symbol")
    if not isinstance(market_id, str) or not market_id:
        raise ProviderError(f"{exchange.id} market metadata has no market id")
    if not isinstance(native_symbol, str) or not native_symbol:
        raise ProviderError(f"{exchange.id} market metadata has no native symbol")
    return ProviderIdentity(
        name=exchange.id,
        ccxt_version=_ccxt_version(),
        market_id=market_id,
        native_symbol=native_symbol,
    )


def _derivative_interpretation(resolved: ResolvedMarket) -> DerivativeInterpretation:
    contract_size = resolved.metadata.get("contractSize")
    return DerivativeInterpretation(
        linear=resolved.metadata.get("linear")
        if isinstance(resolved.metadata.get("linear"), bool)
        else None,
        inverse=resolved.metadata.get("inverse")
        if isinstance(resolved.metadata.get("inverse"), bool)
        else None,
        contract_size=str(contract_size) if contract_size is not None else None,
    )


def _fetch_bars(
    identity: MarketIdentity,
    timeframe: str,
    start: datetime,
    end: datetime,
    *,
    finality_grace: timedelta,
    page_limit: int,
    max_retries: int,
    retry_backoff_base: float,
    sleep: Callable[[float], None] | None,
) -> tuple[pl.DataFrame, CCXTExchange, ResolvedMarket, datetime]:
    time_bar = TimeBar.parse(timeframe)
    client_id = _ccxt_client_id(identity)
    clock = _current_clock()
    try:
        exchange = _get_exchange(client_id)
        resolved = _resolve_market(identity, exchange)
        _ensure_timeframe_supported(exchange, timeframe)
        retry = _RetryConfig(
            max_retries=max_retries,
            backoff=lambda attempt: _default_backoff(attempt, base=retry_backoff_base),
            sleep=sleep if sleep is not None else time.sleep,
        )
        raw_rows = _paginate(
            exchange, resolved.native_symbol, timeframe, time_bar, start, end, page_limit, retry
        )
    except (UnsupportedMarketError, ProviderError):
        raise
    except Exception as exc:  # noqa: BLE001 - reclassified as ProviderError (P-1)
        raise ProviderError(
            f"fetch failed for {identity.exchange}/{identity.symbol}/{identity.market.value}: {exc}"
        ) from exc
    observed_at = clock()
    try:
        finalized_rows = _finalized_rows(
            raw_rows, time_bar, start, end, observed_at, finality_grace
        )
        frame = _build_dataframe(finalized_rows, identity, resolved, timeframe)
    except ProviderError:
        raise
    except Exception as exc:  # noqa: BLE001 - malformed provider data is a provider failure
        raise ProviderError(
            f"invalid OHLCV data for {resolved.native_symbol} on {exchange.id}: {exc}"
        ) from exc
    return frame, exchange, resolved, observed_at


def _observe_bars(
    identity: MarketIdentity,
    timeframe: str,
    start: datetime,
    end: datetime,
) -> _Observation:
    """Observe exactly one requested range without retaining page/cursor detail."""
    frame, exchange, resolved, observed_at = _fetch_bars(
        identity,
        timeframe,
        start,
        end,
        finality_grace=DEFAULT_FINALITY_GRACE,
        page_limit=DEFAULT_PAGE_LIMIT,
        max_retries=DEFAULT_MAX_RETRIES,
        retry_backoff_base=DEFAULT_RETRY_BACKOFF_BASE,
        sleep=None,
    )
    return _Observation(
        frame=frame,
        provider=_provider_identity(exchange, resolved),
        derivative=(
            _derivative_interpretation(resolved) if identity.market is Market.PERPETUAL else None
        ),
        completed_at=observed_at,
    )


def fetch_bars(
    identity: MarketIdentity,
    timeframe: str,
    start: datetime,
    end: datetime,
    *,
    finality_grace: timedelta = DEFAULT_FINALITY_GRACE,
    page_limit: int = DEFAULT_PAGE_LIMIT,
    max_retries: int = DEFAULT_MAX_RETRIES,
    retry_backoff_base: float = DEFAULT_RETRY_BACKOFF_BASE,
    sleep: Callable[[float], None] | None = None,
) -> pl.DataFrame:
    """Fetch, finalize and normalize one closed-bar OHLCV batch (Decision 12).

    Network-only: never reads or writes canonical files or catalog state.
    `start`/`end` must already be a validated, UTC-aware half-open range.
    An unlisted symbol, an unsupported timeframe, or ambiguous/absent
    settlement inference raises `UnsupportedMarketError`; a transport,
    pagination or fetch-path quality failure raises `ProviderError`,
    chained to its cause (P-1).
    """
    return _fetch_bars(
        identity,
        timeframe,
        start,
        end,
        finality_grace=finality_grace,
        page_limit=page_limit,
        max_retries=max_retries,
        retry_backoff_base=retry_backoff_base,
        sleep=sleep,
    )[0]


def resolve_identity(identity: MarketIdentity) -> MarketIdentity:
    """Safely resolve an omitted perpetual `settle` against live provider
    metadata (Decision 9).

    Used by `BarDataset.sync` to resolve `settle` before deriving a
    `DatasetKey`, so a perpetual identity's storage identity is never
    built from an unresolved sentinel. Returns `identity` unchanged when
    `settle` is already given, or when the market is not perpetual.

    Raises:
        UnsupportedMarketError: no listed market matches, or safe
            single-candidate settlement inference is not possible.
        ProviderError: the exchange call failed for a transport reason.
    """
    if identity.settle is not None:
        return identity
    client_id = _ccxt_client_id(identity)
    try:
        exchange = _get_exchange(client_id)
        resolved = _resolve_market(identity, exchange)
    except (UnsupportedMarketError, ProviderError):
        raise
    except Exception as exc:  # noqa: BLE001 - reclassified as ProviderError (P-1)
        raise ProviderError(
            f"identity resolution failed for {identity.exchange}/{identity.symbol}: {exc}"
        ) from exc
    if resolved.settle is None:
        return identity
    return replace(identity, settle=resolved.settle)


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------


def _to_epoch_ms(value: datetime) -> int:
    return int(value.timestamp() * 1000)
