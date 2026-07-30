"""Exhaustive, bounded CCXT OHLCV pagination with explicit observation evidence.

Returned candles and observed time are different facts.  A successful bounded
page proves its half-open request window was observed even when it contains no
candles; rows alone never prove an arbitrary tail was observed.

Only endpoint families whose CCXT ``fetch_ohlcv`` implementation can honor an
explicit ``since``/``until`` window are enabled here.  Unknown families fail
closed instead of falling back to row-driven pagination.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Final

from xret.data.errors import ProviderError, UnsupportedMarketError
from xret.data.providers.contracts import ObservedWindow
from xret.data.timeframe import TimeBar

RawOHLCVRow = Sequence[float]
PageFetcher = Callable[[int, int, dict[str, int]], list[list[float]]]


@dataclass(frozen=True, slots=True)
class PaginationResult:
    """Raw rows plus the exact windows proved by successful provider calls."""

    rows: tuple[tuple[float, ...], ...]
    observed: tuple[ObservedWindow, ...]


@dataclass(frozen=True, slots=True)
class _PaginationProfile:
    max_bars: int


# Conservative maxima for CCXT endpoint families whose OHLCV adapters accept
# an explicit ``until`` bound.  These are correctness facts, not performance
# hints: callers must never build a wider page and assume it was exhaustive.
_PROFILES: Final[dict[str, _PaginationProfile]] = {
    "coinbase": _PaginationProfile(max_bars=300),
    "binance": _PaginationProfile(max_bars=1000),
    "binanceusdm": _PaginationProfile(max_bars=1000),
    "bybit": _PaginationProfile(max_bars=1000),
    "okx": _PaginationProfile(max_bars=100),
}


def _profile(client_id: str) -> _PaginationProfile:
    profile = _PROFILES.get(client_id)
    if profile is None:
        raise UnsupportedMarketError(
            f"{client_id} has no qualified exhaustive fetchOHLCV pagination contract"
        )
    return profile


def _epoch_ms(value: datetime) -> int:
    return int(value.timestamp() * 1000)


def _advance(time_bar: TimeBar, start: datetime, bars: int, end: datetime) -> datetime:
    cursor = start
    for _ in range(bars):
        cursor = time_bar.next_boundary(cursor)
        if cursor >= end:
            return end
    return cursor


def _validate_page(
    rows: Sequence[RawOHLCVRow],
    *,
    native_symbol: str,
    exchange_id: str,
    window_start_ms: int,
    window_end_ms: int,
) -> None:
    """Reject a page that does not honor the bounded window it answers.

    Shape comes first: a value that cannot be read as a timestamp cannot be
    ordered or range-checked. The timestamp is coerced once and reused for both
    later checks, so every conversion failure is attributed as a malformed
    candle instead of escaping as a raw ``ValueError`` or ``OverflowError``.
    Coercion matches what this module already applies to collected rows, so the
    validator and the collector agree on what a timestamp is.

    A row outside the window proves the venue ignored ``until``, and a response
    that spans more than the window cannot prove the window was observed
    exhaustively, so the traversal must not treat it as evidence.
    """
    previous: int | None = None
    for row in rows:
        try:
            if len(row) < 6:
                raise ValueError(f"expected at least 6 OHLCV fields, got {len(row)}")
            timestamp_ms = int(float(row[0]))
            for value in row[1:6]:
                float(value)
        except (TypeError, ValueError, OverflowError) as exc:
            raise ProviderError(
                f"fetchOHLCV returned a malformed candle for {native_symbol} on {exchange_id}"
            ) from exc
        if previous is not None and timestamp_ms < previous:
            raise ProviderError(
                f"fetchOHLCV returned non-ascending candles for {native_symbol} on {exchange_id}"
            )
        if not window_start_ms <= timestamp_ms < window_end_ms:
            raise ProviderError(
                f"fetchOHLCV returned a candle outside the requested window for "
                f"{native_symbol} on {exchange_id}: "
                f"{_describe_ms(timestamp_ms)} not in "
                f"[{_describe_ms(window_start_ms)}, {_describe_ms(window_end_ms)})"
            )
        previous = timestamp_ms


def _describe_ms(value: int) -> str:
    """Epoch milliseconds, with an ISO rendering when one exists.

    A venue emitting microsecond or nanosecond epochs is a real vendor bug, and
    formatting must not replace the attribution with a `datetime` range error.
    """
    try:
        return f"{datetime.fromtimestamp(value / 1000, tz=UTC).isoformat()} ({value}ms)"
    except (OverflowError, OSError, ValueError):
        return f"{value}ms"


def paginate_ohlcv(
    *,
    client_id: str,
    exchange_id: str,
    native_symbol: str,
    time_bar: TimeBar,
    start: datetime,
    end: datetime,
    requested_limit: int,
    fetch_page: PageFetcher,
) -> PaginationResult:
    """Traverse every qualified bounded page in ``[start, end)``.

    The cursor advances by the provider window, never by the last returned
    candle.  Therefore an empty successful page is evidence for that page and
    cannot hide later data.
    """
    if requested_limit <= 0:
        raise ProviderError(f"page limit must be positive, got {requested_limit!r}")
    profile = _profile(client_id)
    effective_limit = min(requested_limit, profile.max_bars)
    cursor = start
    collected: list[tuple[float, ...]] = []
    observed: list[ObservedWindow] = []

    while cursor < end:
        page_end = _advance(time_bar, cursor, effective_limit, end)
        if page_end <= cursor:
            raise ProviderError(
                f"pagination made no progress for {native_symbol} on {exchange_id}: "
                f"cursor={cursor.isoformat()}"
            )
        start_ms = _epoch_ms(cursor)
        end_ms = _epoch_ms(page_end)
        # CCXT's unified `until` denotes the latest candle to fetch and the
        # qualified endpoints accept it inclusively.  Translate Xret's
        # half-open page to that contract so a full page contains at most
        # `effective_limit` candle boundaries.  Passing `end_ms` could offer
        # limit + 1 boundaries and let newest-first endpoints discard start.
        inclusive_until_ms = end_ms - 1
        batch = fetch_page(start_ms, effective_limit, {"until": inclusive_until_ms})
        _validate_page(
            batch,
            native_symbol=native_symbol,
            exchange_id=exchange_id,
            window_start_ms=start_ms,
            window_end_ms=end_ms,
        )
        collected.extend(tuple(float(value) for value in row[:6]) for row in batch)
        observed.append(ObservedWindow(cursor, page_end))
        cursor = page_end

    return PaginationResult(rows=tuple(collected), observed=tuple(observed))
