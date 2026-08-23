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
from datetime import UTC, datetime, timedelta

from xret.data.errors import ProviderError
from xret.data.providers.ccxt.compatibility import ObservationProfile, WindowParameterFormat
from xret.data.providers.contracts import ObservedWindow
from xret.data.timeframe import TimeBar

RawOHLCVRow = Sequence[float]
WindowParameter = int | str
PageFetcher = Callable[[int | None, int | None, dict[str, WindowParameter]], list[list[float]]]


@dataclass(frozen=True, slots=True)
class PaginationResult:
    """Raw rows plus the exact windows proved by successful provider calls."""

    rows: tuple[tuple[float, ...], ...]
    observed: tuple[ObservedWindow, ...]


def _epoch_ms(value: datetime) -> int:
    return int(value.timestamp() * 1000)


def _rfc3339_milliseconds(value: int) -> str:
    return (
        datetime.fromtimestamp(value / 1000, tz=UTC)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def _request_window(
    profile: ObservationProfile,
    *,
    start_ms: int,
    end_ms: int,
) -> tuple[int | None, dict[str, WindowParameter]]:
    native = profile.native_window_parameters
    if native is not None:
        if native.format is WindowParameterFormat.RFC3339_MILLISECONDS:
            return None, {
                native.start: _rfc3339_milliseconds(start_ms),
                native.end: _rfc3339_milliseconds(end_ms - 1),
            }
        raise ProviderError(f"unsupported native CCXT window format: {native.format!r}")

    params: dict[str, WindowParameter] = {}
    if profile.send_unified_until:
        params["until"] = end_ms - 1 if profile.until_inclusive else end_ms
    return start_ms, params


def _advance(
    time_bar: TimeBar,
    start: datetime,
    bars: int,
    end: datetime,
    max_span: timedelta | None,
) -> tuple[datetime, int]:
    cursor = start
    traversed = 0
    for _ in range(bars):
        next_cursor = time_bar.next_boundary(cursor)
        if max_span is not None and next_cursor - start > max_span:
            break
        cursor = next_cursor
        traversed += 1
        if cursor >= end:
            return end, traversed
    return cursor, traversed


def _validate_page(
    rows: Sequence[RawOHLCVRow],
    *,
    native_symbol: str,
    exchange_id: str,
    window_start_ms: int,
    window_end_ms: int,
    accept_end_boundary: bool,
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
        within_window = window_start_ms <= timestamp_ms < window_end_ms
        is_qualified_end_boundary = accept_end_boundary and timestamp_ms == window_end_ms
        if not within_window and not is_qualified_end_boundary:
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
    profile: ObservationProfile,
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
    effective_limit = min(requested_limit, profile.max_bars)
    cursor = start
    collected: list[tuple[float, ...]] = []
    observed: list[ObservedWindow] = []

    while cursor < end:
        page_end, page_bars = _advance(
            time_bar,
            cursor,
            effective_limit,
            end,
            profile.max_span,
        )
        if page_end <= cursor:
            raise ProviderError(
                f"pagination made no progress for {native_symbol} on {exchange_id}: "
                f"cursor={cursor.isoformat()}"
            )
        start_ms = _epoch_ms(cursor)
        end_ms = _epoch_ms(page_end)
        # CCXT's unified `until` denotes the latest candle to fetch, but native
        # adapters disagree on whether it is inclusive. Translate Xret's
        # half-open page using the qualified endpoint contract so a full page
        # contains at most `effective_limit` candle boundaries.
        request_since, params = _request_window(profile, start_ms=start_ms, end_ms=end_ms)
        request_limit = (
            min(profile.max_bars, page_bars + int(profile.accept_end_boundary))
            if profile.send_page_limit
            else None
        )
        batch = fetch_page(request_since, request_limit, params)
        _validate_page(
            batch,
            native_symbol=native_symbol,
            exchange_id=exchange_id,
            window_start_ms=start_ms,
            window_end_ms=end_ms,
            accept_end_boundary=profile.accept_end_boundary,
        )
        collected.extend(
            tuple(float(value) for value in row[:6]) for row in batch if int(float(row[0])) < end_ms
        )
        observed.append(ObservedWindow(cursor, page_end))
        cursor = page_end

    return PaginationResult(rows=tuple(collected), observed=tuple(observed))
